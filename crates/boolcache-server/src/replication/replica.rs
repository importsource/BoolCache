//! Replica-side replication: connects to a primary, performs full resync,
//! then continuously applies streamed write commands.

use std::io;
use std::time::Duration;

use bytes::BytesMut;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;

use boolcache::db::Db;
use boolcache::persistence::rdb;

use crate::resp::{RespParser, RespValue};

/// Spawn a background task that keeps this node synced with `host:port`.
/// Reconnects automatically on failure with exponential back-off.
pub fn start(host: String, port: u16, db: Db) {
    tokio::spawn(async move {
        let mut delay = Duration::from_secs(1);
        loop {
            tracing::info!("Connecting to primary {host}:{port}...");
            match sync_loop(&host, port, &db).await {
                Ok(()) => {
                    tracing::info!("Replication stream ended, reconnecting...");
                    delay = Duration::from_secs(1);
                }
                Err(e) => {
                    tracing::error!("Replication error: {e}. Retrying in {}s...", delay.as_secs());
                    tokio::time::sleep(delay).await;
                    delay = (delay * 2).min(Duration::from_secs(60));
                }
            }
        }
    });
}

async fn sync_loop(host: &str, port: u16, db: &Db) -> io::Result<()> {
    let stream = TcpStream::connect((host, port)).await?;
    stream.set_nodelay(true)?;
    let (reader, mut writer) = stream.into_split();
    let mut reader = BufReader::new(reader);

    // ── handshake ────────────────────────────────────────────────────────────

    // PING
    writer.write_all(b"*1\r\n$4\r\nPING\r\n").await?;
    let mut line = String::new();
    reader.read_line(&mut line).await?; // +PONG

    // REPLCONF listening-port 0  (we don't accept sub-replicas, port 0 = N/A)
    writer.write_all(b"*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$1\r\n0\r\n").await?;
    line.clear();
    reader.read_line(&mut line).await?; // +OK

    // PSYNC ? -1  (always request a full resync)
    writer.write_all(b"*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n").await?;
    line.clear();
    reader.read_line(&mut line).await?; // +FULLRESYNC <id> <offset>

    tracing::info!("Full resync starting: {}", line.trim());

    // ── receive RDB snapshot ─────────────────────────────────────────────────

    // $<len>\r\n   (bulk string header without trailing \r\n after data)
    line.clear();
    reader.read_line(&mut line).await?;
    let rdb_len: usize = line
        .trim()
        .trim_start_matches('$')
        .parse()
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "bad RDB length"))?;

    let mut rdb_bytes = vec![0u8; rdb_len];
    reader.read_exact(&mut rdb_bytes).await?;

    let new_store = rdb::load_from_bytes(&rdb_bytes)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
    db.replace_store(new_store);
    tracing::info!("Replica: RDB loaded ({rdb_len} bytes)");

    // ── stream commands ───────────────────────────────────────────────────────

    let mut parser = RespParser::new();
    let mut buf = BytesMut::with_capacity(4096);

    loop {
        let n = reader.read_buf(&mut buf).await?;
        if n == 0 {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "primary closed connection"));
        }

        // Feed only the newly arrived bytes to the parser
        parser.feed(&buf[buf.len() - n..]);

        loop {
            match parser.take_value() {
                Ok(None) => break,
                Ok(Some(value)) => {
                    if let Some(args) = to_args(value) {
                        db.apply_replicated(args);
                    }
                }
                Err(e) => {
                    return Err(io::Error::new(io::ErrorKind::InvalidData, e.to_string()));
                }
            }
        }

        buf.clear();
    }
}

/// Extract byte-vector args from a RESP array value.
fn to_args(value: RespValue) -> Option<Vec<Vec<u8>>> {
    match value {
        RespValue::Array(Some(items)) => items
            .into_iter()
            .map(|item| match item {
                RespValue::BulkString(Some(b)) => Some(b),
                RespValue::SimpleString(s) => Some(s.into_bytes()),
                _ => None,
            })
            .collect(),
        _ => None,
    }
}
