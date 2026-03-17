//! Per-connection command handler.

use std::time::Duration;

use bytes::BytesMut;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use boolcache::commands::{hash, list, set as set_cmd, string, zset};
use boolcache::db::Db;
use boolcache::replication::state::Role;

use crate::resp::{RespParser, RespValue};

pub struct Connection {
    stream: TcpStream,
    parser: RespParser,
    read_buf: BytesMut,
    db: Db,
    /// Set by PSYNC to switch this connection into replica-streaming mode.
    enter_repl_mode: bool,
    /// Set by QUIT — send OK then close.
    quit: bool,
}

impl Connection {
    pub fn new(stream: TcpStream, db: Db) -> Self {
        Connection {
            stream,
            parser: RespParser::new(),
            read_buf: BytesMut::with_capacity(4096),
            db,
            enter_repl_mode: false,
            quit: false,
        }
    }

    pub async fn run(mut self) {
        let peer = self.stream.peer_addr().map(|a| a.to_string()).unwrap_or_default();
        tracing::debug!("Client connected: {peer}");
        loop {
            // Read more data
            let n = match self.stream.read_buf(&mut self.read_buf).await {
                Ok(0) => break, // connection closed
                Ok(n) => n,
                Err(e) => {
                    tracing::debug!("Read error ({peer}): {e}");
                    break;
                }
            };
            self.parser.feed(&self.read_buf[self.read_buf.len() - n..]);

            // Process all complete commands from the buffer
            loop {
                match self.parser.take_value() {
                    Err(e) => {
                        let resp = RespValue::error(format!("ERR protocol error: {e}"));
                        let _ = self.stream.write_all(&resp.to_bytes()).await;
                        return;
                    }
                    Ok(None) => break, // need more data
                    Ok(Some(value)) => {
                        let response = self.dispatch(value);
                        if let Err(e) = self.stream.write_all(&response.to_bytes()).await {
                            tracing::debug!("Write error ({peer}): {e}");
                            return;
                        }
                        // QUIT: send OK then drop connection.
                        if self.quit { return; }
                        // PSYNC sets this flag — switch into replication streaming mode.
                        if self.enter_repl_mode {
                            self.stream_to_replica(&peer).await;
                            return;
                        }
                    }
                }
            }
            self.read_buf.clear();
        }
        tracing::debug!("Client disconnected: {peer}");
    }

    /// Send the current RDB snapshot then forward the broadcast stream indefinitely.
    /// Called after PSYNC — this connection is now a replica receiver.
    async fn stream_to_replica(&mut self, peer: &str) {
        tracing::info!("Replica {peer} entered streaming mode");

        // Serialize current store to RDB bytes.
        let rdb_bytes = match self.db.rdb_snapshot() {
            Ok(b) => b,
            Err(e) => {
                tracing::error!("RDB for replica {peer}: {e}");
                return;
            }
        };

        // Send RDB as a bulk string WITHOUT trailing \r\n (Redis wire protocol).
        let header = format!("${}\r\n", rdb_bytes.len());
        if self.stream.write_all(header.as_bytes()).await.is_err() { return; }
        if self.stream.write_all(&rdb_bytes).await.is_err() { return; }

        // Subscribe to the broadcast channel and forward every write command.
        let mut rx = self.db.replication.tx.subscribe();
        loop {
            match rx.recv().await {
                Ok(data) => {
                    if self.stream.write_all(&data).await.is_err() { break; }
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                    tracing::warn!("Replica {peer} lagged {n} messages — forcing reconnect");
                    break;
                }
                Err(_) => break,
            }
        }
        tracing::info!("Replica {peer} disconnected");
    }

    fn dispatch(&mut self, value: RespValue) -> RespValue {
        let args = match extract_args(value) {
            None => return RespValue::error("ERR invalid request"),
            Some(a) if a.is_empty() => return RespValue::error("ERR empty command"),
            Some(a) => a,
        };

        let cmd = String::from_utf8_lossy(&args[0]).to_ascii_uppercase();

        match cmd.as_str() {
            // ── server ──────────────────────────────────────────────────────
            "PING" => {
                if args.len() > 1 {
                    RespValue::bulk(args[1].clone())
                } else {
                    RespValue::pong()
                }
            }
            "ECHO" => {
                if args.len() < 2 { return arity_err("ECHO"); }
                RespValue::bulk(args[1].clone())
            }
            "DBSIZE" => {
                RespValue::integer(self.db.dbsize() as i64)
            }
            "FLUSHDB" | "FLUSHALL" => {
                self.db.flush();
                RespValue::ok()
            }
            "SAVE" => {
                match self.db.bgsave() {
                    Ok(()) => RespValue::ok(),
                    Err(e) => RespValue::error(e.to_string()),
                }
            }
            "BGSAVE" => {
                let db = self.db.clone();
                tokio::spawn(async move {
                    if let Err(e) = db.bgsave() {
                        tracing::error!("BGSAVE error: {e}");
                    }
                });
                RespValue::SimpleString("Background saving started".into())
            }
            "BGREWRITEAOF" => {
                let db = self.db.clone();
                tokio::spawn(async move {
                    if let Err(e) = db.bgrewriteaof() {
                        tracing::error!("BGREWRITEAOF error: {e}");
                    }
                });
                RespValue::SimpleString("Background append only file rewriting started".into())
            }
            "SELECT" => {
                // We support only db0; accept but ignore other index
                RespValue::ok()
            }

            // ── connection / protocol ────────────────────────────────────────
            "QUIT" => {
                self.quit = true;
                RespValue::ok()
            }
            "AUTH" => {
                // No authentication configured — always succeed.
                RespValue::ok()
            }
            "HELLO" => {
                // Respond with RESP2 server info regardless of requested version.
                // This satisfies clients that send HELLO 2 or HELLO 3 without
                // forcing them to fall back (which produces warnings in some clients).
                let role = self.db.replication.role.read();
                let role_str = match &*role {
                    Role::Standalone | Role::Primary => "master",
                    Role::Replica { .. } => "slave",
                };
                RespValue::array(vec![
                    RespValue::bulk(b"server".to_vec()),
                    RespValue::bulk(b"boolcache".to_vec()),
                    RespValue::bulk(b"version".to_vec()),
                    RespValue::bulk(env!("CARGO_PKG_VERSION").as_bytes().to_vec()),
                    RespValue::bulk(b"proto".to_vec()),
                    RespValue::integer(2),
                    RespValue::bulk(b"id".to_vec()),
                    RespValue::integer(0),
                    RespValue::bulk(b"mode".to_vec()),
                    RespValue::bulk(b"standalone".to_vec()),
                    RespValue::bulk(b"role".to_vec()),
                    RespValue::bulk(role_str.as_bytes().to_vec()),
                    RespValue::bulk(b"modules".to_vec()),
                    RespValue::array(vec![]),
                ])
            }
            "CLIENT" => {
                let sub = args.get(1).map(|a| a.to_ascii_uppercase());
                match sub.as_deref() {
                    Some(b"SETNAME") => RespValue::ok(),
                    Some(b"GETNAME") => RespValue::nil(),
                    Some(b"ID")      => RespValue::integer(0),
                    Some(b"INFO")    => RespValue::bulk(b"id=0 addr=0.0.0.0:0 cmd=client\n".to_vec()),
                    Some(b"LIST")    => RespValue::bulk(b"id=0 addr=0.0.0.0:0 cmd=client\n".to_vec()),
                    Some(b"NO-EVICT") | Some(b"NO-TOUCH") | Some(b"REPLY") => RespValue::ok(),
                    _ => RespValue::ok(),
                }
            }
            "CONFIG" => {
                let sub = args.get(1).map(|a| a.to_ascii_uppercase());
                match sub.as_deref() {
                    Some(b"GET")       => RespValue::array(vec![]), // empty — no config exposed
                    Some(b"SET")       => RespValue::ok(),
                    Some(b"RESETSTAT") => RespValue::ok(),
                    Some(b"REWRITE")   => RespValue::ok(),
                    _ => RespValue::ok(),
                }
            }
            "COMMAND" => {
                let sub = args.get(1).map(|a| a.to_ascii_uppercase());
                match sub.as_deref() {
                    Some(b"COUNT") => RespValue::integer(0),
                    Some(b"DOCS")  => RespValue::array(vec![]),
                    Some(b"INFO")  => RespValue::array(vec![]),
                    Some(b"LIST")  => RespValue::array(vec![]),
                    _              => RespValue::array(vec![]),
                }
            }
            "RESET" => {
                // RESP3 soft-reset — just acknowledge.
                RespValue::SimpleString("RESET".into())
            }
            "OBJECT" => {
                // OBJECT ENCODING / REFCOUNT / IDLETIME — return a safe stub.
                RespValue::error("ERR no such key")
            }
            "WAIT" => {
                // WAIT numreplicas timeout — return 0 replicas confirmed.
                RespValue::integer(0)
            }
            "DEBUG" => {
                RespValue::ok()
            }
            "LATENCY" => {
                RespValue::array(vec![])
            }
            "SLOWLOG" => {
                RespValue::array(vec![])
            }
            "MEMORY" => {
                let sub = args.get(1).map(|a| a.to_ascii_uppercase());
                match sub.as_deref() {
                    Some(b"USAGE") => RespValue::nil(),
                    _              => RespValue::integer(0),
                }
            }

            // ── replication ──────────────────────────────────────────────────
            "REPLCONF" => {
                // Sent by replicas during handshake; always acknowledge.
                RespValue::ok()
            }
            "PSYNC" => {
                // Promote this node to Primary role (if not already) and enter
                // replication streaming mode after the response is sent.
                {
                    let mut role = self.db.replication.role.write();
                    if matches!(*role, Role::Standalone) {
                        *role = Role::Primary;
                    }
                }
                self.enter_repl_mode = true;
                RespValue::SimpleString(format!(
                    "FULLRESYNC {} {}",
                    self.db.replication.repl_id,
                    self.db.replication.offset(),
                ))
            }
            "REPLICAOF" => {
                if args.len() < 3 { return arity_err("REPLICAOF"); }
                let host = str(&args[1]);
                let port_str = str(&args[2]);
                if host.eq_ignore_ascii_case("no") && port_str.eq_ignore_ascii_case("one") {
                    *self.db.replication.role.write() = Role::Primary;
                    tracing::info!("Promoted to Primary (REPLICAOF NO ONE)");
                    RespValue::ok()
                } else {
                    let port = match parse_u64(&args[2]) {
                        None => return RespValue::error("ERR invalid port"),
                        Some(p) => p as u16,
                    };
                    *self.db.replication.role.write() = Role::Replica {
                        primary_host: host.clone(),
                        primary_port: port,
                    };
                    let db = self.db.clone();
                    crate::replication::replica::start(host, port, db);
                    RespValue::ok()
                }
            }
            "INFO" => {
                let section = args.get(1).map(|a| a.to_ascii_lowercase());
                let role = self.db.replication.role.read();
                let role_str = match &*role {
                    Role::Standalone | Role::Primary => "master",
                    Role::Replica { .. } => "slave",
                };
                let replicas = self.db.replication.connected_replicas();
                let offset = self.db.replication.offset();
                let repl_id = &self.db.replication.repl_id;
                let keyspace_size = self.db.dbsize();

                let replication_info = format!(
                    "# Replication\r\n\
                     role:{role_str}\r\n\
                     connected_slaves:{replicas}\r\n\
                     master_replid:{repl_id}\r\n\
                     master_repl_offset:{offset}\r\n"
                );
                let server_info = format!(
                    "# Server\r\n\
                     redis_version:7.0.0-boolcache\r\n\
                     tcp_port:6379\r\n"
                );
                let keyspace_info = format!("# Keyspace\r\ndb0:keys={keyspace_size}\r\n");

                let info = match section.as_deref() {
                    Some(b"replication") => replication_info,
                    Some(b"server") => server_info,
                    Some(b"keyspace") => keyspace_info,
                    _ => format!("{server_info}\r\n{replication_info}\r\n{keyspace_info}"),
                };
                RespValue::bulk(info.into_bytes())
            }

            // ── key ─────────────────────────────────────────────────────────
            "DEL" => {
                if args.len() < 2 { return arity_err("DEL"); }
                let keys: Vec<&str> = args[1..].iter()
                    .map(|k| std::str::from_utf8(k).unwrap_or(""))
                    .collect();
                let n = self.db.del(&keys);
                RespValue::integer(n as i64)
            }
            "EXISTS" => {
                if args.len() < 2 { return arity_err("EXISTS"); }
                let keys: Vec<&str> = args[1..].iter()
                    .map(|k| std::str::from_utf8(k).unwrap_or(""))
                    .collect();
                let n = self.db.exists(&keys);
                RespValue::integer(n as i64)
            }
            "EXPIRE" => {
                if args.len() < 3 { return arity_err("EXPIRE"); }
                let key = str(&args[1]);
                match parse_u64(&args[2]) {
                    None => RespValue::error("ERR value is not an integer"),
                    Some(secs) => {
                        let ok = self.db.pexpire(&key, secs * 1000);
                        RespValue::integer(ok as i64)
                    }
                }
            }
            "PEXPIRE" => {
                if args.len() < 3 { return arity_err("PEXPIRE"); }
                let key = str(&args[1]);
                match parse_u64(&args[2]) {
                    None => RespValue::error("ERR value is not an integer"),
                    Some(ms) => {
                        let ok = self.db.pexpire(&key, ms);
                        RespValue::integer(ok as i64)
                    }
                }
            }
            "TTL" => {
                if args.len() < 2 { return arity_err("TTL"); }
                let key = str(&args[1]);
                let pttl = self.db.pttl(&key);
                RespValue::integer(if pttl < 0 { pttl } else { pttl / 1000 })
            }
            "PTTL" => {
                if args.len() < 2 { return arity_err("PTTL"); }
                let key = str(&args[1]);
                RespValue::integer(self.db.pttl(&key))
            }
            "PERSIST" => {
                if args.len() < 2 { return arity_err("PERSIST"); }
                let key = str(&args[1]);
                RespValue::integer(self.db.persist_key(&key) as i64)
            }
            "TYPE" => {
                if args.len() < 2 { return arity_err("TYPE"); }
                let key = str(&args[1]);
                RespValue::SimpleString(self.db.type_of(&key).into())
            }
            "RENAME" => {
                if args.len() < 3 { return arity_err("RENAME"); }
                let src = str(&args[1]);
                let dst = str(&args[2]);
                match self.db.rename(&src, &dst) {
                    Ok(()) => RespValue::ok(),
                    Err(e) => RespValue::error(e.to_string()),
                }
            }
            "KEYS" => {
                let pattern = if args.len() >= 2 { str(&args[1]) } else { "*".into() };
                let keys = self.db.keys(&pattern);
                RespValue::array(keys.into_iter().map(|k: String| RespValue::bulk(k.into_bytes())).collect())
            }
            "SCAN" => {
                let cursor = args.get(1).and_then(|a| parse_usize(a)).unwrap_or(0);
                let pattern = find_option_str(&args, b"MATCH").unwrap_or_else(|| "*".into());
                let count = find_option_usize(&args, b"COUNT").unwrap_or(10);
                let (next, keys) = self.db.scan(cursor, &pattern, count);
                RespValue::array(vec![
                    RespValue::bulk(next.to_string().into_bytes()),
                    RespValue::array(keys.into_iter().map(|k: String| RespValue::bulk(k.into_bytes())).collect()),
                ])
            }

            // ── string ───────────────────────────────────────────────────────
            "SET" => {
                if args.len() < 3 { return arity_err("SET"); }
                let key = str(&args[1]);
                let val = args[2].clone();
                let mut opts = string::SetOptions::default();
                let mut i = 3;
                while i < args.len() {
                    match args[i].to_ascii_uppercase().as_slice() {
                        b"EX" => {
                            if let Some(s) = args.get(i + 1).and_then(|a| parse_u64(a)) {
                                opts.ttl = Some(Duration::from_secs(s));
                                i += 2;
                            } else { return RespValue::error("ERR invalid EX"); }
                        }
                        b"PX" => {
                            if let Some(ms) = args.get(i + 1).and_then(|a| parse_u64(a)) {
                                opts.ttl = Some(Duration::from_millis(ms));
                                i += 2;
                            } else { return RespValue::error("ERR invalid PX"); }
                        }
                        b"NX" => { opts.nx = true; i += 1; }
                        b"XX" => { opts.xx = true; i += 1; }
                        b"GET" => { opts.get = true; i += 1; }
                        _ => { i += 1; }
                    }
                }
                match string::set(&self.db, &key, val, opts) {
                    Err(e) => RespValue::error(e.to_string()),
                    Ok(old) => {
                        // With GET flag: return old value; without: return OK
                        match old {
                            Some(v) => RespValue::bulk(v),
                            None => RespValue::ok(),
                        }
                    }
                }
            }
            "GET" => {
                if args.len() < 2 { return arity_err("GET"); }
                let key = str(&args[1]);
                match string::get(&self.db, &key) {
                    Err(e) => RespValue::error(e.to_string()),
                    Ok(None) => RespValue::nil(),
                    Ok(Some(v)) => RespValue::bulk(v),
                }
            }
            "GETSET" => {
                if args.len() < 3 { return arity_err("GETSET"); }
                let key = str(&args[1]);
                match string::getset(&self.db, &key, args[2].clone()) {
                    Err(e) => RespValue::error(e.to_string()),
                    Ok(None) => RespValue::nil(),
                    Ok(Some(v)) => RespValue::bulk(v),
                }
            }
            "MGET" => {
                if args.len() < 2 { return arity_err("MGET"); }
                let keys: Vec<&str> = args[1..].iter()
                    .map(|k| std::str::from_utf8(k).unwrap_or(""))
                    .collect();
                let results = string::mget(&self.db, &keys);
                RespValue::array(results.into_iter().map(|v| match v {
                    None => RespValue::nil(),
                    Some(b) => RespValue::bulk(b),
                }).collect())
            }
            "MSET" => {
                if args.len() < 3 || (args.len() - 1) % 2 != 0 {
                    return RespValue::error("ERR wrong number of arguments for MSET");
                }
                let pairs: Vec<(String, Vec<u8>)> = args[1..].chunks(2)
                    .map(|c| (str(&c[0]), c[1].clone()))
                    .collect();
                string::mset(&self.db, pairs);
                RespValue::ok()
            }
            "SETNX" => {
                if args.len() < 3 { return arity_err("SETNX"); }
                let key = str(&args[1]);
                RespValue::integer(string::setnx(&self.db, &key, args[2].clone()) as i64)
            }
            "SETEX" => {
                if args.len() < 4 { return arity_err("SETEX"); }
                let key = str(&args[1]);
                let secs = match parse_u64(&args[2]) {
                    None => return RespValue::error("ERR invalid expire time"),
                    Some(s) => s,
                };
                string::setex(&self.db, &key, args[3].clone(), Duration::from_secs(secs));
                RespValue::ok()
            }
            "INCR" => {
                if args.len() < 2 { return arity_err("INCR"); }
                let key = str(&args[1]);
                match string::incr(&self.db, &key) {
                    Err(e) => RespValue::error(e.to_string()),
                    Ok(n) => RespValue::integer(n),
                }
            }
            "INCRBY" => {
                if args.len() < 3 { return arity_err("INCRBY"); }
                let key = str(&args[1]);
                let delta = match parse_i64(&args[2]) {
                    None => return RespValue::error("ERR value is not an integer"),
                    Some(n) => n,
                };
                match string::incr_by(&self.db, &key, delta) {
                    Err(e) => RespValue::error(e.to_string()),
                    Ok(n) => RespValue::integer(n),
                }
            }
            "DECR" => {
                if args.len() < 2 { return arity_err("DECR"); }
                let key = str(&args[1]);
                match string::decr(&self.db, &key) {
                    Err(e) => RespValue::error(e.to_string()),
                    Ok(n) => RespValue::integer(n),
                }
            }
            "DECRBY" => {
                if args.len() < 3 { return arity_err("DECRBY"); }
                let key = str(&args[1]);
                let delta = match parse_i64(&args[2]) {
                    None => return RespValue::error("ERR value is not an integer"),
                    Some(n) => n,
                };
                match string::decr_by(&self.db, &key, delta) {
                    Err(e) => RespValue::error(e.to_string()),
                    Ok(n) => RespValue::integer(n),
                }
            }
            "APPEND" => {
                if args.len() < 3 { return arity_err("APPEND"); }
                let key = str(&args[1]);
                match string::append(&self.db, &key, &args[2]) {
                    Err(e) => RespValue::error(e.to_string()),
                    Ok(n) => RespValue::integer(n as i64),
                }
            }
            "STRLEN" => {
                if args.len() < 2 { return arity_err("STRLEN"); }
                let key = str(&args[1]);
                match string::strlen(&self.db, &key) {
                    Err(e) => RespValue::error(e.to_string()),
                    Ok(n) => RespValue::integer(n as i64),
                }
            }

            // ── list ─────────────────────────────────────────────────────────
            "LPUSH" => {
                if args.len() < 3 { return arity_err("LPUSH"); }
                let key = str(&args[1]);
                let vals: Vec<Vec<u8>> = args[2..].iter().cloned().collect();
                match list::lpush(&self.db, &key, vals) {
                    Err(e) => RespValue::error(e.to_string()),
                    Ok(n) => RespValue::integer(n as i64),
                }
            }
            "RPUSH" => {
                if args.len() < 3 { return arity_err("RPUSH"); }
                let key = str(&args[1]);
                let vals: Vec<Vec<u8>> = args[2..].iter().cloned().collect();
                match list::rpush(&self.db, &key, vals) {
                    Err(e) => RespValue::error(e.to_string()),
                    Ok(n) => RespValue::integer(n as i64),
                }
            }
            "LPOP" => {
                if args.len() < 2 { return arity_err("LPOP"); }
                let key = str(&args[1]);
                let count = args.get(2).and_then(|a| parse_usize(a)).unwrap_or(1);
                match list::lpop(&self.db, &key, count) {
                    Err(e) => RespValue::error(e.to_string()),
                    Ok(items) if items.is_empty() => RespValue::nil(),
                    Ok(items) if count == 1 => RespValue::bulk(items.into_iter().next().unwrap()),
                    Ok(items) => RespValue::array(items.into_iter().map(RespValue::bulk).collect()),
                }
            }
            "RPOP" => {
                if args.len() < 2 { return arity_err("RPOP"); }
                let key = str(&args[1]);
                let count = args.get(2).and_then(|a| parse_usize(a)).unwrap_or(1);
                match list::rpop(&self.db, &key, count) {
                    Err(e) => RespValue::error(e.to_string()),
                    Ok(items) if items.is_empty() => RespValue::nil(),
                    Ok(items) if count == 1 => RespValue::bulk(items.into_iter().next().unwrap()),
                    Ok(items) => RespValue::array(items.into_iter().map(RespValue::bulk).collect()),
                }
            }
            "LRANGE" => {
                if args.len() < 4 { return arity_err("LRANGE"); }
                let key = str(&args[1]);
                let start = parse_i64(&args[2]).unwrap_or(0);
                let stop = parse_i64(&args[3]).unwrap_or(-1);
                match list::lrange(&self.db, &key, start, stop) {
                    Err(e) => RespValue::error(e.to_string()),
                    Ok(items) => RespValue::array(items.into_iter().map(RespValue::bulk).collect()),
                }
            }
            "LLEN" => {
                if args.len() < 2 { return arity_err("LLEN"); }
                let key = str(&args[1]);
                match list::llen(&self.db, &key) {
                    Err(e) => RespValue::error(e.to_string()),
                    Ok(n) => RespValue::integer(n as i64),
                }
            }
            "LINDEX" => {
                if args.len() < 3 { return arity_err("LINDEX"); }
                let key = str(&args[1]);
                let idx = parse_i64(&args[2]).unwrap_or(0);
                match list::lindex(&self.db, &key, idx) {
                    Err(e) => RespValue::error(e.to_string()),
                    Ok(None) => RespValue::nil(),
                    Ok(Some(v)) => RespValue::bulk(v),
                }
            }
            "LSET" => {
                if args.len() < 4 { return arity_err("LSET"); }
                let key = str(&args[1]);
                let idx = parse_i64(&args[2]).unwrap_or(0);
                match list::lset(&self.db, &key, idx, args[3].clone()) {
                    Err(e) => RespValue::error(e.to_string()),
                    Ok(()) => RespValue::ok(),
                }
            }
            "LTRIM" => {
                if args.len() < 4 { return arity_err("LTRIM"); }
                let key = str(&args[1]);
                let start = parse_i64(&args[2]).unwrap_or(0);
                let stop = parse_i64(&args[3]).unwrap_or(-1);
                match list::ltrim(&self.db, &key, start, stop) {
                    Err(e) => RespValue::error(e.to_string()),
                    Ok(()) => RespValue::ok(),
                }
            }
            "LINSERT" => {
                if args.len() < 5 { return arity_err("LINSERT"); }
                let key = str(&args[1]);
                let before = match args[2].to_ascii_uppercase().as_slice() {
                    b"BEFORE" => true,
                    b"AFTER" => false,
                    _ => return RespValue::error("ERR syntax error"),
                };
                match list::linsert(&self.db, &key, before, &args[3], args[4].clone()) {
                    Err(e) => RespValue::error(e.to_string()),
                    Ok(n) => RespValue::integer(n),
                }
            }

            // ── hash ─────────────────────────────────────────────────────────
            "HSET" | "HMSET" => {
                if args.len() < 4 || (args.len() - 2) % 2 != 0 {
                    return RespValue::error("ERR wrong number of arguments for HSET");
                }
                let key = str(&args[1]);
                let pairs: Vec<(Vec<u8>, Vec<u8>)> = args[2..].chunks(2)
                    .map(|c| (c[0].clone(), c[1].clone()))
                    .collect();
                match hash::hset(&self.db, &key, pairs) {
                    Err(e) => RespValue::error(e.to_string()),
                    Ok(n) => RespValue::integer(n as i64),
                }
            }
            "HGET" => {
                if args.len() < 3 { return arity_err("HGET"); }
                let key = str(&args[1]);
                match hash::hget(&self.db, &key, &args[2]) {
                    Err(e) => RespValue::error(e.to_string()),
                    Ok(None) => RespValue::nil(),
                    Ok(Some(v)) => RespValue::bulk(v),
                }
            }
            "HMGET" => {
                if args.len() < 3 { return arity_err("HMGET"); }
                let key = str(&args[1]);
                let fields: Vec<Vec<u8>> = args[2..].iter().cloned().collect();
                match hash::hmget(&self.db, &key, &fields) {
                    Err(e) => RespValue::error(e.to_string()),
                    Ok(vals) => RespValue::array(vals.into_iter().map(|v| match v {
                        None => RespValue::nil(),
                        Some(b) => RespValue::bulk(b),
                    }).collect()),
                }
            }
            "HDEL" => {
                if args.len() < 3 { return arity_err("HDEL"); }
                let key = str(&args[1]);
                let fields: Vec<Vec<u8>> = args[2..].iter().cloned().collect();
                match hash::hdel(&self.db, &key, &fields) {
                    Err(e) => RespValue::error(e.to_string()),
                    Ok(n) => RespValue::integer(n as i64),
                }
            }
            "HEXISTS" => {
                if args.len() < 3 { return arity_err("HEXISTS"); }
                let key = str(&args[1]);
                match hash::hexists(&self.db, &key, &args[2]) {
                    Err(e) => RespValue::error(e.to_string()),
                    Ok(b) => RespValue::integer(b as i64),
                }
            }
            "HGETALL" => {
                if args.len() < 2 { return arity_err("HGETALL"); }
                let key = str(&args[1]);
                match hash::hgetall(&self.db, &key) {
                    Err(e) => RespValue::error(e.to_string()),
                    Ok(pairs) => {
                        let mut items = Vec::with_capacity(pairs.len() * 2);
                        for (k, v) in pairs {
                            items.push(RespValue::bulk(k));
                            items.push(RespValue::bulk(v));
                        }
                        RespValue::array(items)
                    }
                }
            }
            "HKEYS" => {
                if args.len() < 2 { return arity_err("HKEYS"); }
                let key = str(&args[1]);
                match hash::hkeys(&self.db, &key) {
                    Err(e) => RespValue::error(e.to_string()),
                    Ok(keys) => RespValue::array(keys.into_iter().map(RespValue::bulk).collect()),
                }
            }
            "HVALS" => {
                if args.len() < 2 { return arity_err("HVALS"); }
                let key = str(&args[1]);
                match hash::hvals(&self.db, &key) {
                    Err(e) => RespValue::error(e.to_string()),
                    Ok(vals) => RespValue::array(vals.into_iter().map(RespValue::bulk).collect()),
                }
            }
            "HLEN" => {
                if args.len() < 2 { return arity_err("HLEN"); }
                let key = str(&args[1]);
                match hash::hlen(&self.db, &key) {
                    Err(e) => RespValue::error(e.to_string()),
                    Ok(n) => RespValue::integer(n as i64),
                }
            }
            "HINCRBY" => {
                if args.len() < 4 { return arity_err("HINCRBY"); }
                let key = str(&args[1]);
                let delta = match parse_i64(&args[3]) {
                    None => return RespValue::error("ERR value is not an integer"),
                    Some(n) => n,
                };
                match hash::hincrby(&self.db, &key, args[2].clone(), delta) {
                    Err(e) => RespValue::error(e.to_string()),
                    Ok(n) => RespValue::integer(n),
                }
            }

            // ── set ──────────────────────────────────────────────────────────
            "SADD" => {
                if args.len() < 3 { return arity_err("SADD"); }
                let key = str(&args[1]);
                let members: Vec<Vec<u8>> = args[2..].iter().cloned().collect();
                match set_cmd::sadd(&self.db, &key, members) {
                    Err(e) => RespValue::error(e.to_string()),
                    Ok(n) => RespValue::integer(n as i64),
                }
            }
            "SREM" => {
                if args.len() < 3 { return arity_err("SREM"); }
                let key = str(&args[1]);
                let members: Vec<Vec<u8>> = args[2..].iter().cloned().collect();
                match set_cmd::srem(&self.db, &key, members) {
                    Err(e) => RespValue::error(e.to_string()),
                    Ok(n) => RespValue::integer(n as i64),
                }
            }
            "SMEMBERS" => {
                if args.len() < 2 { return arity_err("SMEMBERS"); }
                let key = str(&args[1]);
                match set_cmd::smembers(&self.db, &key) {
                    Err(e) => RespValue::error(e.to_string()),
                    Ok(members) => RespValue::array(members.into_iter().map(RespValue::bulk).collect()),
                }
            }
            "SISMEMBER" => {
                if args.len() < 3 { return arity_err("SISMEMBER"); }
                let key = str(&args[1]);
                match set_cmd::sismember(&self.db, &key, &args[2]) {
                    Err(e) => RespValue::error(e.to_string()),
                    Ok(b) => RespValue::integer(b as i64),
                }
            }
            "SCARD" => {
                if args.len() < 2 { return arity_err("SCARD"); }
                let key = str(&args[1]);
                match set_cmd::scard(&self.db, &key) {
                    Err(e) => RespValue::error(e.to_string()),
                    Ok(n) => RespValue::integer(n as i64),
                }
            }
            "SINTER" => {
                if args.len() < 2 { return arity_err("SINTER"); }
                let keys: Vec<&str> = args[1..].iter()
                    .map(|k| std::str::from_utf8(k).unwrap_or(""))
                    .collect();
                match set_cmd::sinter(&self.db, &keys) {
                    Err(e) => RespValue::error(e.to_string()),
                    Ok(m) => RespValue::array(m.into_iter().map(RespValue::bulk).collect()),
                }
            }
            "SUNION" => {
                if args.len() < 2 { return arity_err("SUNION"); }
                let keys: Vec<&str> = args[1..].iter()
                    .map(|k| std::str::from_utf8(k).unwrap_or(""))
                    .collect();
                match set_cmd::sunion(&self.db, &keys) {
                    Err(e) => RespValue::error(e.to_string()),
                    Ok(m) => RespValue::array(m.into_iter().map(RespValue::bulk).collect()),
                }
            }
            "SDIFF" => {
                if args.len() < 2 { return arity_err("SDIFF"); }
                let keys: Vec<&str> = args[1..].iter()
                    .map(|k| std::str::from_utf8(k).unwrap_or(""))
                    .collect();
                match set_cmd::sdiff(&self.db, &keys) {
                    Err(e) => RespValue::error(e.to_string()),
                    Ok(m) => RespValue::array(m.into_iter().map(RespValue::bulk).collect()),
                }
            }
            "SMOVE" => {
                if args.len() < 4 { return arity_err("SMOVE"); }
                let src = str(&args[1]);
                let dst = str(&args[2]);
                match set_cmd::smove(&self.db, &src, &dst, args[3].clone()) {
                    Err(e) => RespValue::error(e.to_string()),
                    Ok(b) => RespValue::integer(b as i64),
                }
            }

            // ── sorted set ───────────────────────────────────────────────────
            "ZADD" => {
                if args.len() < 4 || (args.len() - 2) % 2 != 0 {
                    return RespValue::error("ERR wrong number of arguments for ZADD");
                }
                let key = str(&args[1]);
                let pairs: Vec<(f64, Vec<u8>)> = args[2..].chunks(2).filter_map(|c| {
                    let score = parse_f64(&c[0])?;
                    Some((score, c[1].clone()))
                }).collect();
                match zset::zadd(&self.db, &key, pairs) {
                    Err(e) => RespValue::error(e.to_string()),
                    Ok(n) => RespValue::integer(n as i64),
                }
            }
            "ZREM" => {
                if args.len() < 3 { return arity_err("ZREM"); }
                let key = str(&args[1]);
                let members: Vec<Vec<u8>> = args[2..].iter().cloned().collect();
                match zset::zrem(&self.db, &key, members) {
                    Err(e) => RespValue::error(e.to_string()),
                    Ok(n) => RespValue::integer(n as i64),
                }
            }
            "ZSCORE" => {
                if args.len() < 3 { return arity_err("ZSCORE"); }
                let key = str(&args[1]);
                match zset::zscore(&self.db, &key, &args[2]) {
                    Err(e) => RespValue::error(e.to_string()),
                    Ok(None) => RespValue::nil(),
                    Ok(Some(s)) => RespValue::bulk(s.to_string().into_bytes()),
                }
            }
            "ZRANK" => {
                if args.len() < 3 { return arity_err("ZRANK"); }
                let key = str(&args[1]);
                match zset::zrank(&self.db, &key, &args[2]) {
                    Err(e) => RespValue::error(e.to_string()),
                    Ok(None) => RespValue::nil(),
                    Ok(Some(r)) => RespValue::integer(r as i64),
                }
            }
            "ZREVRANK" => {
                if args.len() < 3 { return arity_err("ZREVRANK"); }
                let key = str(&args[1]);
                match zset::zrevrank(&self.db, &key, &args[2]) {
                    Err(e) => RespValue::error(e.to_string()),
                    Ok(None) => RespValue::nil(),
                    Ok(Some(r)) => RespValue::integer(r as i64),
                }
            }
            "ZCARD" => {
                if args.len() < 2 { return arity_err("ZCARD"); }
                let key = str(&args[1]);
                match zset::zcard(&self.db, &key) {
                    Err(e) => RespValue::error(e.to_string()),
                    Ok(n) => RespValue::integer(n as i64),
                }
            }
            "ZRANGE" => {
                if args.len() < 4 { return arity_err("ZRANGE"); }
                let key = str(&args[1]);
                let start = parse_i64(&args[2]).unwrap_or(0);
                let stop = parse_i64(&args[3]).unwrap_or(-1);
                let with_scores = args.get(4)
                    .map(|a| a.to_ascii_uppercase() == b"WITHSCORES")
                    .unwrap_or(false);
                match zset::zrange(&self.db, &key, start, stop) {
                    Err(e) => RespValue::error(e.to_string()),
                    Ok(items) => zrange_resp(items, with_scores),
                }
            }
            "ZREVRANGE" => {
                if args.len() < 4 { return arity_err("ZREVRANGE"); }
                let key = str(&args[1]);
                let start = parse_i64(&args[2]).unwrap_or(0);
                let stop = parse_i64(&args[3]).unwrap_or(-1);
                let with_scores = args.get(4)
                    .map(|a| a.to_ascii_uppercase() == b"WITHSCORES")
                    .unwrap_or(false);
                match zset::zrevrange(&self.db, &key, start, stop) {
                    Err(e) => RespValue::error(e.to_string()),
                    Ok(items) => zrange_resp(items, with_scores),
                }
            }
            "ZRANGEBYSCORE" => {
                if args.len() < 4 { return arity_err("ZRANGEBYSCORE"); }
                let key = str(&args[1]);
                let min = parse_score_bound(&args[2]).unwrap_or(f64::NEG_INFINITY);
                let max = parse_score_bound(&args[3]).unwrap_or(f64::INFINITY);
                let with_scores = args.windows(1)
                    .any(|w| w[0].to_ascii_uppercase() == b"WITHSCORES");
                match zset::zrangebyscore(&self.db, &key, min, max) {
                    Err(e) => RespValue::error(e.to_string()),
                    Ok(items) => zrange_resp(items, with_scores),
                }
            }
            "ZCOUNT" => {
                if args.len() < 4 { return arity_err("ZCOUNT"); }
                let key = str(&args[1]);
                let min = parse_score_bound(&args[2]).unwrap_or(f64::NEG_INFINITY);
                let max = parse_score_bound(&args[3]).unwrap_or(f64::INFINITY);
                match zset::zcount(&self.db, &key, min, max) {
                    Err(e) => RespValue::error(e.to_string()),
                    Ok(n) => RespValue::integer(n as i64),
                }
            }
            "ZINCRBY" => {
                if args.len() < 4 { return arity_err("ZINCRBY"); }
                let key = str(&args[1]);
                let delta = match parse_f64(&args[2]) {
                    None => return RespValue::error("ERR value is not a float"),
                    Some(f) => f,
                };
                match zset::zincrby(&self.db, &key, delta, args[3].clone()) {
                    Err(e) => RespValue::error(e.to_string()),
                    Ok(s) => RespValue::bulk(s.to_string().into_bytes()),
                }
            }

            _ => RespValue::error(format!("ERR unknown command `{cmd}`")),
        }
    }
}

// ── helpers ───────────────────────────────────────────────────────────────────

fn extract_args(value: RespValue) -> Option<Vec<Vec<u8>>> {
    match value {
        RespValue::Array(Some(items)) => {
            items.into_iter().map(|item| match item {
                RespValue::BulkString(Some(b)) => Some(b),
                RespValue::SimpleString(s) => Some(s.into_bytes()),
                _ => None,
            }).collect()
        }
        _ => None,
    }
}

fn arity_err(cmd: &str) -> RespValue {
    RespValue::error(format!("ERR wrong number of arguments for '{cmd}'"))
}

fn str(b: &[u8]) -> String {
    String::from_utf8_lossy(b).into_owned()
}

fn parse_i64(b: &[u8]) -> Option<i64> {
    std::str::from_utf8(b).ok()?.trim().parse().ok()
}

fn parse_u64(b: &[u8]) -> Option<u64> {
    std::str::from_utf8(b).ok()?.trim().parse().ok()
}

fn parse_usize(b: &[u8]) -> Option<usize> {
    std::str::from_utf8(b).ok()?.trim().parse().ok()
}

fn parse_f64(b: &[u8]) -> Option<f64> {
    std::str::from_utf8(b).ok()?.trim().parse().ok()
}

fn parse_score_bound(b: &[u8]) -> Option<f64> {
    match b {
        b"-inf" | b"-INF" => Some(f64::NEG_INFINITY),
        b"+inf" | b"+INF" | b"inf" | b"INF" => Some(f64::INFINITY),
        _ => parse_f64(b),
    }
}

fn zrange_resp(items: Vec<(Vec<u8>, f64)>, with_scores: bool) -> RespValue {
    let mut out = Vec::with_capacity(if with_scores { items.len() * 2 } else { items.len() });
    for (member, score) in items {
        out.push(RespValue::bulk(member));
        if with_scores {
            out.push(RespValue::bulk(score.to_string().into_bytes()));
        }
    }
    RespValue::array(out)
}

fn find_option_str(args: &[Vec<u8>], option: &[u8]) -> Option<String> {
    args.windows(2).find(|w| w[0].to_ascii_uppercase() == option)
        .map(|w| str(&w[1]))
}

fn find_option_usize(args: &[Vec<u8>], option: &[u8]) -> Option<usize> {
    args.windows(2).find(|w| w[0].to_ascii_uppercase() == option)
        .and_then(|w| parse_usize(&w[1]))
}
