//! RESP (Redis Serialization Protocol) parser and serializer.
//!
//! Supports RESP2 as used by Redis clients.

use std::io;

// ── value type ────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub enum RespValue {
    /// `+OK\r\n`
    SimpleString(String),
    /// `-ERR message\r\n`
    Error(String),
    /// `:1234\r\n`
    Integer(i64),
    /// `$6\r\nfoobar\r\n`  or  `$-1\r\n` for nil
    BulkString(Option<Vec<u8>>),
    /// `*2\r\n...`  or  `*-1\r\n` for nil array
    Array(Option<Vec<RespValue>>),
}

impl RespValue {
    pub fn ok() -> Self { RespValue::SimpleString("OK".into()) }
    pub fn pong() -> Self { RespValue::SimpleString("PONG".into()) }
    pub fn nil() -> Self { RespValue::BulkString(None) }
pub fn integer(n: i64) -> Self { RespValue::Integer(n) }
    pub fn bulk(v: Vec<u8>) -> Self { RespValue::BulkString(Some(v)) }
    pub fn error(msg: impl Into<String>) -> Self { RespValue::Error(msg.into()) }
    pub fn array(items: Vec<RespValue>) -> Self { RespValue::Array(Some(items)) }

    /// Serialize to RESP bytes.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        self.write_to(&mut buf);
        buf
    }

    fn write_to(&self, buf: &mut Vec<u8>) {
        match self {
            RespValue::SimpleString(s) => {
                buf.push(b'+');
                buf.extend_from_slice(s.as_bytes());
                buf.extend_from_slice(b"\r\n");
            }
            RespValue::Error(s) => {
                buf.push(b'-');
                buf.extend_from_slice(s.as_bytes());
                buf.extend_from_slice(b"\r\n");
            }
            RespValue::Integer(n) => {
                buf.push(b':');
                buf.extend_from_slice(n.to_string().as_bytes());
                buf.extend_from_slice(b"\r\n");
            }
            RespValue::BulkString(None) => {
                buf.extend_from_slice(b"$-1\r\n");
            }
            RespValue::BulkString(Some(v)) => {
                buf.push(b'$');
                buf.extend_from_slice(v.len().to_string().as_bytes());
                buf.extend_from_slice(b"\r\n");
                buf.extend_from_slice(v);
                buf.extend_from_slice(b"\r\n");
            }
            RespValue::Array(None) => {
                buf.extend_from_slice(b"*-1\r\n");
            }
            RespValue::Array(Some(items)) => {
                buf.push(b'*');
                buf.extend_from_slice(items.len().to_string().as_bytes());
                buf.extend_from_slice(b"\r\n");
                for item in items {
                    item.write_to(buf);
                }
            }
        }
    }
}

// ── parser ────────────────────────────────────────────────────────────────────

/// Incremental RESP parser. Feed bytes with `feed()`; take complete values with `take_value()`.
pub struct RespParser {
    buf: Vec<u8>,
}

impl RespParser {
    pub fn new() -> Self {
        RespParser { buf: Vec::new() }
    }

    pub fn feed(&mut self, data: &[u8]) {
        self.buf.extend_from_slice(data);
    }

    /// Try to parse one complete RESP value from the buffer.
    /// Returns `Ok(Some(value))` if complete, `Ok(None)` if more data needed,
    /// `Err` on malformed input.
    pub fn take_value(&mut self) -> io::Result<Option<RespValue>> {
        let (value, consumed) = match parse_value(&self.buf)? {
            None => return Ok(None),
            Some(r) => r,
        };
        self.buf.drain(..consumed);
        Ok(Some(value))
    }
}

/// Returns `(value, bytes_consumed)` or `None` if incomplete.
fn parse_value(buf: &[u8]) -> io::Result<Option<(RespValue, usize)>> {
    if buf.is_empty() { return Ok(None); }
    match buf[0] {
        b'+' => parse_simple_string(buf),
        b'-' => parse_error(buf),
        b':' => parse_integer(buf),
        b'$' => parse_bulk_string(buf),
        b'*' => parse_array(buf),
        // Inline command support (e.g. "PING\r\n" from telnet)
        _ => parse_inline(buf),
    }
}

fn read_line(buf: &[u8]) -> Option<(&[u8], usize)> {
    let pos = buf.windows(2).position(|w| w == b"\r\n")?;
    Some((&buf[..pos], pos + 2))
}

fn parse_simple_string(buf: &[u8]) -> io::Result<Option<(RespValue, usize)>> {
    match read_line(&buf[1..]) {
        None => Ok(None),
        Some((line, consumed)) => {
            let s = String::from_utf8_lossy(line).into_owned();
            Ok(Some((RespValue::SimpleString(s), 1 + consumed)))
        }
    }
}

fn parse_error(buf: &[u8]) -> io::Result<Option<(RespValue, usize)>> {
    match read_line(&buf[1..]) {
        None => Ok(None),
        Some((line, consumed)) => {
            let s = String::from_utf8_lossy(line).into_owned();
            Ok(Some((RespValue::Error(s), 1 + consumed)))
        }
    }
}

fn parse_integer(buf: &[u8]) -> io::Result<Option<(RespValue, usize)>> {
    match read_line(&buf[1..]) {
        None => Ok(None),
        Some((line, consumed)) => {
            let s = std::str::from_utf8(line)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            let n: i64 = s.trim().parse()
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, format!("int parse: {e}")))?;
            Ok(Some((RespValue::Integer(n), 1 + consumed)))
        }
    }
}

fn parse_bulk_string(buf: &[u8]) -> io::Result<Option<(RespValue, usize)>> {
    let (len_line, header_consumed) = match read_line(&buf[1..]) {
        None => return Ok(None),
        Some(r) => r,
    };
    let len: i64 = std::str::from_utf8(len_line)
        .ok()
        .and_then(|s| s.trim().parse().ok())
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "bulk len"))?;
    if len == -1 {
        return Ok(Some((RespValue::BulkString(None), 1 + header_consumed)));
    }
    let len = len as usize;
    let data_start = 1 + header_consumed;
    if buf.len() < data_start + len + 2 {
        return Ok(None); // need more data
    }
    let data = buf[data_start..data_start + len].to_vec();
    Ok(Some((RespValue::BulkString(Some(data)), data_start + len + 2)))
}

fn parse_array(buf: &[u8]) -> io::Result<Option<(RespValue, usize)>> {
    let (len_line, header_consumed) = match read_line(&buf[1..]) {
        None => return Ok(None),
        Some(r) => r,
    };
    let count: i64 = std::str::from_utf8(len_line)
        .ok()
        .and_then(|s| s.trim().parse().ok())
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "array len"))?;
    if count == -1 {
        return Ok(Some((RespValue::Array(None), 1 + header_consumed)));
    }
    let count = count as usize;
    let mut items = Vec::with_capacity(count);
    let mut offset = 1 + header_consumed;
    for _ in 0..count {
        match parse_value(&buf[offset..])? {
            None => return Ok(None),
            Some((val, consumed)) => {
                offset += consumed;
                items.push(val);
            }
        }
    }
    Ok(Some((RespValue::Array(Some(items)), offset)))
}

fn parse_inline(buf: &[u8]) -> io::Result<Option<(RespValue, usize)>> {
    match read_line(buf) {
        None => Ok(None),
        Some((line, consumed)) => {
            let parts: Vec<RespValue> = line
                .split(|&b| b == b' ')
                .filter(|p| !p.is_empty())
                .map(|p| RespValue::BulkString(Some(p.to_vec())))
                .collect();
            Ok(Some((RespValue::Array(Some(parts)), consumed)))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_simple() {
        let mut p = RespParser::new();
        p.feed(b"+OK\r\n");
        let v = p.take_value().unwrap().unwrap();
        assert!(matches!(v, RespValue::SimpleString(s) if s == "OK"));
    }

    #[test]
    fn parse_array() {
        let mut p = RespParser::new();
        p.feed(b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n");
        if let Some(RespValue::Array(Some(items))) = p.take_value().unwrap() {
            assert_eq!(items.len(), 2);
        } else {
            panic!("expected array");
        }
    }

    #[test]
    fn serialize_bulk() {
        let v = RespValue::bulk(b"hello".to_vec());
        assert_eq!(v.to_bytes(), b"$5\r\nhello\r\n");
    }

    #[test]
    fn parse_pipelined() {
        let mut p = RespParser::new();
        p.feed(b"+PONG\r\n+OK\r\n");
        assert!(p.take_value().unwrap().is_some());
        assert!(p.take_value().unwrap().is_some());
        assert!(p.take_value().unwrap().is_none());
    }
}
