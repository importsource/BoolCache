//! AOF (Append-Only File) persistence.
//!
//! Each write command is serialized as a RESP array and appended to the log.
//! On startup the file is replayed by re-executing each command against the store.

use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::Path;

use crate::error::{Error, Result};
use crate::store::Store;
use crate::types::{Entry, Value};

// ── fsync policy ──────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub enum FsyncPolicy {
    /// fsync after every write (safest, slowest).
    Always,
    /// fsync once per second via background thread (default).
    EverySec,
    /// Never fsync explicitly (fastest, least safe).
    No,
}

// ── writer ────────────────────────────────────────────────────────────────────

pub struct AofWriter {
    inner: BufWriter<File>,
    policy: FsyncPolicy,
}

impl AofWriter {
    pub fn open(path: &Path, policy: FsyncPolicy) -> Result<Self> {
        let file = OpenOptions::new().create(true).append(true).open(path)?;
        Ok(AofWriter { inner: BufWriter::new(file), policy })
    }

    /// Append a command represented as raw byte slices.
    pub fn append(&mut self, args: &[&[u8]]) -> Result<()> {
        // RESP array: *<count>\r\n  then for each arg: $<len>\r\n<data>\r\n
        write!(self.inner, "*{}\r\n", args.len())?;
        for arg in args {
            write!(self.inner, "${}\r\n", arg.len())?;
            self.inner.write_all(arg)?;
            self.inner.write_all(b"\r\n")?;
        }
        if matches!(self.policy, FsyncPolicy::Always) {
            self.inner.flush()?;
            self.inner.get_ref().sync_data()?;
        }
        Ok(())
    }

    pub fn flush(&mut self) -> Result<()> {
        self.inner.flush().map_err(Into::into)
    }

    pub fn fsync(&mut self) -> Result<()> {
        self.inner.flush()?;
        self.inner.get_ref().sync_data().map_err(Into::into)
    }
}

// ── replay ────────────────────────────────────────────────────────────────────

/// Replay an AOF file into a `Store`. Returns the number of commands executed.
pub fn replay(path: &Path, store: &mut Store) -> Result<usize> {
    let file = File::open(path)?;
    let mut reader = BufReader::new(file);
    let mut count = 0usize;

    loop {
        // Read array header: *<n>\r\n
        let mut line = String::new();
        let n = reader.read_line(&mut line)?;
        if n == 0 { break; } // EOF
        let line = line.trim_end_matches(|c| c == '\r' || c == '\n');
        if line.is_empty() { continue; }
        if !line.starts_with('*') {
            return Err(Error::Other(format!("AOF: expected '*', got: {line}")));
        }
        let argc: usize = line[1..].parse().map_err(|_| Error::Other("AOF: bad argc".into()))?;

        let mut args: Vec<Vec<u8>> = Vec::with_capacity(argc);
        for _ in 0..argc {
            // $<len>\r\n
            let mut len_line = String::new();
            reader.read_line(&mut len_line)?;
            let len_line = len_line.trim_end_matches(|c| c == '\r' || c == '\n');
            if !len_line.starts_with('$') {
                return Err(Error::Other(format!("AOF: expected '$', got: {len_line}")));
            }
            let arg_len: usize = len_line[1..].parse()
                .map_err(|_| Error::Other("AOF: bad arg len".into()))?;
            let mut buf = vec![0u8; arg_len + 2]; // +2 for \r\n
            use std::io::Read;
            reader.read_exact(&mut buf)?;
            buf.truncate(arg_len);
            args.push(buf);
        }

        apply_command(store, &args);
        count += 1;
    }
    Ok(count)
}

/// Re-apply a single AOF command to the store (best-effort, ignores errors).
/// Also called by the replica when applying commands received from the primary.
pub fn apply_command(store: &mut Store, args: &[Vec<u8>]) {
    if args.is_empty() { return; }
    let cmd = args[0].to_ascii_uppercase();
    match cmd.as_slice() {
        // ── string ────────────────────────────────────────────────────────────
        b"SET" => {
            if args.len() >= 3 {
                let key = str_arg(&args[1]);
                let val = args[2].clone();
                // Parse optional PX / EX
                let mut expires_ms: Option<u64> = None;
                let mut i = 3;
                while i < args.len() {
                    match args[i].to_ascii_uppercase().as_slice() {
                        b"PX" if i + 1 < args.len() => {
                            if let Some(ms) = parse_u64(&args[i + 1]) {
                                expires_ms = Some(crate::types::unix_ms_now() + ms);
                            }
                            i += 2;
                        }
                        b"EX" if i + 1 < args.len() => {
                            if let Some(s) = parse_u64(&args[i + 1]) {
                                expires_ms = Some(crate::types::unix_ms_now() + s * 1000);
                            }
                            i += 2;
                        }
                        _ => { i += 1; }
                    }
                }
                store.data.insert(key, Entry { value: Value::String(val), expires_ms });
            }
        }
        b"MSET" => {
            let mut i = 1;
            while i + 1 < args.len() {
                store.data.insert(str_arg(&args[i]), Entry::new(Value::String(args[i + 1].clone())));
                i += 2;
            }
        }
        // ── key management ────────────────────────────────────────────────────
        b"DEL" => {
            for key in &args[1..] { store.data.remove(&str_arg(key)); }
        }
        b"RENAME" => {
            if args.len() >= 3 {
                let _ = store.rename(&str_arg(&args[1]), &str_arg(&args[2]));
            }
        }
        b"EXPIRE" => {
            if args.len() >= 3 {
                if let Some(s) = parse_u64(&args[2]) {
                    store.pexpire(&str_arg(&args[1]), s * 1000);
                }
            }
        }
        b"PEXPIRE" => {
            if args.len() >= 3 {
                if let Some(ms) = parse_u64(&args[2]) {
                    store.pexpire(&str_arg(&args[1]), ms);
                }
            }
        }
        b"PERSIST" => {
            if args.len() >= 2 { store.persist(&str_arg(&args[1])); }
        }
        b"FLUSHDB" | b"FLUSHALL" => { store.flush(); }
        // ── list ─────────────────────────────────────────────────────────────
        b"LPUSH" => {
            if args.len() >= 3 {
                let key = str_arg(&args[1]);
                let entry = store.data.entry(key).or_insert_with(|| {
                    Entry::new(Value::List(std::collections::VecDeque::new()))
                });
                if let Value::List(l) = &mut entry.value {
                    for v in args[2..].iter().rev() { l.push_front(v.clone()); }
                }
            }
        }
        b"RPUSH" => {
            if args.len() >= 3 {
                let key = str_arg(&args[1]);
                let entry = store.data.entry(key).or_insert_with(|| {
                    Entry::new(Value::List(std::collections::VecDeque::new()))
                });
                if let Value::List(l) = &mut entry.value {
                    for v in &args[2..] { l.push_back(v.clone()); }
                }
            }
        }
        b"LPOP" => {
            if args.len() >= 2 {
                let key = str_arg(&args[1]);
                let n = args.get(2).and_then(|a| parse_usize(a)).unwrap_or(1);
                if let Some(e) = store.data.get_mut(&key) {
                    if let Value::List(l) = &mut e.value {
                        for _ in 0..n { l.pop_front(); }
                        if l.is_empty() { store.data.remove(&key); }
                    }
                }
            }
        }
        b"RPOP" => {
            if args.len() >= 2 {
                let key = str_arg(&args[1]);
                let n = args.get(2).and_then(|a| parse_usize(a)).unwrap_or(1);
                if let Some(e) = store.data.get_mut(&key) {
                    if let Value::List(l) = &mut e.value {
                        for _ in 0..n { l.pop_back(); }
                        if l.is_empty() { store.data.remove(&key); }
                    }
                }
            }
        }
        b"LSET" => {
            if args.len() >= 4 {
                let key = str_arg(&args[1]);
                if let Some(idx) = parse_i64(&args[2]) {
                    if let Some(e) = store.data.get_mut(&key) {
                        if let Value::List(l) = &mut e.value {
                            let i = crate::types::resolve_index(idx, l.len() as i64);
                            if i < l.len() { l[i] = args[3].clone(); }
                        }
                    }
                }
            }
        }
        b"LTRIM" => {
            if args.len() >= 4 {
                let key = str_arg(&args[1]);
                if let (Some(start), Some(stop)) = (parse_i64(&args[2]), parse_i64(&args[3])) {
                    if let Some(e) = store.data.get_mut(&key) {
                        if let Value::List(l) = &mut e.value {
                            let len = l.len() as i64;
                            let s = crate::types::resolve_index(start, len);
                            let end = crate::types::resolve_index(stop, len);
                            if s > end || s >= l.len() {
                                *l = std::collections::VecDeque::new();
                            } else {
                                *l = l.drain(s..=end.min(l.len() - 1)).collect();
                            }
                        }
                    }
                }
            }
        }
        b"LINSERT" => {
            if args.len() >= 5 {
                let key = str_arg(&args[1]);
                let before = args[2].to_ascii_uppercase() == b"BEFORE";
                let pivot = &args[3];
                let value = args[4].clone();
                if let Some(e) = store.data.get_mut(&key) {
                    if let Value::List(l) = &mut e.value {
                        if let Some(pos) = l.iter().position(|x| x == pivot) {
                            let insert_at = if before { pos } else { pos + 1 };
                            l.insert(insert_at, value);
                        }
                    }
                }
            }
        }
        // ── hash ─────────────────────────────────────────────────────────────
        b"HSET" | b"HMSET" => {
            if args.len() >= 4 {
                let key = str_arg(&args[1]);
                let entry = store.data.entry(key).or_insert_with(|| {
                    Entry::new(Value::Hash(std::collections::HashMap::new()))
                });
                if let Value::Hash(h) = &mut entry.value {
                    let mut i = 2;
                    while i + 1 < args.len() {
                        h.insert(args[i].clone(), args[i + 1].clone());
                        i += 2;
                    }
                }
            }
        }
        b"HDEL" => {
            if args.len() >= 3 {
                let key = str_arg(&args[1]);
                if let Some(e) = store.data.get_mut(&key) {
                    if let Value::Hash(h) = &mut e.value {
                        for f in &args[2..] { h.remove(f.as_slice()); }
                        if h.is_empty() { store.data.remove(&key); }
                    }
                }
            }
        }
        // ── set ───────────────────────────────────────────────────────────────
        b"SADD" => {
            if args.len() >= 3 {
                let key = str_arg(&args[1]);
                let entry = store.data.entry(key).or_insert_with(|| {
                    Entry::new(Value::Set(std::collections::HashSet::new()))
                });
                if let Value::Set(s) = &mut entry.value {
                    for m in &args[2..] { s.insert(m.clone()); }
                }
            }
        }
        b"SREM" => {
            if args.len() >= 3 {
                let key = str_arg(&args[1]);
                if let Some(e) = store.data.get_mut(&key) {
                    if let Value::Set(s) = &mut e.value {
                        for m in &args[2..] { s.remove(m.as_slice()); }
                        if s.is_empty() { store.data.remove(&key); }
                    }
                }
            }
        }
        b"SMOVE" => {
            if args.len() >= 4 {
                let src = str_arg(&args[1]);
                let dst = str_arg(&args[2]);
                let member = args[3].clone();
                let removed = store.data.get_mut(&src).and_then(|e| {
                    if let Value::Set(s) = &mut e.value { Some(s.remove(member.as_slice())) }
                    else { None }
                }).unwrap_or(false);
                if removed {
                    if let Some(e) = store.data.get(&src) {
                        if let Value::Set(s) = &e.value { if s.is_empty() { store.data.remove(&src); } }
                    }
                    let dst_entry = store.data.entry(dst).or_insert_with(|| {
                        Entry::new(Value::Set(std::collections::HashSet::new()))
                    });
                    if let Value::Set(s) = &mut dst_entry.value { s.insert(member); }
                }
            }
        }
        // ── sorted set ────────────────────────────────────────────────────────
        b"ZADD" => {
            if args.len() >= 4 {
                let key = str_arg(&args[1]);
                let entry = store.data.entry(key).or_insert_with(|| {
                    Entry::new(Value::ZSet(crate::types::ZSet::new()))
                });
                if let Value::ZSet(z) = &mut entry.value {
                    let mut i = 2;
                    while i + 1 < args.len() {
                        if let Some(score) = parse_f64(&args[i]) {
                            z.add(args[i + 1].clone(), score);
                        }
                        i += 2;
                    }
                }
            }
        }
        b"ZREM" => {
            if args.len() >= 3 {
                let key = str_arg(&args[1]);
                if let Some(e) = store.data.get_mut(&key) {
                    if let Value::ZSet(z) = &mut e.value {
                        for m in &args[2..] { z.remove(m.as_slice()); }
                        if z.is_empty() { store.data.remove(&key); }
                    }
                }
            }
        }
        _ => {} // unknown or read-only command; skip
    }
}

fn parse_u64(b: &[u8]) -> Option<u64> {
    std::str::from_utf8(b).ok()?.trim().parse().ok()
}

fn parse_i64(b: &[u8]) -> Option<i64> {
    std::str::from_utf8(b).ok()?.trim().parse().ok()
}

fn parse_usize(b: &[u8]) -> Option<usize> {
    std::str::from_utf8(b).ok()?.trim().parse().ok()
}

fn parse_f64(b: &[u8]) -> Option<f64> {
    std::str::from_utf8(b).ok()?.trim().parse().ok()
}

// ── AOF rewrite ───────────────────────────────────────────────────────────────

/// Write a minimal AOF representation of the current store state to `path`.
pub fn rewrite(path: &Path, store: &Store) -> Result<()> {
    let file = File::create(path)?;
    let mut w = BufWriter::new(file);

    for (key, entry) in &store.data {
        if entry.is_expired() { continue; }
        match &entry.value {
            Value::String(v) => {
                write_resp_array(&mut w, &[b"SET", key.as_bytes(), v])?;
            }
            Value::List(l) => {
                for item in l {
                    write_resp_array(&mut w, &[b"RPUSH", key.as_bytes(), item])?;
                }
            }
            Value::Hash(h) => {
                for (field, val) in h {
                    write_resp_array(&mut w, &[b"HSET", key.as_bytes(), field, val])?;
                }
            }
            Value::Set(s) => {
                for member in s {
                    write_resp_array(&mut w, &[b"SADD", key.as_bytes(), member])?;
                }
            }
            Value::ZSet(z) => {
                for (member, &score) in &z.member_scores {
                    let score_str = score.to_string();
                    write_resp_array(
                        &mut w,
                        &[b"ZADD", key.as_bytes(), score_str.as_bytes(), member],
                    )?;
                }
            }
        }
        // Restore expiry
        if let Some(exp_ms) = entry.expires_ms {
            let now_ms = crate::types::unix_ms_now();
            if exp_ms > now_ms {
                let remaining = exp_ms - now_ms;
                let ms_str = remaining.to_string();
                write_resp_array(&mut w, &[b"PEXPIRE", key.as_bytes(), ms_str.as_bytes()])?;
            }
        }
    }
    w.flush()?;
    Ok(())
}

fn write_resp_array(w: &mut impl Write, args: &[&[u8]]) -> Result<()> {
    write!(w, "*{}\r\n", args.len())?;
    for arg in args {
        write!(w, "${}\r\n", arg.len())?;
        w.write_all(arg)?;
        w.write_all(b"\r\n")?;
    }
    Ok(())
}

fn str_arg(b: &[u8]) -> String {
    String::from_utf8_lossy(b).into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{Entry, Value};

    #[test]
    fn aof_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.aof");

        {
            let mut w = AofWriter::open(&path, FsyncPolicy::Always).unwrap();
            w.append(&[b"SET", b"foo", b"bar"]).unwrap();
            w.append(&[b"RPUSH", b"mylist", b"a", b"b"]).unwrap();
        }

        let mut store = Store::new();
        let count = replay(&path, &mut store).unwrap();
        assert_eq!(count, 2);
        assert!(store.data.contains_key("foo"));
    }

    #[test]
    fn aof_rewrite_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("rewrite.aof");

        let mut store = Store::new();
        store.data.insert("k".into(), Entry::new(Value::String(b"v".to_vec())));

        rewrite(&path, &store).unwrap();

        let mut store2 = Store::new();
        let count = replay(&path, &mut store2).unwrap();
        assert_eq!(count, 1);
        assert!(store2.data.contains_key("k"));
    }
}
