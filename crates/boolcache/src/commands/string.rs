use std::time::Duration;

use crate::db::Db;
use crate::error::{Error, Result};
use crate::types::{Entry, Value};

// ── SET options ───────────────────────────────────────────────────────────────

#[derive(Default)]
pub struct SetOptions {
    pub ttl: Option<Duration>,
    pub nx: bool,
    pub xx: bool,
    pub get: bool,
}

// ── SET / GET ─────────────────────────────────────────────────────────────────

pub fn set(db: &Db, key: &str, value: Vec<u8>, opts: SetOptions) -> Result<Option<Vec<u8>>> {
    let mut store = db.store.write();

    let exists = store.get_entry(key).is_some();
    if opts.nx && exists { return Ok(None); }
    if opts.xx && !exists { return Ok(None); }

    let old = if opts.get {
        match store.get_entry(key) {
            Some(e) => match &e.value {
                Value::String(v) => Some(v.clone()),
                _ => return Err(Error::WrongType),
            },
            None => None,
        }
    } else {
        None
    };

    // Build AOF command before moving `value`
    let mut aof: Vec<Vec<u8>> = vec![b"SET".to_vec(), key.as_bytes().to_vec(), value.clone()];
    if let Some(ttl) = opts.ttl {
        aof.push(b"PX".to_vec());
        aof.push(ttl.as_millis().to_string().into_bytes());
    }

    let entry = match opts.ttl {
        Some(d) => Entry::with_expiry(Value::String(value), d),
        None => Entry::new(Value::String(value)),
    };
    store.data.insert(key.to_string(), entry);
    store.dirty += 1;
    drop(store);

    db.aof_write(aof);
    Ok(old)
}

pub fn get(db: &Db, key: &str) -> Result<Option<Vec<u8>>> {
    let store = db.store.read();
    match store.get_entry(key) {
        None => Ok(None),
        Some(e) => match &e.value {
            Value::String(v) => Ok(Some(v.clone())),
            _ => Err(Error::WrongType),
        },
    }
}

pub fn getset(db: &Db, key: &str, new_val: Vec<u8>) -> Result<Option<Vec<u8>>> {
    let mut store = db.store.write();
    let old = match store.get_entry(key) {
        None => None,
        Some(e) => match &e.value {
            Value::String(v) => Some(v.clone()),
            _ => return Err(Error::WrongType),
        },
    };
    let aof_val = new_val.clone();
    store.data.insert(key.to_string(), Entry::new(Value::String(new_val)));
    store.dirty += 1;
    drop(store);
    db.aof_write(vec![b"SET".to_vec(), key.as_bytes().to_vec(), aof_val]);
    Ok(old)
}

pub fn mget(db: &Db, keys: &[&str]) -> Vec<Option<Vec<u8>>> {
    let store = db.store.read();
    keys.iter()
        .map(|k| match store.get_entry(k) {
            None => None,
            Some(e) => match &e.value {
                Value::String(v) => Some(v.clone()),
                _ => None,
            },
        })
        .collect()
}

pub fn mset(db: &Db, pairs: Vec<(String, Vec<u8>)>) {
    let mut aof: Vec<Vec<u8>> = vec![b"MSET".to_vec()];
    for (k, v) in &pairs {
        aof.push(k.as_bytes().to_vec());
        aof.push(v.clone());
    }
    let mut store = db.store.write();
    for (k, v) in &pairs {
        store.data.insert(k.clone(), Entry::new(Value::String(v.clone())));
    }
    store.dirty += pairs.len() as u64;
    drop(store);
    db.aof_write(aof);
}

pub fn setnx(db: &Db, key: &str, value: Vec<u8>) -> bool {
    let mut store = db.store.write();
    if store.get_entry(key).is_some() { return false; }
    let aof_val = value.clone();
    store.data.insert(key.to_string(), Entry::new(Value::String(value)));
    store.dirty += 1;
    drop(store);
    db.aof_write(vec![b"SET".to_vec(), key.as_bytes().to_vec(), aof_val]);
    true
}

pub fn setex(db: &Db, key: &str, value: Vec<u8>, ttl: Duration) {
    let aof_val = value.clone();
    let ms = ttl.as_millis().to_string().into_bytes();
    let mut store = db.store.write();
    store.data.insert(key.to_string(), Entry::with_expiry(Value::String(value), ttl));
    store.dirty += 1;
    drop(store);
    db.aof_write(vec![b"SET".to_vec(), key.as_bytes().to_vec(), aof_val, b"PX".to_vec(), ms]);
}

// ── numeric ops ───────────────────────────────────────────────────────────────

pub fn incr_by(db: &Db, key: &str, delta: i64) -> Result<i64> {
    let mut store = db.store.write();
    let val = match store.get_entry(key) {
        None => 0i64,
        Some(e) => match &e.value {
            Value::String(v) => std::str::from_utf8(v)
                .ok()
                .and_then(|s| s.trim().parse::<i64>().ok())
                .ok_or(Error::NotInteger)?,
            _ => return Err(Error::WrongType),
        },
    };
    let new_val = val.checked_add(delta).ok_or_else(|| Error::Other("overflow".into()))?;
    let bytes = new_val.to_string().into_bytes();
    let expires_ms = store.get_entry(key).and_then(|e| e.expires_ms);
    store.data.insert(key.to_string(), Entry { value: Value::String(bytes.clone()), expires_ms });
    store.dirty += 1;
    drop(store);
    // Log as SET so replay is always idempotent regardless of prior state
    db.aof_write(vec![b"SET".to_vec(), key.as_bytes().to_vec(), bytes]);
    Ok(new_val)
}

pub fn incr(db: &Db, key: &str) -> Result<i64> { incr_by(db, key, 1) }
pub fn decr(db: &Db, key: &str) -> Result<i64> { incr_by(db, key, -1) }
pub fn decr_by(db: &Db, key: &str, delta: i64) -> Result<i64> { incr_by(db, key, -delta) }

// ── string ops ────────────────────────────────────────────────────────────────

pub fn append(db: &Db, key: &str, extra: &[u8]) -> Result<usize> {
    let mut store = db.store.write();
    let entry = store.data.entry(key.to_string()).or_insert_with(|| {
        Entry::new(Value::String(Vec::new()))
    });
    if entry.is_expired() {
        *entry = Entry::new(Value::String(Vec::new()));
    }
    match &mut entry.value {
        Value::String(v) => {
            v.extend_from_slice(extra);
            let full = v.clone();
            let len = full.len();
            store.dirty += 1;
            drop(store);
            // Log the resulting full value as SET to keep replay simple
            db.aof_write(vec![b"SET".to_vec(), key.as_bytes().to_vec(), full]);
            Ok(len)
        }
        _ => Err(Error::WrongType),
    }
}

pub fn strlen(db: &Db, key: &str) -> Result<usize> {
    let store = db.store.read();
    match store.get_entry(key) {
        None => Ok(0),
        Some(e) => match &e.value {
            Value::String(v) => Ok(v.len()),
            _ => Err(Error::WrongType),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mem_db() -> Db { Db::open_memory() }

    #[test]
    fn set_get() {
        let db = mem_db();
        set(&db, "k", b"hello".to_vec(), SetOptions::default()).unwrap();
        assert_eq!(get(&db, "k").unwrap(), Some(b"hello".to_vec()));
    }

    #[test]
    fn incr_decr() {
        let db = mem_db();
        set(&db, "n", b"10".to_vec(), SetOptions::default()).unwrap();
        assert_eq!(incr(&db, "n").unwrap(), 11);
        assert_eq!(decr_by(&db, "n", 5).unwrap(), 6);
    }

    #[test]
    fn append_strlen() {
        let db = mem_db();
        set(&db, "s", b"hello".to_vec(), SetOptions::default()).unwrap();
        append(&db, "s", b" world").unwrap();
        assert_eq!(strlen(&db, "s").unwrap(), 11);
    }

    #[test]
    fn setnx_nx() {
        let db = mem_db();
        assert!(setnx(&db, "k", b"v1".to_vec()));
        assert!(!setnx(&db, "k", b"v2".to_vec()));
        assert_eq!(get(&db, "k").unwrap(), Some(b"v1".to_vec()));
    }
}
