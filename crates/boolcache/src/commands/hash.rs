use std::collections::HashMap;

use crate::db::Db;
use crate::error::{Error, Result};
use crate::types::{Entry, Value};

fn ensure_hash<'a>(
    store: &'a mut crate::store::Store,
    key: &str,
) -> Result<&'a mut HashMap<Vec<u8>, Vec<u8>>> {
    let entry = store.data.entry(key.to_string()).or_insert_with(|| {
        Entry::new(Value::Hash(HashMap::new()))
    });
    if entry.is_expired() {
        *entry = Entry::new(Value::Hash(HashMap::new()));
    }
    match &mut entry.value {
        Value::Hash(h) => Ok(h),
        _ => Err(Error::WrongType),
    }
}

pub fn hset(db: &Db, key: &str, pairs: Vec<(Vec<u8>, Vec<u8>)>) -> Result<usize> {
    let mut aof = vec![b"HSET".to_vec(), key.as_bytes().to_vec()];
    for (f, v) in &pairs {
        aof.push(f.clone());
        aof.push(v.clone());
    }
    let mut store = db.store.write();
    let h = ensure_hash(&mut store, key)?;
    let mut added = 0usize;
    for (field, val) in pairs {
        if h.insert(field, val).is_none() { added += 1; }
    }
    store.dirty += 1;
    drop(store);
    db.aof_write(aof);
    Ok(added)
}

pub fn hget(db: &Db, key: &str, field: &[u8]) -> Result<Option<Vec<u8>>> {
    let store = db.store.read();
    match store.get_entry(key) {
        None => Ok(None),
        Some(e) => match &e.value {
            Value::Hash(h) => Ok(h.get(field).cloned()),
            _ => Err(Error::WrongType),
        },
    }
}

pub fn hmget(db: &Db, key: &str, fields: &[Vec<u8>]) -> Result<Vec<Option<Vec<u8>>>> {
    let store = db.store.read();
    match store.get_entry(key) {
        None => Ok(fields.iter().map(|_| None).collect()),
        Some(e) => match &e.value {
            Value::Hash(h) => Ok(fields.iter().map(|f| h.get(f.as_slice()).cloned()).collect()),
            _ => Err(Error::WrongType),
        },
    }
}

pub fn hdel(db: &Db, key: &str, fields: &[Vec<u8>]) -> Result<usize> {
    let mut aof = vec![b"HDEL".to_vec(), key.as_bytes().to_vec()];
    aof.extend(fields.iter().cloned());
    let mut store = db.store.write();
    match store.data.get_mut(key) {
        None => return Ok(0),
        Some(e) if e.is_expired() => return Ok(0),
        Some(e) => match &mut e.value {
            Value::Hash(h) => {
                let removed = fields.iter().filter(|f| h.remove(f.as_slice()).is_some()).count();
                if h.is_empty() { store.data.remove(key); }
                store.dirty += 1;
                drop(store);
                if removed > 0 { db.aof_write(aof); }
                Ok(removed)
            }
            _ => Err(Error::WrongType),
        },
    }
}

pub fn hexists(db: &Db, key: &str, field: &[u8]) -> Result<bool> {
    let store = db.store.read();
    match store.get_entry(key) {
        None => Ok(false),
        Some(e) => match &e.value {
            Value::Hash(h) => Ok(h.contains_key(field)),
            _ => Err(Error::WrongType),
        },
    }
}

pub fn hgetall(db: &Db, key: &str) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
    let store = db.store.read();
    match store.get_entry(key) {
        None => Ok(vec![]),
        Some(e) => match &e.value {
            Value::Hash(h) => Ok(h.iter().map(|(k, v)| (k.clone(), v.clone())).collect()),
            _ => Err(Error::WrongType),
        },
    }
}

pub fn hkeys(db: &Db, key: &str) -> Result<Vec<Vec<u8>>> {
    let store = db.store.read();
    match store.get_entry(key) {
        None => Ok(vec![]),
        Some(e) => match &e.value {
            Value::Hash(h) => Ok(h.keys().cloned().collect()),
            _ => Err(Error::WrongType),
        },
    }
}

pub fn hvals(db: &Db, key: &str) -> Result<Vec<Vec<u8>>> {
    let store = db.store.read();
    match store.get_entry(key) {
        None => Ok(vec![]),
        Some(e) => match &e.value {
            Value::Hash(h) => Ok(h.values().cloned().collect()),
            _ => Err(Error::WrongType),
        },
    }
}

pub fn hlen(db: &Db, key: &str) -> Result<usize> {
    let store = db.store.read();
    match store.get_entry(key) {
        None => Ok(0),
        Some(e) => match &e.value {
            Value::Hash(h) => Ok(h.len()),
            _ => Err(Error::WrongType),
        },
    }
}

pub fn hincrby(db: &Db, key: &str, field: Vec<u8>, delta: i64) -> Result<i64> {
    let mut store = db.store.write();
    let h = ensure_hash(&mut store, key)?;
    let cur: i64 = match h.get(&field) {
        None => 0,
        Some(v) => std::str::from_utf8(v)
            .ok()
            .and_then(|s| s.trim().parse().ok())
            .ok_or(Error::NotInteger)?,
    };
    let new_val = cur.checked_add(delta).ok_or_else(|| Error::Other("overflow".into()))?;
    let result_bytes = new_val.to_string().into_bytes();
    h.insert(field.clone(), result_bytes.clone());
    store.dirty += 1;
    drop(store);
    // Log as HSET with the resulting value so replay is idempotent
    db.aof_write(vec![b"HSET".to_vec(), key.as_bytes().to_vec(), field, result_bytes]);
    Ok(new_val)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn db() -> Db { Db::open_memory() }

    #[test]
    fn hset_get_del() {
        let db = db();
        hset(&db, "h", vec![
            (b"f1".to_vec(), b"v1".to_vec()),
            (b"f2".to_vec(), b"v2".to_vec()),
        ]).unwrap();
        assert_eq!(hget(&db, "h", b"f1").unwrap(), Some(b"v1".to_vec()));
        assert_eq!(hlen(&db, "h").unwrap(), 2);
        hdel(&db, "h", &[b"f1".to_vec()]).unwrap();
        assert_eq!(hlen(&db, "h").unwrap(), 1);
    }

    #[test]
    fn hincrby_test() {
        let db = db();
        hset(&db, "h", vec![(b"count".to_vec(), b"10".to_vec())]).unwrap();
        assert_eq!(hincrby(&db, "h", b"count".to_vec(), 5).unwrap(), 15);
    }
}
