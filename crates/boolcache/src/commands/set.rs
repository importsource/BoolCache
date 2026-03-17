use std::collections::HashSet;

use crate::db::Db;
use crate::error::{Error, Result};
use crate::types::{Entry, Value};

fn ensure_set<'a>(
    store: &'a mut crate::store::Store,
    key: &str,
) -> Result<&'a mut HashSet<Vec<u8>>> {
    let entry = store.data.entry(key.to_string()).or_insert_with(|| {
        Entry::new(Value::Set(HashSet::new()))
    });
    if entry.is_expired() {
        *entry = Entry::new(Value::Set(HashSet::new()));
    }
    match &mut entry.value {
        Value::Set(s) => Ok(s),
        _ => Err(Error::WrongType),
    }
}

pub fn sadd(db: &Db, key: &str, members: Vec<Vec<u8>>) -> Result<usize> {
    let mut aof = vec![b"SADD".to_vec(), key.as_bytes().to_vec()];
    aof.extend(members.iter().cloned());
    let mut store = db.store.write();
    let s = ensure_set(&mut store, key)?;
    let added = members.into_iter().filter(|m| s.insert(m.clone())).count();
    store.dirty += 1;
    drop(store);
    if added > 0 { db.aof_write(aof); }
    Ok(added)
}

pub fn srem(db: &Db, key: &str, members: Vec<Vec<u8>>) -> Result<usize> {
    let mut aof = vec![b"SREM".to_vec(), key.as_bytes().to_vec()];
    aof.extend(members.iter().cloned());
    let mut store = db.store.write();
    match store.data.get_mut(key) {
        None => return Ok(0),
        Some(e) if e.is_expired() => return Ok(0),
        Some(e) => match &mut e.value {
            Value::Set(s) => {
                let removed = members.iter().filter(|m| s.remove(m.as_slice())).count();
                if s.is_empty() { store.data.remove(key); }
                store.dirty += 1;
                drop(store);
                if removed > 0 { db.aof_write(aof); }
                Ok(removed)
            }
            _ => Err(Error::WrongType),
        },
    }
}

pub fn smembers(db: &Db, key: &str) -> Result<Vec<Vec<u8>>> {
    let store = db.store.read();
    match store.get_entry(key) {
        None => Ok(vec![]),
        Some(e) => match &e.value {
            Value::Set(s) => Ok(s.iter().cloned().collect()),
            _ => Err(Error::WrongType),
        },
    }
}

pub fn sismember(db: &Db, key: &str, member: &[u8]) -> Result<bool> {
    let store = db.store.read();
    match store.get_entry(key) {
        None => Ok(false),
        Some(e) => match &e.value {
            Value::Set(s) => Ok(s.contains(member)),
            _ => Err(Error::WrongType),
        },
    }
}

pub fn scard(db: &Db, key: &str) -> Result<usize> {
    let store = db.store.read();
    match store.get_entry(key) {
        None => Ok(0),
        Some(e) => match &e.value {
            Value::Set(s) => Ok(s.len()),
            _ => Err(Error::WrongType),
        },
    }
}

fn read_set(store: &crate::store::Store, key: &str) -> Result<HashSet<Vec<u8>>> {
    match store.get_entry(key) {
        None => Ok(HashSet::new()),
        Some(e) => match &e.value {
            Value::Set(s) => Ok(s.clone()),
            _ => Err(Error::WrongType),
        },
    }
}

pub fn sinter(db: &Db, keys: &[&str]) -> Result<Vec<Vec<u8>>> {
    let store = db.store.read();
    if keys.is_empty() { return Ok(vec![]); }
    let mut result = read_set(&store, keys[0])?;
    for &k in &keys[1..] {
        let other = read_set(&store, k)?;
        result.retain(|m| other.contains(m));
    }
    Ok(result.into_iter().collect())
}

pub fn sunion(db: &Db, keys: &[&str]) -> Result<Vec<Vec<u8>>> {
    let store = db.store.read();
    let mut result = HashSet::new();
    for &k in keys { result.extend(read_set(&store, k)?); }
    Ok(result.into_iter().collect())
}

pub fn sdiff(db: &Db, keys: &[&str]) -> Result<Vec<Vec<u8>>> {
    let store = db.store.read();
    if keys.is_empty() { return Ok(vec![]); }
    let mut result = read_set(&store, keys[0])?;
    for &k in &keys[1..] {
        let other = read_set(&store, k)?;
        result.retain(|m| !other.contains(m));
    }
    Ok(result.into_iter().collect())
}

pub fn smove(db: &Db, src: &str, dst: &str, member: Vec<u8>) -> Result<bool> {
    let mut store = db.store.write();
    let exists = match store.get_entry(src) {
        None => false,
        Some(e) => match &e.value {
            Value::Set(s) => s.contains(member.as_slice()),
            _ => return Err(Error::WrongType),
        },
    };
    if !exists { return Ok(false); }
    if let Some(e) = store.data.get_mut(src) {
        if let Value::Set(s) = &mut e.value {
            s.remove(member.as_slice());
            if s.is_empty() { store.data.remove(src); }
        }
    }
    let dst_entry = store.data.entry(dst.to_string()).or_insert_with(|| {
        Entry::new(Value::Set(HashSet::new()))
    });
    match &mut dst_entry.value {
        Value::Set(s) => { s.insert(member.clone()); }
        _ => return Err(Error::WrongType),
    }
    store.dirty += 1;
    drop(store);
    db.aof_write(vec![
        b"SMOVE".to_vec(),
        src.as_bytes().to_vec(),
        dst.as_bytes().to_vec(),
        member,
    ]);
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn db() -> Db { Db::open_memory() }

    #[test]
    fn add_members() {
        let db = db();
        sadd(&db, "s", vec![b"a".to_vec(), b"b".to_vec(), b"a".to_vec()]).unwrap();
        assert_eq!(scard(&db, "s").unwrap(), 2);
        assert!(sismember(&db, "s", b"a").unwrap());
    }

    #[test]
    fn set_ops() {
        let db = db();
        sadd(&db, "s1", vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]).unwrap();
        sadd(&db, "s2", vec![b"b".to_vec(), b"c".to_vec(), b"d".to_vec()]).unwrap();
        let mut inter = sinter(&db, &["s1", "s2"]).unwrap();
        inter.sort();
        assert_eq!(inter, vec![b"b", b"c"]);
        let mut diff = sdiff(&db, &["s1", "s2"]).unwrap();
        diff.sort();
        assert_eq!(diff, vec![b"a"]);
    }
}
