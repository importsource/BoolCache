use std::collections::VecDeque;

use crate::db::Db;
use crate::error::{Error, Result};
use crate::types::{Entry, Value, resolve_index};

fn ensure_list<'a>(
    store: &'a mut crate::store::Store,
    key: &str,
) -> Result<&'a mut VecDeque<Vec<u8>>> {
    let entry = store.data.entry(key.to_string()).or_insert_with(|| {
        Entry::new(Value::List(VecDeque::new()))
    });
    if entry.is_expired() {
        *entry = Entry::new(Value::List(VecDeque::new()));
    }
    match &mut entry.value {
        Value::List(l) => Ok(l),
        _ => Err(Error::WrongType),
    }
}

pub fn lpush(db: &Db, key: &str, values: Vec<Vec<u8>>) -> Result<usize> {
    let mut aof = vec![b"LPUSH".to_vec(), key.as_bytes().to_vec()];
    aof.extend(values.iter().cloned());
    let mut store = db.store.write();
    let list = ensure_list(&mut store, key)?;
    for v in values {
        list.push_front(v);
    }
    let len = list.len();
    store.dirty += 1;
    drop(store);
    db.aof_write(aof);
    Ok(len)
}

pub fn rpush(db: &Db, key: &str, values: Vec<Vec<u8>>) -> Result<usize> {
    let mut aof = vec![b"RPUSH".to_vec(), key.as_bytes().to_vec()];
    aof.extend(values.iter().cloned());
    let mut store = db.store.write();
    let list = ensure_list(&mut store, key)?;
    for v in values {
        list.push_back(v);
    }
    let len = list.len();
    store.dirty += 1;
    drop(store);
    db.aof_write(aof);
    Ok(len)
}

pub fn lpop(db: &Db, key: &str, count: usize) -> Result<Vec<Vec<u8>>> {
    let mut store = db.store.write();
    match store.data.get_mut(key) {
        None => return Ok(vec![]),
        Some(e) if e.is_expired() => return Ok(vec![]),
        Some(e) => match &mut e.value {
            Value::List(l) => {
                let n = count.min(l.len());
                let result: Vec<Vec<u8>> = (0..n).filter_map(|_| l.pop_front()).collect();
                if l.is_empty() { store.data.remove(key); }
                store.dirty += 1;
                drop(store);
                db.aof_write(vec![
                    b"LPOP".to_vec(),
                    key.as_bytes().to_vec(),
                    n.to_string().into_bytes(),
                ]);
                Ok(result)
            }
            _ => Err(Error::WrongType),
        },
    }
}

pub fn rpop(db: &Db, key: &str, count: usize) -> Result<Vec<Vec<u8>>> {
    let mut store = db.store.write();
    match store.data.get_mut(key) {
        None => return Ok(vec![]),
        Some(e) if e.is_expired() => return Ok(vec![]),
        Some(e) => match &mut e.value {
            Value::List(l) => {
                let n = count.min(l.len());
                let result: Vec<Vec<u8>> = (0..n).filter_map(|_| l.pop_back()).collect();
                if l.is_empty() { store.data.remove(key); }
                store.dirty += 1;
                drop(store);
                db.aof_write(vec![
                    b"RPOP".to_vec(),
                    key.as_bytes().to_vec(),
                    n.to_string().into_bytes(),
                ]);
                Ok(result)
            }
            _ => Err(Error::WrongType),
        },
    }
}

pub fn lrange(db: &Db, key: &str, start: i64, stop: i64) -> Result<Vec<Vec<u8>>> {
    let store = db.store.read();
    match store.get_entry(key) {
        None => Ok(vec![]),
        Some(e) => match &e.value {
            Value::List(l) => {
                let len = l.len() as i64;
                let s = resolve_index(start, len);
                let e_idx = resolve_index(stop, len);
                if s > e_idx || s >= l.len() { return Ok(vec![]); }
                Ok(l.range(s..=e_idx).cloned().collect())
            }
            _ => Err(Error::WrongType),
        },
    }
}

pub fn llen(db: &Db, key: &str) -> Result<usize> {
    let store = db.store.read();
    match store.get_entry(key) {
        None => Ok(0),
        Some(e) => match &e.value {
            Value::List(l) => Ok(l.len()),
            _ => Err(Error::WrongType),
        },
    }
}

pub fn lindex(db: &Db, key: &str, index: i64) -> Result<Option<Vec<u8>>> {
    let store = db.store.read();
    match store.get_entry(key) {
        None => Ok(None),
        Some(e) => match &e.value {
            Value::List(l) => Ok(l.get(resolve_index(index, l.len() as i64)).cloned()),
            _ => Err(Error::WrongType),
        },
    }
}

pub fn lset(db: &Db, key: &str, index: i64, value: Vec<u8>) -> Result<()> {
    let mut store = db.store.write();
    match store.get_entry_mut(key) {
        None => Err(Error::NoSuchKey),
        Some(e) => match &mut e.value {
            Value::List(l) => {
                let i = resolve_index(index, l.len() as i64);
                if i >= l.len() { return Err(Error::IndexOutOfRange); }
                let aof_val = value.clone();
                l[i] = value;
                store.dirty += 1;
                drop(store);
                db.aof_write(vec![
                    b"LSET".to_vec(),
                    key.as_bytes().to_vec(),
                    index.to_string().into_bytes(),
                    aof_val,
                ]);
                Ok(())
            }
            _ => Err(Error::WrongType),
        },
    }
}

pub fn ltrim(db: &Db, key: &str, start: i64, stop: i64) -> Result<()> {
    let mut store = db.store.write();
    match store.data.get_mut(key) {
        None => return Ok(()),
        Some(e) if e.is_expired() => return Ok(()),
        Some(e) => match &mut e.value {
            Value::List(l) => {
                let len = l.len() as i64;
                let s = resolve_index(start, len);
                let end = resolve_index(stop, len);
                if s > end || s >= l.len() {
                    *l = VecDeque::new();
                } else {
                    *l = l.drain(s..=end.min(l.len() - 1)).collect();
                }
                store.dirty += 1;
                drop(store);
                db.aof_write(vec![
                    b"LTRIM".to_vec(),
                    key.as_bytes().to_vec(),
                    start.to_string().into_bytes(),
                    stop.to_string().into_bytes(),
                ]);
                Ok(())
            }
            _ => Err(Error::WrongType),
        },
    }
}

pub fn linsert(db: &Db, key: &str, before: bool, pivot: &[u8], value: Vec<u8>) -> Result<i64> {
    let mut store = db.store.write();
    match store.data.get_mut(key) {
        None => return Ok(-1),
        Some(e) if e.is_expired() => return Ok(-1),
        Some(e) => match &mut e.value {
            Value::List(l) => {
                let pos = l.iter().position(|x| x.as_slice() == pivot);
                match pos {
                    None => Ok(-1),
                    Some(i) => {
                        let insert_at = if before { i } else { i + 1 };
                        let aof_val = value.clone();
                        l.insert(insert_at, value);
                        let len = l.len() as i64;
                        store.dirty += 1;
                        drop(store);
                        db.aof_write(vec![
                            b"LINSERT".to_vec(),
                            key.as_bytes().to_vec(),
                            if before { b"BEFORE".to_vec() } else { b"AFTER".to_vec() },
                            pivot.to_vec(),
                            aof_val,
                        ]);
                        Ok(len)
                    }
                }
            }
            _ => Err(Error::WrongType),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn db() -> Db { Db::open_memory() }

    #[test]
    fn push_range() {
        let db = db();
        rpush(&db, "l", vec![b"a".to_vec(), b"b".to_vec()]).unwrap();
        lpush(&db, "l", vec![b"z".to_vec()]).unwrap();
        let r = lrange(&db, "l", 0, -1).unwrap();
        assert_eq!(r, vec![b"z", b"a", b"b"]);
    }

    #[test]
    fn pop() {
        let db = db();
        rpush(&db, "l", vec![b"1".to_vec(), b"2".to_vec(), b"3".to_vec()]).unwrap();
        assert_eq!(lpop(&db, "l", 1).unwrap(), vec![b"1".to_vec()]);
        assert_eq!(rpop(&db, "l", 1).unwrap(), vec![b"3".to_vec()]);
        assert_eq!(llen(&db, "l").unwrap(), 1);
    }
}
