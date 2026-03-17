use crate::db::Db;
use crate::error::{Error, Result};
use crate::types::{Entry, Value, ZSet};

fn ensure_zset<'a>(
    store: &'a mut crate::store::Store,
    key: &str,
) -> Result<&'a mut ZSet> {
    let entry = store.data.entry(key.to_string()).or_insert_with(|| {
        Entry::new(Value::ZSet(ZSet::new()))
    });
    if entry.is_expired() {
        *entry = Entry::new(Value::ZSet(ZSet::new()));
    }
    match &mut entry.value {
        Value::ZSet(z) => Ok(z),
        _ => Err(Error::WrongType),
    }
}

pub fn zadd(db: &Db, key: &str, pairs: Vec<(f64, Vec<u8>)>) -> Result<usize> {
    let mut aof = vec![b"ZADD".to_vec(), key.as_bytes().to_vec()];
    for (score, member) in &pairs {
        aof.push(score.to_string().into_bytes());
        aof.push(member.clone());
    }
    let mut store = db.store.write();
    let z = ensure_zset(&mut store, key)?;
    let added = pairs.into_iter().filter(|(score, member)| z.add(member.clone(), *score)).count();
    store.dirty += 1;
    drop(store);
    db.aof_write(aof);
    Ok(added)
}

pub fn zrem(db: &Db, key: &str, members: Vec<Vec<u8>>) -> Result<usize> {
    let mut aof = vec![b"ZREM".to_vec(), key.as_bytes().to_vec()];
    aof.extend(members.iter().cloned());
    let mut store = db.store.write();
    match store.data.get_mut(key) {
        None => return Ok(0),
        Some(e) if e.is_expired() => return Ok(0),
        Some(e) => match &mut e.value {
            Value::ZSet(z) => {
                let removed = members.iter().filter(|m| z.remove(m.as_slice())).count();
                if z.is_empty() { store.data.remove(key); }
                store.dirty += 1;
                drop(store);
                if removed > 0 { db.aof_write(aof); }
                Ok(removed)
            }
            _ => Err(Error::WrongType),
        },
    }
}

pub fn zscore(db: &Db, key: &str, member: &[u8]) -> Result<Option<f64>> {
    let store = db.store.read();
    match store.get_entry(key) {
        None => Ok(None),
        Some(e) => match &e.value {
            Value::ZSet(z) => Ok(z.score(member)),
            _ => Err(Error::WrongType),
        },
    }
}

pub fn zrank(db: &Db, key: &str, member: &[u8]) -> Result<Option<usize>> {
    let store = db.store.read();
    match store.get_entry(key) {
        None => Ok(None),
        Some(e) => match &e.value {
            Value::ZSet(z) => Ok(z.rank(member)),
            _ => Err(Error::WrongType),
        },
    }
}

pub fn zrevrank(db: &Db, key: &str, member: &[u8]) -> Result<Option<usize>> {
    let store = db.store.read();
    match store.get_entry(key) {
        None => Ok(None),
        Some(e) => match &e.value {
            Value::ZSet(z) => Ok(z.revrank(member)),
            _ => Err(Error::WrongType),
        },
    }
}

pub fn zcard(db: &Db, key: &str) -> Result<usize> {
    let store = db.store.read();
    match store.get_entry(key) {
        None => Ok(0),
        Some(e) => match &e.value {
            Value::ZSet(z) => Ok(z.len()),
            _ => Err(Error::WrongType),
        },
    }
}

pub fn zrange(db: &Db, key: &str, start: i64, stop: i64) -> Result<Vec<(Vec<u8>, f64)>> {
    let store = db.store.read();
    match store.get_entry(key) {
        None => Ok(vec![]),
        Some(e) => match &e.value {
            Value::ZSet(z) => Ok(z.range(start, stop)),
            _ => Err(Error::WrongType),
        },
    }
}

pub fn zrevrange(db: &Db, key: &str, start: i64, stop: i64) -> Result<Vec<(Vec<u8>, f64)>> {
    let store = db.store.read();
    match store.get_entry(key) {
        None => Ok(vec![]),
        Some(e) => match &e.value {
            Value::ZSet(z) => Ok(z.revrange(start, stop)),
            _ => Err(Error::WrongType),
        },
    }
}

pub fn zrangebyscore(db: &Db, key: &str, min: f64, max: f64) -> Result<Vec<(Vec<u8>, f64)>> {
    let store = db.store.read();
    match store.get_entry(key) {
        None => Ok(vec![]),
        Some(e) => match &e.value {
            Value::ZSet(z) => Ok(z.range_by_score(min, max)),
            _ => Err(Error::WrongType),
        },
    }
}

pub fn zcount(db: &Db, key: &str, min: f64, max: f64) -> Result<usize> {
    let store = db.store.read();
    match store.get_entry(key) {
        None => Ok(0),
        Some(e) => match &e.value {
            Value::ZSet(z) => Ok(z.count_in_range(min, max)),
            _ => Err(Error::WrongType),
        },
    }
}

pub fn zincrby(db: &Db, key: &str, delta: f64, member: Vec<u8>) -> Result<f64> {
    let mut store = db.store.write();
    let z = ensure_zset(&mut store, key)?;
    let new_score = z.incr_by(member.clone(), delta);
    store.dirty += 1;
    drop(store);
    // Log as ZADD with the resulting score so replay is idempotent
    db.aof_write(vec![
        b"ZADD".to_vec(),
        key.as_bytes().to_vec(),
        new_score.to_string().into_bytes(),
        member,
    ]);
    Ok(new_score)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn db() -> Db { Db::open_memory() }

    #[test]
    fn add_rank_range() {
        let db = db();
        zadd(&db, "z", vec![
            (1.0, b"alice".to_vec()),
            (2.0, b"bob".to_vec()),
            (3.0, b"carol".to_vec()),
        ]).unwrap();
        assert_eq!(zcard(&db, "z").unwrap(), 3);
        assert_eq!(zrank(&db, "z", b"alice").unwrap(), Some(0));
        assert_eq!(zrank(&db, "z", b"carol").unwrap(), Some(2));
        assert_eq!(zscore(&db, "z", b"bob").unwrap(), Some(2.0));
        let r = zrange(&db, "z", 0, -1).unwrap();
        assert_eq!(r[0].0, b"alice");
        assert_eq!(r[2].0, b"carol");
    }

    #[test]
    fn rangebyscore() {
        let db = db();
        zadd(&db, "z", vec![
            (1.0, b"a".to_vec()),
            (2.0, b"b".to_vec()),
            (3.0, b"c".to_vec()),
        ]).unwrap();
        let r = zrangebyscore(&db, "z", 1.5, 2.5).unwrap();
        assert_eq!(r.len(), 1);
        assert_eq!(r[0].0, b"b");
    }

    #[test]
    fn zincrby_test() {
        let db = db();
        zadd(&db, "z", vec![(1.0, b"a".to_vec())]).unwrap();
        let s = zincrby(&db, "z", 2.5, b"a".to_vec()).unwrap();
        assert!((s - 3.5).abs() < 1e-9);
    }
}
