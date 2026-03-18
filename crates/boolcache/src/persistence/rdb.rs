//! RDB persistence: binary snapshot using bincode + serde.
//!
//! File format:
//!   magic bytes b"BOOLRDB0"  (8 bytes)
//!   bincode-encoded Vec<(String, Entry)>

use std::io::{BufReader, BufWriter, Read, Write};
use std::path::Path;

use crate::error::{Error, Result};
use crate::persistence::atomic_rename;
use crate::store::Store;

const MAGIC: &[u8; 8] = b"BOOLRDB0";

/// Serialize the store into an in-memory byte buffer (used for network transfer).
pub fn save_to_bytes(store: &Store) -> Result<Vec<u8>> {
    let mut buf = Vec::new();
    buf.extend_from_slice(MAGIC);
    let entries: Vec<(&String, &crate::types::Entry)> = store
        .data
        .iter()
        .filter(|(_, e)| !e.is_expired())
        .collect();
    let encoded = bincode::serialize(&entries)
        .map_err(|e| Error::Serialize(e.to_string()))?;
    let len = encoded.len() as u64;
    buf.extend_from_slice(&len.to_le_bytes());
    buf.extend_from_slice(&encoded);
    Ok(buf)
}

/// Deserialize a store from an in-memory byte slice (used after receiving over network).
pub fn load_from_bytes(bytes: &[u8]) -> Result<Store> {
    use std::io::Read;
    let mut cursor = std::io::Cursor::new(bytes);
    let mut magic = [0u8; 8];
    cursor.read_exact(&mut magic)?;
    if &magic != MAGIC {
        return Err(Error::Other("invalid RDB magic".into()));
    }
    let mut len_bytes = [0u8; 8];
    cursor.read_exact(&mut len_bytes)?;
    let len = u64::from_le_bytes(len_bytes) as usize;
    let mut buf = vec![0u8; len];
    cursor.read_exact(&mut buf)?;
    let entries: Vec<(String, crate::types::Entry)> =
        bincode::deserialize(&buf).map_err(|e| Error::Serialize(e.to_string()))?;
    let mut store = Store::new();
    for (key, entry) in entries {
        store.restore_entry(key, entry);
    }
    Ok(store)
}

pub fn save(path: &Path, store: &Store) -> Result<()> {
    // Write to a temp file then atomically rename.
    let tmp = path.with_extension("rdb.tmp");
    {
        let file = std::fs::File::create(&tmp)?;
        let mut w = BufWriter::new(file);
        w.write_all(MAGIC)?;
        let entries: Vec<(&String, &crate::types::Entry)> = store.data
            .iter()
            .filter(|(_, e)| !e.is_expired())
            .collect();
        let encoded = bincode::serialize(&entries)
            .map_err(|e| Error::Serialize(e.to_string()))?;
        let len = encoded.len() as u64;
        w.write_all(&len.to_le_bytes())?;
        w.write_all(&encoded)?;
        w.flush()?;
    }
    atomic_rename(&tmp, path)?;
    Ok(())
}

pub fn load(path: &Path) -> Result<Store> {
    let file = std::fs::File::open(path)?;
    let mut r = BufReader::new(file);

    // Verify magic
    let mut magic = [0u8; 8];
    r.read_exact(&mut magic)?;
    if &magic != MAGIC {
        return Err(Error::Other("invalid RDB magic".into()));
    }

    // Read length-prefixed bincode blob
    let mut len_bytes = [0u8; 8];
    r.read_exact(&mut len_bytes)?;
    let len = u64::from_le_bytes(len_bytes) as usize;
    let mut buf = vec![0u8; len];
    r.read_exact(&mut buf)?;

    let entries: Vec<(String, crate::types::Entry)> =
        bincode::deserialize(&buf).map_err(|e| Error::Serialize(e.to_string()))?;

    let mut store = Store::new();
    for (key, entry) in entries {
        store.restore_entry(key, entry);
    }
    Ok(store)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{Entry, Value};

    #[test]
    fn roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("dump.rdb");

        let mut store = Store::new();
        store.data.insert(
            "hello".into(),
            Entry::new(Value::String(b"world".to_vec())),
        );

        save(&path, &store).unwrap();
        let loaded = load(&path).unwrap();
        assert_eq!(loaded.data.len(), 1);
        match &loaded.data["hello"].value {
            Value::String(v) => assert_eq!(v, b"world"),
            _ => panic!("wrong type"),
        }
    }
}
