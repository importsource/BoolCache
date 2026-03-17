use std::collections::HashMap;

use crate::error::{Error, Result};
use crate::types::{Entry, Value};

/// Raw in-memory store. Not thread-safe on its own; wrap in `Db` for shared use.
#[derive(Debug, Default, Clone)]
pub struct Store {
    pub(crate) data: HashMap<String, Entry>,
    /// Number of write operations since last RDB save.
    pub(crate) dirty: u64,
}

impl Store {
    pub fn new() -> Self {
        Self::default()
    }

    // ── generic key ops ───────────────────────────────────────────────────────

    /// Get a live entry by key, returning `None` if absent or expired.
    pub fn get_entry(&self, key: &str) -> Option<&Entry> {
        self.data.get(key).filter(|e| !e.is_expired())
    }

    /// Get a mutable live entry, removing it if expired.
    pub fn get_entry_mut(&mut self, key: &str) -> Option<&mut Entry> {
        if let Some(e) = self.data.get(key) {
            if e.is_expired() {
                self.data.remove(key);
                return None;
            }
        }
        self.data.get_mut(key)
    }

    /// Assert that the entry for `key` holds a `Value` of the expected variant.
    /// Returns `Err(WrongType)` if it exists but is a different type.
    pub fn check_type<F>(&self, key: &str, predicate: F) -> Result<()>
    where
        F: Fn(&Value) -> bool,
    {
        if let Some(e) = self.get_entry(key) {
            if !predicate(&e.value) {
                return Err(Error::WrongType);
            }
        }
        Ok(())
    }

    /// Remove expired keys from `data`. Called by the background sweeper.
    pub fn purge_expired(&mut self) -> usize {
        let before = self.data.len();
        self.data.retain(|_, e| !e.is_expired());
        before - self.data.len()
    }

    // ── key management ────────────────────────────────────────────────────────

    pub fn del(&mut self, keys: &[&str]) -> usize {
        let mut removed = 0;
        for &k in keys {
            if self.data.remove(k).is_some() {
                removed += 1;
            }
        }
        self.dirty += removed as u64;
        removed
    }

    pub fn exists(&self, keys: &[&str]) -> usize {
        keys.iter().filter(|&&k| self.get_entry(k).is_some()).count()
    }

    pub fn rename(&mut self, src: &str, dst: &str) -> Result<()> {
        let entry = self.data.remove(src).ok_or(Error::NoSuchKey)?;
        if entry.is_expired() {
            return Err(Error::NoSuchKey);
        }
        self.data.insert(dst.to_string(), entry);
        self.dirty += 1;
        Ok(())
    }

    pub fn type_of(&self, key: &str) -> &'static str {
        self.get_entry(key).map_or("none", |e| e.value.type_name())
    }

    /// Set expiry in milliseconds from now. Returns `false` if key doesn't exist.
    pub fn pexpire(&mut self, key: &str, ttl_ms: u64) -> bool {
        if let Some(e) = self.get_entry_mut(key) {
            let exp = crate::types::unix_ms_now() + ttl_ms;
            e.expires_ms = Some(exp);
            self.dirty += 1;
            true
        } else {
            false
        }
    }

    /// Remove expiry (make persistent). Returns `false` if key doesn't exist or has no expiry.
    pub fn persist(&mut self, key: &str) -> bool {
        if let Some(e) = self.get_entry_mut(key) {
            if e.expires_ms.is_some() {
                e.expires_ms = None;
                self.dirty += 1;
                return true;
            }
        }
        false
    }

    /// TTL in milliseconds: `None` if no expiry, `-1` if no expiry set, `-2` if not found.
    pub fn pttl(&self, key: &str) -> i64 {
        match self.get_entry(key) {
            None => -2,
            Some(e) => e.ttl_ms().unwrap_or(-1),
        }
    }

    /// Keys matching a glob pattern (* and ? wildcards).
    pub fn keys(&self, pattern: &str) -> Vec<String> {
        self.data
            .iter()
            .filter(|(k, e)| !e.is_expired() && glob_match(pattern, k))
            .map(|(k, _)| k.clone())
            .collect()
    }

    /// SCAN-style cursor iteration. Returns (next_cursor, keys).
    /// `cursor == 0` starts from the beginning; returns `0` when complete.
    pub fn scan(&self, cursor: usize, pattern: &str, count: usize) -> (usize, Vec<String>) {
        let all: Vec<&String> = self
            .data
            .keys()
            .filter(|k| {
                self.data.get(*k).map_or(false, |e| !e.is_expired())
                    && glob_match(pattern, k)
            })
            .collect();
        let total = all.len();
        if total == 0 {
            return (0, vec![]);
        }
        let start = cursor % total;
        let end = (start + count).min(total);
        let keys: Vec<String> = all[start..end].iter().map(|k| k.to_string()).collect();
        let next = if end >= total { 0 } else { end };
        (next, keys)
    }

    pub fn dbsize(&self) -> usize {
        self.data.iter().filter(|(_, e)| !e.is_expired()).count()
    }

    pub fn flush(&mut self) {
        self.data.clear();
        self.dirty = 0;
    }

    // ── raw insert (used by persistence layer) ────────────────────────────────

    pub fn restore_entry(&mut self, key: String, entry: Entry) {
        if !entry.is_expired() {
            self.data.insert(key, entry);
        }
    }
}

// ── glob matching ─────────────────────────────────────────────────────────────

/// Minimal glob matcher supporting `*` (any sequence) and `?` (any char).
pub fn glob_match(pattern: &str, text: &str) -> bool {
    let p: Vec<char> = pattern.chars().collect();
    let t: Vec<char> = text.chars().collect();
    glob_match_inner(&p, &t)
}

fn glob_match_inner(p: &[char], t: &[char]) -> bool {
    match (p.first(), t.first()) {
        (None, None) => true,
        (Some(&'*'), _) => {
            // Try matching 0..=n chars with the star
            for i in 0..=t.len() {
                if glob_match_inner(&p[1..], &t[i..]) {
                    return true;
                }
            }
            false
        }
        (Some(&'?'), Some(_)) => glob_match_inner(&p[1..], &t[1..]),
        (Some(pc), Some(tc)) if pc == tc => glob_match_inner(&p[1..], &t[1..]),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn glob_star() {
        assert!(glob_match("h?llo", "hello"));
        assert!(glob_match("h*llo", "heeello"));
        assert!(glob_match("*", "anything"));
        assert!(!glob_match("h?llo", "hllo"));
    }

    #[test]
    fn expiry_purge() {
        use crate::types::Value;
        use std::time::Duration;
        let mut s = Store::new();
        s.data.insert(
            "k".into(),
            Entry::with_expiry(Value::String(b"v".to_vec()), Duration::from_millis(1)),
        );
        std::thread::sleep(Duration::from_millis(5));
        let purged = s.purge_expired();
        assert_eq!(purged, 1);
        assert!(s.data.is_empty());
    }
}
