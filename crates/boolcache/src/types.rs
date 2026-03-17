use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};
use std::time::Duration;

use ordered_float::OrderedFloat;
use serde::{Deserialize, Serialize};

// ── ZSet ──────────────────────────────────────────────────────────────────────

/// Sorted set with O(1) score lookup and O(log n) ordered iteration.
///
/// Mirrors Redis ZSet: each member has exactly one score; members with equal
/// scores are ordered lexicographically.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ZSet {
    /// member → score  (O(1) lookup)
    pub member_scores: HashMap<Vec<u8>, f64>,
    /// (score, member) ordered set for range queries
    pub ordered: BTreeSet<(OrderedFloat<f64>, Vec<u8>)>,
}

impl ZSet {
    pub fn new() -> Self {
        Self::default()
    }

    /// Add or update a member.  Returns `true` if the member was newly added.
    pub fn add(&mut self, member: Vec<u8>, score: f64) -> bool {
        let is_new = !self.member_scores.contains_key(&member);
        if let Some(&old) = self.member_scores.get(&member) {
            self.ordered.remove(&(OrderedFloat(old), member.clone()));
        }
        self.member_scores.insert(member.clone(), score);
        self.ordered.insert((OrderedFloat(score), member));
        is_new
    }

    /// Remove a member. Returns `true` if it existed.
    pub fn remove(&mut self, member: &[u8]) -> bool {
        if let Some(score) = self.member_scores.remove(member) {
            self.ordered.remove(&(OrderedFloat(score), member.to_vec()));
            true
        } else {
            false
        }
    }

    pub fn score(&self, member: &[u8]) -> Option<f64> {
        self.member_scores.get(member).copied()
    }

    /// 0-based rank (ascending).
    pub fn rank(&self, member: &[u8]) -> Option<usize> {
        let score = self.member_scores.get(member)?;
        let key = (OrderedFloat(*score), member.to_vec());
        Some(self.ordered.range(..=key).count() - 1)
    }

    /// 0-based rank (descending).
    pub fn revrank(&self, member: &[u8]) -> Option<usize> {
        let rank = self.rank(member)?;
        Some(self.len() - 1 - rank)
    }

    pub fn len(&self) -> usize {
        self.member_scores.len()
    }

    pub fn is_empty(&self) -> bool {
        self.member_scores.is_empty()
    }

    /// Increment score of a member; inserts with score 0 + increment if absent.
    pub fn incr_by(&mut self, member: Vec<u8>, delta: f64) -> f64 {
        let new_score = self.member_scores.get(&member).copied().unwrap_or(0.0) + delta;
        self.add(member, new_score);
        new_score
    }

    /// Members in ascending score order, slice `[start, stop]` (inclusive, 0-based).
    pub fn range(&self, start: i64, stop: i64) -> Vec<(Vec<u8>, f64)> {
        let len = self.len() as i64;
        let s = resolve_index(start, len);
        let e = resolve_index(stop, len);
        if s > e || s >= len as usize {
            return vec![];
        }
        self.ordered
            .iter()
            .skip(s)
            .take(e - s + 1)
            .map(|(score, m)| (m.clone(), score.0))
            .collect()
    }

    /// Members in descending score order, slice `[start, stop]` (inclusive, 0-based).
    pub fn revrange(&self, start: i64, stop: i64) -> Vec<(Vec<u8>, f64)> {
        let len = self.len() as i64;
        let s = resolve_index(start, len);
        let e = resolve_index(stop, len);
        if s > e || s >= len as usize {
            return vec![];
        }
        self.ordered
            .iter()
            .rev()
            .skip(s)
            .take(e - s + 1)
            .map(|(score, m)| (m.clone(), score.0))
            .collect()
    }

    /// Members with score in `[min, max]`.
    pub fn range_by_score(&self, min: f64, max: f64) -> Vec<(Vec<u8>, f64)> {
        self.ordered
            .range((OrderedFloat(min), vec![])..)
            .take_while(|(s, _)| s.0 <= max)
            .map(|(score, m)| (m.clone(), score.0))
            .collect()
    }

    /// Count members with score in `[min, max]`.
    pub fn count_in_range(&self, min: f64, max: f64) -> usize {
        self.range_by_score(min, max).len()
    }
}

// ── Value ─────────────────────────────────────────────────────────────────────

/// All value types supported by BoolCache.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Value {
    String(Vec<u8>),
    List(VecDeque<Vec<u8>>),
    Hash(HashMap<Vec<u8>, Vec<u8>>),
    Set(HashSet<Vec<u8>>),
    ZSet(ZSet),
}

impl Value {
    pub fn type_name(&self) -> &'static str {
        match self {
            Value::String(_) => "string",
            Value::List(_) => "list",
            Value::Hash(_) => "hash",
            Value::Set(_) => "set",
            Value::ZSet(_) => "zset",
        }
    }
}

// ── Entry ─────────────────────────────────────────────────────────────────────

/// A stored key entry: value + optional expiry (stored as Unix ms).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Entry {
    pub value: Value,
    /// Absolute expiry in Unix milliseconds, or `None` for persistent.
    pub expires_ms: Option<u64>,
}

impl Entry {
    pub fn new(value: Value) -> Self {
        Entry { value, expires_ms: None }
    }

    pub fn with_expiry(value: Value, ttl: Duration) -> Self {
        let expires_ms = unix_ms_now() + ttl.as_millis() as u64;
        Entry { value, expires_ms: Some(expires_ms) }
    }

    pub fn is_expired(&self) -> bool {
        self.expires_ms.map_or(false, |exp| unix_ms_now() >= exp)
    }

    /// Remaining TTL in milliseconds: `None` = no expiry, `-2` = already expired.
    pub fn ttl_ms(&self) -> Option<i64> {
        self.expires_ms.map(|exp| {
            let now = unix_ms_now();
            if now >= exp { -2 } else { (exp - now) as i64 }
        })
    }
}

// ── helpers ───────────────────────────────────────────────────────────────────

pub fn unix_ms_now() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// Convert a possibly-negative Redis index into an absolute 0-based index.
pub fn resolve_index(idx: i64, len: i64) -> usize {
    if idx >= 0 {
        idx as usize
    } else {
        let abs = (-idx) as i64;
        if abs > len { 0 } else { (len - abs) as usize }
    }
    .min(len.max(0) as usize)
}
