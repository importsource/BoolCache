use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use tokio::sync::broadcast;

/// Capacity of the in-memory replication broadcast channel (number of frames).
/// If a replica falls this far behind it is forced to reconnect and full-resync.
const CHANNEL_CAPACITY: usize = 4096;

// ── role ──────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub enum Role {
    /// Standalone (no replication configured).
    Standalone,
    /// This node accepts replicas and streams writes to them.
    Primary,
    /// This node replicates from the given primary.
    Replica { primary_host: String, primary_port: u16 },
}

// ── state ─────────────────────────────────────────────────────────────────────

pub struct ReplicationState {
    pub role: parking_lot::RwLock<Role>,
    /// 40-char hex replication ID — uniquely identifies this replication stream.
    pub repl_id: String,
    /// Byte offset of the last byte sent to replicas.
    pub offset: AtomicU64,
    /// Broadcast sender — primary writes RESP bytes; replica tasks read them.
    pub tx: broadcast::Sender<Bytes>,
}

impl ReplicationState {
    pub fn new() -> Self {
        let (tx, _) = broadcast::channel(CHANNEL_CAPACITY);
        ReplicationState {
            role: parking_lot::RwLock::new(Role::Standalone),
            repl_id: generate_repl_id(),
            offset: AtomicU64::new(0),
            tx,
        }
    }

    /// Send `data` to all subscribed replica tasks.
    /// Only increments the offset when at least one replica is connected.
    pub fn broadcast(&self, data: Bytes) {
        if self.tx.receiver_count() > 0 {
            self.offset.fetch_add(data.len() as u64, Ordering::Relaxed);
            let _ = self.tx.send(data);
        }
    }

    pub fn offset(&self) -> u64 {
        self.offset.load(Ordering::Relaxed)
    }

    pub fn connected_replicas(&self) -> usize {
        self.tx.receiver_count()
    }
}

impl Default for ReplicationState {
    fn default() -> Self { Self::new() }
}

// ── helpers ───────────────────────────────────────────────────────────────────

fn generate_repl_id() -> String {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let mut h = DefaultHasher::new();
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
        .hash(&mut h);
    std::process::id().hash(&mut h);
    let a = h.finish();
    let b = a.wrapping_mul(0x9e3779b97f4a7c15).wrapping_add(0x6c62272e07bb0142);
    format!("{a:016x}{b:016x}{:08x}", a ^ b)
}
