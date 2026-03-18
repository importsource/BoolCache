use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use parking_lot::RwLock;

use crate::error::Result;
use crate::persistence::aof::{AofWriter, FsyncPolicy};
use crate::persistence::rdb;
use crate::replication::state::{ReplicationState, Role};
use crate::store::Store;

/// Configuration for opening a `Db`.
#[derive(Debug, Clone)]
pub struct Config {
    pub dir: PathBuf,
    pub aof_enabled: bool,
    pub aof_fsync: FsyncPolicy,
    /// RDB auto-save rules: list of (seconds, min_dirty_keys).
    pub save_rules: Vec<(u64, u64)>,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            dir: PathBuf::from("."),
            aof_enabled: true,
            aof_fsync: FsyncPolicy::EverySec,
            save_rules: vec![(900, 1), (300, 10), (60, 10_000)],
        }
    }
}

/// Thread-safe database handle. Clone freely — all clones share the same store.
#[derive(Clone)]
pub struct Db {
    pub(crate) store: Arc<RwLock<Store>>,
    pub(crate) aof: Option<Arc<parking_lot::Mutex<AofWriter>>>,
    pub(crate) config: Arc<Config>,
    /// Shared replication state (role, offset, broadcast channel).
    pub replication: Arc<ReplicationState>,
}

impl Db {
    pub fn open(config: Config) -> Result<Self> {
        std::fs::create_dir_all(&config.dir)?;

        let rdb_path = config.dir.join("dump.rdb");
        let aof_path = config.dir.join("appendonly.aof");

        let mut store = if rdb_path.exists() {
            tracing::info!("Loading RDB from {}", rdb_path.display());
            match rdb::load(&rdb_path) {
                Ok(s) => { tracing::info!("RDB loaded: {} keys", s.data.len()); s }
                Err(e) => { tracing::warn!("RDB load failed ({e}), starting empty"); Store::new() }
            }
        } else {
            Store::new()
        };

        if config.aof_enabled && aof_path.exists() {
            tracing::info!("Replaying AOF from {}", aof_path.display());
            let count = crate::persistence::aof::replay(&aof_path, &mut store)?;
            tracing::info!("AOF replay: {count} commands");
        }

        let aof = if config.aof_enabled {
            let writer = AofWriter::open(&aof_path, config.aof_fsync.clone())?;
            Some(Arc::new(parking_lot::Mutex::new(writer)))
        } else {
            None
        };

        let db = Db {
            store: Arc::new(RwLock::new(store)),
            aof,
            config: Arc::new(config),
            replication: Arc::new(ReplicationState::new()),
        };

        db.spawn_expiry_sweeper();
        db.spawn_rdb_autosave();
        if db.aof.is_some() {
            db.spawn_aof_fsync();
        }

        Ok(db)
    }

    pub fn open_memory() -> Self {
        Db {
            store: Arc::new(RwLock::new(Store::new())),
            aof: None,
            config: Arc::new(Config { aof_enabled: false, save_rules: vec![], ..Default::default() }),
            replication: Arc::new(ReplicationState::new()),
        }
    }

    // ── persistence ───────────────────────────────────────────────────────────

    /// Write a command to the AOF and broadcast it to connected replicas.
    pub(crate) fn aof_write(&self, args: Vec<Vec<u8>>) {
        // Encode as RESP once — shared by AOF and replication.
        let resp = encode_resp(&args);

        if let Some(aof) = &self.aof {
            let refs: Vec<&[u8]> = args.iter().map(|v| v.as_slice()).collect();
            let mut w = aof.lock();
            if let Err(e) = w.append(&refs) {
                tracing::error!("AOF write error: {e}");
            }
        }

        if matches!(*self.replication.role.read(), Role::Primary) {
            self.replication.broadcast(Bytes::from(resp));
        }
    }

    /// Apply a command received from the primary (replica-side).
    /// Writes to local AOF but does NOT re-broadcast.
    pub fn apply_replicated(&self, args: Vec<Vec<u8>>) {
        {
            let mut store = self.store.write();
            crate::persistence::aof::apply_command(&mut store, &args);
        }
        if let Some(aof) = &self.aof {
            let refs: Vec<&[u8]> = args.iter().map(|v| v.as_slice()).collect();
            let mut w = aof.lock();
            if let Err(e) = w.append(&refs) {
                tracing::error!("AOF write error (replication): {e}");
            }
        }
    }

    /// Atomically replace the entire store (used by replica after full resync).
    pub fn replace_store(&self, new_store: Store) {
        *self.store.write() = new_store;
    }

    /// Serialize the current store to an in-memory RDB snapshot (used for full resync).
    pub fn rdb_snapshot(&self) -> Result<Vec<u8>> {
        let store = self.store.read().clone();
        rdb::save_to_bytes(&store)
    }

    pub fn bgsave(&self) -> Result<()> {
        let store = self.store.read().clone();
        let path = self.config.dir.join("dump.rdb");
        rdb::save(&path, &store)?;
        self.store.write().dirty = 0;
        tracing::info!("RDB saved to {}", path.display());
        Ok(())
    }

    pub fn shutdown(&self) {
        tracing::info!("Shutting down: flushing AOF...");
        if let Some(aof) = &self.aof {
            let mut w = aof.lock();
            if let Err(e) = w.fsync() {
                tracing::error!("AOF flush on shutdown failed: {e}");
            }
        }
        tracing::info!("Shutting down: saving RDB...");
        if let Err(e) = self.bgsave() {
            tracing::error!("RDB save on shutdown failed: {e}");
        }
        tracing::info!("Shutdown complete.");
    }

    pub fn bgrewriteaof(&self) -> Result<()> {
        if let Some(aof_arc) = &self.aof {
            let store = self.store.read().clone();
            let path = self.config.dir.join("appendonly.aof");
            let tmp = self.config.dir.join("appendonly.aof.tmp");
            crate::persistence::aof::rewrite(&tmp, &store)?;
            crate::persistence::atomic_rename(&tmp, &path)?;
            let writer = AofWriter::open(&path, self.config.aof_fsync.clone())?;
            *aof_arc.lock() = writer;
            tracing::info!("AOF rewrite complete");
        }
        Ok(())
    }

    // ── public store delegates ────────────────────────────────────────────────

    pub fn dbsize(&self) -> usize { self.store.read().dbsize() }

    pub fn flush(&self) {
        self.store.write().flush();
        self.aof_write(vec![b"FLUSHDB".to_vec()]);
    }

    pub fn del(&self, keys: &[&str]) -> usize {
        let n = self.store.write().del(keys);
        if n > 0 {
            let mut args = vec![b"DEL".to_vec()];
            args.extend(keys.iter().map(|k| k.as_bytes().to_vec()));
            self.aof_write(args);
        }
        n
    }

    pub fn exists(&self, keys: &[&str]) -> usize { self.store.read().exists(keys) }

    pub fn pexpire(&self, key: &str, ms: u64) -> bool {
        let ok = self.store.write().pexpire(key, ms);
        if ok {
            self.aof_write(vec![
                b"PEXPIRE".to_vec(),
                key.as_bytes().to_vec(),
                ms.to_string().into_bytes(),
            ]);
        }
        ok
    }

    pub fn pttl(&self, key: &str) -> i64 { self.store.read().pttl(key) }

    pub fn persist_key(&self, key: &str) -> bool {
        let ok = self.store.write().persist(key);
        if ok {
            self.aof_write(vec![b"PERSIST".to_vec(), key.as_bytes().to_vec()]);
        }
        ok
    }

    pub fn type_of(&self, key: &str) -> &'static str { self.store.read().type_of(key) }

    pub fn rename(&self, src: &str, dst: &str) -> Result<()> {
        self.store.write().rename(src, dst)?;
        self.aof_write(vec![
            b"RENAME".to_vec(),
            src.as_bytes().to_vec(),
            dst.as_bytes().to_vec(),
        ]);
        Ok(())
    }

    pub fn keys(&self, pattern: &str) -> Vec<String> { self.store.read().keys(pattern) }
    pub fn scan(&self, cursor: usize, pattern: &str, count: usize) -> (usize, Vec<String>) {
        self.store.read().scan(cursor, pattern, count)
    }

    // ── background workers ────────────────────────────────────────────────────

    fn spawn_expiry_sweeper(&self) {
        let store = Arc::clone(&self.store);
        std::thread::spawn(move || loop {
            std::thread::sleep(Duration::from_millis(100));
            let purged = store.write().purge_expired();
            if purged > 0 {
                tracing::debug!("Expiry sweep removed {purged} keys");
            }
        });
    }

    fn spawn_rdb_autosave(&self) {
        if self.config.save_rules.is_empty() { return; }
        let db = self.clone();
        std::thread::spawn(move || {
            let mut last_save = std::time::Instant::now();
            loop {
                std::thread::sleep(Duration::from_secs(1));
                let elapsed = last_save.elapsed().as_secs();
                let dirty = db.store.read().dirty;
                let should_save = db.config.save_rules.iter()
                    .any(|&(secs, min_dirty)| elapsed >= secs && dirty >= min_dirty);
                if should_save {
                    if let Err(e) = db.bgsave() {
                        tracing::error!("Auto-save RDB failed: {e}");
                    } else {
                        last_save = std::time::Instant::now();
                    }
                }
            }
        });
    }

    fn spawn_aof_fsync(&self) {
        if !matches!(self.config.aof_fsync, FsyncPolicy::EverySec) { return; }
        let aof = Arc::clone(self.aof.as_ref().unwrap());
        std::thread::spawn(move || loop {
            std::thread::sleep(Duration::from_secs(1));
            let mut w = aof.lock();
            if let Err(e) = w.fsync() {
                tracing::error!("AOF fsync error: {e}");
            }
        });
    }
}

// ── helpers ───────────────────────────────────────────────────────────────────

/// Encode command args as a RESP array (for AOF + replication).
fn encode_resp(args: &[Vec<u8>]) -> Vec<u8> {
    let mut buf = Vec::new();
    let _ = write!(buf, "*{}\r\n", args.len());
    for arg in args {
        let _ = write!(buf, "${}\r\n", arg.len());
        buf.extend_from_slice(arg);
        buf.extend_from_slice(b"\r\n");
    }
    buf
}

use std::io::Write;
