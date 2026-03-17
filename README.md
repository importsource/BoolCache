# BoolCache

A Redis-compatible in-memory cache engine written in Rust. Drop-in replacement for Redis clients — connect with any `redis-cli` or Redis SDK.

```
  ____              _ ____           _
 | __ )  ___   ___ | / ___|__ _  ___| |__   ___
 |  _ \ / _ \ / _ \| \___ / _` |/ __| '_ \ / _ \
 | |_) | (_) | (_) | |___) (_| | (__| | | |  __/
 |____/ \___/ \___/|_|____\__,_|\___|_| |_|\___|
```

## Features

- **All core Redis data types** — String, List, Hash, Set, Sorted Set
- **Dual persistence** — AOF (append-only file) + RDB (binary snapshot)
- **RESP2 protocol** — works with `redis-cli` and any Redis client library
- **Primary-replica replication** — stream writes to multiple read replicas
- **Graceful shutdown** — flushes AOF and saves RDB on `Ctrl-C` / `SIGTERM`
- **Key expiry** — `EX`, `PX`, `EXPIRE`, `PEXPIRE` with lazy + active sweep
- **Embeddable library** — use `boolcache` as a crate in your own Rust project

---

## Quick Start

### Prerequisites

- [Rust](https://rustup.rs/) 1.75 or later

### Build

```bash
git clone https://github.com/importsource/BoolCache.git
cd BoolCache
cargo build --release
```

### Run

```bash
# Start with defaults (127.0.0.1:6379, AOF + RDB enabled, data in current dir)
cargo run --release -p boolcache-server

# Or use the compiled binary
./target/release/boolcache-server
```

### Connect

```bash
redis-cli ping
# PONG

redis-cli set greeting "hello boolcache"
redis-cli get greeting
```

---

## CLI Options

```
Usage: boolcache-server [OPTIONS]

Options:
      --host <HOST>            Bind address          [default: 127.0.0.1]
  -p, --port <PORT>            Port to listen on     [default: 6379]
      --dir <DIR>              Data directory        [default: .]
      --no-aof                 Disable AOF persistence
      --aof-fsync <POLICY>     Fsync policy: always | everysec | no  [default: everysec]
      --log-level <LEVEL>      Log level: trace | debug | info | warn | error  [default: info]
      --replicaof <HOST:PORT>  Connect to a primary as a replica
  -h, --help                   Print help
```

---

## Supported Commands

### String
| Command | Description |
|---------|-------------|
| `SET key value [EX s] [PX ms] [NX] [XX] [GET]` | Set a value |
| `GET key` | Get a value |
| `MGET key [key ...]` | Get multiple values |
| `MSET key value [key value ...]` | Set multiple values |
| `GETSET key value` | Set and return old value |
| `SETNX key value` | Set only if key does not exist |
| `SETEX key seconds value` | Set with TTL in seconds |
| `INCR / DECR key` | Increment / decrement by 1 |
| `INCRBY / DECRBY key delta` | Increment / decrement by delta |
| `APPEND key value` | Append to string |
| `STRLEN key` | String length |

### Key
| Command | Description |
|---------|-------------|
| `DEL key [key ...]` | Delete keys |
| `EXISTS key [key ...]` | Count existing keys |
| `EXPIRE / PEXPIRE key ttl` | Set TTL in seconds / milliseconds |
| `TTL / PTTL key` | Remaining TTL in seconds / milliseconds |
| `PERSIST key` | Remove TTL |
| `RENAME src dst` | Rename a key |
| `TYPE key` | Get value type |
| `KEYS pattern` | List keys matching glob pattern |
| `SCAN cursor [MATCH p] [COUNT n]` | Incremental key scan |
| `DBSIZE` | Number of keys |
| `FLUSHDB / FLUSHALL` | Delete all keys |

### List
`LPUSH` `RPUSH` `LPOP` `RPOP` `LRANGE` `LLEN` `LINDEX` `LSET` `LINSERT` `LTRIM`

### Hash
`HSET` `HGET` `HMGET` `HMSET` `HDEL` `HEXISTS` `HGETALL` `HKEYS` `HVALS` `HLEN` `HINCRBY`

### Set
`SADD` `SREM` `SMEMBERS` `SISMEMBER` `SCARD` `SINTER` `SUNION` `SDIFF` `SMOVE`

### Sorted Set
`ZADD` `ZREM` `ZSCORE` `ZRANK` `ZREVRANK` `ZRANGE` `ZREVRANGE` `ZRANGEBYSCORE` `ZCARD` `ZCOUNT` `ZINCRBY`

### Server
`PING` `ECHO` `SELECT` `DBSIZE` `FLUSHDB` `FLUSHALL` `SAVE` `BGSAVE` `BGREWRITEAOF` `INFO` `REPLCONF` `PSYNC` `REPLICAOF`

---

## Persistence

BoolCache writes data in two complementary ways:

### AOF (Append-Only File)
Every write command is appended to `appendonly.aof` in RESP format. On restart the file is replayed to restore state. Fsync policy:

| Policy | Behaviour |
|--------|-----------|
| `everysec` (default) | Fsync once per second — good balance of safety and speed |
| `always` | Fsync after every write — safest, slowest |
| `no` | OS decides when to flush — fastest, some risk |

### RDB (Snapshot)
A full binary snapshot is saved to `dump.rdb`. Auto-save rules (same as Redis defaults):

| Rule | Meaning |
|------|---------|
| 900 seconds, 1 change | Save if at least 1 key changed in the last 15 min |
| 300 seconds, 10 changes | Save if at least 10 keys changed in 5 min |
| 60 seconds, 10 000 changes | Save if at least 10 000 keys changed in 1 min |

You can also trigger a manual save:
```bash
redis-cli save      # synchronous
redis-cli bgsave    # background
```

### Startup order
1. Load `dump.rdb` (fast baseline)
2. Replay `appendonly.aof` (apply changes since last snapshot)

---

## Replication

Scale reads horizontally with primary-replica replication.

```bash
# Start primary
boolcache-server --port 6379

# Start replicas
boolcache-server --port 6380 --replicaof 127.0.0.1:6379
boolcache-server --port 6381 --replicaof 127.0.0.1:6379
```

Replicas perform a full resync on connect (RDB snapshot), then receive every subsequent write in real time. They reconnect automatically if the connection drops.

```bash
# Check replication status
redis-cli -p 6379 INFO replication

# Promote a replica to primary
redis-cli -p 6380 REPLICAOF NO ONE
```

See [docs/replication.md](docs/replication.md) for full details.

---

## Embed as a Library

Add to your `Cargo.toml`:

```toml
[dependencies]
boolcache = { path = "crates/boolcache" }
```

Basic usage:

```rust
use boolcache::db::{Config, Db};
use boolcache::commands::string::{set, get, SetOptions};

let db = Db::open(Config::default()).unwrap();

set(&db, "greeting", b"hello".to_vec(), SetOptions::default()).unwrap();

let val = get(&db, "greeting").unwrap();
println!("{:?}", val); // Some(b"hello")
```

---

## Project Layout

```
BoolCache/
├── Cargo.toml                        # workspace
├── crates/
│   ├── boolcache/                    # library crate
│   │   └── src/
│   │       ├── commands/             # String / List / Hash / Set / ZSet
│   │       ├── persistence/          # AOF + RDB
│   │       ├── replication/          # replication state & broadcast
│   │       ├── db.rs                 # thread-safe Db handle
│   │       ├── store.rs              # HashMap storage + expiry
│   │       └── types.rs              # Value enum, Entry
│   └── boolcache-server/             # binary crate
│       └── src/
│           ├── connection.rs         # per-connection command dispatch
│           ├── replication/          # replica sync task
│           ├── resp.rs               # RESP2 parser + serializer
│           ├── server.rs             # tokio TCP listener
│           └── main.rs               # CLI entry point
└── docs/
    └── replication.md                # replication guide
```

---

## License

MIT
