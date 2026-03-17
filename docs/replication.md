# BoolCache Replication

BoolCache supports **primary-replica replication**, allowing one primary node to stream every write to one or more read replicas in real time. This is the recommended approach for horizontal read scalability.

---

## Architecture

```
          Writes
clients ──────────► Primary (:6379)
                        │
              broadcast channel
                 ┌──────┴──────┐
                 ▼             ▼
          Replica A       Replica B
           (:6380)         (:6381)
              ▲               ▲
              └──── Reads ────┘
              (clients connect directly)
```

- **One primary** accepts all writes.
- **One or more replicas** receive an initial full snapshot, then a continuous stream of write commands.
- Replicas reconnect automatically on failure with exponential back-off (1 s → 2 s → 4 s … up to 60 s).

---

## Starting Nodes

### Primary

```bash
cargo run -p boolcache-server -- --port 6379
```

A node starts in `standalone` mode and is automatically promoted to `primary` the moment its first replica connects.

### Replica

```bash
cargo run -p boolcache-server -- --port 6380 --replicaof 127.0.0.1:6379
```

Multiple replicas can be attached to the same primary:

```bash
cargo run -p boolcache-server -- --port 6381 --replicaof 127.0.0.1:6379
cargo run -p boolcache-server -- --port 6382 --replicaof 127.0.0.1:6379
```

#### Flag reference

| Flag | Description |
|------|-------------|
| `--replicaof HOST:PORT` | Connect to a primary on startup |
| `--port PORT` | Port this node listens on (default `6379`) |
| `--no-aof` | Disable AOF on this node (replicas often skip local AOF) |

---

## Handshake & Full Resync

When a replica first connects to the primary it performs the following handshake (compatible with the Redis RESP protocol):

```
Replica → Primary:  PING
Primary → Replica:  +PONG

Replica → Primary:  REPLCONF listening-port 0
Primary → Replica:  +OK

Replica → Primary:  PSYNC ? -1
Primary → Replica:  +FULLRESYNC <repl_id> <offset>
Primary → Replica:  $<rdb_len>\r\n<raw RDB bytes>
```

After the RDB snapshot is received the replica atomically replaces its entire in-memory store and starts applying the live command stream.

---

## Incremental Streaming

After the full resync every write command executed on the primary is:

1. Written to the primary's AOF (if enabled).
2. Encoded as a RESP array.
3. Broadcast over an in-memory channel (capacity: **4 096 frames**) to all connected replica tasks.
4. Written to each replica's TCP socket.

The replica applies each received command to its store and appends it to its own local AOF (if enabled).

### Lag protection

If a replica's consumer falls more than 4 096 frames behind (e.g. due to a slow network) it is disconnected and must perform a new full resync on reconnect.

---

## Verifying Replication

### Check status on the primary

```
$ redis-cli -p 6379 INFO replication
# Replication
role:master
connected_slaves:2
master_replid:a3f1c09e...
master_repl_offset:1024
```

### Check status on a replica

```
$ redis-cli -p 6380 INFO replication
# Replication
role:slave
connected_slaves:0
master_replid:a3f1c09e...
master_repl_offset:1024
```

### End-to-end smoke test

```bash
# Write to primary
redis-cli -p 6379 SET greeting "hello"

# Read from replicas (both should return "hello")
redis-cli -p 6380 GET greeting
redis-cli -p 6381 GET greeting
```

---

## Promoting a Replica to Primary

Send `REPLICAOF NO ONE` to any replica to detach it from the primary and promote it:

```bash
redis-cli -p 6380 REPLICAOF NO ONE
```

After this command the replica:
- Stops consuming from the primary's stream.
- Switches its role to `primary`.
- Can itself accept new replicas.

You can also point a running node at a new primary dynamically:

```bash
redis-cli -p 6380 REPLICAOF 127.0.0.1 6382
```

---

## Persistence Interaction

| Node | AOF | RDB |
|------|-----|-----|
| Primary | Enabled (recommended) | Auto-saved |
| Replica | Optional (`--no-aof` to disable) | Receives snapshot on resync |

Replicas write received commands to their local AOF (when enabled), so they can recover their state independently if they restart before reconnecting to the primary.

On restart a replica with `--replicaof` will:
1. Load its own local RDB + AOF (if present).
2. Reconnect to the primary and perform a new full resync, replacing its store with the primary's current snapshot.

---

## Current Limitations

| Feature | Status |
|---------|--------|
| Full resync | Supported |
| Incremental streaming | Supported |
| Auto-reconnect with back-off | Supported |
| Automatic read routing | Not included — clients connect to replica ports directly |
| Automatic failover / Sentinel | Not included |
| Partial resync (PSYNC with offset) | Not included — always full resync |
| Sub-replicas (replica of replica) | Not included |
