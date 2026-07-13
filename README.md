# Wednesday

Wednesday is a distributed hash table (DHT) storage system written in C++. Storage nodes form a consistent-hashing ring and route block read/write requests to the node responsible for each file. Communication uses gRPC over Protocol Buffers.

## Overview

Each node is identified by the SHA-256 hash of its `ip:port` address. Files are assigned to nodes by hashing the file path the same way. A node that does not own a block forwards the request to its successor until the responsible node is reached.

The ring is maintained with Chord-style join logic:

1. A new node contacts a bootstrap (genesis) node via `JoinNetwork`.
2. The bootstrap routes the join request around the ring until the correct insertion point is found.
3. The new node receives its successor and notifies that successor via `NotifyNode`, updating the predecessor link.

There is no replication or consensus layer yet—each block lives on a single node.

## Architecture

```
                    ┌─────────────┐
                    │   Client    │
                    └──────┬──────┘
                           │ gRPC (ReadBlock / WriteBlock)
                           ▼
              ┌────────────────────────┐
              │   Storage Node Ring    │
              │                        │
              │  N1 ──► N2 ──► N3 ──► N1
              │   ▲                  │
              │   └──── predecessor ─┘
              └────────────────────────┘
                           │
                    local filesystem
```

### Components

| Path | Description |
|------|-------------|
| `proto/filesystem.proto` | gRPC service and message definitions |
| `include/Wednesday.hpp` | `StorageNodeService` implementation (routing, join, I/O) |
| `src/server.cpp` | Node process: CLI, server startup, bootstrap join |
| `src/client.cpp` | Simple test client for write-then-read across nodes |
| `cmakelists.txt` | CMake build (C++17, gRPC, Protobuf, OpenSSL) |

### gRPC API

Defined in `proto/filesystem.proto`:

| RPC | Purpose |
|-----|---------|
| `ReadBlock` | Read a byte range from a file at a given offset |
| `WriteBlock` | Write bytes to a file at a given offset |
| `JoinNetwork` | Insert a new node into the ring; returns assigned successor |
| `NotifyNode` | Tell a successor to update its predecessor pointer |

Node IDs and file ownership are determined by SHA-256 hashes. Ring membership checks handle the wrap-around case when predecessor ID is greater than the current node ID.

## Prerequisites

- CMake 3.15+
- C++17 compiler
- gRPC and Protobuf (expected under `grpc_installed/` at the project root)
- OpenSSL (for SHA-256 in `Wednesday.hpp`)

## Build

```bash
mkdir build && cd build
cmake ..
cmake --build .
```

This produces two executables:

- `server` — storage node
- `client` — test client

## Running

### Start the genesis node

The first node in the ring has no bootstrap address:

```bash
./server 50051
```

### Join additional nodes

Pass `bootstrap_ip:port` as the second argument:

```bash
./server 50052 127.0.0.1:50051
./server 50053 127.0.0.1:50051
```

Each node logs its address, node ID (hash), and join status.

### Run the test client

The client writes a test file through one node and reads it back through another to verify DHT routing:

```bash
./client
```

By default it writes via `127.0.0.1:50051` and reads via `127.0.0.1:50053`. Edit `src/client.cpp` if your ports differ.

## Current Limitations

- **Single copy per block** — no replication; a node failure loses its data.
- **No membership repair** — nodes cannot leave gracefully; failed nodes are not detected or removed.
- **Insecure transport** — gRPC uses plaintext credentials.
- **Local filesystem paths** — `file_path` in requests is used directly on disk; there is no namespace or bucket abstraction.
- **No concurrency control** — concurrent writes to the same block are not coordinated across the cluster.

## Future Work: Paxos Consensus

The next major step is adding **Paxos** (or a practical variant such as Multi-Paxos) to provide fault-tolerant, strongly consistent coordination and replication. Currently, ring metadata (successor/predecessor) and file blocks are updated on a single node with no agreement protocol. Paxos would let a quorum of nodes agree on shared state before it becomes visible, which addresses durability and consistency gaps in the current design.

### Proposed integration areas

1. **Replicated metadata log**
   - Use Multi-Paxos to maintain an append-only log of ring events: joins, leaves, successor/predecessor changes.
   - All nodes apply the same ordered log so every replica has a consistent view of membership.

2. **Block replication**
   - For each file path hash, replicate `WriteBlock` operations to a Paxos group (e.g. the node and its *N* clockwise successors).
   - Reads could be served from the leader or any replica that has caught up on the log.
   - A failed primary would not lose data if a quorum has committed the write.

3. **Leader election**
   - Elect a stable leader per Paxos group (or per hash range) to serialize writes and reduce dueling-proposer conflicts.
   - `JoinNetwork` could be handled by the metadata Paxos group rather than best-effort pointer updates alone.

4. **Membership and failure detection**
   - Combine heartbeats or gossip with Paxos-proposed "node failed" records to remove stale entries from the ring safely.
   - Rebalance hash ranges only after a quorum agrees a node is gone.

5. **Write ordering**
   - Paxos instance per file (or per file block) to guarantee linearizable writes when multiple clients update the same offset.

### Suggested implementation phases

| Phase | Goal |
|-------|------|
| 1 | Standalone Paxos library: proposers, acceptors, learners; persistent log on disk |
| 2 | Multi-Paxos leader + log for cluster metadata (membership only) |
| 3 | Replicate `WriteBlock` / `ReadBlock` through the log for a configurable replication factor |
| 4 | Failure detection, rebalancing, and client-facing consistency guarantees |

### Design notes

- **Instance numbering**: tie Paxos instance IDs to logical timestamps or a separate metadata service keyed by file path hash.
- **Quorum size**: for `N` replicas, use majority quorums (`⌊N/2⌋ + 1`) for both accept and learn steps.
- **Separation of concerns**: keep the existing DHT routing layer for locating the responsible replica set; use Paxos inside that set for agreement.
- **Proto changes**: new RPCs or messages will likely be needed for prepare/accept/learn phases, or an internal library can hide Paxos behind the existing `WriteBlock` handler.

## License

Not specified in the repository.
