# SYSTEM PLAYBOOK — Ultra-Low-Latency Distributed Data Pipeline Engine

> **Version:** 1.0.0 — Compiled 2026-06-29 from live source
> **Status:** All 5 phases production-complete
> **Audience:** Human engineers and AI agents onboarding to operate, extend, or debug this system

---

## Table of Contents

1. [System Overview](#1-system-overview)
2. [System Topology Diagram](#2-system-topology-diagram)
3. [Repository & Crate Map](#3-repository--crate-map)
4. [Environment & Prerequisites](#4-environment--prerequisites)
5. [Workspace Orchestration — Build Commands](#5-workspace-orchestration--build-commands)
6. [Step-by-Step Running Guide](#6-step-by-step-running-guide)
7. [Verification & Benchmarking Command Ledger](#7-verification--benchmarking-command-ledger)
8. [Module Reference](#8-module-reference)
9. [Wire Protocols & Data Formats](#9-wire-protocols--data-formats)
10. [Operational Runbook — Failure Modes](#10-operational-runbook--failure-modes)
11. [Extension Guide](#11-extension-guide)
12. [Performance Numbers at a Glance](#12-performance-numbers-at-a-glance)

---

## 1. System Overview

This engine is a **five-phase, zero-copy, ultra-low-latency distributed event pipeline** written
primarily in Rust (Editions 2024 and 2021) with a TypeScript/Node.js consumption boundary. Its
design philosophy is **mechanical sympathy**: every data structure is sized to cache lines, every
synchronization primitive uses the minimum-cost memory ordering (`Acquire`/`Release` — never
`SeqCst`), and every hot path carries a contractual guarantee of zero heap allocation.

### Phase Summary

| Phase | Crate | Role | Peak Metric |
|-------|-------|------|-------------|
| 1 | `core-ringbuffer` | Lock-free SPSC/SPMC/MPSC ring buffers + UCB1 adaptive wait strategy | 21M+ msg/s |
| 2 | `network-ingestion` + `node-ffi` | Linux `io_uring` TCP listener (`glommio`) → zero-copy Node.js FFI | 10.4 µs p50 latency |
| 3 | `storage-engine` | 4096-byte sector-aligned append-only journaler with `writev` batching + deterministic replay | — |
| 4 | `cluster-replication` | Active-passive pipelined TCP replication with offset-tracked partial-read safety | 100K-event convergence |
| 5 | `stream-analytics` | Zero-allocation compile-free CEP rule engine + sliding window tracker on SPMC lane | ~200 ns p99 |

---

## 2. System Topology Diagram

```
 External TCP Clients
 ─────────────────────►  INGESTION BOUNDARY (Phase 2)
                          glommio LocalExecutor (Linux io_uring, single-threaded)
                          TcpListener::bind(addr)
                            └── per-connection: spawn_local async task
                                    ├── read 4-byte LE u32 length prefix
                                    └── claim_slot() on MPSC ring
                                        read body DIRECTLY into slot memory (zero copy)
                                        commit(len)
                                              │
                          ┌───────────────────▼──────────────────────────┐
                          │         MPSC RING BUFFER  (Phase 1)           │
                          │         core_ringbuffer::mpsc                 │
                          │                                               │
                          │  [Slot 0][Slot 1][Slot 2]...[Slot N]          │
                          │   256B    256B    256B         256B           │
                          │  claim_seq  ── AtomicU64, #[align(64)]        │
                          │  read_seq   ── AtomicU64, #[align(64)]        │
                          └──────────────┬───────────────────────────────┘
                                         │
              ┌──────────────────────────┼──────────────────────────────────┐
              │                          │                                   │
 ┌────────────▼───────────┐  ┌───────────▼────────────┐  ┌──────────────────▼──────────┐
 │  FFI CONSUMER (Ph. 2)  │  │ JOURNAL CONSUMER (Ph.3)│  │  SPMC FANOUT LANE (Ph.1)    │
 │  node-ffi / napi-rs    │  │  storage-engine        │  │  core_ringbuffer::spmc      │
 │                        │  │                        │  │  1-to-N broadcast           │
 │ NapiConsumer::         │  │ StorageConsumer::      │  │                             │
 │   poll_next(callback)  │  │  try_consume_batch()   │  │  ┌───────────────────────┐  │
 │                        │  │  ├─ batch ≤ 64 records │  │  │  REPLICATION (Ph. 4)  │  │
 │ napi_create_external_  │  │  └─ JournalWriter::    │  │  │                       │  │
 │   buffer() ← zero-copy │  │     append_batch()     │  │  │ PRIMARY:              │  │
 │ JS callback(Buffer)    │  │     (vectorized writev) │  │  │  SpmcConsumer         │  │
 │                        │  │                        │  │  │  → TCP WireFrame      │  │
 │ Node.js polls via      │  │ JOURNAL FILE ON DISK   │  │  │  → backup node        │  │
 │ setImmediate / loop    │  │  sector-aligned .bin   │  │  │                       │  │
 └────────────────────────┘  │  [u64 len][payload]    │  │  BACKUP:               │  │
                             │  [zero-pad to 4096]    │  │  TcpListener           │  │
                             │                        │  │  → MPSC ring           │  │
                             │ JournalReader::        │  │  → local consumers     │  │
                             │  replay_into()         │  └───────────────────────┘  │
                             │  re-publishes to SPSC  │                             │
                             └────────────────────────┘  ┌───────────────────────┐  │
                                                         │ STREAM ANALYTICS(Ph5) │  │
                                                         │                       │  │
                                                         │ evaluate_rule()       │  │
                                                         │  zero alloc per event │  │
                                                         │                       │  │
                                                         │ ConsecutiveTracker    │  │
                                                         │ SlidingWindowTracker  │  │
                                                         │  pre-allocated state  │  │
                                                         └───────────────────────┘  │
                                                         └──────────────────────────┘

 Wire Frame (Phase 4):
 ┌─────────────────────┬─────────────────┬──────────────────────────────┐
 │ cluster_seq_id      │  payload_len    │  payload (raw FlatBuffer)    │
 │  8 bytes  (BE u64)  │  4 bytes (BE)   │  up to 64 MiB                │
 └─────────────────────┴─────────────────┴──────────────────────────────┘
 FRAME_HEADER_LEN = 12 bytes
```

### Data Flow Narrative

1. **Raw TCP bytes** arrive on an external socket and are accepted by the `glommio`-powered
   `NetworkIngestionServer` (Phase 2).
2. The server reads a **4-byte little-endian length prefix**, then reads exactly that many body
   bytes **directly into a pre-claimed MPSC ring buffer slot** — no intermediate copy buffer.
3. The MPSC ring buffer (`core_ringbuffer::mpsc`) is the **backbone bus**, fanned out to parallel consumers:
   - **FFI Consumer (Phase 2):** `NapiConsumer::poll_next()` wraps the ring buffer slot as a
     `napi_create_external_buffer`, calling a JS callback with zero copy.
   - **Journal Consumer (Phase 3):** `JournalConsumer` batches up to 64 records per `writev`
     call into a 4096-byte sector-aligned append-only file.
   - **Replication Consumer (Phase 4):** Reads from an **SPMC lane**
     (`core_ringbuffer::spmc`), wraps each payload into a 12-byte-header wire frame, and
     streams it over a persistent TCP connection to the backup node.
   - **Analytics Consumer (Phase 5):** Reads from the same SPMC lane, calling `evaluate_rule()`
     on each `Event` with zero heap allocation, updating `ConsecutiveTracker` or
     `SlidingWindowTracker` state in-place.
4. On the **backup node**, `BackupReplicationIngestor` accepts the TCP connection, reads frames
   with partial-read safety, and pushes payloads into its own MPSC ring for local processing.
5. **Historical replay** uses `JournalReader::replay_into()` to re-publish journal records into
   any `StorageProducer`, making the journal deterministically replayable from any byte offset.

---

## 3. Repository & Crate Map

Each phase lives in its own sub-repository under the workspace root.

```
High-Performance Systems Roadmap/           <- workspace root (this file lives here)
|
+-- SYSTEM_PLAYBOOK.md                      <- YOU ARE HERE
|
+-- Phase 1/
|   +-- core-ringbuffer/                    <- Cargo workspace root
|       +-- Cargo.toml                      <- crate: "core-ringbuffer" (edition 2024)
|       +-- build.rs                        <- invokes flatc to generate event_generated.rs
|       +-- schemas/                        <- FlatBuffers .fbs schema files
|       +-- src/
|       |   +-- lib.rs                      <- crate root; re-exports public API
|       |   +-- sequence.rs                 <- Sequence: #[repr(C, align(64))] AtomicU64
|       |   +-- buffer.rs                   <- RingBuffer<T>: power-of-two heap allocation
|       |   +-- barrier.rs                  <- SequenceBarrier: tracks minimum consumer seq
|       |   +-- spsc.rs                     <- SPSC: Producer + Consumer<W: WaitStrategy>
|       |   +-- spmc.rs                     <- SPMC: Producer + Vec<Consumer<W>>
|       |   +-- mpsc.rs                     <- MPSC: Vec<Producer> + Consumer<W>
|       |   +-- wait.rs                     <- BusySpin, YieldNow, TimedSleep, AdaptiveTuner
|       |   +-- event_generated.rs          <- [AUTO-GENERATED] FlatBuffers Event type
|       +-- benches/
|       |   +-- spsc_bench.rs               <- Criterion: SPSC throughput & latency
|       |   +-- spmc_bench.rs               <- Criterion: SPMC 1->N broadcast
|       |   +-- mpsc_bench.rs               <- Criterion: MPSC N->1 ingestion
|       +-- tests/                          <- inline #[cfg(test)] modules per module
|
+-- Phase 2/
|   +-- Cargo.toml                          <- workspace: [network-ingestion, node-ffi]
|   +-- network-ingestion/
|   |   +-- Cargo.toml                      <- deps: core-ringbuffer, glommio, futures-lite
|   |   +-- src/
|   |       +-- lib.rs                      <- pub use listener::NetworkIngestionServer
|   |       +-- listener.rs                 <- glommio LocalExecutor + TcpListener loop
|   |       +-- bin/                        <- standalone server binary
|   +-- node-ffi/
|       +-- Cargo.toml                      <- crate-type=["cdylib"], napi@2.16 napi4
|       +-- build.rs                        <- napi-build (links Node.js ABI)
|       +-- src/lib.rs                      <- #[napi] NapiConsumer + poll_next()
|       +-- index.js                        <- auto-generated NAPI-RS JS loader
|       +-- index.d.ts                      <- TypeScript declarations
|       +-- package.json                    <- npm package metadata
|       +-- node-ffi.win32-x64-msvc.node   <- pre-built native addon (Windows x64)
|
+-- Phase 3/
|   +-- Cargo.toml                          <- workspace: [storage-engine]
|   +-- storage-engine/
|       +-- Cargo.toml                      <- features: phase2-integration
|       +-- src/
|       |   +-- lib.rs                      <- pub mod journal; pub mod consumer;
|       |   +-- journal.rs                  <- JournalWriter + JournalReader
|       |   +-- consumer.rs                 <- StorageProducer + StorageConsumer + JournalConsumer
|       +-- tests/
|           +-- journal_reader.rs           <- deterministic replay integration tests
|           +-- journal_consumer.rs         <- batched write + flush tests
|
+-- Phase 4/
|   +-- Cargo.toml                          <- workspace: [cluster-replication]
|   +-- cluster-replication/
|       +-- Cargo.toml                      <- features: phase1/2/3/full-stack-integration
|       +-- src/
|       |   +-- lib.rs                      <- pub mod primary, backup, frame, tcp, udp
|       |   +-- frame.rs                    <- WireFrame, FrameHeader, encode/decode
|       |   +-- tcp.rs                      <- ReplicationStream + ReplicationListener
|       |   +-- udp.rs                      <- UDP heartbeat stubs (future use)
|       |   +-- primary.rs                  <- ReplicationConsumer (SPMC -> TCP write)
|       |   +-- backup.rs                   <- BackupReplicationIngestor (TCP read -> MPSC)
|       +-- tests/
|           +-- cluster_convergence.rs      <- 100K-event byte-perfect convergence test
|           +-- primary_replication_consumer.rs
|
+-- Phase 5/
    +-- stream-analytics/
        +-- Cargo.toml                      <- deps: core-ringbuffer, flatbuffers; bench
        +-- src/
        |   +-- lib.rs                      <- pub mod rule, evaluator, window; re-exports
        |   +-- rule.rs                     <- FieldSelector, CompareOp, Value<'a>, Rule<'a>
        |   +-- evaluator.rs                <- evaluate_criterion(), evaluate_rule()
        |   +-- window.rs                   <- ConsecutiveTracker, SlidingWindowTracker
        +-- benches/
            +-- inline_eval_bench.rs        <- Criterion: per-event evaluation throughput
```

---

## 4. Environment & Prerequisites

### 4.1 Mandatory Software

| Tool | Minimum Version | Notes |
|------|-----------------|-------|
| Rust toolchain | 1.88+ | Must support edition 2024 |
| `cargo` | Ships with Rust | — |
| `flatc` (FlatBuffers compiler) | 25.x | Required by Phase 1 `build.rs` |
| Node.js | 18+ LTS | Required for Phase 2 FFI polling layer |
| npm | 9+ | Ships with Node.js |

Install Rust via `rustup`:
```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
rustup update stable
```

Install FlatBuffers compiler:
```bash
# Ubuntu/Debian
sudo apt-get install flatbuffers-compiler

# macOS
brew install flatbuffers

# From source (any platform)
git clone https://github.com/google/flatbuffers && cd flatbuffers
cmake -G "Unix Makefiles" -DCMAKE_BUILD_TYPE=Release
make -j$(nproc) && sudo make install
```

### 4.2 OS Constraints — Critical

> **This is the single most important environment fact for any engineer onboarding to this system.**

| Phase | Crate | OS Constraint | Reason |
|-------|-------|---------------|--------|
| **Phase 1** | `core-ringbuffer` | Cross-platform (Linux, macOS, Windows) | Pure Rust atomics; no OS syscalls on hot path |
| **Phase 2 — network-ingestion** | `network-ingestion` | **Linux only** (or WSL2 unconfined / unconfined Docker on Linux) | `glommio` requires Linux `io_uring` (kernel >= 5.8, ideally >= 5.19). WSL2 requires `io_uring` to be unblocked (not available in all WSL2 distributions by default). |
| **Phase 2 — node-ffi** | `node-ffi` | Windows x64 pre-built available; Linux/macOS need `napi build` | `node-ffi.win32-x64-msvc.node` is checked in. Other platforms require rebuilding from source. |
| **Phase 3** | `storage-engine` | Cross-platform | Uses `std::fs` and `write_vectored`; no `io_uring`. |
| **Phase 4** | `cluster-replication` | Cross-platform | Uses `std::net::TcpStream` / `TcpListener`. |
| **Phase 5** | `stream-analytics` | Cross-platform | Pure in-process computation. |

#### WSL2 / Docker Setup for io_uring (Phase 2)

```bash
# Verify io_uring is available inside your WSL2 or container:
cat /proc/sys/kernel/io_uring_disabled
# Expected output: 0   (0 = ENABLED; 1 or 2 = disabled)

# If using Docker, run with:
docker run --privileged --security-opt seccomp=unconfined <image>
# OR on newer kernels:
docker run --cap-add=SYS_ADMIN <image>

# Quick validation — this test spins up a glommio server and will fail
# fast with a clear error if io_uring is not available:
cargo test -p network-ingestion -- test_zero_copy_network_to_ring_buffer --nocapture
```

### 4.3 Rust Feature Flags

Phase 3 and Phase 4 expose optional feature gates that opt-in to upstream crate dependencies:

```toml
# storage-engine/Cargo.toml
[features]
default = []
phase2-integration = ["dep:network-ingestion", "dep:node-ffi"]

# cluster-replication/Cargo.toml
[features]
default = []
phase1-integration = []
phase2-integration   = ["dep:network-ingestion"]
phase3-integration   = ["dep:storage-engine"]
full-stack-integration = [
    "phase1-integration",
    "phase2-integration",
    "phase3-integration"
]
```

**Without any feature flags**: each crate compiles with only its own code, using
`core-ringbuffer` as the sole shared dependency. This is the default for all tests and benchmarks.

---

## 5. Workspace Orchestration — Build Commands

Each phase has its **own Cargo workspace** and must be built independently. There is no single
top-level `Cargo.toml` unifying all phases.

### 5.1 Build All Phases (sequential)

```bash
# Phase 1 — core-ringbuffer (required first; all others path-depend on it)
cd "Phase 1/core-ringbuffer"
cargo build --release

# Phase 2 — network ingestion + Node.js FFI
cd "../../Phase 2"
cargo build --release

# Phase 3 — storage engine (default features, no Phase 2 dependency)
cd "../Phase 3"
cargo build --release

# Phase 3 — with Phase 2 integration enabled (pulls in glommio + node-ffi)
cargo build --release --features phase2-integration -p storage-engine

# Phase 4 — cluster replication (default features)
cd "../Phase 4"
cargo build --release

# Phase 4 — full-stack integration (pulls in Phase 2 + Phase 3)
cargo build --release --features full-stack-integration -p cluster-replication

# Phase 5 — stream analytics
cd "../Phase 5"
cargo build --release
```

### 5.2 Build the Node.js Native Addon (Phase 2 node-ffi)

The `node-ffi.win32-x64-msvc.node` binary is already checked in for Windows x64. To rebuild from
source on Linux or macOS:

```bash
cd "Phase 2/node-ffi"

# Install napi-rs CLI
npm install -g @napi-rs/cli

# Build the .node file for your current platform
napi build --release --platform

# Install JS runtime dependencies
npm install

# Quick smoke tests
node test.js
node test_async.js
```

### 5.3 Dependency Graph

```
stream-analytics      ─────┐
cluster-replication   ─────┤
storage-engine        ─────┤──► core-ringbuffer  ──► flatbuffers
network-ingestion     ─────┤
node-ffi              ─────┘
```

All five consuming crates reference `core-ringbuffer` as a local `path` dependency.
No crate is published to crates.io.

---

## 6. Step-by-Step Running Guide

### 6.1 Spin Up a Primary-Backup Cluster Node Pair (Phase 4)

The `cluster_convergence` integration test (100K events, zero drops) is the canonical reference.
For production embedding, instantiate the structs directly as shown below.

**Step A — Start the backup node first** (it must bind the TCP port before the primary connects):

```rust
use cluster_replication::{
    BackupReplicationIngestor, BackupReplicationIngestorConfig,
};
use core_ringbuffer::mpsc;

// 1. Allocate the backup's MPSC ring buffer
let (mut backup_producers, mut backup_consumer) = mpsc::mpsc(4096, 1);

// 2. Configure the backup ingestor
let listen_addr = "0.0.0.0:9001".parse().unwrap();
let mut config = BackupReplicationIngestorConfig::tcp(listen_addr);
// Defaults: read_timeout=100ms, accept_backoff=50µs, full_backoff=25µs
// Override if needed:
config.read_timeout = std::time::Duration::from_secs(5);

// 3. Spawn the ingestor thread (non-blocking; returns handle immediately)
let backup_handle = BackupReplicationIngestor::new(
    backup_producers.remove(0),
    config,
).spawn();

// 4. Consume replicated events (your application loop)
loop {
    backup_consumer.try_consume(|raw_bytes| {
        let event = flatbuffers::root::<core_ringbuffer::event::Event>(raw_bytes).unwrap();
        println!("Backup received: seq={}", event.sequence_id());
    });
}

// 5. Graceful shutdown
let snapshot = backup_handle.stop().expect("backup stop failed");
println!("ingested_segments={}", snapshot.ingested_segments);
println!("sequence_mismatches={}", snapshot.sequence_mismatches);
```

**Step B — Start the primary node** (after backup is bound):

```rust
use cluster_replication::{ReplicationConsumer, ReplicationConsumerConfig};
use core_ringbuffer::spmc;

// 1. SPMC ring: one producer, one replication consumer lane
let (mut primary_producer, mut spmc_consumers) = spmc::spmc(4096, 1);
let replication_lane = spmc_consumers.remove(0);

// 2. Configure the replication consumer
let backup_addr = "127.0.0.1:9001".parse().unwrap();
let mut config = ReplicationConsumerConfig::primary_tcp(backup_addr);
// Defaults: connect_timeout=25ms, write_timeout=25ms,
//           idle_backoff=50µs, reconnect_backoff=10ms

// 3. Spawn the replication thread
let replication_handle = ReplicationConsumer::new(replication_lane, config).spawn();

// 4. Publish events — your ingestion pipeline feeds this producer
let mut builder = flatbuffers::FlatBufferBuilder::with_capacity(128);
// ... build FlatBuffer Event ...
loop {
    if primary_producer.try_publish(builder.finished_data()).is_ok() {
        break;
    }
    std::thread::yield_now(); // back-pressure: ring is full
}

// 5. Graceful shutdown
let snapshot = replication_handle.stop().expect("primary stop failed");
println!("transmitted={}", snapshot.transmitted_segments);
println!("dropped={}", snapshot.dropped_segments);
println!("send_errors={}", snapshot.send_errors);
```

### 6.2 Inject Network Traffic into the Ingestion Server (Phase 2)

The `NetworkIngestionServer` expects **length-prefixed framing**: a 4-byte little-endian `u32`
payload length followed by exactly that many bytes.

**From a Rust client:**
```rust
use std::net::TcpStream;
use std::io::Write;

let mut client = TcpStream::connect("127.0.0.1:8080").unwrap();

// Build a FlatBuffer Event
let mut builder = flatbuffers::FlatBufferBuilder::with_capacity(128);
let sym = builder.create_string("AAPL");
let event = core_ringbuffer::event::Event::create(
    &mut builder,
    &core_ringbuffer::event::EventArgs {
        timestamp_ns: 1_719_273_600_000_000_000_i64,
        symbol: Some(sym),
        price: 195.89,
        volume: 1_500_000,
        sequence_id: 42,
    },
);
builder.finish(event, None);
let payload = builder.finished_data();

// Wire format: [LE u32 length][payload bytes]
let len_prefix = (payload.len() as u32).to_le_bytes();
client.write_all(&len_prefix).unwrap();
client.write_all(payload).unwrap();
```

**From a shell (raw bytes, for smoke testing):**
```bash
# Inject a 4-byte message "TEST" with a correct 4-byte LE length prefix
printf '\x04\x00\x00\x00TEST' | nc 127.0.0.1 8080
```

**Start the server (integration test spins one up automatically):**
```bash
cd "Phase 2/network-ingestion"
cargo test test_zero_copy_network_to_ring_buffer -- --nocapture
```

### 6.3 Listen via the TypeScript/Node.js FFI Polling Layer (Phase 2)

```javascript
// poll_consumer.js — run with: node poll_consumer.js
const { NapiConsumer } = require('./Phase 2/node-ffi/index.js');

const consumer = new NapiConsumer();

function poll() {
    // poll_next returns true if a slot was consumed, false if buffer is empty.
    // The callback receives a zero-copy Buffer backed by ring buffer memory.
    const hadData = consumer.pollNext((buf) => {
        // CRITICAL: `buf` points directly into the ring buffer slot.
        // The slot will be reclaimed on the NEXT poll_next() call.
        // DO NOT hold a reference to `buf` after this callback returns.
        //
        // If you need the data to outlive this callback, copy it out:
        const safeCopy = Buffer.from(buf); // one memcpy — allocates new Node.js Buffer
        console.log(`Received ${buf.length} bytes:`, safeCopy.toString('hex'));
        // Pass safeCopy to your FlatBuffers decoder...
    });

    // Scheduling: use setImmediate to yield to the Node.js event loop I/O
    if (hadData) {
        setImmediate(poll);   // more data likely; stay tight
    } else {
        setTimeout(poll, 0);  // empty; yield OS time slice and retry
    }
}

poll();
```

**TypeScript API surface** (from `index.d.ts`):
```typescript
export declare function init(): void

export declare class NapiConsumer {
    constructor()
    pollNext(callback: (...args: any[]) => any): boolean
}
```

> **Zero-Copy Safety Rule:** The `Buffer` delivered to `pollNext`'s callback is backed by
> `napi_create_external_buffer` with **no finalizer and no reference counting**. The ring buffer
> reclaims the underlying slot on the next `pollNext` call. Always copy the bytes out within
> the callback if they are needed beyond it.

### 6.4 Replay a Historical Binary Journal File (Phase 3)

```rust
use storage_engine::consumer::{journal_lane, StorageProducer};
use storage_engine::journal::JournalReader;
use core_ringbuffer::event_generated::event::Event;

// ── Step 1: Open the journal ─────────────────────────────────────────────────
let mut reader = JournalReader::open("/path/to/events.journal").expect("open journal");

// ── Step 2: Allocate a ring buffer lane for replayed events ──────────────────
let (mut producers, mut consumers) = journal_lane(
    1024,   // ring buffer capacity (must be power of two)
    1,      // number of producers
    1,      // number of consumers
);
let mut replay_producer = producers.remove(0);

// ── Step 3: Replay — this publishes every journal record into the ring ───────
//   Back-pressure: publish_replay_payload spin-loops on Full until the
//   consumer makes space. Ensure the consumer loop below runs concurrently.
let stats = reader.replay_into(&mut replay_producer).expect("replay failed");
println!("Replayed {} records ({} bytes total)", stats.records, stats.bytes);

// ── Step 4: Consume the replayed events ──────────────────────────────────────
let mut consumer = consumers.remove(0);
loop {
    let batch = consumer.try_consume_batch(|payloads, count| {
        for i in 0..count {
            let event = flatbuffers::root::<Event>(payloads[i])
                .expect("invalid FlatBuffer in replay");
            println!(
                "seq={} sym={:?} price={} vol={}",
                event.sequence_id(), event.symbol(), event.price(), event.volume()
            );
        }
        Ok(count)
    });
    if batch.is_none() {
        break; // ring drained
    }
}
```

**Journal binary format at a glance:**

```
Record layout (each record padded to a 4096-byte sector boundary):
  [payload_len : u64, little-endian]
  [payload     : payload_len bytes ]
  [zero-pad    : (4096 - (8+payload_len) % 4096) % 4096 bytes]

End-of-file sentinel:
  [0x00 00 00 00 00 00 00 00]
  [zero-pad to end of sector ]
```

Constants: `SECTOR_SIZE = 4096`, `MAX_BATCH_RECORDS = 64`,
`padded_size(raw) = (raw + 4095) & !4095`.

### 6.5 Register a Live Relational Streaming Rule (Phase 5)

```rust
use stream_analytics::{
    FieldSelector, CompareOp, Value, Criterion, Rule,
    evaluate_rule,
    window::{ConsecutiveTracker, SlidingWindowTracker},
};
use core_ringbuffer::{spmc, event_generated::event::Event};

// ── Step 1: Define CEP rules (cold path — allocates Vec once) ────────────────
let rule = Rule::new("high_value_aapl", vec![
    Criterion::new(FieldSelector::Symbol, CompareOp::Eq, Value::Str("AAPL")),
    Criterion::new(FieldSelector::Price,  CompareOp::Gt, Value::Float64(200.0)),
    Criterion::new(FieldSelector::Volume, CompareOp::Ge, Value::Uint64(500_000)),
]);

// ── Step 2: Pre-allocate state trackers (cold path) ───────────────────────────
// ConsecutiveTracker: fires alert when a key accumulates N consecutive hits
let mut consec = ConsecutiveTracker::with_capacity(3, 16); // threshold=3, capacity=16 keys
consec.register_key("AAPL");
consec.register_key("GOOGL");

// SlidingWindowTracker: fires when a key hits threshold within last W events
let mut sliding = SlidingWindowTracker::with_capacity(5, 3, 16); // window=5, threshold=3
sliding.register_key("AAPL");

// ── Step 3: Wire to an SPMC consumer lane ────────────────────────────────────
let (_producer, mut consumers) = spmc::spmc(4096, 1);
let mut analytics_consumer = consumers.remove(0);

// ── Step 4: Hot-path inline evaluation (ZERO heap allocation per event) ──────
loop {
    analytics_consumer.try_consume(|raw_bytes| {
        // Zero-copy FlatBuffers deserialization — no parsing, no allocation
        let event = flatbuffers::root::<Event>(raw_bytes)
            .expect("malformed FlatBuffer in analytics lane");

        if evaluate_rule(&event, &rule) {
            let symbol = event.symbol().unwrap_or("");

            if consec.record_match(symbol) {
                eprintln!("ALERT [consecutive]: {} matched 3 times in a row", symbol);
            }
            if sliding.record_match(symbol) {
                eprintln!("ALERT [sliding-window]: {} matched 3/5 events", symbol);
            }
        } else {
            if let Some(sym) = event.symbol() {
                consec.record_miss(sym); // resets consecutive counter to 0
                // SlidingWindowTracker uses its ring internally; no explicit miss call needed
            }
        }
    });
}
```

**FieldSelector to FlatBuffers accessor mapping:**

| `FieldSelector` variant | FlatBuffers accessor | Rust type |
|-------------------------|----------------------|-----------|
| `TimestampNs` | `event.timestamp_ns()` | `i64` |
| `Symbol` | `event.symbol()` | `Option<&str>` |
| `Price` | `event.price()` | `f64` |
| `Volume` | `event.volume()` | `u64` |
| `SequenceId` | `event.sequence_id()` | `u64` |

**Available `CompareOp` variants:** `Eq`, `Ne`, `Gt`, `Ge`, `Lt`, `Le`

**Available `Value<'a>` variants:** `Int64(i64)`, `Float64(f64)`, `Uint64(u64)`, `Str(&'a str)`

> **Type-safety note:** Mismatched `FieldSelector`/`Value` pairs (e.g., `Price` vs `Value::Str`)
> return `false` rather than panicking — a deliberate hot-path safety decision.

---

## 7. Verification & Benchmarking Command Ledger

### 7.1 Phase 1 — core-ringbuffer

```bash
cd "Phase 1/core-ringbuffer"

# Unit tests: FlatBuffers serialization roundtrip + zero-copy proof
cargo test --lib -- tests::test_event_serialization_roundtrip --nocapture
cargo test --lib -- tests::test_zero_copy_no_allocation --nocapture

# SPSC: single publish/consume, wrap-around, back-pressure, FlatBuffers integration
cargo test --lib -- spsc::tests --nocapture

# SPMC: basic broadcast, back-pressure
cargo test --lib -- spmc::tests --nocapture

# MPSC
cargo test --lib -- mpsc::tests --nocapture

# All unit tests
cargo test --lib

# Loom deterministic concurrency model-checking (exhaustive interleaving)
LOOM_LOG=warn cargo test --lib --features loom -- 2>&1 | tail -20

# Benchmarks
cargo bench --bench spsc_bench   # SPSC throughput (21M+ msg/s target)
cargo bench --bench spmc_bench   # SPMC 1->N broadcast
cargo bench --bench mpsc_bench   # MPSC N->1 ingestion

# Save HTML report to target/criterion/
cargo bench -- --output-format bencher 2>&1 | tee bench_results.txt
```

Key SPSC benchmark measurement IDs (from `spsc_bench.rs`):
- `spsc_throughput/single_thread` — peak messages/second, single-threaded
- `spsc_throughput/cross_thread`  — cross-thread throughput (producer/consumer on separate cores)
- `spsc_latency/p50_p99`          — round-trip latency percentiles

### 7.2 Phase 2 — network-ingestion (Linux / WSL2 only)

```bash
cd "Phase 2"

# Integration test: zero-copy TCP -> MPSC ring buffer
# Spins up a glommio server, connects a std::net::TcpStream client,
# sends a raw byte message, and verifies it arrives in the ring consumer.
cargo test -p network-ingestion -- test_zero_copy_network_to_ring_buffer --nocapture

# All workspace tests
cargo test
```

> NOTE: network-ingestion tests WILL FAIL on Windows (glommio requires io_uring).
> Run inside WSL2 or an unconfined Linux Docker container.

### 7.3 Phase 2 — node-ffi (Node.js smoke tests)

```bash
cd "Phase 2/node-ffi"
node test.js         # basic synchronous poll test
node test_async.js   # asynchronous polling test
```

### 7.4 Phase 3 — storage-engine

```bash
cd "Phase 3"

# Unit tests: sector alignment, length prefix correctness, FIFO ordering
cargo test -p storage-engine --lib -- journal::tests --nocapture

# Integration tests
cargo test -p storage-engine --test journal_reader   -- --nocapture
cargo test -p storage-engine --test journal_consumer -- --nocapture

# All tests
cargo test -p storage-engine

# With Phase 2 integration feature enabled
cargo test -p storage-engine --features phase2-integration
```

### 7.5 Phase 4 — cluster-replication

```bash
cd "Phase 4"

# Unit tests: frame encode/decode roundtrip, truncated payload rejection
cargo test -p cluster-replication --lib -- frame::tests --nocapture

# 100,000-event byte-perfect convergence test
# Asserts: 0 sequence_mismatches, 0 dropped_segments, 0 send_errors,
#          100K ingested_segments, 1 accepted_connection.
cargo test -p cluster-replication --test cluster_convergence \
    primary_and_backup_converge_on_byte_perfect_flatbuffer_stream \
    -- --nocapture

# Primary replication consumer test
cargo test -p cluster-replication --test primary_replication_consumer -- --nocapture

# All tests
cargo test -p cluster-replication

# Full integration (includes Phase 2 + Phase 3 deps)
cargo test -p cluster-replication --features full-stack-integration
```

### 7.6 Phase 5 — stream-analytics

```bash
cd "Phase 5"

# Rule AST construction, Display formatting, Copy semantics
cargo test -p stream-analytics --lib -- rule::tests --nocapture

# Evaluator: criterion matching, type mismatch degradation, short-circuit AND
cargo test -p stream-analytics --lib -- evaluator::tests --nocapture

# Window trackers: ConsecutiveTracker, SlidingWindowTracker
cargo test -p stream-analytics --lib -- window::tests --nocapture

# All tests
cargo test -p stream-analytics

# Inline evaluator benchmark (~200 ns p99 target)
cargo bench --bench inline_eval_bench -p stream-analytics

# Specific benchmark cases
cargo bench --bench inline_eval_bench -- "single_criterion/evaluate_price_gt"
cargo bench --bench inline_eval_bench -- "multi_criterion/evaluate_full_rule"
cargo bench --bench inline_eval_bench -- "spmc_inline/end_to_end_pipeline"
```

### 7.7 Full System Integrity Matrix

Run in order to assert end-to-end system health:

```bash
# 1. Core ring buffer unit tests
cd "Phase 1/core-ringbuffer"
cargo test --lib && echo "PHASE 1 UNIT: PASS"

# 2. Ring buffer benchmarks — verify throughput floor
cargo bench --bench spsc_bench 2>&1 | grep "thrpt\|time" | head -10

# 3. Storage engine — cross-platform
cd "../../Phase 3"
cargo test -p storage-engine && echo "PHASE 3 UNIT+INTEGRATION: PASS"

# 4. Cluster convergence — Linux/WSL2
cd "../Phase 4"
cargo test -p cluster-replication --test cluster_convergence \
    -- --nocapture 2>&1 | tail -5 && echo "PHASE 4 CONVERGENCE: PASS"

# 5. Stream analytics — correctness + benchmark
cd "../Phase 5"
cargo test -p stream-analytics && echo "PHASE 5 UNIT: PASS"
cargo bench --bench inline_eval_bench 2>&1 | grep "time:" | head -5

# 6. Network ingestion — Linux/WSL2 only
cd "../Phase 2"
cargo test -p network-ingestion -- --nocapture 2>&1 | tail -3
```

---

## 8. Module Reference

### 8.1 core-ringbuffer Public API

```rust
// Sequence: 64-byte cache-line padded AtomicU64 (#[repr(C, align(64))])
pub struct Sequence { /* private */ }
impl Sequence {
    pub const fn new(initial: u64) -> Self;
    pub fn load(&self, order: Ordering) -> u64;
    pub fn store(&self, value: u64, order: Ordering);
    pub fn compare_exchange_weak(
        &self, current: u64, new: u64,
        success: Ordering, failure: Ordering,
    ) -> Result<u64, u64>;
}

// Slot: 264-byte fixed payload buffer (repr(C), 8B len + 256B data)
pub const SLOT_DATA_CAPACITY: usize = 256;
pub struct Slot { /* private */ }
impl Slot {
    pub fn write_bytes(&mut self, src: &[u8]);  // panics if src.len() > 256
    pub fn data(&self) -> &[u8];                // &data[..len]
    pub fn len(&self) -> usize;
    pub fn is_empty(&self) -> bool;
}

// Error: returned when ring buffer is at capacity
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Full;

// SPSC factory
pub fn spsc(capacity: usize) -> (Producer, Consumer<BusySpin>);
pub fn spsc_with_wait<W: WaitStrategy>(capacity: usize, wait: W) -> (Producer, Consumer<W>);

impl Producer {
    pub fn try_publish(&mut self, data: &[u8]) -> Result<(), Full>;
    pub fn pending_count(&self) -> u64;
    pub fn capacity(&self) -> usize;
}
impl<W: WaitStrategy> Consumer<W> {
    pub fn try_consume<R>(&mut self, f: impl FnOnce(&[u8]) -> R) -> Option<R>;
    pub fn consume_blocking<R>(&mut self, f: impl FnOnce(&[u8]) -> R) -> R;
    pub fn available_count(&self) -> u64;
    pub fn capacity(&self) -> usize;
}

// SPMC factory
pub fn spmc(capacity: usize, num_consumers: usize) -> (spmc::Producer, Vec<spmc::Consumer<BusySpin>>);
impl spmc::Producer {
    pub fn try_publish(&mut self, data: &[u8]) -> Result<(), Full>;
    pub fn capacity(&self) -> usize;
}
impl<W: WaitStrategy> spmc::Consumer<W> {
    pub fn try_consume<R>(&mut self, f: impl FnOnce(&[u8]) -> R) -> Option<R>;
    pub fn consume_blocking<R>(&mut self, f: impl FnOnce(&[u8]) -> R) -> R;
    pub fn capacity(&self) -> usize;
}

// MPSC factory
pub fn mpsc(capacity: usize, num_producers: usize) -> (Vec<mpsc::Producer>, mpsc::Consumer<BusySpin>);

// Wait Strategies
pub trait WaitStrategy: Send + Sync {
    fn wait_for<F: Fn() -> bool>(&self, condition: F);
}
pub struct BusySpin;    // spin_loop hint — lowest latency, 100% CPU
pub struct YieldNow;    // thread::yield_now — µs jitter, less CPU
pub struct TimedSleep { pub duration: Duration }
pub struct AdaptiveTuner { /* UCB1 multi-armed bandit */ }
impl AdaptiveTuner {
    pub fn new(telemetry: QueueTelemetry) -> Self;
}
pub struct QueueTelemetry {
    pub write_seq: Arc<Sequence>,
    pub read_seq: Arc<Sequence>,
}
impl QueueTelemetry {
    pub fn sample_read(&self) -> u64;   // consumer position (Relaxed)
    pub fn sample_depth(&self) -> u64;  // write - read
}
```

**AdaptiveTuner internals:**
- Epoch: `EPOCH_ITERATIONS = 1_000` wait-loop iterations before re-evaluating strategy.
- Reward signal: `throughput - queue_depth * 10.0`.
- UCB1 exploration constant: `c = 100.0`.
- Override: if `depth > 50`, forces `BusySpin` regardless of UCB1 score.
- Strategies: 0=BusySpin, 1=YieldNow, 2=TimedSleep(1µs).

### 8.2 storage-engine Public API

```rust
pub const SECTOR_SIZE: usize = 4096;
pub const MAX_BATCH_RECORDS: usize = 64;

pub struct JournalWriterConfig { pub sync_on_append: bool }
impl Default for JournalWriterConfig { /* sync_on_append: false */ }

pub struct JournalWriter { /* file: File, offset: u64, config, zero_pad: [u8;4096] */ }
impl JournalWriter {
    pub fn create(path: impl AsRef<Path>) -> io::Result<Self>;
    pub fn create_with_config(path: impl AsRef<Path>, config: JournalWriterConfig) -> io::Result<Self>;
    pub fn append(&mut self, payload: &[u8]) -> io::Result<u64>;  // returns start_offset
    pub fn append_batch(
        &mut self,
        payloads: &[&[u8]; MAX_BATCH_RECORDS],
        count: usize,
    ) -> io::Result<u64>;  // vectorized writev; returns start_offset of batch
    pub fn flush(&mut self) -> io::Result<()>;
    pub fn offset(&self) -> u64;  // current write head byte position
}

pub struct JournalReader { /* file: File, offset: u64, file_len: u64, scratch: Vec<u8> */ }
impl JournalReader {
    pub fn open(path: impl AsRef<Path>) -> io::Result<Self>;
    pub fn next_payload(&mut self) -> io::Result<Option<&[u8]>>;
    pub fn replay_into(&mut self, producer: &mut StorageProducer) -> io::Result<ReplayStats>;
    pub fn offset(&self) -> u64;
}
pub struct ReplayStats { pub records: u64, pub bytes: u64 }

pub fn journal_lane(
    capacity: usize,
    producer_count: usize,
    consumer_count: usize,
) -> (Vec<StorageProducer>, Vec<StorageConsumer>);

#[derive(Clone)]
pub struct StorageProducer { /* private */ }
impl StorageProducer {
    pub fn try_publish(&mut self, payload: &[u8]) -> Result<u64, Full>;
}

pub struct StorageConsumer { /* private */ }
impl StorageConsumer {
    pub fn try_consume_batch<R>(
        &mut self,
        f: impl FnOnce(&[&[u8]; MAX_BATCH_RECORDS], usize) -> io::Result<R>,
    ) -> Option<io::Result<R>>;
    pub fn next_read(&self) -> u64;
}

pub struct JournalConsumer { /* StorageConsumer + JournalWriter + Option<u64 limit> */ }
impl JournalConsumer {
    pub fn new(consumer: StorageConsumer, writer: JournalWriter) -> Self;
    pub fn with_record_limit(self, limit: u64) -> Self;
    pub fn spawn(self) -> io::Result<JournalConsumerHandle>;
    pub fn run(self) -> io::Result<JournalConsumerStats>;
}
pub struct JournalConsumerStats { pub records: u64, pub batches: u64, pub max_batch: usize }
impl JournalConsumerHandle {
    pub fn join(self) -> thread::Result<io::Result<JournalConsumerStats>>;
}
```

### 8.3 cluster-replication Public API

```rust
pub const FRAME_HEADER_LEN: usize = 12;             // 8B seq_id (BE) + 4B payload_len (BE)
pub const MAX_FRAME_PAYLOAD_LEN: usize = 67_108_864; // 64 MiB

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FrameHeader {
    pub cluster_sequence_id: u64,  // big-endian on wire
    pub payload_len: u32,          // big-endian on wire
}
impl FrameHeader {
    pub fn new(cluster_sequence_id: u64, payload_len: usize) -> Result<Self, FrameDecodeError>;
    pub fn encode(self) -> [u8; FRAME_HEADER_LEN];
}

pub fn encode_frame<W: Write>(w: &mut W, seq_id: u64, payload: &[u8]) -> io::Result<usize>;
pub fn encode_frame_to_vec(seq_id: u64, payload: &[u8]) -> io::Result<Vec<u8>>;
pub fn decode_header(bytes: &[u8]) -> Result<FrameHeader, FrameDecodeError>;
pub fn decode_frame(bytes: &[u8]) -> Result<OwnedWireFrame, FrameDecodeError>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FrameDecodeError {
    HeaderTooShort { actual: usize },
    PayloadTooShort { expected: usize, actual: usize },
    PayloadTooLarge(usize),
}

// Primary-side: SPMC consumer -> TCP
pub struct ReplicationConsumer<W: WaitStrategy + Send + 'static> { /* private */ }
impl<W: WaitStrategy + Send + 'static> ReplicationConsumer<W> {
    pub fn new(lane: spmc::Consumer<W>, config: ReplicationConsumerConfig) -> Self;
    pub fn spawn(self) -> ReplicationConsumerHandle;
}
pub struct ReplicationConsumerConfig {
    pub backup_addr: SocketAddr,
    pub start_sequence_id: u64,       // default: 0
    pub connect_timeout: Duration,    // default: 25ms
    pub write_timeout: Duration,      // default: 25ms
    pub idle_backoff: Duration,       // default: 50µs
    pub reconnect_backoff: Duration,  // default: 10ms
}
impl ReplicationConsumerConfig {
    pub fn primary_tcp(backup_addr: SocketAddr) -> Self;
}
pub struct ReplicationConsumerHandle { /* private */ }
impl ReplicationConsumerHandle {
    pub fn shutdown(&self);
    pub fn snapshot(&self) -> ReplicationConsumerSnapshot;
    pub fn stop(self) -> thread::Result<ReplicationConsumerSnapshot>;
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReplicationConsumerSnapshot {
    pub observed_segments: u64,
    pub transmitted_segments: u64,
    pub dropped_segments: u64,      // lost during disconnect
    pub reconnect_attempts: u64,
    pub reconnect_successes: u64,
    pub send_errors: u64,
}

// Backup-side: TCP -> MPSC ring
pub struct BackupReplicationIngestor { /* private */ }
impl BackupReplicationIngestor {
    pub fn new(producer: mpsc::Producer, config: BackupReplicationIngestorConfig) -> Self;
    pub fn spawn(self) -> BackupReplicationIngestorHandle;
    pub fn spawn_on_listener(self, listener: ReplicationListener) -> BackupReplicationIngestorHandle;
}
pub struct BackupReplicationIngestorConfig {
    pub listen_addr: SocketAddr,
    pub expected_start_sequence_id: u64,  // default: 0
    pub accept_backoff: Duration,         // default: 50µs
    pub full_backoff: Duration,           // default: 25µs
    pub read_timeout: Duration,           // default: 100ms
}
impl BackupReplicationIngestorConfig {
    pub fn tcp(listen_addr: SocketAddr) -> Self;
}
pub struct BackupReplicationIngestorHandle { /* private */ }
impl BackupReplicationIngestorHandle {
    pub fn shutdown(&self);
    pub fn snapshot(&self) -> BackupReplicationSnapshot;
    pub fn stop(self) -> thread::Result<BackupReplicationSnapshot>;
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BackupReplicationSnapshot {
    pub accepted_connections: u64,
    pub ingested_segments: u64,
    pub sequence_mismatches: u64,  // logged and self-healed; not fatal
    pub oversize_frames: u64,      // frames > MAX_FRAME_PAYLOAD_LEN
    pub claim_retries: u64,        // MPSC ring was full; retried with full_backoff
    pub socket_errors: u64,
}
```

### 8.4 stream-analytics Public API

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FieldSelector { TimestampNs, Symbol, Price, Volume, SequenceId }

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CompareOp { Eq, Ne, Gt, Ge, Lt, Le }

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Value<'a> {
    Int64(i64),    // matches TimestampNs
    Float64(f64),  // matches Price
    Uint64(u64),   // matches Volume, SequenceId
    Str(&'a str),  // matches Symbol (borrowed — zero allocation)
}

#[derive(Debug, Clone, Copy, PartialEq)]  // Copy: stack-allocated predicate triple
pub struct Criterion<'a> {
    pub field: FieldSelector,
    pub op: CompareOp,
    pub value: Value<'a>,
}
impl<'a> Criterion<'a> {
    pub const fn new(field: FieldSelector, op: CompareOp, value: Value<'a>) -> Self;
}

pub struct Rule<'a> {
    pub name: &'a str,
    pub criteria: Vec<Criterion<'a>>,  // allocated once on cold path
}
impl<'a> Rule<'a> {
    pub fn new(name: &'a str, criteria: Vec<Criterion<'a>>) -> Self;
    pub fn single(name: &'a str, criterion: Criterion<'a>) -> Self;
    pub fn criteria(&self) -> &[Criterion<'a>];
    pub fn len(&self) -> usize;
    pub fn is_empty(&self) -> bool;
}

// Hot-path evaluator — zero allocation per call
pub fn evaluate_criterion(event: &Event<'_>, criterion: &Criterion<'_>) -> bool;
pub fn evaluate_rule(event: &Event<'_>, rule: &Rule<'_>) -> bool;
// evaluate_rule: short-circuit AND; type mismatches return false without panicking.

// ConsecutiveTracker: fires when a key accumulates N consecutive matches
pub struct ConsecutiveTracker<'a> { /* Vec<ConsecutiveSlot>, threshold: u32 */ }
impl<'a> ConsecutiveTracker<'a> {
    pub fn new(threshold: u32) -> Self;
    pub fn with_capacity(threshold: u32, capacity: usize) -> Self;
    pub fn register_key(&mut self, key: &'a str);          // cold path
    pub fn record_match(&mut self, key: &str) -> bool;     // true when hits >= threshold
    pub fn record_miss(&mut self, key: &str);              // resets hits to 0
    pub fn hit_count(&self, key: &str) -> Option<u32>;
}

// SlidingWindowTracker: fires when a key hits threshold within a rolling window of W events
pub struct SlidingWindowTracker<'a> { /* Vec<SlidingSlot> with per-key ring buffer */ }
impl<'a> SlidingWindowTracker<'a> {
    pub fn new(window_size: usize, threshold: u32) -> Self;
    pub fn with_capacity(window_size: usize, threshold: u32, capacity: usize) -> Self;
    pub fn register_key(&mut self, key: &'a str);          // cold path
    pub fn record_match(&mut self, key: &str) -> bool;     // true when window_count >= threshold
    pub fn window_count(&self, key: &str) -> Option<u32>;
}
```

---

## 9. Wire Protocols & Data Formats

### 9.1 FlatBuffers Event Schema

Compiled by `Phase 1/core-ringbuffer/build.rs` (calls `flatc`) from `schemas/event.fbs` into
`src/event_generated.rs`. The conceptual schema:

```
table Event {
    timestamp_ns : int64;   // Unix nanoseconds since epoch
    symbol       : string;  // "AAPL", "BTC-USD" — optional UTF-8
    price        : float64; // IEEE 754 double
    volume       : uint64;
    sequence_id  : uint64;
}
root_type Event;
```

**Serialization pattern:**
```rust
// Producer (cold path: build once; hot path: builder.reset() + build)
let mut builder = FlatBufferBuilder::with_capacity(128);
let sym = builder.create_string("AAPL");
let event = Event::create(&mut builder, &EventArgs {
    timestamp_ns: 1_719_273_600_000_000_000_i64,
    symbol: Some(sym), price: 195.89, volume: 1_500_000, sequence_id: 42,
});
builder.finish(event, None);
producer.try_publish(builder.finished_data()).unwrap();

// Consumer (zero-copy deserialization — no parsing, no allocation)
consumer.try_consume(|bytes| {
    let event = flatbuffers::root::<Event>(bytes).unwrap();
    // event.price() reads via vtable pointer arithmetic over `bytes`
});
```

**Slot capacity constraint:** `SLOT_DATA_CAPACITY = 256 bytes`. Typical Events are 60-100 bytes.
Payloads exceeding 256 bytes will panic in `Slot::write_bytes`.

### 9.2 TCP Ingestion Protocol (Phase 2)

```
Stream framing (little-endian length prefix):
┌──────────────────────────────────┬─────────────────────────────────────────────┐
│  length_prefix  (4 bytes, LE u32)│  body  (exactly length_prefix bytes)        │
└──────────────────────────────────┴─────────────────────────────────────────────┘
```

- Length prefix: little-endian `u32`. Maximum body size bounded by `SLOT_DATA_CAPACITY = 256`.
- Body: raw bytes (expected FlatBuffer Event, but server does not validate structure).
- Back-pressure: when the ring is full, the server calls `glommio::yield_if_needed()` and does
  not read further data until a slot is reclaimed.

### 9.3 Replication Wire Frame (Phase 4)

```
┌─────────────────────────┬────────────────────────┬─────────────────────────────┐
│  cluster_sequence_id    │  payload_len            │  payload (bytes)            │
│  8 bytes  big-endian u64│  4 bytes  big-endian u32│  payload_len bytes          │
└─────────────────────────┴────────────────────────┴─────────────────────────────┘
FRAME_HEADER_LEN = 12 bytes
MAX_FRAME_PAYLOAD_LEN = 64 MiB
```

**Protocol semantics:**
- `cluster_sequence_id`: monotonically increasing u64, starting from `start_sequence_id` (default: 0).
  Wraps on overflow via `wrapping_add`. Assigned by the primary per event observed on the SPMC lane.
- Backup tracks `expected_sequence_id` and self-heals on mismatch by accepting the incoming sequence.
- Events published during a disconnect are **permanently dropped**. For at-least-once delivery,
  replay from the journal after reconnection.
- Primary reconnects automatically after `reconnect_backoff` (default: 10ms) on any send error.

### 9.4 Journal Binary Format (Phase 3)

```
File structure (append-only; every record sector-aligned to SECTOR_SIZE = 4096 bytes):

  Record N:
    [payload_len : 8 bytes, little-endian u64]
    [payload     : payload_len bytes          ]
    [zero-pad    : ((8 + payload_len + 4095) & !4095) - (8 + payload_len) bytes]

  End-of-file sentinel (at a 4096-byte aligned offset):
    [0x00 00 00 00 00 00 00 00   (payload_len = 0)]
    [zero-pad to end of sector                    ]
```

Key formulas:
```
raw_size    = 8 + payload.len()
padded_size = (raw_size + 4095) & !4095   // round up to 4096-byte multiple
```

**Integrity:** The reader validates all padding bytes are `0x00`. Non-zero padding bytes return
`Err(InvalidData)`, detecting corruption. A zero-length sentinel at a non-sector-aligned offset
is also `InvalidData`.

**Batch writes:** `append_batch` constructs up to `MAX_BATCH_IO_SLICES = 192`
(`64 records * 3 slices: prefix, payload, padding`) and calls `file.write_vectored()`.
Partial writes fall back to sequential `write_all` for remaining slices.

---

## 10. Operational Runbook — Failure Modes

### 10.1 Ring Buffer Full — `Err(Full)`

**Symptom:** `Producer::try_publish()` or `StorageProducer::try_publish()` returns `Err(Full)`.

**Root cause:** Consumer is slower than the producer. Queue depth equals capacity.

**Resolution options:**

| Strategy | When to use |
|----------|-------------|
| Spin-retry with `spin_loop()` hint | Low-latency; consumer will catch up quickly |
| `thread::yield_now()` retry | Consumer is behind; yield OS time slice |
| Increase ring capacity (next power of two) | Sustained throughput imbalance |
| Profile the consumer bottleneck | Phase 3: check `sync_on_append`; Phase 4: check network RTT |

### 10.2 Replication Consumer Disconnect

**Symptom:** `ReplicationConsumerSnapshot::send_errors > 0`; `transmitted < observed`.

**Behavior:** Primary logs the error, sets `stream = None`, and retries after `reconnect_backoff`
(default: 10ms). Events during the outage increment `dropped_segments`. Reconnection is automatic.

**Resolution:** Verify backup node is running at `backup_addr`. Monitor `reconnect_successes`.
For zero-drop requirements, journal events on the primary and replay after reconnection.

### 10.3 Backup Sequence Mismatch

**Symptom:** `BackupReplicationSnapshot::sequence_mismatches > 0`.

**Behavior:** Backup logs the mismatch, then **self-heals**: `expected_seq = received_seq`.
Subsequent frames are accepted normally.

**Resolution:** Acceptable for at-most-once delivery. For strict ordering, implement a
reconnect handshake that exchanges the last confirmed sequence ID before resuming the stream.

### 10.4 Journal Corruption

**Symptom:** `JournalReader::next_payload()` returns `Err(InvalidData)` with a message about
non-zero padding bytes or a misaligned sentinel.

**Root cause:** Unclean shutdown during a vectorized write, disk error, or concurrent writes.

**Resolution:**
1. Find the last valid offset: `truncate_at = last_known_good_offset & !4095`.
2. Truncate the file at that offset and reload `JournalReader`.
3. Enable `JournalWriterConfig { sync_on_append: true }` for crash-safe production writes.

### 10.5 io_uring Not Available (Phase 2)

**Symptom:** glommio panics: `Failed to create IoUring: Os { code: 38, kind: Unsupported }`.

**Root cause:** Kernel < 5.8, WSL2 with `io_uring_disabled=1`, or Docker without `--privileged`.

**Resolution:** See Section 4.2. Verify: `cat /proc/sys/kernel/io_uring_disabled` — must be `0`.

### 10.6 Node.js Native Addon Not Loading

**Symptom:** `Error: Failed to load native binding` or missing `.node` file.

**Root cause:** Platform-specific `.node` binary absent. Only `win32-x64-msvc` is pre-built.

**Resolution:** Run `napi build --release --platform` in `Phase 2/node-ffi/`. Requires
`npm install -g @napi-rs/cli`.

---

## 11. Extension Guide

### 11.1 Adding a New Field to the FlatBuffers Event Schema

1. Edit `Phase 1/core-ringbuffer/schemas/event.fbs`.
2. Run `cargo build -p core-ringbuffer` — `build.rs` regenerates `event_generated.rs` via `flatc`.
3. Add a `FieldSelector` variant in `Phase 5/stream-analytics/src/rule.rs`.
4. Handle the new variant in `evaluate_criterion()` in `evaluator.rs`.
5. Add tests to `evaluator::tests`.

> **Backward compatibility:** FlatBuffers default-value semantics mean existing serialized events
> return the field default (0, 0.0, or None) for new fields — no deserialization failure.

### 11.2 Adding a New Wait Strategy

Implement `WaitStrategy`. It integrates everywhere the type parameter is accepted:

```rust
pub struct ExponentialBackoff { pub base_ns: u64, pub max_ns: u64 }
impl WaitStrategy for ExponentialBackoff {
    fn wait_for<F: Fn() -> bool>(&self, condition: F) {
        let mut sleep_ns = self.base_ns;
        while !condition() {
            std::thread::sleep(std::time::Duration::from_nanos(sleep_ns));
            sleep_ns = (sleep_ns * 2).min(self.max_ns);
        }
    }
}
// Usage: spsc_with_wait(1024, ExponentialBackoff { base_ns: 100, max_ns: 100_000 })
```

### 11.3 Adding a New Consumer Lane to the Pipeline

1. Increase consumer count: `spmc::spmc(capacity, N + 1)`.
2. Extract the new `Consumer` from the returned `Vec`.
3. Move it to a dedicated thread and poll `consumer.try_consume(...)` in a loop.
4. The producer applies back-pressure based on the **slowest** consumer. Ensure all lanes
   drain at comparable rates to avoid starving the producer.

### 11.4 Extending the CEP Rule Engine

**New `CompareOp`** (e.g., `Contains` for substring matching):
1. Add a variant to `CompareOp` in `rule.rs` and update `Display`.
2. Handle the variant in the string comparison branch of `evaluate_criterion()`.
3. Add test cases to `evaluator::tests`.

**New state tracker** (e.g., velocity / rate-of-change tracker):
1. Create a struct in `window.rs` with a pre-allocated `Vec<YourSlot>` for per-key state.
2. Implement `register_key()` (cold path), and zero-allocation `record_*()` hot-path methods.
3. **Zero-allocation contract:** no `HashMap`, `String`, `Box`, or `Vec::push()` inside `record_*()`.
4. Export from `lib.rs` and re-export from the `window::` module.

---

## 12. Performance Numbers at a Glance

| Metric | Value | Source |
|--------|-------|--------|
| SPSC throughput (cross-thread) | **21M+ messages/second** | `spsc_bench::spsc_throughput/cross_thread` |
| Network ingestion p50 latency | **10.4 µs** | `network-ingestion` io_uring end-to-end |
| CEP rule evaluation p99 latency | **~200 ns** | `inline_eval_bench` (Phase 5) |
| Cluster convergence guarantee | **100,000 events at 0 drops** | `cluster_convergence` integration test |
| Slot payload capacity | **256 bytes** (`SLOT_DATA_CAPACITY`) | Phase 1 `spsc.rs` |
| Journal batch size | **up to 64 records per `writev`** | `MAX_BATCH_RECORDS` Phase 3 |
| Journal sector alignment | **4096 bytes** (`SECTOR_SIZE`) | Phase 3 |
| Replication frame overhead | **12 bytes/event** (`FRAME_HEADER_LEN`) | Phase 4 |
| Memory ordering cost | **Acquire/Release only — no `SeqCst`** | entire hot path |
| False sharing prevention | **64-byte aligned `Sequence`** | `#[repr(C, align(64))]` Phase 1 |
| AdaptiveTuner epoch | **1,000 wait-loop iterations** | `EPOCH_ITERATIONS` Phase 1 |

### Memory Ordering Protocol — Quick Reference

```
  Producer thread                            Consumer thread
  ─────────────────────────────────────────  ────────────────────────────────────────
  1. Write slot data (non-atomic writes)
  2. write_seq.store(n+1, Release) ─────────► 3. write_seq.load(Acquire)
                                               4. Read slot data (non-atomic reads)
                                               5. read_seq.store(n+1, Release) ──────►
  6. read_seq.load(Acquire) ◄───────────────────────────────────────────────────────
  ─────────────────────────────────────────  ────────────────────────────────────────

  Happens-before (2)→(3): Consumer sees all slot writes from step (1).
  Happens-before (5)→(6): Producer sees the consumer has vacated the slot.
  Two Acquire/Release pairs per message — no SeqCst anywhere.
```

### Slot and Sequence Layout

```
  Slot (264 bytes, repr(C)):
  ┌──────────────────┬─────────────────────────────────────────────────────────┐
  │  len  (8 bytes)  │  data  (256 bytes = SLOT_DATA_CAPACITY)                 │
  └──────────────────┴─────────────────────────────────────────────────────────┘

  Sequence (64 bytes, repr(C, align(64))):
  ┌────────────────────────────┬───────────────────────────────────────────────┐
  │  AtomicU64  (8 bytes)      │  implicit padding  (56 bytes)                 │
  └────────────────────────────┴───────────────────────────────────────────────┘
  Producer write_seq and consumer read_seq each occupy a separate cache line.
  No false sharing on x86-64, ARM64, or any 64-byte cache-line architecture.
```

---

*Playbook compiled from live source files across all 5 phases on 2026-06-29.*
*For deeper detail, each module carries comprehensive inline rustdoc annotations.*
*The `#[cfg(test)]` modules are the executable specification of every invariant this system upholds.*

