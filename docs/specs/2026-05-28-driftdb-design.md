---
title: driftdb — LSM-tree KV engine with WAL + MVCC (P5)
status: draft
date: 2026-05-28
related:
    - ../../../backend-cloud-roadmap.md
    - ../../../projects-l3-l4.md
---

# driftdb — Design Spec

> **Note (Phase 2):** The implementation has superseded several details in this spec.
> For the authoritative module contracts and byte layouts, see
> [`docs/plans/2026-09-25-driftdb-phase-2.md`](../plans/2026-09-25-driftdb-phase-2.md)
> and read the source files directly (`src/wal.rs`, `src/sstable.rs`, `src/manifest.rs`).
> This spec remains useful for understanding the high-level architecture and design rationale.

> Companion spec lifted from `projects-l3-l4.md` § "P5 — LSM-tree KV
> engine with WAL + MVCC (driftdb)". Code blocks are the authoritative
> implementation reference for the scaffold; downstream phases extend,
> they do not contradict. Default stack pins live in
> `backend-cloud-roadmap.md` § 3.

## Problem
Most app-tier code reaches for Postgres + JSON files for state. Adding an embedded engine — WAL durability, MVCC snapshot reads, leveled compaction — is the canonical storage-engineering interview project. **Interview pitch:** "I wanted to understand fsync semantics, so I wrote my own LSM. Here's the write amplification I measured vs leveled-vs-tiered."

## Architecture

```
        Write path:                           Read path:
        ----------                            ---------
        put(k,v) ──► WAL.append ──► fsync     get(k) ──► memtable.get(k)
                                                            │ miss
                                                            v
                       (durable now)                     l0.get(k) (newest SST first)
                                                            │ miss
        ──► memtable.insert(k,v)                            v
                │                                       l1.get(k) ─► bloom filter ─► block read
                │ size > threshold                      ...
                v
        freeze memtable (arc-swap)
                │
                v
        new memtable starts taking writes
                │
                v
        flush thread: write SST in L0
                │
                v
        compactor: merge L0 → L1 → L2 (size-based)

  All keys in SST carry (key, seqno) — MVCC. Snapshot reads ignore seqno > snapshot_seq.
```

## Stack
- `crossbeam-skiplist` (concurrent memtable).
- `bytes`, `byteorder` (binary framing).
- `zstd` (block compression).
- `crc32fast` (WAL checksums).
- `growable-bloom-filter` (per-SST bloom).
- `memmap2` (mmap SSTable reads).
- `parking_lot` (low-overhead Mutex for compactor state).
- Custom: WAL writer, SSTable r/w, memtable, manifest, compaction scheduler.

## Key Rust code

**WAL record format + group-commit (`crates/driftdb/src/wal.rs`):**
```rust
// Record: [u32 crc] [u64 seqno] [u32 key_len] [u32 val_len] [key] [val]
// Files: wal-NNNNNN.log, rotated at 64MB.

use bytes::{BytesMut, BufMut};
use std::io::Write;
use tokio::sync::mpsc;

pub struct WalWriter {
    file: std::fs::File,
    next_seqno: u64,
    pending: Vec<(u64, oneshot::Sender<()>)>,
}

pub enum WalMsg {
    Write { key: Vec<u8>, val: Vec<u8>, ack: oneshot::Sender<u64> },
    Sync,
}

impl WalWriter {
    /// Group-commit: collect writes for up to `commit_window`, then one fsync.
    /// Amortizes the ~100µs fsync cost across many writes.
    pub async fn run(mut self, mut rx: mpsc::Receiver<WalMsg>, commit_window: std::time::Duration) {
        let mut ticker = tokio::time::interval(commit_window);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut acks: Vec<oneshot::Sender<u64>> = Vec::new();
        let mut last_seq = self.next_seqno;
        loop {
            tokio::select! {
                Some(msg) = rx.recv() => match msg {
                    WalMsg::Write { key, val, ack } => {
                        let seq = self.next_seqno;
                        self.next_seqno += 1;
                        self.append_record(seq, &key, &val).ok();
                        last_seq = seq;
                        acks.push(ack);
                        // (no fsync here — wait for window or N batched)
                        if acks.len() >= 128 { self.flush_and_ack(&mut acks, last_seq); }
                    }
                    WalMsg::Sync => self.flush_and_ack(&mut acks, last_seq),
                },
                _ = ticker.tick() => {
                    if !acks.is_empty() { self.flush_and_ack(&mut acks, last_seq); }
                }
            }
        }
    }

    fn flush_and_ack(&mut self, acks: &mut Vec<oneshot::Sender<u64>>, seq: u64) {
        // fdatasync is enough — file size doesn't change relative to last fsync metadata
        use std::os::unix::io::AsRawFd;
        unsafe { libc::fdatasync(self.file.as_raw_fd()); }
        for ack in acks.drain(..) { let _ = ack.send(seq); }
    }

    fn append_record(&mut self, seq: u64, key: &[u8], val: &[u8]) -> std::io::Result<()> {
        let mut buf = BytesMut::with_capacity(20 + key.len() + val.len());
        buf.put_u64(seq);
        buf.put_u32(key.len() as u32);
        buf.put_u32(val.len() as u32);
        buf.put_slice(key);
        buf.put_slice(val);
        let crc = crc32fast::hash(&buf);
        self.file.write_all(&crc.to_be_bytes())?;
        self.file.write_all(&buf)?;
        Ok(())
    }
}
```

**Memtable + freeze (`crates/driftdb/src/memtable.rs`):**
```rust
use crossbeam_skiplist::SkipMap;
use std::sync::Arc;

#[derive(Clone)]
pub struct InternalKey { pub user_key: Vec<u8>, pub seqno: u64 }

impl Ord for InternalKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // user_key ASC, then seqno DESC (newest version first on iteration)
        self.user_key.cmp(&other.user_key)
            .then_with(|| other.seqno.cmp(&self.seqno))
    }
}
impl PartialOrd for InternalKey { fn partial_cmp(&self, o: &Self) -> Option<std::cmp::Ordering> { Some(self.cmp(o)) } }
impl PartialEq for InternalKey { fn eq(&self, o: &Self) -> bool { self.user_key == o.user_key && self.seqno == o.seqno } }
impl Eq for InternalKey {}

pub enum Value { Put(Vec<u8>), Delete }

pub struct Memtable {
    map: SkipMap<InternalKey, Value>,
    approx_bytes: std::sync::atomic::AtomicUsize,
}

impl Memtable {
    pub fn insert(&self, key: Vec<u8>, seqno: u64, val: Value) {
        let bytes = key.len() + match &val { Value::Put(v) => v.len(), Value::Delete => 0 };
        self.approx_bytes.fetch_add(bytes + 16, std::sync::atomic::Ordering::Relaxed);
        self.map.insert(InternalKey { user_key: key, seqno }, val);
    }

    pub fn get(&self, user_key: &[u8], snapshot_seq: u64) -> Option<Vec<u8>> {
        // Find first version <= snapshot_seq for this user_key
        for entry in self.map.range(InternalKey { user_key: user_key.to_vec(), seqno: u64::MAX }..) {
            if entry.key().user_key != user_key { return None; }
            if entry.key().seqno > snapshot_seq { continue; }
            return match entry.value() {
                Value::Put(v) => Some(v.clone()),
                Value::Delete => None,
            };
        }
        None
    }

    pub fn size(&self) -> usize { self.approx_bytes.load(std::sync::atomic::Ordering::Relaxed) }
}
```

**SSTable writer with Bloom filter (`crates/driftdb/src/sstable.rs`):**
```rust
// Format:
//   [block 0] [block 1] ... [block N]
//   [index block: (last_key, block_offset) for each block]
//   [bloom block]
//   [footer: u64 index_off, u64 bloom_off, u64 magic]

use growable_bloom_filter::GrowableBloom;
use std::io::Write;

const BLOCK_SIZE: usize = 4096;

pub struct SstWriter<W: Write> {
    w: W,
    block_buf: Vec<u8>,
    index: Vec<(Vec<u8>, u64)>,
    bloom: GrowableBloom,
    offset: u64,
    last_key: Vec<u8>,
}

impl<W: Write> SstWriter<W> {
    pub fn new(w: W) -> Self {
        Self {
            w, block_buf: Vec::with_capacity(BLOCK_SIZE),
            index: Vec::new(),
            bloom: GrowableBloom::new(0.01, 100_000),
            offset: 0, last_key: Vec::new(),
        }
    }

    pub fn add(&mut self, key: &[u8], seqno: u64, val: &Value) -> std::io::Result<()> {
        self.bloom.insert(&key);
        // Block entry: [u32 klen][u32 vlen][u64 seq][u8 kind][key][val]
        let vlen = match val { Value::Put(v) => v.len() as u32, Value::Delete => 0 };
        let kind: u8 = match val { Value::Put(_) => 1, Value::Delete => 0 };
        self.block_buf.extend_from_slice(&(key.len() as u32).to_be_bytes());
        self.block_buf.extend_from_slice(&vlen.to_be_bytes());
        self.block_buf.extend_from_slice(&seqno.to_be_bytes());
        self.block_buf.push(kind);
        self.block_buf.extend_from_slice(key);
        if let Value::Put(v) = val { self.block_buf.extend_from_slice(v); }
        self.last_key = key.to_vec();
        if self.block_buf.len() >= BLOCK_SIZE { self.flush_block()?; }
        Ok(())
    }

    fn flush_block(&mut self) -> std::io::Result<()> {
        let compressed = zstd::encode_all(&self.block_buf[..], 3)?;
        self.w.write_all(&(compressed.len() as u32).to_be_bytes())?;
        self.w.write_all(&compressed)?;
        self.index.push((self.last_key.clone(), self.offset));
        self.offset += 4 + compressed.len() as u64;
        self.block_buf.clear();
        Ok(())
    }

    pub fn finish(mut self) -> std::io::Result<()> {
        if !self.block_buf.is_empty() { self.flush_block()?; }
        let index_off = self.offset;
        let index_bytes = bincode::serialize(&self.index).unwrap();
        self.w.write_all(&index_bytes)?;
        let bloom_off = index_off + index_bytes.len() as u64;
        let bloom_bytes = bincode::serialize(&self.bloom).unwrap();
        self.w.write_all(&bloom_bytes)?;
        self.w.write_all(&index_off.to_be_bytes())?;
        self.w.write_all(&bloom_off.to_be_bytes())?;
        self.w.write_all(&0xDEADBEEFu64.to_be_bytes())?;
        Ok(())
    }
}
```

**Compaction scheduler skeleton:**
```rust
// Leveled: L0 = N most recent flushes (may overlap), L1+ = non-overlapping sorted runs.
// Trigger: |L_n| > base * mult^n
// Pick: oldest L_n SST + all overlapping L_{n+1} SSTs → merge → write L_{n+1}
// Defend: leveled gives O(log N) reads + low space amp, at cost of write amp ~10x.
// For read-heavy agent metadata this wins; would pick tiered for write-dominated logs.

pub async fn compactor(state: Arc<DbState>) {
    loop {
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        if let Some(plan) = state.pick_compaction() {
            run_compaction(&state, plan).await.ok();
        }
    }
}
```

## Deployment
Library, not service. Embed into P1 (rustyq) as drop-in for SQLite/Postgres-backed metadata, prove the API. Publish to crates.io as `driftdb` (or whatever name).

## Eval / benchmarks
- YCSB workload A (50% read / 50% write), B (95R/5W), C (100R), F (RMW). Compare vs RocksDB.
- Write amplification (bytes-written-to-disk / bytes-of-user-data) — target 5–10× for leveled.
- Recovery: kill mid-write, restart, verify last acked seq is present in memtable after WAL replay.
- p99 read latency during compaction storm — the *real* test.
- Throughput: writes/sec sustained at p99 < 10ms.

## Stretch / writeup angle
- Wrap with openraft for distributed replication (Project 3 from L4 research).
- Replace bincode with custom binary for index — every µs counts.
- Write a 2000-word blog: "I built a tiny LSM and benchmarked it against RocksDB" — the kind of post that hits HN.

## Source references
- SlateDB design analysis (materializedview.io).
- LevelDB paper.
- TiKV blog on raft-rs storage layer.
- gemini-research.md Domain 3.
