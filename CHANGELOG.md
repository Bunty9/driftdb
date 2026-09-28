# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project uses
[Semantic Versioning](https://semver.org/). While the version is `0.x`, a minor
bump (`0.1` to `0.2`) may break the API **or the on-disk format**.

## [Unreleased]

## [0.1.0] - unreleased

First public release, published as `driftdb-lsm` and imported as `driftdb`.

### Added

- `Db` handle with an async API: `open` / `open_with(Options)`, `put`, `get`,
  `delete`, `write_batch` (atomic), `scan` (range), `snapshot` (RAII MVCC read
  view), `flush`, `compact`, `stats`, `close`.
- Write-ahead log with natural group commit on a dedicated writer thread. A write
  is acknowledged only after `fdatasync`. A failed fsync poisons the engine.
- One WAL file per memtable. Recovery replays at most about
  `memtable_size × 3` of WAL, and truncates a torn tail on replay.
- SSTables: zstd-compressed 4 KiB blocks with per-block CRC32, a bloom filter,
  a CRC-protected index/bloom footer, mmap reads, and a small per-table block
  cache.
- An append-only manifest of atomic edits, rewritten as a snapshot on open.
  Only the final frame may be torn; earlier corruption is a hard error.
- Leveled compaction on a background thread. Its GC keeps every version a live
  snapshot or an in-flight read can still see.
- Directory lock (`flock` on `LOCK`) so only one `Db` can open a directory at a
  time.
- Tests: model-based engine tests, crash-recovery tests, and a `kill -9`
  child-process crash test. Benches: criterion throughput and YCSB A/B/C/F,
  plus a one-shot `report` bench.

### Platform

- Linux only. Minimum supported Rust version is 1.85.
