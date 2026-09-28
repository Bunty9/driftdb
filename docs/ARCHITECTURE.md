# driftdb internals

This is the guide for anyone about to change `src/`. It describes what the
code actually does, module by module, citing function names (not line
numbers, which rot). Where this disagrees with the code, the code is right —
file a fix here.

For on-disk byte layouts, see the README's "On-disk formats" section and the
module docs atop `wal.rs`, `sstable.rs`, `manifest.rs`. This document only
sketches formats where they matter for a code path.

## 1. Module map and dependency direction

```
        error.rs   memtable.rs
            ^  ^        ^
            |   \       |
         wal.rs   \   iter.rs
            ^       \    ^
       manifest.rs   sstable.rs
            ^  ^          ^
            |   \_________|____________
            |                          |
       compaction.rs  <-------->   db.rs
                (Options)   (Table, Version)
                                 ^
                              lib.rs
```

`memtable.rs` and `error.rs` are the base — `memtable::Value`/`Entry` and
`error::{Error, Result}` are used everywhere and depend on nothing else in
the crate. `wal.rs` and `sstable.rs` both build on `memtable`'s record shape.
`iter.rs` merges/filters streams of `memtable::Entry` and is used by both
`db.rs` (`Inner::scan_at`) and `compaction.rs` (`compaction::run`).
`manifest.rs` depends only on `error.rs`.

Every module above is private (`mod`, not `pub mod`) — the public surface `lib.rs`
re-exports is just `Db`, `Options`, `Snapshot`, `Stats`, `WriteBatch`, `Error`, `Result`,
and the root constants `MAX_KEY_LEN`/`MAX_VALUE_LEN`. `#[doc(hidden)] pub mod __bench`
also re-exports a few `wal`/`memtable` items for `benches/report.rs`'s raw-replay bench;
it carries no semver guarantee.

There is exactly one two-way edge: `db.rs` and `compaction.rs` depend on each
other's types. `compaction::pick`/`pick_inner`/`run` take `crate::db::Options`
as a parameter, and `db.rs` imports `compaction::{CompactionPlan, Table,
Version}` to hold the live file set in `Inner::version` and to call
`compaction::pick`/`pick_forced`/`run` from `bg_thread`/`do_compact`. That's
deliberate — the picker needs the tuning knobs, `db.rs` needs the plan and
file types. Everything else's arrows point one way, down toward
`memtable.rs`/`error.rs`. `lib.rs` only re-exports the public surface.

## 2. Threads and ownership

Three execution contexts touch engine state: caller tasks, the writer
thread, and the background thread (both plain `std::thread`s, spawned in
`open_sync`, owned by `Guard`).

**Writer thread** (`writer_thread`): owns the current `WalFile`; the only
thing that appends to `mem.active` or freezes it into `mem.immutables`
(push side). Loop: block on `rx.recv()` for the first `Req`, then drain more
via `rx.try_recv()` up to `MAX_BATCH_REQUESTS` (1024) requests or until the
batch's op bytes reach `budget` (`memtable_size` minus the active memtable's
current size) — that's group commit. If `Options::commit_window` is
nonzero, wait that long via `rx.recv_timeout()` for more under the same
caps. Encode every op into the WAL buffer, one `wal.sync()` (one
`fdatasync`) per batch, insert into `mem.active`, publish `visible_seq`,
ack. Calls `rotate()` once the active memtable crosses `memtable_size` (or a
`Req::Rotate` — from `Db::flush` — is in the batch); `rotate()` stalls (50ms
condvar poll on `stall_cv`) while `mem.immutables` already holds
`MAX_IMMUTABLE_MEMTABLES` (= 2) frozen memtables — this is write
backpressure.

**Background thread** (`bg_thread`): the only thing that mutates
`Inner::manifest`, `Inner::version`, or removes entries from
`mem.immutables`. Loop: while a frozen memtable exists, `do_flush()` the
oldest one; once none are left, ask `compaction::pick`/`pick_forced` for a
plan and `do_compact()` it; repeat until both are empty, then sleep on
`work_cv` (200ms timeout). `bg_busy` is true only strictly inside
`do_flush`/`do_compact` (read by `Stats::background_idle`); `bg_alive` is
true for the thread's whole life, flipped false on any exit (including
panic) by the RAII `BgAliveGuard`, so stalled callers elsewhere notice
instead of polling forever. `force_compact` is an `AtomicUsize` (not a
`bool` — a bool livelocks two concurrent `compact()` callers, see the field
doc) bumped by `compact_blocking`; while nonzero the picker uses
`pick_forced`, ignoring normal trigger thresholds.

tokio's role is deliberately narrow (see the comment above the `tokio`
dependency in `Cargo.toml`) — neither thread is a tokio task. tokio supplies
(a) `oneshot` channels as the ack side of `Req::Write`/`Req::Rotate`, and
(b) `spawn_blocking`, used for `open_sync`, `Inner::scan_at`,
`Inner::wait_flushed`, `Inner::compact_blocking`, and
`Guard::shutdown_and_join`, so blocking work never stalls other tasks on the
same tokio worker. `Db::get` is the exception: it runs `Inner::get_at`
inline since a point read's cost is small and bounded.

## 3. Shared state in `Inner`

| Field | Protects | Written by |
|---|---|---|
| `version: RwLock<Arc<Version>>` | live SST set (copy-on-write) | background thread only |
| `mem: RwLock<MemState>` | active + frozen memtables | writer pushes (`rotate`); background removes (`do_flush`) |
| `manifest: Mutex<Manifest>` | manifest log handle | background thread only |
| `visible_seq: AtomicU64` | newest seqno safe to read | writer thread, after `wal.sync()` succeeds |
| `next_file_number: AtomicU64` | SST/WAL number allocator | writer (`rotate`, recovery) and background (flush/compaction outputs) |
| `snapshots: Mutex<BTreeMap<u64,usize>>` | live snapshot seqno refcounts | any caller, via `register_snapshot`/`unregister_snapshot` |
| `fatal: Mutex<Option<String>>` | one-way poison switch | writer (fsync failure) or background (flush/compaction failure) |
| `shutdown: AtomicBool` | shutdown flag | `Guard::shutdown_and_join`, only after writer is joined |
| `force_compact: AtomicUsize` | in-flight `compact()` count | `compact_blocking` |
| `bg_busy`/`bg_alive: AtomicBool` | background activity/liveness | background thread only |
| `work_mu/cv`, `stall_mu/cv`, `flush_mu/cv` | background wake-up, `rotate` stall, flush/compact waiters | whichever thread changes the condition |
| `stats: StatsInner` | write-amp counters | writer (`user_bytes`,`wal_bytes`); background (`bytes_flushed`,`bytes_compacted`) |

Lock-ordering rule: **never nest** `version`/`mem`/`manifest`. Every function
takes exactly one, finishes, drops it, then (if needed) takes another —
`do_flush`/`do_compact` each touch `manifest.lock()`, then separately
`version.write()`, then (`do_flush` only) separately `mem.write()`, never
together. Because mutation of `manifest`/`version` is confined to the
background thread, there's no ordering to get wrong: only one writer, no
lock ever held across another's acquisition.

Every condvar wait (`work_cv`, `stall_cv`, `flush_cv`) uses a bounded timeout
(50ms or 200ms), never an indefinite wait. This is belt-and-braces against a
background thread that has exited unexpectedly: the loops around each wait
re-check `fatal`/`bg_alive` on every wake, so a caller stalled in `rotate`,
`wait_flushed`, or `compact_blocking` notices within one timeout and returns
an error instead of blocking forever on a notify that will never come.

## 4. Write path

1. `Db::put`/`Db::delete` build a one-op `WriteBatch` and call
   `Db::write_batch`.
2. `write_batch` validates every key against `wal::MAX_KEY_LEN` and put
   value against `wal::MAX_VALUE_LEN` — an oversized op fails the whole
   batch with `Error::InvalidArgument` before it ever reaches the writer
   thread.
3. Sends `Req::Write { batch, ack }` over the `mpsc::Sender<Req>`
   (`Db::send_req`) and awaits the `oneshot` ack.
4. `writer_thread` drains a group-commit batch (§2).
5. If `fatal` is already set, every request in the batch fails immediately
   with that message.
6. Otherwise, per op: allocate the next seqno, `wal.append(seq, key, val)`
   (buffers in memory), remember `(seq, key, val)` for the memtable insert.
7. One `wal.sync()` for the whole batch. On success: insert every buffered
   op into `mem.active`, store `visible_seq` with `Ordering::Release`,
   update byte counters, ack every request `Ok(last_seq)`. On failure: set
   `fatal`, fail every ack (including queued `Rotate` acks), insert nothing.
   The `WalFile` itself is marked `poisoned` (`WalFile::sync`) so a later
   `sync()` on it fails immediately rather than retrying an fsync the
   kernel gives no retry guarantee for.
8. If the active memtable is now `>= memtable_size` (or a `Req::Rotate` was
   queued), `rotate()` freezes it into `mem.immutables`, opens a fresh WAL,
   and `notify_bg()`s the background thread.

**Visibility.** `visible_seq` advances only after `wal.sync()` succeeds,
never before. Every reader (`get_at`, `scan_at`, `register_snapshot`) loads
it with `Ordering::Acquire` before touching memtables/version, so nothing
not yet on disk is ever visible — that's "ack == durable."

**Fsync failure poisoning.** Once `fatal` is set it is permanent for that
`Inner`'s lifetime — checked by `Inner::check_fatal` at the top of
`get_at`, `scan_at`, `wait_flushed`, `compact_blocking`, and `rotate`'s
stall loop, and by the writer thread before each new batch. No un-poisoning;
a fresh `Db::open` is the only way forward, because a failed `fdatasync`
gives no guarantee the dirty pages are still queued for retry (see
`WalFile::sync` and `Manifest::append`, which apply the identical policy).

## 5. Read path

`Inner::get_at(key, seq)` (backing `Db::get`/`Snapshot::get`) checks, in
order: `mem.active`, `mem.immutables` newest-first, `version.levels[0]` (L0,
newest-flush-first, linear scan filtered by each table's `[smallest,
largest]`), then every level `>= 1` via `partition_point` binary search
(each is one non-overlapping sorted run, so at most one table can hold the
key). Returns on the first hit — `Put` or tombstone (`Delete` maps to `None`
via `as_option`).

`Inner::scan_at(range, seq)` instead collects one `BoxIter` per memtable and
one per overlapping SST across every level, in the same
active/frozen-newest-first/level order, feeds them through `iter::merge`
(ties broken toward the lower — newer — source index) then
`iter::visible(_, seq)` (collapses each key's version run to what's visible
at `seq`, drops tombstones), then applies the range bounds by hand (an
`Excluded` start needs its own filter since the iterators seek inclusively).

**Why `get`/`scan` register a snapshot too**, not only `Db::snapshot()`: the
window is *before* the memtable/version `Arc` clones happen. Between
computing a `seq` and taking those clones, an in-flight compaction could
finish and drop (via `oldest_snapshot()`) a pre-`seq` version this read
still needs — if that `seq` hadn't been registered yet. Registering the
seqno and reading `visible_seq` happen under the same `snapshots` lock that
`oldest_snapshot()` reads (`Inner::register_snapshot`), so `oldest_snapshot`
can never be computed newer than an already-registered `seq`: the race is
closed by lock ordering. `Db::get`/`Db::scan` register/unregister around one
call; `Db::snapshot()` registers on creation, unregisters on `Drop`.

## 6. Flush and compaction

**Picker (`compaction::pick_inner`, via `pick`/`pick_forced`).** Pure
function of per-level `SstMeta` lists + `Options`, unit-tested in isolation:
if L0 is non-empty and (forced, or file count `>= l0_compaction_trigger`),
pick all of L0 plus every overlapping L1 file, output level 1. Else, the
first level `n` in `1..max_levels-1` whose bytes exceed `l1_max_bytes *
level_multiplier^(n-1)` (or, forced, the first non-empty such level): pick
the file with the lexicographically smallest `smallest` key plus every
overlapping file one level down, output level `n+1`. `pick_forced` is the
same with threshold checks skipped — used by `Db::compact()` to drain
everything to the bottom.

**Executor (`compaction::run`).** Opens real `SstReader`s for the plan's
inputs (from the live `Version`), merges them, runs the merged stream
through `iter::compaction_filter(_, oldest_snapshot, drop_tombstones)`.
`drop_tombstones` is computed once: **true only if no file in any level
below the output level overlaps the plan's combined key range** — i.e. this
output is provably the bottom-most place the range could still be
un-deleted from.

**Output splitting.** Rolls to a new output SST once the current one's
`estimated_size()` reaches `target_file_size`, but only at a user-key
boundary (`key != *lk` in `run`'s `need_rollover` check) — never mid-version.
Every key's version run stays inside one SST, which `get_at`'s range check
and `compaction_filter`'s per-key boundary logic both depend on.

**Install order.** Both follow manifest, then `Version`, then filesystem:

- `do_flush`: write + `sync_all` the SST + directory, append the manifest
  edit (`SstAdded` + `WalFlushed` + `NextFileNumber`), install the new
  `Version`, *then* remove the flushed entry from `mem.immutables`, *then*
  delete the old WAL. Version-before-memtable-removal matters because
  `get_at`/`scan_at` snapshot memtables before version (§5's ordering), so a
  reader either still sees the memtable or already sees the new SST — never
  neither.
- `do_compact`: write outputs, append the manifest edit (`SstAdded`s +
  `SstDeleted`s + `NextFileNumber`), install the new `Version`, *then*
  unlink the input files. Manifest-before-`Version` means a crash in
  between leaves the manifest pointing at files still valid on disk
  (recovery just rebuilds `Version` from the manifest); `Version`-before-
  unlink means no reader is ever handed a `Table` whose file is already gone.

## 7. Recovery (`open_sync`)

1. `create_dir_all`, then `acquire_dir_lock` — non-blocking exclusive
   `flock` on `dir/LOCK`; fails with `Error::Locked` if another live `Db`
   holds it (two writers sharing a WAL/manifest would corrupt each other's
   state; mmap-based replay racing a live writer could SIGBUS).
2. `Manifest::open(dir)` replays the log into a `ManifestState`, then
   rewrites `MANIFEST` as one snapshot edit (tmp + `sync_all` + rename +
   `sync_dir`) so it never grows unbounded across open/close cycles.
3. **Orphan cleanup:** delete every `*.sst` not in the manifest's live set
   (a flush/compaction that wrote its file but crashed before the manifest
   append), and every `wal-*.log` with number `<= last_flushed_wal`.
4. Replay remaining WAL files in order into one temporary `Memtable` via
   `wal::replay` (truncates any torn tail).
5. If that memtable is non-empty, flush it synchronously right here (SST +
   manifest edit covering the max replayed WAL number), then delete the
   redundant WALs. If every remaining WAL replayed to nothing, they're still
   deleted so empty WALs don't pile up.
6. Create a fresh `WalFile` at the next file number; one more manifest
   append persists the advanced `next_file_number`.
7. Build `Inner`, spawn the writer and background threads, construct
   `Guard` (sender, both `JoinHandle`s, lock file), return `Db { guard }`.

**Recovery bound**, by design, not replay speed (see `Options::memtable_size`
and `MAX_IMMUTABLE_MEMTABLES = 2`): because the writer freezes+rotates every
time it crosses `memtable_size` and stalls once `MAX_IMMUTABLE_MEMTABLES`
frozen memtables are waiting, a crash can leave at most roughly
`memtable_size * (1 + MAX_IMMUTABLE_MEMTABLES)` bytes of WAL, plus one
group-commit request's worth of overshoot per memtable (the writer's byte
`budget` check stops draining a batch once over budget, but never splits one
request). Default options: on the order of 12 MiB, never gigabytes,
regardless of runtime before the crash.

## 8. Shutdown ordering

`Guard::shutdown_and_join` (from `Db::close`, or `Guard::drop`), under
`shutdown_mu`: (1) drop the `mpsc::Sender<Req>`; (2) join the writer thread
— its blocking `rx.recv()` returns once the channel drains; (3) **only now**
set `shutdown = true`, notify `work_cv` and the stall/flush condvars; (4)
join the background thread; (5) drop the `LOCK` file handle.

The 2-before-3 order is load-bearing: if `shutdown` were set before joining
the writer, the background thread could observe `shutdown == true` with
`mem.immutables` empty and exit — while the writer was still mid-batch,
about to `rotate` one more memtable into `immutables`. That memtable would
then sit forever unflushed, and any `flush()`/`compact()` in flight would
poll forever. Joining the writer first guarantees every immutable memtable
it will ever produce is already in `mem.immutables` (or already flushed)
before the background thread is told to stop.

## 9. On-disk formats

Briefly — the README and each module's header own the exact layouts:

- **WAL** (`wal.rs`): one file per memtable generation, `wal-NNNNNN.log`,
  record `[crc32][seq][kind][klen][vlen][key][val]`.
- **SSTable** (`sstable.rs`): zstd-compressed 4 KiB data blocks, a bincode
  index (`Vec<(last_key, block_offset)>`), a bincode `GrowableBloom`, a
  32-byte footer with offsets, a region CRC, a format version, and a magic
  number.
- **Manifest** (`manifest.rs`): append-only log of framed edits
  (`[len][crc32][bincode(Vec<ManifestRecord>)]`), compacted to one snapshot
  frame on every `Manifest::open`.
- **Format version**: `FORMAT_VERSION` (`lib.rs`) is shared by the manifest's
  `ManifestRecord::FormatVersion` and the SST footer's `format_version` field.
  `Manifest::open` checks it as each frame replays — before doing anything
  else with the rest of the log, and before any rewrite, WAL replay, or
  deletion — and refuses with `Error::UnsupportedFormat` on a mismatch;
  `SstReader::open` does the same per-file, also with `Error::UnsupportedFormat`.

See the README's "On-disk formats" section for full field layouts, and its
"Design tradeoffs" for `fdatasync` vs `fsync`, the MVCC GC watermark, and
bincode.

## 10. Invariants

1. `manifest`/`version`/removals-from-`mem.immutables` are mutated only by
   the background thread; `mem.active`/pushes-to-`mem.immutables` only by
   the writer thread.
2. No function holds two of `version`/`mem`/`manifest`'s locks at once.
3. `visible_seq` advances only after the corresponding WAL bytes survive an
   `fdatasync`.
4. `fatal`, once set, never clears; every write/flush/compaction attempt
   must check it first.
5. `do_flush` installs the new `Version` before removing the flushed
   memtable from `mem.immutables`, and deletes the WAL only after both.
6. `do_compact` appends the manifest edit before installing the new
   `Version`, and unlinks input files only after the `Version` swap.
7. A snapshot's seqno is registered (under the lock `oldest_snapshot()`
   reads) before any memtable/version data it will read is cloned.
8. `compaction_filter` never drops a version with `seqno > oldest_snapshot`,
   and drops a boundary tombstone only when `drop_tombstones` is true, which
   is only true when no lower level overlaps the compaction's key range.
9. A flush/compaction output never splits one user key's version run across
   two files.
10. `shutdown_and_join` joins the writer (draining queued requests) before
    setting `shutdown`.
11. At most one live `Db` may hold a given directory (the `dir/LOCK` flock).
12. Manifest replay drops a torn/CRC-invalid frame silently only when it is
    the final frame; the same corruption with data following it is a hard
    `ManifestCorrupt` error. WAL replay is deliberately more lenient
    (LevelDB-style): the first torn, truncated or CRC-invalid record is
    treated as the tail wherever it sits, and the file is truncated there.
13. Every installed SST is immutable — flush/compaction only add or remove
    whole files, never mutate one in place.
14. Keys/values exceeding `wal::MAX_KEY_LEN`/`MAX_VALUE_LEN` are rejected by
    `Db::write_batch` before any request reaches the writer thread.

## 11. Testing strategy

- **Unit tests**, colocated in every module (`wal.rs`, `manifest.rs`,
  `memtable.rs`, `sstable.rs`, `compaction.rs`, `iter.rs`): codec
  round-trips, torn-tail/corruption at specific byte offsets, the picker's
  plan selection for hand-built level layouts.
- **Property tests** (`proptest`) for the properties easiest to get subtly
  wrong: `iter.rs`'s `merge_is_sorted_and_complete` and
  `compaction_filter_preserves_visibility_for_all_live_snapshots` (the key
  correctness property behind invariant 8 — filter-then-read equals
  read-then-filter for every live snapshot seqno), and `sstable.rs`'s
  `iter_and_get_match_model`.
- **Model-based engine tests** (`tests/engine.rs`): a `BTreeMap` model driven
  by the same random workload as the real `Db`, forced flushes/compactions
  interleaved, snapshot isolation against a racing compactor, write-batch
  atomicity, exact scan-bound semantics, and regressions for three real bugs
  this project hit: `rotate()` returning `None` when active is empty but a
  flush is still pending, the `force_compact` bool-vs-livelock bug, and the
  snapshot-registration race from §5.
- **`tests/crash_recovery.rs`**: every reopen scenario from §7, a
  deliberately torn WAL tail, an orphaned `.sst`, directory-lock
  exclusivity, the flush-failure shutdown-hang regression from §8, and the
  WAL-size bound from §7.
- **`tests/crash_kill.rs`**: a real `kill -9` harness — the binary re-execs
  itself (`crash_child_worker`, gated on `DRIFTDB_CRASH_CHILD`) as a child
  that hammers the engine and prints each acked write to stdout; the parent
  reads that stream, `SIGKILL`s the child with no graceful shutdown,
  reopens the directory, and asserts every acked write survived correctly.
  Three iterations per run of `kill_minus_9_preserves_every_acked_write`.

**Benches:** `cargo bench --bench report` is a one-shot binary printing a
markdown table (write throughput by concurrency, write amplification, read
latency during a compaction storm, YCSB-style get latency, WAL replay
throughput, post-`kill -9` recovery time), tunable via the `DRIFTDB_BENCH_*`
env vars in its header. `cargo bench --bench throughput` and `cargo bench
--bench ycsb` are Criterion benchmarks for concurrent puts/batches and a
Zipfian YCSB A/B/C/F workload.

## 12. Known ceilings

Every deliberate simplification is marked `ponytail:` in the code. As of
`grep -rn ponytail src`, there are seven:

1. **`db.rs`, `Inner::register_snapshot`.** One mutex lock/unlock per
   snapshot registration; a lock-free epoch scheme would remove it from the
   read path if it ever shows up in a profile.
2. **`db.rs`, `Inner::scan_at`.** Memtable entries are collected eagerly
   into a `Vec` (not streamed) to avoid a `BoxIter<'a>: Send` lifetime
   conflict with the skiplist's range iterator — fine while memtables are
   capped at a few MiB by `memtable_size`; revisit for much larger active
   memtables.
3. **`compaction.rs`, `pick_inner`.** Always picks the globally smallest
   `smallest` key within a level, which can starve files further right in
   the keyspace. Upgrade path: a persisted per-level cursor key for true
   round-robin, if a workload ever shows a level growing unboundedly despite
   compaction running.
4. **`wal.rs`, `WalFile::append`.** Writes straight into the internal
   buffer (CRC placeholder patched after hashing in place) instead of a
   separate per-record `Vec` — already the fast path, not debt.
5. **`wal.rs`, `replay`.** Reads via `mmap` instead of `read_to_end`, saving
   one full-file copy; per-record key/value copies remain since the
   memtable must own its data eventually.
6. **`sstable.rs`, `BlockCache`.** An 8-entry per-table LRU aimed at `get()`
   repeat-hitting the same hot block under a skewed workload; won't help a
   scan/compaction touching every block once (`SstIter` bypasses it —
   `decode_block` vs `read_block`). Widen or shard across tables if
   point-read p99 is ever shown to still be decompression-bound.
7. **`manifest.rs`, `sync_dir`.** Duplicated rather than shared with
   `wal::sync_dir`; dedup once `wal.rs`'s version is worth depending on from
   here.
