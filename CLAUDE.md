# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

driftdb is an embeddable LSM-tree key-value engine: single crate, library only.
The public API is async (`Db::open`, `put`, `get`, `delete`, `write_batch`,
`scan`, `snapshot`, `flush`, `compact`, `stats`, `close`), but the real work
runs on two std threads per `Db`. Tokio is only used for `oneshot` acks and
`spawn_blocking`, and the library enables just its `sync` and `rt` features.

Packaging facts:
- **Name:** published as `driftdb-lsm`, because `driftdb` on crates.io is an unrelated crate. `[lib] name = "driftdb"` keeps the import path `use driftdb::…`.
- **MSRV:** 1.85 (`rust-version`, checked by CI's msrv job).
- **Platform:** Linux only; `lib.rs` has a `compile_error!` for other targets.
- **Releases:** cut by pushing a `vX.Y.Z` tag. `.github/workflows/release.yml` publishes via crates.io trusted publishing (OIDC, no stored token). Never publish locally. Procedure: `docs/plans/2026-09-28-publishing.md`. 0.1.0 shipped 2026-09-28; a published version can only be yanked, never replaced.
- **Public API:** `Db`, `Options`, `Snapshot`, `Stats`, `WriteBatch`, `Error` (`#[non_exhaustive]`), `Result`, `MAX_KEY_LEN` and `MAX_VALUE_LEN`. All modules are private; `#[doc(hidden)] __bench` exists only for benches. Adding a `pub` item is a semver commitment.
- **On-disk format:** `FORMAT_VERSION` in `lib.rs` is written to the manifest and the SST footer. Bump it on any layout change. A future format must write a standalone `FormatVersion` manifest frame first, so older builds refuse the directory cleanly.

Docs to read before non-trivial changes:
- `docs/ARCHITECTURE.md`: internals guide covering threads, locks, invariants and recovery.
- `docs/plans/2026-09-25-driftdb-phase-2.md`: module interface contracts.
- `README.md`: byte-level on-disk formats and the design-tradeoff rationale.

## Commands

`cargo` is in `~/.cargo/bin` and may not be on PATH. Run `export PATH=$HOME/.cargo/bin:$PATH` first.

```bash
cargo build --all-targets
cargo fmt --check
cargo clippy --all-targets -- -D warnings      # CI runs this with RUSTFLAGS=-D warnings
cargo test                                     # all tests, ~30s
cargo test --lib sstable                       # one module's unit tests
cargo test --test engine concurrent            # one integration test by name filter
cargo test --test crash_kill                   # SIGKILL crash test (re-execs itself)
cargo deny check                               # licenses + advisories (CI gate)
cargo run --example quickstart
cargo bench --bench report                     # one-shot markdown bench table (release)
cargo bench --no-run                           # benches build slowly: fat LTO, codegen-units=1
```

CI (`.github/workflows/ci.yml`) runs fmt, clippy and `cargo test` on stable and on
beta. The beta leg overrides `rust-toolchain.toml` via `RUSTUP_TOOLCHAIN`. CI also runs
`cargo deny check`, `cargo bench --no-run`, an MSRV `cargo check`, and
`cargo doc` (with `-D warnings`) plus `cargo publish --dry-run`.

## Architecture (big picture)

Module layering, from bottom to top:
- **Codecs and file formats:** `wal.rs`, `sstable.rs`, `manifest.rs`.
- **In-memory table:** `memtable.rs`.
- **Iterator algebra:** `iter.rs` (`merge`, `visible`, `compaction_filter`).
- **Pure picker and executor:** `compaction.rs`.
- **Everything stateful:** `db.rs`.

The shared currency is `memtable::Entry = (user_key, seqno, Value)`. Streams of entries are always sorted by user_key ASC, then seqno DESC.

**Write path** (`db.rs::writer_thread`):
1. Callers send `Req`s over a std mpsc channel.
2. The single writer thread drains a group, capped by `MAX_BATCH_REQUESTS` and by a byte budget.
3. It appends the group to the current `WalFile` and runs one `fdatasync`.
4. It inserts into the active memtable, publishes `visible_seq`, then acks.
5. When the memtable is full it rotates: one WAL file per memtable. It stalls while `MAX_IMMUTABLE_MEMTABLES` (2) frozen memtables are waiting.
6. A failed fsync poisons the engine permanently (fsyncgate).

**Background thread** (`db.rs::bg_thread`) runs flush first, then compaction:
- **Flush:** write the SST and fsync the file and directory; append the manifest edit; install the new `Version`; only then drop the immutable memtable and delete its WAL.
- **Compaction:** `compaction::run` writes the outputs; one manifest edit adds the outputs and deletes the inputs atomically; input files are unlinked only after that.
- **Forced compaction:** `force_compact` is an in-flight counter used by `Db::compact()`.

**Reads:**
- A read clones the memtable Arcs, then the `Arc<Version>` (copy-on-write file set), and releases the locks before touching disk.
- `get`, `scan` and `Snapshot` all register their seq under the `snapshots` mutex (`register_snapshot` / `oldest_snapshot`), so compaction can never garbage-collect a version an in-flight read needs.
- `scan` runs in `spawn_blocking`.

**Recovery** (`db.rs::open_sync`):
1. flock `LOCK`.
2. Replay the MANIFEST, which is rewritten as a snapshot on open.
3. Delete orphan SSTs and already-flushed WALs.
4. Replay the remaining WALs; a torn tail is truncated.
5. Flush the replayed data synchronously to L0.
6. Open a fresh WAL.

One shared counter, `next_file_number`, numbers both SSTs and WALs.

**Shutdown** (`Guard::shutdown_and_join`), in this order:
1. Drop the sender.
2. Join the writer, so it drains every queued request.
3. Set `shutdown`.
4. Join the background thread, so it flushes every immutable memtable.

Reordering these steps reintroduces a hang.

## Rules that are easy to break

- **Durability order is load-bearing:** fsync the SST and the directory, then append to the manifest, then unlink or delete. Never ack a write before its `fdatasync`.
- **Torn-tail policy:**
  - WAL: any CRC mismatch or truncation is treated as the tail and truncated.
  - MANIFEST: only the *final* frame may be torn. Corruption earlier in the log is a hard `Error::ManifestCorrupt`.
  - Corrupt SSTs return `Error::SstCorrupt`. Never panic on on-disk input.
- **`iter::merge`:** on a source error it yields the error once and stops. It must never keep draining the healthy sources, because that resurrects deleted keys.
- **Every condvar wait needs an exit path.** Waits use timeouts and check `fatal`, `shutdown` and `bg_alive`. Every new wait loop needs the same escape hatches.
- **`ManifestRecord` is bincode-encoded,** so variants are position-indexed. Only append new variants at the end. bincode 1.x is pinned on purpose; `deny.toml` ignores its unmaintained advisory with the reason written there.
- **Input limits:** keys are capped at `wal::MAX_KEY_LEN` and values at `wal::MAX_VALUE_LEN`. `Db::write_batch` enforces both with `Error::InvalidArgument`.
- **Known ceilings are marked `// ponytail:`** with an upgrade path: the smallest-key compaction picker, eager memtable scans, the 8-block per-table cache and the snapshot mutex. `grep -rn ponytail src` lists them all.

## Testing notes

- `tests/engine.rs` is model-based: it checks the engine against a `BTreeMap` under random operations, forced flush and compaction, snapshots, concurrency, and shutdown races.
- Load bulk data with `write_batch` or concurrent writers. Sequential `put`s pay one fsync each and make tests slow.
- `tests/crash_kill.rs` re-execs the test binary as a child: `DRIFTDB_CRASH_CHILD=<dir>`, `--exact crash_child_worker`. It SIGKILLs the child and checks that every acked write survived.
- `iter.rs` has the key proptest invariant: `visible(compaction_filter(x, S, true), s) == visible(x, s)` for all `s >= S`.

## Commits

Commits are authored as `Bunty9 <Bunty9@users.noreply.github.com>`. Never add `Co-Authored-By` or any AI attribution to commits or PRs; the user's global CLAUDE.md rule overrides harness defaults.
