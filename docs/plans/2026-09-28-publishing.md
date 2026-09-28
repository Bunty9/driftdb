# Publishing plan: `driftdb-lsm` 0.1.0 on crates.io

Status: **done**. `driftdb-lsm` 0.1.0 was published to crates.io on 2026-09-28, tagged `v0.1.0`, with a GitHub release. The blockers below were all fixed before publishing.
Publishing is irreversible: a version can be yanked but never deleted or
re-uploaded. So every step marked **(owner)** needs the crate owner to act or
approve.

## Decisions already made

| Item | Decision | Why |
|---|---|---|
| Package name | `driftdb-lsm` | `driftdb` is taken on crates.io by an unrelated crate (paulgb's real-time backend, v0.1.4). `drift-db` would be confusable with it. |
| Import path | `use driftdb::…` | `[lib] name = "driftdb"` in `Cargo.toml`. Repo, README title and docs keep the driftdb name. |
| License | `MIT OR Apache-2.0` | Rust ecosystem default. `LICENSE-MIT` and `LICENSE-APACHE` are in the repo root and in the package. README has the standard contribution clause. |
| MSRV | `rust-version = "1.85"` | The highest `rust-version` among normal dependencies (getrandom, jobserver). Verified with `cargo +1.85.0 check` and `test --no-run`; CI job `msrv (1.85)` keeps it honest. |
| Platform | Linux only | `libc::fdatasync` does not exist on Apple targets, and Windows has neither fdatasync nor flock. `lib.rs` has a `compile_error!` for non-Linux targets; docs.rs builds `x86_64-unknown-linux-gnu` only. |
| Package contents | `include = [src, tests, benches, examples, README, CHANGELOG, LICENSE-*]` | Design notes, CI config and CLAUDE.md stay repo-only. `cargo package --list` shows 24 files. |
| Versioning | SemVer, `0.x` | While `0.x`, a minor bump may break the API **or the on-disk format** (stated in CHANGELOG.md). |

Verified locally: `cargo publish --dry-run` passes, and `cargo doc --no-deps`
with `RUSTDOCFLAGS=-D warnings` is clean. CI job `rustdoc + package` runs both
on every push.

## Blockers to fix before 0.1.0

These are cheap now and expensive after release. Once published, every `pub`
item is covered by semver.

1. **Shrink the public API.** `lib.rs` exports all internal modules (`wal`,
   `sstable`, `manifest`, `memtable`, `iter`, `compaction`, `db`, `error`) as
   `pub mod`. Any refactor of them would then be a breaking change.
   - The intended surface is only `Db`, `Options`, `Snapshot`, `Stats`,
     `WriteBatch`, `Error` and `Result` (already re-exported at the root).
   - Recommended: make the modules `pub(crate)`, and re-export the input
     limits at the root (`pub use wal::{MAX_KEY_LEN, MAX_VALUE_LEN}`).
   - Two outside users need attention:
     - `tests/engine.rs` uses `driftdb::wal::MAX_*`. Switch it to the root
       re-export.
     - `benches/report.rs`'s raw-replay bench calls `driftdb::wal::*`. Either
       drop that row or expose it behind a `#[doc(hidden)] pub mod __bench`
       that is documented as unstable.
2. **Document every public item.** `-W missing_docs` reports 40 gaps (db.rs 12,
   manifest.rs 13, compaction.rs 6, sstable.rs 5, memtable.rs 4). Most go away
   with item 1. Document what remains (for example `Stats` fields and
   `Snapshot::seq`), then add `#![warn(missing_docs)]` to `lib.rs`.
3. **Add an on-disk format version.** No format currently carries a version
   marker. A future 0.2 that changes the WAL, SST or manifest layout would
   misread 0.1 data instead of refusing it.
   - Add a `FormatVersion(u32)` record, appended at the end of the
     `ManifestRecord` enum as bincode requires. Write it in every
     `snapshot_edit`.
   - `Db::open` returns a clear error on an unknown version.
   - A manifest with no version record means version 1.
   - The SST footer's reserved `u32` can carry the SST format version too.
4. **Fill in the CHANGELOG date** when tagging (`## [0.1.0] - YYYY-MM-DD`), and
   add compare links.

## Release procedure

1. On a branch, land blockers 1–3. Wait for green CI: tests on stable and beta,
   msrv, rustdoc + package, cargo-deny.
2. Bump nothing (the version is already 0.1.0). Date the CHANGELOG entry and
   commit it.
3. **(owner)** Run `cargo login` with a crates.io API token scoped to
   `publish-new` and `publish-update` for `driftdb-lsm`. The token is the
   owner's; do not store it in the repo.
4. `cargo publish --dry-run`, then **(owner)** `cargo publish`.
5. Tag and release: `git tag -a v0.1.0 -m "driftdb-lsm 0.1.0"`, then
   `git push origin v0.1.0`. Then
   `gh release create v0.1.0 --notes-from-tag` or paste the CHANGELOG section.
6. Verify:
   - <https://crates.io/crates/driftdb-lsm> renders the README.
   - <https://docs.rs/driftdb-lsm> builds. docs.rs builds asynchronously, so
     check its build log.
   - In a scratch project, `cargo add driftdb-lsm` then
     `cargo run --example quickstart` equivalent.
7. README badges already point at crates.io and docs.rs; they go live on their own.

## Optional: automate later releases

- **Trusted publishing** (crates.io OIDC with GitHub Actions) avoids long-lived
  tokens. Configure the trusted publisher on crates.io for `Bunty9/driftdb`
  and workflow `release.yml`. The workflow runs on `push: tags: ['v*']`: it
  uses `rust-lang/crates-io-auth-action` to get a short-lived token, then
  `cargo publish`. The first publish must still be manual, because the crate
  has to exist before a trusted publisher can be attached.
- Add `cargo semver-checks` to CI once 0.1.0 exists, so accidental API breaks
  are caught before a patch release.

## After 0.1.0 (not blocking)

- **macOS support:** use `fcntl(F_FULLFSYNC)` in `WalFile::sync` (and for
  directory sync). Plain `fsync` on macOS does not flush the drive cache.
  Replace the `compile_error!` with that `cfg` split and add a macOS CI leg.
- **Performance and features** listed in `PROGRESS.md` "Next": RocksDB
  comparison, shared block cache, round-robin compaction cursor, streaming
  memtable scan.
- **Yank policy:** yank only for data-loss or soundness bugs, never to "hide"
  an API mistake. Ship a fixed patch release the same day as the yank.
