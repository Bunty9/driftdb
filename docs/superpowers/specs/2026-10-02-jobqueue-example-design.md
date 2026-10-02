# Design: `jobqueue` reference example, 0.1.1 release with trusted publishing, launch posts

Date: 2026-10-02. Status: approved in chat (sections 1 and 2); this spec is for
written review.

## Intent

Give someone adopting `driftdb-lsm` one runnable project that shows how a real
application embeds the engine: key design, a typed wrapper, atomic multi-key
writes, secondary indexes, snapshots, scans, read-modify-write safety,
maintenance, graceful shutdown, crash recovery, and error handling. Readers
should be able to copy patterns from it directly.

Ship it with a 0.1.1 release published through crates.io **Trusted Publishing**,
and draft a LinkedIn post and a dev.to article about the project.

Success criteria:

- `cargo run -p jobqueue -- demo` runs end to end with narrated output and
  exits 0. It exercises every item in the feature map below.
- `cargo run -p jobqueue -- crash-demo` proves acked jobs survive `SIGKILL`.
- `cargo run -p jobqueue -- serve` serves the HTTP API and shuts down cleanly
  on Ctrl-C.
- `cargo test -p jobqueue` covers the store invariants and the HTTP routes.
  CI runs it.
- `driftdb-lsm` 0.1.1 is published by the GitHub Actions release workflow
  using OIDC, with no stored token, and a GitHub release is created.
- `scratchpad/linkedin.md` and `scratchpad/devto.md` exist locally and are
  gitignored. Every number and claim in them matches the repo.

Non-goals:

- No library API changes. If the example exposes a real gap, record it in
  PROGRESS.md "Next" instead of widening the API in a patch release.
- No published example crate (`publish = false`).
- No auth or multi-tenant concerns in the HTTP API.

## Placement

The root `Cargo.toml` gains `[workspace] members = ["examples/jobqueue"]`. The
root package stays the default member, so a plain `cargo test` at the root
still tests only the library.

- **Dependency line:** the example uses
  `driftdb-lsm = { version = "0.1", path = "../.." }`, so a reader can drop
  `path` and copy it.
- **Packaging:** the library's `include` changes `/examples` to
  `/examples/quickstart.rs`, so the jobqueue crate never ships inside the
  `driftdb-lsm` package.
- **Shared lockfile:** the workspace shares one `Cargo.lock` and `target/`.
  The MSRV job stays scoped to the library (`-p driftdb-lsm`). The example may
  need a newer Rust because of axum; that is acceptable and documented in its
  README.

## Example crate layout

```
examples/jobqueue/
  Cargo.toml    name = "jobqueue", publish = false
                deps: driftdb-lsm, tokio (full), axum, serde, serde_json,
                      clap (derive), anyhow, thiserror, tracing, tracing-subscriber
                dev-deps: tower (ServiceExt), http-body-util, tempfile
  README.md     feature-to-code map, patterns, pitfalls, curl examples
  src/lib.rs    pub mod keys, model, store, api
  src/keys.rs   key schema + prefix-range helper
  src/model.rs  Job, JobStatus, NewJob, Report
  src/store.rs  JobStore
  src/api.rs    axum router + error mapping
  src/main.rs   clap subcommands: demo, crash-demo, serve
  tests/store.rs, tests/api.rs
```

## Key schema (`keys.rs`)

| Key | Value | Purpose |
|---|---|---|
| `job/<id>` | JSON `Job` | primary record |
| `idx/status/<status>/<id>` | empty | secondary index by status |
| `meta/next_id` | u64 big-endian | id allocator |

- **Ids** are encoded at fixed width as 16 lowercase hex digits, so byte order
  equals numeric order and keys stay readable in a debugger.
- **`prefix_range(prefix)`** returns `prefix.to_vec()..upper_bound(prefix)`.
  The upper bound increments the last non-0xFF byte and truncates after it.
  This helper is the standard way to scan a key prefix in an ordered KV store,
  and it has unit tests for its edge cases.
- **Record/index invariant:** the record and its index entry change in the
  same `write_batch`, so they can never diverge, even across a crash.

## Model (`model.rs`)

- `JobStatus`: `Pending | Running | Done | Dead`. `Dead` means the job failed
  and exhausted its retries.
- `Job { id, kind: String, payload: serde_json::Value, status, attempts: u32,
  max_attempts: u32, lease_until: Option<u64 /* unix ms */>, created_at,
  updated_at, last_error: Option<String> }`
- `Report { counts: per-status, oldest_pending: Option<Job>, snapshot_seq }`

## `JobStore` (`store.rs`)

`JobStore` is `Clone` (it holds a `driftdb::Db` plus `Arc` state) and is safe
to share across tasks.

| Method | Behaviour | driftdb feature shown |
|---|---|---|
| `open(dir, Options)` | `Db::open_with`; reads `meta/next_id`. Maps `Error::Locked`, `InvalidArgument` and `UnsupportedFormat` to actionable messages | `open_with`, `Options`, error variants |
| `enqueue(NewJob)` | Checks the payload size against `driftdb::MAX_VALUE_LEN`. Under the write lock: allocate id, then one `WriteBatch` with the record, the pending index entry and `meta/next_id` | atomic `write_batch` |
| `get(id)` | point read and decode | `get` |
| `claim(lease)` | Under the write lock: scan the first pending index entry, then one batch: record to `Running` with `lease_until`, delete the pending index, add the running index | `scan` + read-modify-write |
| `complete(id)` | Under the write lock: running to done (index move) | batch put + delete |
| `fail(id, err)` | Under the write lock: `attempts += 1`; back to pending, or `Dead` once `attempts >= max_attempts` | batch |
| `requeue_expired(now)` | Under the write lock: scan the running index; jobs whose lease expired go back to pending | prefix `scan` |
| `list(status, limit)` | Prefix scan of the index, then point reads | `scan`, `get` |
| `report()` | Takes `db.snapshot()`. In `spawn_blocking`, because `Snapshot::get`/`scan` are synchronous and touch disk, it counts each status prefix and finds the oldest pending job. The result is consistent while writers continue | `snapshot` |
| `export(path)` | Snapshot scan of the `job/` prefix to JSONL, in `spawn_blocking` | snapshot `scan` |
| `purge(status, older_than)` | Under the write lock: delete matching records and index entries in batches of ≤1,000 ops | `delete` via batch |
| `maintenance()` | `flush()` then `compact()`; returns `stats()` before and after | `flush`, `compact`, `stats` |
| `stats()` | Passes `Stats` through | `stats` |
| `close()` | `db.close().await` | graceful `close` |

**Read-modify-write safety.** driftdb has no compare-and-swap or transactions.
Every operation that reads state and then writes based on it holds a single
`tokio::sync::Mutex<()>`; plain reads take no lock. Without the lock, two
workers could claim the same job. The README states this as the main
integration lesson and names the upgrade path: per-key or sharded locks for
higher write concurrency.

Errors use a store error enum (`thiserror`): `NotFound`, `InvalidState`,
`PayloadTooLarge`, `Db(driftdb::Error)`, `Codec(serde_json::Error)`.

## Binary (`main.rs`)

**`demo [--dir D] [--jobs N=2000]`** prints each step as it runs:

1. Open with small `Options` (memtable 256 KiB, `l1_max_bytes` 1 MiB,
   `target_file_size` 256 KiB) so flushes and compactions happen in-run.
2. 4 producer tasks enqueue N jobs concurrently.
3. 8 worker tasks claim and complete jobs. Payload `"fail": true` (about 10%)
   fails and retries; `max_attempts` 3 makes some jobs `Dead`. One worker
   "crashes": it claims a job and never completes it. The demo then advances
   time and calls `requeue_expired` to recover the job.
4. Mid-run, print `report()` while workers keep writing.
5. After workers drain: `export` a JSONL backup of every job from a snapshot
   and print its line count.
6. Purge `Done`, run `maintenance()`, and print levels and write
   amplification before and after.
7. `close()`, reopen, and assert the report counts are unchanged.

**`crash-demo [--dir D]`** re-execs itself with a hidden `crash-child`
subcommand. The child enqueues in a loop and prints each acked id. The parent
reads ids for about 1 s, `SIGKILL`s the child (`Child::kill`), reopens the
store, and asserts every acked id is present. It then prints the summary.

**`serve [--dir D] [--addr 127.0.0.1:3000]`** starts the HTTP API. On
`tokio::signal::ctrl_c`, axum's graceful shutdown runs and then
`store.close().await`.

## HTTP API (`api.rs`)

| Route | Result |
|---|---|
| `POST /jobs {kind, payload, max_attempts?}` | 201 + `Job` |
| `GET /jobs/{id}` | 200 / 404 |
| `GET /jobs?status=pending&limit=50` | 200 `[Job]` |
| `POST /jobs/claim {lease_ms}` | 200 `Job` / 204 if none pending |
| `POST /jobs/{id}/complete` | 200 / 404 / 409 if not running |
| `POST /jobs/{id}/fail {error}` | 200 / 404 / 409 |
| `GET /report` | 200 `Report` |
| `GET /stats` | 200 (a serializable mirror of `Stats`) |
| `POST /admin/maintenance` | 200 with stats before and after |

Error mapping: `NotFound` → 404, `InvalidState` → 409, `PayloadTooLarge` → 413,
`Db(_)` → 500, logged with `tracing`.

## Tests

`tests/store.rs`:

- Concurrent claimers never receive the same job.
- After a random operation mix, every record's status has exactly one index
  entry, and no orphan index entries exist.
- A report taken from a snapshot is unchanged by later writes.
- Close and reopen preserve the id counter and all jobs, and new ids don't
  collide with old ones.
- An expired lease requeues the job.
- `prefix_range` edge cases.

`tests/api.rs` drives each route through `Router::oneshot`, with no network.

## CI

- **New `example` job:** `cargo fmt --check -p jobqueue`,
  `cargo clippy -p jobqueue --all-targets -- -D warnings`,
  `cargo test -p jobqueue`, and `cargo run -p jobqueue -- demo --jobs 300`
  as a smoke test.
- **Existing jobs:**
  - MSRV and rustdoc jobs pin `-p driftdb-lsm`.
  - `cargo deny check` covers the whole workspace; allow-list additions are
    made only if a new license appears.

## Release 0.1.1 and trusted publishing

- **Library side:**
  - Bump to 0.1.1.
  - CHANGELOG `[0.1.1]`: "Added: jobqueue reference example; release
    workflow", plus the docs and `include` change.
  - README "Examples" section.
- **`.github/workflows/release.yml`:**
  - Trigger: `on: push: tags: ['v*']`. Job `publish` runs in
    `environment: release` with `permissions: { id-token: write, contents: write }`.
  - Checks: the tag (without `v`) must equal the `Cargo.toml` version, then
    `cargo publish -p driftdb-lsm --dry-run`.
  - `rust-lang/crates-io-auth-action@v1` (id `auth`), then
    `cargo publish -p driftdb-lsm` with `CARGO_REGISTRY_TOKEN: ${{ steps.auth.outputs.token }}`.
  - Finally `gh release create $TAG` with that version's CHANGELOG section as
    notes.
- **GitHub:** create environment `release` with `gh api`.
- **crates.io (owner action):** add a Trusted Publisher with owner `Bunty9`,
  repository `driftdb`, workflow `release.yml`, environment `release`.
- **Cut the release:** push tag `v0.1.1`. The workflow run is the end-to-end
  test of trusted publishing.
- **Afterwards (owner, optional):** enable trusted-publishing-only on the
  crate, and revoke the local API token.
- `docs/plans/2026-09-28-publishing.md` and CLAUDE.md are updated to the new
  release procedure: bump, CHANGELOG, tag, push.

## Posts (`./scratchpad/`, gitignored)

- **`linkedin.md`:** about 200 words. Hook, three concrete lessons (group
  commit vs fsync cost, a merge bug that resurrects deleted keys, why recovery
  is bounded by design), the measured numbers with a hardware caveat, and
  crates.io/GitHub links. A few hashtags.
- **`devto.md`:**
  - Front matter: `title`, `published: false`, `tags: rust, database,
    systems, opensource`, `canonical_url` unset.
  - About 1,800–2,500 words. Sections: why, architecture diagram, write path
    and fsync semantics, reads and MVCC snapshots, compaction and GC, the bugs
    review caught, numbers and caveats, using it (the jobqueue example),
    limits and what's next.
- **Accuracy:** numbers come only from README/PROGRESS. Claims are checked
  against the code. State the limits plainly: Linux only, no transactions or
  CAS, no RocksDB comparison yet.

## Execution

1. **Workspace and `JobStore`** (Sonnet): keys, model, store, `tests/store.rs`.
2. **Binary and API** (Sonnet), after step 1: `api.rs`, `main.rs`,
   `tests/api.rs`, example README, CI job.
3. **In parallel with steps 1 and 2:**
   - release workflow and docs (Sonnet), touching only `.github/` and docs;
   - posts (Sonnet), touching only `scratchpad/`.
4. **Opus review** of the example, the workflow and the posts' accuracy; fix
   what it finds.
5. **Release:**
   1. Bump to 0.1.1 and commit.
   2. Owner adds the trusted publisher.
   3. Push tag `v0.1.1`.
   4. Watch the workflow, then verify crates.io and docs.rs.
