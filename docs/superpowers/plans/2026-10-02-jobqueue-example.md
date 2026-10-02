# jobqueue reference example + 0.1.1 trusted-publishing release: implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a workspace example crate `examples/jobqueue` — a durable job queue (typed store, HTTP API, narrated demo, kill -9 demo) on top of `driftdb-lsm` — then ship `driftdb-lsm` 0.1.1 through a GitHub Actions trusted-publishing workflow, and draft LinkedIn/dev.to posts locally.

**Architecture:** The repo root becomes a workspace (`members = ["examples/jobqueue"]`; root package stays the default member). `jobqueue` is a lib + bin: `keys` (key schema), `model` (types), `store` (`JobStore`, the integration pattern), `api` (axum router), `main` (clap subcommands). Release automation is a tag-triggered workflow using `rust-lang/crates-io-auth-action` (OIDC → short-lived crates.io token).

**Tech Stack:** Rust 2021, driftdb-lsm 0.1 (lib name `driftdb`), tokio 1 (full), axum 0.8, serde/serde_json 1, clap 4 (derive), thiserror 2, anyhow 1, tracing 0.1 + tracing-subscriber 0.3; dev: tower 0.5 (util), http-body-util 0.1, tempfile 3.

**Spec:** `docs/superpowers/specs/2026-10-02-jobqueue-example-design.md`

## Global Constraints

- `cargo` lives in `~/.cargo/bin`: every shell starts with `export PATH=$HOME/.cargo/bin:$PATH`.
- No change to the `driftdb-lsm` public API in this plan (0.1.1 is a patch release). Library gaps found → add a bullet to `PROGRESS.md` "Next", don't patch `src/`.
- The example uses only the public API: `driftdb::{Db, Options, Snapshot, Stats, WriteBatch, Error, Result, MAX_KEY_LEN, MAX_VALUE_LEN}`. Never `driftdb::__bench`.
- Example dependency line is exactly `driftdb-lsm = { version = "0.1", path = "../.." }`; the crate is `publish = false`.
- Library `include` must ship `/examples/quickstart.rs` only — `cargo package --list -p driftdb-lsm` must not list anything under `examples/jobqueue`.
- MSRV 1.85 applies to the library only (`cargo +1.85.0 check -p driftdb-lsm --lib` stays green).
- Commits authored `Bunty9 <Bunty9@users.noreply.github.com>` (preconfigured). **Never** add `Co-Authored-By`, "Generated with", or any AI attribution.
- Never run `cargo publish` without `--dry-run` locally; the real 0.1.1 publish happens only via the release workflow on tag push (Task 8).
- Posts live in `./scratchpad/` (gitignored) and are never committed.
- Gates for every code task: `cargo fmt --all --check`, `cargo clippy --workspace --all-targets -- -D warnings`, tests named in the task.

## Review Focus

1. **Same data dir opened twice** (e.g. `serve` while `demo` runs on the same `--dir`): expect a clear "locked by another process" message, never a panic or corruption → test in Task 2 (`second_open_reports_locked`).
2. **Bad client input on the HTTP API** (`?status=bogus`, non-numeric `/jobs/abc`, malformed JSON): expect 4xx with a JSON error, never 500 → tests in Task 4.
3. **Restart while jobs are `running`** (server killed mid-lease): expect those jobs to return to `pending` after their lease expires without manual action → `serve` runs a periodic `requeue_expired` sweep (Task 5) and Task 2's lease test pins the store behaviour.
4. **Oversized payload**: expect 413 and nothing written (no id consumed) → tests in Task 2 and Task 4.
5. **Index/record divergence after many mixed operations**: expect exactly one index entry per job matching its status → Task 2 `index_matches_records_after_mixed_ops`.

---

## File map

| Path | Responsibility |
|---|---|
| `Cargo.toml` (root) | add `[workspace]`, `resolver = "3"`; narrow `include` |
| `examples/jobqueue/Cargo.toml` | example crate manifest |
| `examples/jobqueue/src/lib.rs` | module declarations + `now_ms()` |
| `examples/jobqueue/src/keys.rs` | key schema, `prefix_range` |
| `examples/jobqueue/src/model.rs` | `Job`, `JobStatus`, `NewJob`, `Report`, `StatsView` |
| `examples/jobqueue/src/store.rs` | `JobStore`, `StoreError` |
| `examples/jobqueue/src/api.rs` | axum router, `ApiError` |
| `examples/jobqueue/src/main.rs` | `demo`, `crash-demo`, `crash-child`, `serve` |
| `examples/jobqueue/tests/store.rs` | store integration tests |
| `examples/jobqueue/tests/api.rs` | HTTP tests via `oneshot` |
| `examples/jobqueue/README.md` | feature→code map, patterns, pitfalls, curl |
| `.github/workflows/ci.yml` | new `example` job; scope msrv/doc jobs with `-p driftdb-lsm` |
| `.github/workflows/release.yml` | tag-triggered trusted publishing |
| `README.md`, `CHANGELOG.md`, `PROGRESS.md`, `CLAUDE.md`, `docs/plans/2026-09-28-publishing.md` | docs |
| `scratchpad/linkedin.md`, `scratchpad/devto.md` | posts (gitignored) |

---

### Task 1: Workspace, crate skeleton, key schema, model

**Files:**
- Modify: `Cargo.toml` (root)
- Create: `examples/jobqueue/Cargo.toml`, `examples/jobqueue/src/lib.rs`, `examples/jobqueue/src/keys.rs`, `examples/jobqueue/src/model.rs`, `examples/jobqueue/src/main.rs` (stub)

**Interfaces:**
- Produces: `jobqueue::now_ms() -> u64`; `keys::{JOB_PREFIX, NEXT_ID_KEY, job_key(u64) -> Vec<u8>, status_prefix(JobStatus) -> Vec<u8>, status_key(JobStatus, u64) -> Vec<u8>, id_from_key(&[u8]) -> Option<u64>, prefix_range(&[u8]) -> (Bound<Vec<u8>>, Bound<Vec<u8>>)}`; `model::{JobStatus (Pending|Running|Done|Dead; ALL; as_str; Display; FromStr), Job, NewJob, Report, StatsView}`.

- [ ] **Step 1: Root workspace + include**

In root `Cargo.toml`, change the `include` entry `"/examples",` to `"/examples/quickstart.rs",` and append:

```toml
[workspace]
members = ["examples/jobqueue"]
# MSRV-aware dependency resolution so the shared lockfile keeps the library buildable on 1.85.
resolver = "3"
```

- [ ] **Step 2: Example manifest**

`examples/jobqueue/Cargo.toml`:

```toml
[package]
name = "jobqueue"
version = "0.1.0"
edition = "2021"
publish = false
license = "MIT OR Apache-2.0"
description = "Reference example: a durable job queue embedding driftdb-lsm"

[dependencies]
# In your own project: `driftdb-lsm = "0.1"` (drop `path`). The library is imported as `driftdb`.
driftdb-lsm = { version = "0.1", path = "../.." }
tokio = { version = "1", features = ["full"] }
axum = "0.8"
serde = { version = "1", features = ["derive"] }
serde_json = "1"
clap = { version = "4", features = ["derive"] }
anyhow = "1"
thiserror = "2"
tracing = "0.1"
tracing-subscriber = { version = "0.3", features = ["env-filter"] }

[dev-dependencies]
tower = { version = "0.5", features = ["util"] }
http-body-util = "0.1"
tempfile = "3"
```

- [ ] **Step 3: Write failing key-schema tests** in `examples/jobqueue/src/keys.rs` (module body follows in Step 5):

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::JobStatus;
    use std::ops::Bound;

    #[test]
    fn ids_sort_numerically_as_bytes() {
        assert!(job_key(9) < job_key(10));
        assert!(job_key(255) < job_key(256));
        assert_eq!(job_key(26), b"job/000000000000001a".to_vec());
    }

    #[test]
    fn status_key_round_trips_id() {
        let k = status_key(JobStatus::Running, 42);
        assert_eq!(k, b"idx/status/running/000000000000002a".to_vec());
        assert_eq!(id_from_key(&k), Some(42));
        assert_eq!(id_from_key(&job_key(7)), Some(7));
        assert_eq!(id_from_key(b"idx/status/running/zz"), None);
    }

    #[test]
    fn prefix_range_edges() {
        assert_eq!(prefix_range(b"a/"), (Bound::Included(b"a/".to_vec()), Bound::Excluded(b"a0".to_vec())));
        assert_eq!(prefix_range(&[0x61, 0xFF]), (Bound::Included(vec![0x61, 0xFF]), Bound::Excluded(vec![0x62])));
        assert_eq!(prefix_range(&[0xFF, 0xFF]), (Bound::Included(vec![0xFF, 0xFF]), Bound::Unbounded));
        assert_eq!(prefix_range(b""), (Bound::Included(vec![]), Bound::Unbounded));
    }
}
```

- [ ] **Step 4: Run, expect compile failure**

Run: `cargo test -p jobqueue --lib keys` → FAIL (items not defined).

- [ ] **Step 5: Implement `lib.rs`, `model.rs`, `keys.rs`, stub `main.rs`**

`src/lib.rs`:

```rust
//! A durable job queue built on driftdb — a reference for embedding `driftdb-lsm`.
//! Start with `store.rs` (the integration pattern), then `keys.rs` (key design).

pub mod api;
pub mod keys;
pub mod model;
pub mod store;

/// Wall-clock milliseconds since the Unix epoch. Store methods take `now` as a parameter
/// instead of calling this, so tests can control time.
pub fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock before 1970")
        .as_millis() as u64
}
```

(Create `src/api.rs` and `src/store.rs` as empty files with a `//!` doc line in this task so the crate compiles; Tasks 2–4 fill them.)

`src/model.rs`:

```rust
//! Domain types. Jobs are stored as JSON; JSON keeps the example readable — a real system
//! might pick a binary codec, the key/value layout would not change.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fmt;
use std::str::FromStr;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum JobStatus {
    Pending,
    Running,
    Done,
    Dead,
}

impl JobStatus {
    pub const ALL: [JobStatus; 4] = [JobStatus::Pending, JobStatus::Running, JobStatus::Done, JobStatus::Dead];

    pub fn as_str(self) -> &'static str {
        match self {
            JobStatus::Pending => "pending",
            JobStatus::Running => "running",
            JobStatus::Done => "done",
            JobStatus::Dead => "dead",
        }
    }
}

impl fmt::Display for JobStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for JobStatus {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        JobStatus::ALL
            .into_iter()
            .find(|st| st.as_str() == s)
            .ok_or_else(|| format!("unknown status {s:?} (expected pending|running|done|dead)"))
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Job {
    pub id: u64,
    pub kind: String,
    pub payload: serde_json::Value,
    pub status: JobStatus,
    pub attempts: u32,
    pub max_attempts: u32,
    /// Unix ms after which a `running` job is considered abandoned.
    pub lease_until: Option<u64>,
    pub created_at: u64,
    pub updated_at: u64,
    pub last_error: Option<String>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct NewJob {
    pub kind: String,
    #[serde(default)]
    pub payload: serde_json::Value,
    #[serde(default)]
    pub max_attempts: Option<u32>,
}

/// Consistent point-in-time view (built from one driftdb snapshot).
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Report {
    pub counts: BTreeMap<String, u64>,
    pub oldest_pending: Option<Job>,
    pub snapshot_seq: u64,
}

/// Serializable copy of `driftdb::Stats` (which is not `Serialize`).
#[derive(Clone, Debug, Serialize)]
pub struct StatsView {
    pub level_files: Vec<usize>,
    pub level_bytes: Vec<u64>,
    pub memtable_bytes: u64,
    pub user_bytes_written: u64,
    pub disk_bytes_written: u64,
    pub wal_bytes_written: u64,
    pub write_amplification: f64,
    pub background_idle: bool,
}

impl From<&driftdb::Stats> for StatsView {
    fn from(s: &driftdb::Stats) -> Self {
        StatsView {
            level_files: s.level_files.clone(),
            level_bytes: s.level_bytes.clone(),
            memtable_bytes: s.memtable_bytes,
            user_bytes_written: s.user_bytes_written,
            disk_bytes_written: s.disk_bytes_written,
            wal_bytes_written: s.wal_bytes_written,
            write_amplification: s.write_amplification(),
            background_idle: s.background_idle,
        }
    }
}
```

`src/keys.rs` (above the test module from Step 3):

```rust
//! Key schema. driftdb orders keys bytewise, so the layout *is* the query plan:
//!
//! | key                          | value        |
//! |------------------------------|--------------|
//! | `job/<id>`                   | JSON `Job`   |
//! | `idx/status/<status>/<id>`   | empty        |
//! | `meta/next_id`               | u64 BE       |
//!
//! Ids are 16 lowercase hex digits (fixed width) so byte order == numeric order and keys
//! stay readable when debugging. A record and its index entry always change in the same
//! `WriteBatch`, so they cannot diverge — not even across a crash.

use crate::model::JobStatus;
use std::ops::Bound;

pub const JOB_PREFIX: &[u8] = b"job/";
pub const NEXT_ID_KEY: &[u8] = b"meta/next_id";

pub fn job_key(id: u64) -> Vec<u8> {
    format!("job/{id:016x}").into_bytes()
}

pub fn status_prefix(status: JobStatus) -> Vec<u8> {
    format!("idx/status/{}/", status.as_str()).into_bytes()
}

pub fn status_key(status: JobStatus, id: u64) -> Vec<u8> {
    let mut k = status_prefix(status);
    k.extend_from_slice(format!("{id:016x}").as_bytes());
    k
}

/// Parse the trailing `<id>` of a `job/…` or `idx/status/…/…` key.
pub fn id_from_key(key: &[u8]) -> Option<u64> {
    let tail = key.rsplit(|b| *b == b'/').next()?;
    if tail.len() != 16 {
        return None;
    }
    u64::from_str_radix(std::str::from_utf8(tail).ok()?, 16).ok()
}

/// Range covering every key that starts with `prefix` — the standard prefix scan for an
/// ordered KV store. The end bound is `prefix` with its last non-0xFF byte incremented
/// (bytes after it dropped); an all-0xFF (or empty) prefix has no finite upper bound.
pub fn prefix_range(prefix: &[u8]) -> (Bound<Vec<u8>>, Bound<Vec<u8>>) {
    let start = Bound::Included(prefix.to_vec());
    let mut end = prefix.to_vec();
    while let Some(last) = end.pop() {
        if last < 0xFF {
            end.push(last + 1);
            return (start, Bound::Excluded(end));
        }
    }
    (start, Bound::Unbounded)
}
```

`src/main.rs` (stub, replaced in Task 5):

```rust
fn main() {
    println!("jobqueue: subcommands land in Task 5");
}
```

- [ ] **Step 6: Run tests**

Run: `cargo test -p jobqueue --lib keys` → PASS (3 tests). Also `cargo test` at root still runs only the library (check output names `driftdb-lsm` targets only) and `cargo package --list -p driftdb-lsm | grep -c jobqueue` prints `0`.

- [ ] **Step 7: Commit**

```bash
git add Cargo.toml Cargo.lock examples/jobqueue
git commit -m "feat(example): jobqueue workspace crate with key schema and model"
```

---

### Task 2: `JobStore` — open, enqueue, get, claim, complete, fail, requeue, list

**Files:**
- Modify: `examples/jobqueue/src/store.rs`
- Create: `examples/jobqueue/tests/store.rs`

**Interfaces:**
- Consumes: Task 1 `keys::*`, `model::*`.
- Produces (all `async` unless noted, `Result<T> = std::result::Result<T, StoreError>`):
  - `JobStore::open(dir: impl AsRef<Path>, options: driftdb::Options) -> Result<JobStore>`
  - `enqueue(&self, new: NewJob, now: u64) -> Result<Job>`
  - `get(&self, id: u64) -> Result<Option<Job>>`
  - `claim(&self, lease_ms: u64, now: u64) -> Result<Option<Job>>`
  - `complete(&self, id: u64, now: u64) -> Result<Job>`
  - `fail(&self, id: u64, error: String, now: u64) -> Result<Job>`
  - `requeue_expired(&self, now: u64) -> Result<usize>`
  - `list(&self, status: JobStatus, limit: usize) -> Result<Vec<Job>>`
  - `close(&self) -> Result<()>`; sync `db(&self) -> &driftdb::Db`
  - `pub const MAX_PAYLOAD_BYTES: usize = 64 * 1024;` `pub const DEFAULT_MAX_ATTEMPTS: u32 = 3;`
  - `StoreError::{NotFound(u64), InvalidState { id, status, expected }, PayloadTooLarge { size, limit }, Db(driftdb::Error), Codec(serde_json::Error), CorruptIndex(String), Join(tokio::task::JoinError), Io(std::io::Error)}`

- [ ] **Step 1: Write failing tests** `examples/jobqueue/tests/store.rs`:

```rust
use driftdb::Options;
use jobqueue::keys::{prefix_range, status_key, JOB_PREFIX};
use jobqueue::model::{Job, JobStatus, NewJob};
use jobqueue::store::{JobStore, StoreError, MAX_PAYLOAD_BYTES};
use std::collections::HashSet;

fn new_job(n: u64) -> NewJob {
    NewJob { kind: "email".into(), payload: serde_json::json!({ "n": n }), max_attempts: None }
}

fn small_opts() -> Options {
    Options { memtable_size: 64 * 1024, target_file_size: 32 * 1024, l1_max_bytes: 128 * 1024, ..Default::default() }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_claimers_never_share_a_job() {
    let dir = tempfile::tempdir().unwrap();
    let store = JobStore::open(dir.path(), small_opts()).await.unwrap();
    for n in 0..200 {
        store.enqueue(new_job(n), 1_000).await.unwrap();
    }
    let mut tasks = Vec::new();
    for _ in 0..8 {
        let s = store.clone();
        tasks.push(tokio::spawn(async move {
            let mut got = Vec::new();
            while let Some(job) = s.claim(60_000, 2_000).await.unwrap() {
                got.push(job.id);
            }
            got
        }));
    }
    let mut all = Vec::new();
    for t in tasks {
        all.extend(t.await.unwrap());
    }
    let unique: HashSet<u64> = all.iter().copied().collect();
    assert_eq!(all.len(), 200, "every job claimed exactly once");
    assert_eq!(unique.len(), 200, "no job claimed twice");
    store.close().await.unwrap();
}

#[tokio::test]
async fn enqueue_get_complete_lifecycle() {
    let dir = tempfile::tempdir().unwrap();
    let store = JobStore::open(dir.path(), Options::default()).await.unwrap();
    let job = store.enqueue(new_job(1), 10).await.unwrap();
    assert_eq!(job.id, 1);
    assert_eq!(store.get(1).await.unwrap().unwrap().status, JobStatus::Pending);
    let claimed = store.claim(1_000, 20).await.unwrap().unwrap();
    assert_eq!((claimed.id, claimed.status, claimed.lease_until), (1, JobStatus::Running, Some(1_020)));
    let done = store.complete(1, 30).await.unwrap();
    assert_eq!(done.status, JobStatus::Done);
    assert!(matches!(store.complete(1, 40).await, Err(StoreError::InvalidState { .. })));
    assert!(matches!(store.complete(99, 40).await, Err(StoreError::NotFound(99))));
    assert_eq!(store.get(99).await.unwrap(), None);
    store.close().await.unwrap();
}

#[tokio::test]
async fn fail_retries_then_dies() {
    let dir = tempfile::tempdir().unwrap();
    let store = JobStore::open(dir.path(), Options::default()).await.unwrap();
    store.enqueue(NewJob { max_attempts: Some(2), ..new_job(1) }, 0).await.unwrap();
    store.claim(1_000, 1).await.unwrap().unwrap();
    let j = store.fail(1, "boom".into(), 2).await.unwrap();
    assert_eq!((j.status, j.attempts, j.last_error.as_deref()), (JobStatus::Pending, 1, Some("boom")));
    store.claim(1_000, 3).await.unwrap().unwrap();
    let j = store.fail(1, "boom again".into(), 4).await.unwrap();
    assert_eq!((j.status, j.attempts), (JobStatus::Dead, 2));
    assert!(store.claim(1_000, 5).await.unwrap().is_none(), "dead jobs are never claimed");
    store.close().await.unwrap();
}

#[tokio::test]
async fn expired_lease_is_requeued() {
    let dir = tempfile::tempdir().unwrap();
    let store = JobStore::open(dir.path(), Options::default()).await.unwrap();
    store.enqueue(new_job(1), 0).await.unwrap();
    store.claim(100, 1_000).await.unwrap().unwrap(); // lease_until = 1_100
    assert_eq!(store.requeue_expired(1_050).await.unwrap(), 0);
    assert_eq!(store.requeue_expired(1_100).await.unwrap(), 1);
    let j = store.get(1).await.unwrap().unwrap();
    assert_eq!((j.status, j.lease_until), (JobStatus::Pending, None));
    assert_eq!(j.last_error.as_deref(), Some("lease expired"));
    store.close().await.unwrap();
}

#[tokio::test]
async fn reopen_preserves_jobs_and_id_counter() {
    let dir = tempfile::tempdir().unwrap();
    {
        let store = JobStore::open(dir.path(), Options::default()).await.unwrap();
        for n in 0..5 {
            store.enqueue(new_job(n), 0).await.unwrap();
        }
        store.close().await.unwrap();
    }
    let store = JobStore::open(dir.path(), Options::default()).await.unwrap();
    for id in 1..=5 {
        assert!(store.get(id).await.unwrap().is_some(), "job {id} survived reopen");
    }
    assert_eq!(store.enqueue(new_job(9), 0).await.unwrap().id, 6, "ids never reused");
    store.close().await.unwrap();
}

#[tokio::test]
async fn oversized_payload_is_rejected_without_consuming_an_id() {
    let dir = tempfile::tempdir().unwrap();
    let store = JobStore::open(dir.path(), Options::default()).await.unwrap();
    let big = NewJob { kind: "x".into(), payload: serde_json::json!("y".repeat(MAX_PAYLOAD_BYTES + 1)), max_attempts: None };
    assert!(matches!(store.enqueue(big, 0).await, Err(StoreError::PayloadTooLarge { .. })));
    assert_eq!(store.enqueue(new_job(1), 0).await.unwrap().id, 1);
    store.close().await.unwrap();
}

#[tokio::test]
async fn second_open_reports_locked() {
    let dir = tempfile::tempdir().unwrap();
    let store = JobStore::open(dir.path(), Options::default()).await.unwrap();
    let err = JobStore::open(dir.path(), Options::default()).await.err().expect("second open must fail");
    assert!(err.to_string().contains("locked by another process"), "got: {err}");
    store.close().await.unwrap();
}

#[tokio::test]
async fn index_matches_records_after_mixed_ops() {
    let dir = tempfile::tempdir().unwrap();
    let store = JobStore::open(dir.path(), small_opts()).await.unwrap();
    for n in 0..300 {
        store.enqueue(NewJob { max_attempts: Some(2), ..new_job(n) }, n).await.unwrap();
    }
    let mut t = 1_000;
    while let Some(job) = store.claim(50, t).await.unwrap() {
        t += 1;
        match job.id % 4 {
            0 => { store.complete(job.id, t).await.unwrap(); }
            1 => { store.fail(job.id, "x".into(), t).await.unwrap(); }
            2 => {} // abandoned: stays running until the lease expires
            _ => { store.complete(job.id, t).await.unwrap(); }
        }
        if job.id % 50 == 0 {
            store.db().flush().await.unwrap();
        }
    }
    store.requeue_expired(t + 1_000).await.unwrap();
    let records = store.db().scan(prefix_range(JOB_PREFIX)).await.unwrap();
    let index = store.db().scan(prefix_range(b"idx/status/")).await.unwrap();
    assert_eq!(records.len(), 300);
    assert_eq!(index.len(), 300, "exactly one index entry per job");
    for (_, v) in &records {
        let job: Job = serde_json::from_slice(v).unwrap();
        let k = status_key(job.status, job.id);
        assert!(index.iter().any(|(ik, _)| *ik == k), "index entry for job {} ({})", job.id, job.status);
    }
    store.close().await.unwrap();
}
```

- [ ] **Step 2: Run, expect failure**

Run: `cargo test -p jobqueue --test store` → FAIL (`JobStore` not defined).

- [ ] **Step 3: Implement `src/store.rs`**

```rust
//! `JobStore`: the integration pattern this example exists to show.
//!
//! * One `driftdb::Db`, cloned freely (it is an `Arc` handle) and shared by every task.
//! * Every state change is ONE `WriteBatch` (record + index + counter), so a crash can never
//!   leave a job half-moved. driftdb acks a batch only after `fdatasync`.
//! * driftdb has no compare-and-swap or transactions. Anything that reads state and writes
//!   based on it (allocate an id, claim, complete, fail, requeue) runs under one async
//!   mutex; plain reads take no lock. Without it two workers could claim the same job.
//!   ponytail: one global write lock caps write concurrency at "one RMW at a time"; shard
//!   the lock by key (e.g. per queue) if that ever becomes the bottleneck.

use crate::keys::{job_key, prefix_range, status_key, status_prefix, id_from_key, NEXT_ID_KEY};
use crate::model::{Job, JobStatus, NewJob};
use driftdb::{Db, Options, WriteBatch};
use std::path::Path;
use std::sync::Arc;
use tokio::sync::Mutex;

/// Application-level payload cap (driftdb itself allows values up to `driftdb::MAX_VALUE_LEN`).
pub const MAX_PAYLOAD_BYTES: usize = 64 * 1024;
pub const DEFAULT_MAX_ATTEMPTS: u32 = 3;

pub type Result<T> = std::result::Result<T, StoreError>;

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("job {0} not found")]
    NotFound(u64),
    #[error("job {id} is {status}, expected {expected}")]
    InvalidState { id: u64, status: JobStatus, expected: JobStatus },
    #[error("payload is {size} bytes; the limit is {limit}")]
    PayloadTooLarge { size: usize, limit: usize },
    #[error("{}", describe_db_error(.0))]
    Db(#[from] driftdb::Error),
    #[error("codec: {0}")]
    Codec(#[from] serde_json::Error),
    #[error("corrupt index entry {0}")]
    CorruptIndex(String),
    #[error("background task failed: {0}")]
    Join(#[from] tokio::task::JoinError),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
}

/// Turn the driftdb errors an operator can act on into actionable messages.
fn describe_db_error(e: &driftdb::Error) -> String {
    match e {
        driftdb::Error::Locked(path) => format!(
            "database directory {} is locked by another process (only one Db may open a directory at a time)",
            path.display()
        ),
        driftdb::Error::UnsupportedFormat { found, supported } => format!(
            "data was written by on-disk format v{found}; this build supports v{supported} — use a matching driftdb-lsm version"
        ),
        driftdb::Error::InvalidArgument(msg) => format!("invalid argument: {msg}"),
        other => format!("storage error: {other}"),
    }
}

#[derive(Clone, Debug)]
pub struct JobStore {
    db: Db,
    /// Serializes read-modify-write operations. Guards the next id to allocate.
    write: Arc<Mutex<u64>>,
}

impl JobStore {
    pub async fn open(dir: impl AsRef<Path>, options: Options) -> Result<Self> {
        let db = Db::open_with(dir, options).await?;
        let next_id = match db.get(NEXT_ID_KEY).await? {
            Some(bytes) => u64::from_be_bytes(
                bytes.as_slice().try_into().map_err(|_| StoreError::CorruptIndex("meta/next_id".into()))?,
            ),
            None => 1,
        };
        Ok(Self { db, write: Arc::new(Mutex::new(next_id)) })
    }

    /// Escape hatch for maintenance and tests (flush, compact, raw scans).
    pub fn db(&self) -> &Db {
        &self.db
    }

    pub async fn enqueue(&self, new: NewJob, now: u64) -> Result<Job> {
        let mut next_id = self.write.lock().await;
        let job = Job {
            id: *next_id,
            kind: new.kind,
            payload: new.payload,
            status: JobStatus::Pending,
            attempts: 0,
            max_attempts: new.max_attempts.unwrap_or(DEFAULT_MAX_ATTEMPTS).max(1),
            lease_until: None,
            created_at: now,
            updated_at: now,
            last_error: None,
        };
        let value = serde_json::to_vec(&job)?;
        let limit = MAX_PAYLOAD_BYTES.min(driftdb::MAX_VALUE_LEN);
        if value.len() > limit {
            return Err(StoreError::PayloadTooLarge { size: value.len(), limit });
        }
        let batch = WriteBatch::new()
            .put(job_key(job.id), value)
            .put(status_key(JobStatus::Pending, job.id), Vec::new())
            .put(NEXT_ID_KEY, (job.id + 1).to_be_bytes().to_vec());
        self.db.write_batch(batch).await?; // durable once this returns
        *next_id += 1;
        Ok(job)
    }

    pub async fn get(&self, id: u64) -> Result<Option<Job>> {
        match self.db.get(&job_key(id)).await? {
            Some(bytes) => Ok(Some(serde_json::from_slice(&bytes)?)),
            None => Ok(None),
        }
    }

    pub async fn claim(&self, lease_ms: u64, now: u64) -> Result<Option<Job>> {
        let _guard = self.write.lock().await;
        // ponytail: scans the whole pending index to take its first entry; fine for
        // thousands of jobs, add a scan limit to driftdb if queues grow to millions.
        let pending = self.db.scan(prefix_range(&status_prefix(JobStatus::Pending))).await?;
        let Some((key, _)) = pending.first() else { return Ok(None) };
        let id = id_from_key(key).ok_or_else(|| StoreError::CorruptIndex(String::from_utf8_lossy(key).into()))?;
        let mut job = self.get(id).await?.ok_or(StoreError::NotFound(id))?;
        job.status = JobStatus::Running;
        job.lease_until = Some(now + lease_ms);
        job.updated_at = now;
        self.db.write_batch(move_batch(WriteBatch::new(), JobStatus::Pending, &job)?).await?;
        Ok(Some(job))
    }

    pub async fn complete(&self, id: u64, now: u64) -> Result<Job> {
        let _guard = self.write.lock().await;
        let mut job = self.running_job(id).await?;
        job.status = JobStatus::Done;
        job.lease_until = None;
        job.updated_at = now;
        self.db.write_batch(move_batch(WriteBatch::new(), JobStatus::Running, &job)?).await?;
        Ok(job)
    }

    pub async fn fail(&self, id: u64, error: String, now: u64) -> Result<Job> {
        let _guard = self.write.lock().await;
        let mut job = self.running_job(id).await?;
        job.attempts += 1;
        job.status = if job.attempts >= job.max_attempts { JobStatus::Dead } else { JobStatus::Pending };
        job.lease_until = None;
        job.last_error = Some(error);
        job.updated_at = now;
        self.db.write_batch(move_batch(WriteBatch::new(), JobStatus::Running, &job)?).await?;
        Ok(job)
    }

    /// Return `running` jobs whose lease ended at or before `now` to `pending` (their worker
    /// died). All moves go in one batch.
    pub async fn requeue_expired(&self, now: u64) -> Result<usize> {
        let _guard = self.write.lock().await;
        let running = self.db.scan(prefix_range(&status_prefix(JobStatus::Running))).await?;
        let mut batch = WriteBatch::new();
        let mut moved = 0;
        for (key, _) in running {
            let id = id_from_key(&key).ok_or_else(|| StoreError::CorruptIndex(String::from_utf8_lossy(&key).into()))?;
            let Some(mut job) = self.get(id).await? else { continue };
            if job.lease_until.is_some_and(|until| until <= now) {
                job.status = JobStatus::Pending;
                job.lease_until = None;
                job.last_error = Some("lease expired".into());
                job.updated_at = now;
                batch = move_batch(batch, JobStatus::Running, &job)?;
                moved += 1;
            }
        }
        if moved > 0 {
            self.db.write_batch(batch).await?;
        }
        Ok(moved)
    }

    /// Jobs currently in `status`, oldest id first. Lock-free: a job may change status
    /// between the index scan and the record read, so records are re-checked.
    pub async fn list(&self, status: JobStatus, limit: usize) -> Result<Vec<Job>> {
        let index = self.db.scan(prefix_range(&status_prefix(status))).await?;
        let mut jobs = Vec::new();
        for (key, _) in index.into_iter().take(limit) {
            let Some(id) = id_from_key(&key) else { continue };
            if let Some(job) = self.get(id).await? {
                if job.status == status {
                    jobs.push(job);
                }
            }
        }
        Ok(jobs)
    }

    pub async fn close(&self) -> Result<()> {
        Ok(self.db.close().await?)
    }

    async fn running_job(&self, id: u64) -> Result<Job> {
        let job = self.get(id).await?.ok_or(StoreError::NotFound(id))?;
        if job.status != JobStatus::Running {
            return Err(StoreError::InvalidState { id, status: job.status, expected: JobStatus::Running });
        }
        Ok(job)
    }
}

/// Append "move `job` from `from`'s index to its current status + rewrite the record".
fn move_batch(batch: WriteBatch, from: JobStatus, job: &Job) -> Result<WriteBatch> {
    Ok(batch
        .delete(status_key(from, job.id))
        .put(status_key(job.status, job.id), Vec::new())
        .put(job_key(job.id), serde_json::to_vec(job)?))
}
```

- [ ] **Step 4: Run tests**

Run: `cargo test -p jobqueue --test store` → PASS (8 tests). Run twice to check the concurrency test is stable.

- [ ] **Step 5: Commit**

```bash
git add examples/jobqueue
git commit -m "feat(example): JobStore with atomic batches, secondary index, leases"
```

---

### Task 3: `JobStore` — snapshot report, export, purge, maintenance

**Files:**
- Modify: `examples/jobqueue/src/store.rs`, `examples/jobqueue/tests/store.rs`

**Interfaces:**
- Consumes: Task 2 `JobStore`, `StoreError`, `Result`.
- Produces: `report(&self) -> Result<Report>`; `report_at(&self, snap: driftdb::Snapshot) -> Result<Report>`; `export(&self, path: impl AsRef<Path>) -> Result<usize>`; `purge(&self, status: JobStatus, older_than: u64) -> Result<usize>`; `maintenance(&self) -> Result<(StatsView, StatsView)>`; sync `stats(&self) -> StatsView`.

- [ ] **Step 1: Append failing tests** to `tests/store.rs`:

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn snapshot_report_ignores_later_writes() {
    let dir = tempfile::tempdir().unwrap();
    let store = JobStore::open(dir.path(), small_opts()).await.unwrap();
    for n in 0..20 {
        store.enqueue(new_job(n), n).await.unwrap();
    }
    let snap = store.db().snapshot();
    let before = store.report().await.unwrap();
    for n in 20..40 {
        store.enqueue(new_job(n), n).await.unwrap();
    }
    store.claim(1_000, 100).await.unwrap();
    store.db().flush().await.unwrap();
    let at_snap = store.report_at(snap).await.unwrap();
    assert_eq!(at_snap.counts, before.counts, "a snapshot sees exactly the state it was taken at");
    assert_eq!(at_snap.counts["pending"], 20);
    assert_eq!(at_snap.oldest_pending.as_ref().map(|j| j.id), Some(1));
    let now = store.report().await.unwrap();
    assert_eq!((now.counts["pending"], now.counts["running"]), (39, 1));
    store.close().await.unwrap();
}

#[tokio::test]
async fn export_purge_and_maintenance() {
    let dir = tempfile::tempdir().unwrap();
    let store = JobStore::open(dir.path(), small_opts()).await.unwrap();
    for n in 0..100 {
        store.enqueue(new_job(n), 10).await.unwrap();
    }
    for _ in 0..60 {
        let j = store.claim(1_000, 20).await.unwrap().unwrap();
        store.complete(j.id, 30).await.unwrap();
    }
    let out = dir.path().join("backup.jsonl");
    assert_eq!(store.export(&out).await.unwrap(), 100);
    let lines = std::fs::read_to_string(&out).unwrap();
    assert_eq!(lines.lines().count(), 100);
    let first: Job = serde_json::from_str(lines.lines().next().unwrap()).unwrap();
    assert_eq!(first.id, 1);

    assert_eq!(store.purge(JobStatus::Done, 30).await.unwrap(), 0, "older_than is exclusive");
    assert_eq!(store.purge(JobStatus::Done, 31).await.unwrap(), 60);
    let (before, after) = store.maintenance().await.unwrap();
    assert!(before.user_bytes_written > 0);
    assert_eq!(after.memtable_bytes, 0, "maintenance flushes the memtable");
    let report = store.report().await.unwrap();
    assert_eq!((report.counts["done"], report.counts["pending"]), (0, 40));
    store.close().await.unwrap();
}
```

- [ ] **Step 2: Run, expect failure**

Run: `cargo test -p jobqueue --test store snapshot_report export_purge` → FAIL (methods missing).

- [ ] **Step 3: Implement** — add to `impl JobStore` in `store.rs` (and `use crate::model::{Report, StatsView}; use crate::keys::JOB_PREFIX; use driftdb::Snapshot; use std::collections::BTreeMap; use std::io::Write;`):

```rust
    /// Consistent counts + oldest pending job, read from one snapshot while writers keep going.
    pub async fn report(&self) -> Result<Report> {
        self.report_at(self.db.snapshot()).await
    }

    /// `Snapshot::get`/`scan` are synchronous and may touch disk, so run them off the async
    /// executor with `spawn_blocking` — the same reason `Db::scan` does internally.
    pub async fn report_at(&self, snap: Snapshot) -> Result<Report> {
        tokio::task::spawn_blocking(move || -> Result<Report> {
            let mut counts = BTreeMap::new();
            let mut oldest_pending = None;
            for status in JobStatus::ALL {
                let index = snap.scan(prefix_range(&status_prefix(status)))?;
                if status == JobStatus::Pending {
                    if let Some(id) = index.first().and_then(|(k, _)| id_from_key(k)) {
                        if let Some(bytes) = snap.get(&job_key(id))? {
                            oldest_pending = Some(serde_json::from_slice::<Job>(&bytes)?);
                        }
                    }
                }
                counts.insert(status.as_str().to_string(), index.len() as u64);
            }
            Ok(Report { counts, oldest_pending, snapshot_seq: snap.seq() })
        })
        .await?
    }

    /// Write every job as one JSON line, from a snapshot (a consistent online backup).
    /// Returns the number of jobs written.
    pub async fn export(&self, path: impl AsRef<Path>) -> Result<usize> {
        let snap = self.db.snapshot();
        let path = path.as_ref().to_path_buf();
        tokio::task::spawn_blocking(move || -> Result<usize> {
            let records = snap.scan(prefix_range(JOB_PREFIX))?;
            let mut out = std::io::BufWriter::new(std::fs::File::create(&path)?);
            for (_, value) in &records {
                out.write_all(value)?;
                out.write_all(b"\n")?;
            }
            out.flush()?;
            Ok(records.len())
        })
        .await?
    }

    /// Delete jobs in `status` last updated strictly before `older_than`. Deletes are
    /// tombstones until compaction drops them (see `maintenance`).
    pub async fn purge(&self, status: JobStatus, older_than: u64) -> Result<usize> {
        let _guard = self.write.lock().await;
        let index = self.db.scan(prefix_range(&status_prefix(status))).await?;
        let mut batch = WriteBatch::new();
        let mut purged = 0;
        for (key, _) in index {
            let Some(id) = id_from_key(&key) else { continue };
            let Some(job) = self.get(id).await? else { continue };
            if job.updated_at < older_than {
                batch = batch.delete(key).delete(job_key(id));
                purged += 1;
                if batch.len() >= 1_000 {
                    self.db.write_batch(std::mem::take(&mut batch)).await?;
                }
            }
        }
        if !batch.is_empty() {
            self.db.write_batch(batch).await?;
        }
        Ok(purged)
    }

    /// Flush the memtable and run a full compaction (drops purged tombstones).
    /// Returns stats before and after.
    pub async fn maintenance(&self) -> Result<(StatsView, StatsView)> {
        let before = self.stats();
        self.db.flush().await?;
        self.db.compact().await?;
        Ok((before, self.stats()))
    }

    pub fn stats(&self) -> StatsView {
        StatsView::from(&self.db.stats())
    }
```

`std::mem::take` needs `WriteBatch: Default`. Check: `grep -n "impl Default for WriteBatch\|derive(.*Default" src/db.rs`. If `WriteBatch` is not `Default`, replace that line with `self.db.write_batch(std::mem::replace(&mut batch, WriteBatch::new())).await?;` (do not change the library).

- [ ] **Step 4: Run tests**

Run: `cargo test -p jobqueue` → PASS (all store tests + keys tests).

- [ ] **Step 5: Commit**

```bash
git add examples/jobqueue
git commit -m "feat(example): snapshot report, JSONL export, purge, maintenance"
```

---

### Task 4: HTTP API

**Files:**
- Modify: `examples/jobqueue/src/api.rs`
- Create: `examples/jobqueue/tests/api.rs`

**Interfaces:**
- Consumes: `JobStore` (Tasks 2–3), `jobqueue::now_ms()`.
- Produces: `api::router(store: JobStore) -> axum::Router`; `api::ApiError` (`IntoResponse`).

- [ ] **Step 1: Write failing tests** `tests/api.rs`:

```rust
use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use jobqueue::{api::router, store::JobStore};
use serde_json::{json, Value};
use tower::ServiceExt;

async fn call(app: &axum::Router, method: &str, uri: &str, body: Option<Value>) -> (StatusCode, Value) {
    let mut req = Request::builder().method(method).uri(uri);
    let body = match body {
        Some(v) => {
            req = req.header("content-type", "application/json");
            Body::from(v.to_string())
        }
        None => Body::empty(),
    };
    let res = app.clone().oneshot(req.body(body).unwrap()).await.unwrap();
    let status = res.status();
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    let json = if bytes.is_empty() { Value::Null } else { serde_json::from_slice(&bytes).unwrap_or(Value::Null) };
    (status, json)
}

#[tokio::test]
async fn full_job_lifecycle_over_http() {
    let dir = tempfile::tempdir().unwrap();
    let store = JobStore::open(dir.path(), driftdb::Options::default()).await.unwrap();
    let app = router(store.clone());

    let (s, job) = call(&app, "POST", "/jobs", Some(json!({"kind": "email", "payload": {"to": "a@b.c"}}))).await;
    assert_eq!(s, StatusCode::CREATED);
    assert_eq!(job["id"], 1);
    assert_eq!(job["status"], "pending");

    assert_eq!(call(&app, "GET", "/jobs/1", None).await.0, StatusCode::OK);
    assert_eq!(call(&app, "GET", "/jobs/99", None).await.0, StatusCode::NOT_FOUND);

    let (s, claimed) = call(&app, "POST", "/jobs/claim", Some(json!({"lease_ms": 30000}))).await;
    assert_eq!((s, claimed["status"].as_str()), (StatusCode::OK, Some("running")));
    assert_eq!(call(&app, "POST", "/jobs/claim", Some(json!({"lease_ms": 30000}))).await.0, StatusCode::NO_CONTENT);

    assert_eq!(call(&app, "POST", "/jobs/1/complete", None).await.0, StatusCode::OK);
    assert_eq!(call(&app, "POST", "/jobs/1/complete", None).await.0, StatusCode::CONFLICT);
    assert_eq!(call(&app, "POST", "/jobs/1/fail", Some(json!({"error": "x"}))).await.0, StatusCode::CONFLICT);

    let (s, done) = call(&app, "GET", "/jobs?status=done&limit=10", None).await;
    assert_eq!((s, done.as_array().map(Vec::len)), (StatusCode::OK, Some(1)));

    let (s, report) = call(&app, "GET", "/report", None).await;
    assert_eq!((s, &report["counts"]["done"]), (StatusCode::OK, &json!(1)));
    assert_eq!(call(&app, "GET", "/stats", None).await.0, StatusCode::OK);
    let (s, m) = call(&app, "POST", "/admin/maintenance", None).await;
    assert_eq!(s, StatusCode::OK);
    assert!(m["after"]["level_files"].is_array());

    drop(app);
    store.close().await.unwrap();
}

#[tokio::test]
async fn bad_input_is_4xx_never_500() {
    let dir = tempfile::tempdir().unwrap();
    let store = JobStore::open(dir.path(), driftdb::Options::default()).await.unwrap();
    let app = router(store.clone());

    let (s, body) = call(&app, "GET", "/jobs?status=bogus", None).await;
    assert_eq!(s, StatusCode::BAD_REQUEST);
    assert!(body["error"].as_str().unwrap().contains("unknown status"));
    assert_eq!(call(&app, "GET", "/jobs/abc", None).await.0, StatusCode::BAD_REQUEST);

    let req = Request::builder().method("POST").uri("/jobs").header("content-type", "application/json").body(Body::from("{not json")).unwrap();
    assert!(app.clone().oneshot(req).await.unwrap().status().is_client_error());

    let huge = json!({"kind": "x", "payload": "y".repeat(jobqueue::store::MAX_PAYLOAD_BYTES + 1)});
    assert_eq!(call(&app, "POST", "/jobs", Some(huge)).await.0, StatusCode::PAYLOAD_TOO_LARGE);

    drop(app);
    store.close().await.unwrap();
}
```

- [ ] **Step 2: Run, expect failure**

Run: `cargo test -p jobqueue --test api` → FAIL (`router` missing).

- [ ] **Step 3: Implement `src/api.rs`**

```rust
//! HTTP API over `JobStore`. Shows: sharing the store through axum state, and mapping
//! store/driftdb errors to status codes (client mistakes are 4xx; only storage faults are 500).

use crate::model::{JobStatus, NewJob};
use crate::now_ms;
use crate::store::{JobStore, StoreError};
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::json;

pub fn router(store: JobStore) -> Router {
    Router::new()
        .route("/jobs", post(create).get(list))
        .route("/jobs/claim", post(claim))
        .route("/jobs/{id}", get(get_job))
        .route("/jobs/{id}/complete", post(complete))
        .route("/jobs/{id}/fail", post(fail))
        .route("/report", get(report))
        .route("/stats", get(stats))
        .route("/admin/maintenance", post(maintenance))
        // The body limit must exceed MAX_PAYLOAD_BYTES so oversized jobs reach the store and get
        // a 413 from our own check, with a JSON error body.
        .layer(axum::extract::DefaultBodyLimit::max(crate::store::MAX_PAYLOAD_BYTES * 4))
        .with_state(store)
}

pub enum ApiError {
    Store(StoreError),
    BadRequest(String),
}

impl From<StoreError> for ApiError {
    fn from(e: StoreError) -> Self {
        ApiError::Store(e)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, msg) = match self {
            ApiError::BadRequest(m) => (StatusCode::BAD_REQUEST, m),
            ApiError::Store(e) => {
                let status = match &e {
                    StoreError::NotFound(_) => StatusCode::NOT_FOUND,
                    StoreError::InvalidState { .. } => StatusCode::CONFLICT,
                    StoreError::PayloadTooLarge { .. } => StatusCode::PAYLOAD_TOO_LARGE,
                    _ => {
                        tracing::error!(error = %e, "storage failure");
                        StatusCode::INTERNAL_SERVER_ERROR
                    }
                };
                (status, e.to_string())
            }
        };
        (status, Json(json!({ "error": msg }))).into_response()
    }
}

type ApiResult<T> = Result<T, ApiError>;

async fn create(State(store): State<JobStore>, Json(new): Json<NewJob>) -> ApiResult<(StatusCode, Json<crate::model::Job>)> {
    Ok((StatusCode::CREATED, Json(store.enqueue(new, now_ms()).await?)))
}

#[derive(Deserialize)]
struct ListQuery {
    status: String,
    #[serde(default = "default_limit")]
    limit: usize,
}

fn default_limit() -> usize {
    50
}

async fn list(State(store): State<JobStore>, Query(q): Query<ListQuery>) -> ApiResult<Response> {
    let status: JobStatus = q.status.parse().map_err(ApiError::BadRequest)?;
    Ok(Json(store.list(status, q.limit.min(1_000)).await?).into_response())
}

async fn get_job(State(store): State<JobStore>, Path(id): Path<u64>) -> ApiResult<Response> {
    let job = store.get(id).await?.ok_or(StoreError::NotFound(id))?;
    Ok(Json(job).into_response())
}

#[derive(Deserialize)]
struct ClaimBody {
    #[serde(default = "default_lease")]
    lease_ms: u64,
}

fn default_lease() -> u64 {
    30_000
}

async fn claim(State(store): State<JobStore>, Json(body): Json<ClaimBody>) -> ApiResult<Response> {
    Ok(match store.claim(body.lease_ms, now_ms()).await? {
        Some(job) => Json(job).into_response(),
        None => StatusCode::NO_CONTENT.into_response(),
    })
}

async fn complete(State(store): State<JobStore>, Path(id): Path<u64>) -> ApiResult<Response> {
    Ok(Json(store.complete(id, now_ms()).await?).into_response())
}

#[derive(Deserialize)]
struct FailBody {
    error: String,
}

async fn fail(State(store): State<JobStore>, Path(id): Path<u64>, Json(body): Json<FailBody>) -> ApiResult<Response> {
    Ok(Json(store.fail(id, body.error, now_ms()).await?).into_response())
}

async fn report(State(store): State<JobStore>) -> ApiResult<Response> {
    Ok(Json(store.report().await?).into_response())
}

async fn stats(State(store): State<JobStore>) -> Response {
    Json(store.stats()).into_response()
}

async fn maintenance(State(store): State<JobStore>) -> ApiResult<Response> {
    let (before, after) = store.maintenance().await?;
    Ok(Json(json!({ "before": before, "after": after })).into_response())
}
```

Note: `JobStore` must be `Clone` (it is) for `State`. axum's `Path<u64>` rejection on `/jobs/abc` is 400 by default.

- [ ] **Step 4: Run tests**

Run: `cargo test -p jobqueue --test api` → PASS (2 tests). If `/jobs/claim` collides with `/jobs/{id}` (it must not in axum 0.8 — static segments win), fix the route table, not the test.

- [ ] **Step 5: Commit**

```bash
git add examples/jobqueue
git commit -m "feat(example): axum HTTP API with error-to-status mapping"
```

---

### Task 5: Binary (`demo`, `crash-demo`, `serve`), example README, CI, root docs

**Files:**
- Modify: `examples/jobqueue/src/main.rs`, `.github/workflows/ci.yml`, `README.md`, `CHANGELOG.md` (Unreleased section)
- Create: `examples/jobqueue/README.md`

**Interfaces:**
- Consumes: everything above.
- Produces: CLI `jobqueue demo [--dir D] [--jobs N]`, `jobqueue crash-demo [--dir D]`, hidden `jobqueue crash-child --dir D`, `jobqueue serve [--dir D] [--addr A]`.

- [ ] **Step 1: Implement `src/main.rs`**

```rust
use anyhow::{bail, Context};
use clap::{Parser, Subcommand};
use driftdb::Options;
use jobqueue::model::{JobStatus, NewJob};
use jobqueue::store::JobStore;
use jobqueue::{api, now_ms};
use std::io::{BufRead, BufReader, Write};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Parser)]
#[command(about = "Durable job queue on driftdb-lsm — a reference integration")]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Scripted end-to-end run: producers, workers, failures, snapshot report, backup,
    /// purge + compaction, close and reopen.
    Demo {
        #[arg(long)]
        dir: Option<PathBuf>,
        #[arg(long, default_value_t = 2_000)]
        jobs: u64,
    },
    /// SIGKILL a writer mid-flight, reopen, and verify every acknowledged job survived.
    CrashDemo {
        #[arg(long)]
        dir: Option<PathBuf>,
    },
    #[command(hide = true)]
    CrashChild {
        #[arg(long)]
        dir: PathBuf,
    },
    /// Run the HTTP API.
    Serve {
        #[arg(long, default_value = "./jobqueue-data")]
        dir: PathBuf,
        #[arg(long, default_value = "127.0.0.1:3000")]
        addr: SocketAddr,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();
    match Cli::parse().cmd {
        Cmd::Demo { dir, jobs } => demo(dir.unwrap_or_else(|| temp_dir("demo")), jobs).await,
        Cmd::CrashDemo { dir } => crash_demo(dir.unwrap_or_else(|| temp_dir("crash"))).await,
        Cmd::CrashChild { dir } => crash_child(dir).await,
        Cmd::Serve { dir, addr } => serve(dir, addr).await,
    }
}

fn temp_dir(tag: &str) -> PathBuf {
    std::env::temp_dir().join(format!("jobqueue-{tag}-{}", std::process::id()))
}

/// Small sizes so a short demo actually flushes and compacts.
fn demo_options() -> Options {
    Options { memtable_size: 256 * 1024, l1_max_bytes: 1024 * 1024, target_file_size: 256 * 1024, ..Default::default() }
}

fn step(n: u32, text: &str) {
    println!("\n[{n}] {text}");
}

async fn demo(dir: PathBuf, jobs: u64) -> anyhow::Result<()> {
    let _ = std::fs::remove_dir_all(&dir);
    step(1, &format!("open {} with small Options so flushes/compactions happen", dir.display()));
    let store = JobStore::open(&dir, demo_options()).await?;

    step(2, "a worker claims one job with a 1s lease and then 'crashes' (never finishes it)");
    store.enqueue(NewJob { kind: "report".into(), payload: serde_json::json!({ "orphan": true }), max_attempts: None }, now_ms()).await?;
    let orphan = store.claim(1_000, now_ms()).await?.context("orphan claim")?;
    println!("    job {} is running with nobody working on it", orphan.id);

    step(3, &format!("4 producers enqueue {jobs} jobs while 8 workers drain the queue"));
    let producers_done = Arc::new(AtomicBool::new(false));
    let mut producers = Vec::new();
    for p in 0..4u64 {
        let store = store.clone();
        producers.push(tokio::spawn(async move {
            let mut n = p;
            while n < jobs {
                // Every 10th job always fails: it retries max_attempts (3) times, then goes dead.
                let new = NewJob { kind: "email".into(), payload: serde_json::json!({ "n": n, "fail": n % 10 == 0 }), max_attempts: Some(3) };
                store.enqueue(new, now_ms()).await?;
                n += 4;
            }
            anyhow::Ok(())
        }));
    }
    let mut workers = Vec::new();
    for _ in 0..8 {
        let store = store.clone();
        let producers_done = producers_done.clone();
        workers.push(tokio::spawn(async move {
            let mut processed = 0u64;
            loop {
                match store.claim(30_000, now_ms()).await? {
                    Some(job) if job.payload["fail"] == true => {
                        store.fail(job.id, "simulated failure".into(), now_ms()).await?;
                    }
                    Some(job) => {
                        store.complete(job.id, now_ms()).await?;
                        processed += 1;
                    }
                    None if producers_done.load(Ordering::Acquire) => break,
                    None => tokio::time::sleep(Duration::from_millis(2)).await,
                }
            }
            anyhow::Ok(processed)
        }));
    }
    for p in producers {
        p.await??;
    }
    producers_done.store(true, Ordering::Release);

    step(4, "snapshot report while the workers are still writing (consistent point-in-time view)");
    let r = store.report().await?;
    println!("    at seq {}: {:?}", r.snapshot_seq, r.counts);

    let mut completed = 0;
    for w in workers {
        completed += w.await??;
    }
    println!("    workers completed {completed} jobs");

    step(5, "the crashed worker's lease expires; requeue_expired puts its job back");
    let requeued = store.requeue_expired(now_ms() + 2_000).await?;
    let job = store.claim(30_000, now_ms()).await?.context("requeued job")?;
    store.complete(job.id, now_ms()).await?;
    println!("    requeued {requeued} job(s); job {} finished by another worker", job.id);

    let report = store.report().await?;
    println!("    final: {:?}", report.counts);
    let expected_dead = jobs.div_ceil(10);
    if report.counts["dead"] != expected_dead || report.counts["done"] != jobs + 1 - expected_dead {
        bail!("unexpected final counts {:?}", report.counts);
    }

    step(6, "online backup: export every job from a snapshot to JSONL");
    let backup = dir.with_extension("jsonl");
    let lines = store.export(&backup).await?;
    println!("    wrote {lines} jobs to {}", backup.display());

    step(7, "purge done jobs, then flush + full compaction to drop their tombstones");
    let purged = store.purge(JobStatus::Done, now_ms() + 1).await?;
    let (before, after) = store.maintenance().await?;
    println!("    purged {purged}");
    println!("    before: files per level {:?}, write amp {:.2}x", before.level_files, before.write_amplification);
    println!("    after:  files per level {:?}, write amp {:.2}x", after.level_files, after.write_amplification);

    step(8, "close, reopen, and check nothing changed (recovery)");
    let counts = store.report().await?.counts;
    store.close().await?;
    let reopened = JobStore::open(&dir, demo_options()).await?;
    let again = reopened.report().await?.counts;
    if counts != again {
        bail!("counts changed across reopen: {counts:?} vs {again:?}");
    }
    println!("    identical after reopen: {again:?}");
    reopened.close().await?;
    std::fs::remove_dir_all(&dir).ok();
    std::fs::remove_file(&backup).ok();
    println!("\ndemo ok");
    Ok(())
}

async fn crash_demo(dir: PathBuf) -> anyhow::Result<()> {
    let _ = std::fs::remove_dir_all(&dir);
    let exe = std::env::current_exe()?;
    let mut child = Command::new(exe)
        .args(["crash-child", "--dir"])
        .arg(&dir)
        .stdout(Stdio::piped())
        .spawn()?;
    let stdout = child.stdout.take().context("child stdout")?;
    // Read acked ids on a thread; the parent kills the child after ~1s.
    let reader = std::thread::spawn(move || {
        let mut acked = Vec::new();
        for line in BufReader::new(stdout).lines() {
            let Ok(line) = line else { break };
            if let Some(id) = line.strip_prefix("acked ").and_then(|s| s.parse::<u64>().ok()) {
                acked.push(id);
            }
        }
        acked
    });
    let started = Instant::now();
    tokio::time::sleep(Duration::from_millis(1_000)).await;
    child.kill()?; // SIGKILL: no destructors, no flush, no close()
    child.wait()?;
    let acked = reader.join().expect("reader thread");
    println!("killed the writer after {:?}; it had acknowledged {} jobs", started.elapsed(), acked.len());
    if acked.is_empty() {
        bail!("child acknowledged nothing before the kill");
    }

    let store = JobStore::open(&dir, Options::default()).await?;
    let mut missing = Vec::new();
    for id in &acked {
        if store.get(*id).await?.is_none() {
            missing.push(*id);
        }
    }
    let report = store.report().await?;
    store.close().await?;
    std::fs::remove_dir_all(&dir).ok();
    if !missing.is_empty() {
        bail!("{} acknowledged jobs were lost: {:?}", missing.len(), &missing[..missing.len().min(10)]);
    }
    println!("all {} acknowledged jobs survived SIGKILL (store holds {} pending — in-flight unacked writes may also land)", acked.len(), report.counts["pending"]);
    println!("crash-demo ok");
    Ok(())
}

async fn crash_child(dir: PathBuf) -> anyhow::Result<()> {
    let store = JobStore::open(&dir, Options { memtable_size: 64 * 1024, ..Default::default() }).await?;
    let mut tasks = Vec::new();
    for t in 0..8u64 {
        let store = store.clone();
        tasks.push(tokio::spawn(async move {
            for n in 0.. {
                let job = store.enqueue(NewJob { kind: "crash".into(), payload: serde_json::json!({ "t": t, "n": n }), max_attempts: None }, now_ms()).await?;
                // Printed only after enqueue returned, i.e. after fdatasync.
                let mut out = std::io::stdout().lock();
                writeln!(out, "acked {}", job.id)?;
                out.flush()?;
            }
            anyhow::Ok(())
        }));
    }
    for t in tasks {
        t.await??;
    }
    Ok(())
}

async fn serve(dir: PathBuf, addr: SocketAddr) -> anyhow::Result<()> {
    let store = JobStore::open(&dir, Options::default()).await?;
    // Jobs left running by a previous crash (or a dead worker) come back after their lease.
    let sweeper = {
        let store = store.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(5));
            loop {
                tick.tick().await;
                match store.requeue_expired(now_ms()).await {
                    Ok(0) => {}
                    Ok(n) => tracing::info!(requeued = n, "expired leases requeued"),
                    Err(e) => tracing::warn!(error = %e, "requeue sweep failed"),
                }
            }
        })
    };
    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!(%addr, dir = %dir.display(), "jobqueue listening");
    axum::serve(listener, api::router(store.clone()))
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
            tracing::info!("shutting down");
        })
        .await?;
    sweeper.abort();
    store.close().await?; // drains the WAL writer and flushes memtables
    Ok(())
}
```

- [ ] **Step 2: Run the binary end to end**

Run: `cargo run -p jobqueue -- demo --jobs 300` → ends with `demo ok`, exit 0.
Run: `cargo run -p jobqueue -- crash-demo` → ends with `crash-demo ok`.
Run: `cargo run -p jobqueue -- serve --dir /tmp/claude-jq-serve --addr 127.0.0.1:3917 &` then
`curl -s -XPOST localhost:3917/jobs -H 'content-type: application/json' -d '{"kind":"email","payload":{}}'` → JSON with `"id":1`; `curl -s localhost:3917/report`; then `kill -INT %1` and confirm the log shows `shutting down` and the process exits 0; `rm -rf /tmp/claude-jq-serve`.

- [ ] **Step 3: Write `examples/jobqueue/README.md`** with these sections (real content, no placeholders):
  1. *What this is* — durable job queue, how to run the three subcommands (commands from Step 2) and sample `demo` output (paste a real run).
  2. *Using driftdb in your project* — the `Cargo.toml` line (`driftdb-lsm = "0.1"`, imported as `driftdb`), Linux-only, Rust 1.85+ for the library.
  3. *Feature → code map* — table: Options/open errors → `JobStore::open`; atomic multi-key write → `enqueue`, `move_batch`; secondary index + prefix scan → `keys.rs`, `list`; read-modify-write safety → the `write` mutex; snapshots → `report_at`, `export`; deletes/tombstones/compaction → `purge`, `maintenance`; stats → `StatsView`; graceful shutdown → `serve` + `close`; crash safety → `crash-demo`.
  4. *Patterns and pitfalls* — (a) design keys for byte order (fixed-width ids); (b) put record + index in one batch; (c) no CAS: serialize RMW or shard locks, never read-then-write unguarded; (d) `Snapshot::get/scan` are sync → `spawn_blocking`; (e) `scan` returns a `Vec` — bound your ranges; (f) one process per directory (`Locked`); (g) call `close().await` on shutdown, dropping the last handle also works but blocks; (h) deletes are tombstones until compaction.
  5. *HTTP API* — route table from the spec + curl examples.

- [ ] **Step 4: CI** — in `.github/workflows/ci.yml`:
  - `msrv` job: change `cargo check --lib` to `cargo check -p driftdb-lsm --lib`.
  - `docs` job: `cargo doc --no-deps -p driftdb-lsm` and `cargo publish -p driftdb-lsm --dry-run`.
  - add job:

```yaml
  example:
    name: example (jobqueue)
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
        with:
          components: clippy, rustfmt
      - uses: Swatinem/rust-cache@v2
      - run: cargo fmt -p jobqueue --check
      - run: cargo clippy -p jobqueue --all-targets -- -D warnings
      - run: cargo test -p jobqueue
      - run: cargo run -p jobqueue -- demo --jobs 300
      - run: cargo run -p jobqueue -- crash-demo
```

- [ ] **Step 5: Root docs** — `README.md`: add an "Examples" section after "Usage" pointing at `examples/jobqueue` (one paragraph + the three commands); fix the repository-layout block to list `examples/jobqueue/`. `CHANGELOG.md` `[Unreleased]`: `### Added` — "`examples/jobqueue`: reference integration (typed store, secondary index, snapshots, HTTP API, crash demo)".

- [ ] **Step 6: Full gates**

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cargo +1.85.0 check -p driftdb-lsm --lib
cargo deny check
cargo package --list -p driftdb-lsm | grep -c jobqueue   # must print 0
cargo publish -p driftdb-lsm --dry-run
```

Expected: all pass. If `cargo deny` rejects a new license from axum/clap deps, add only that SPDX id to `deny.toml` `allow` with a comment naming the crate.

- [ ] **Step 7: Commit**

```bash
git add examples/jobqueue .github/workflows/ci.yml README.md CHANGELOG.md deny.toml Cargo.lock
git commit -m "feat(example): demo, crash-demo and serve subcommands; example README; CI job"
```

---

### Task 6: Release workflow + trusted-publishing docs (parallel with Tasks 1–5; touches only these files)

**Files:**
- Create: `.github/workflows/release.yml`
- Modify: `docs/plans/2026-09-28-publishing.md`, `CLAUDE.md` (Packaging facts → release procedure)

**Interfaces:**
- Produces: workflow `release` triggered by tags `v*`, GitHub environment `release`.

- [ ] **Step 1: Write `.github/workflows/release.yml`**

```yaml
name: release

on:
  push:
    tags: ["v*"]

permissions:
  contents: read

jobs:
  publish:
    name: publish driftdb-lsm to crates.io (trusted publishing)
    runs-on: ubuntu-latest
    environment: release
    permissions:
      id-token: write   # OIDC token exchanged for a short-lived crates.io token
      contents: write   # create the GitHub release
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
      - uses: Swatinem/rust-cache@v2
      - name: Tag must match Cargo.toml version
        run: |
          tag="${GITHUB_REF_NAME#v}"
          ver=$(cargo metadata --no-deps --format-version 1 | jq -r '.packages[] | select(.name == "driftdb-lsm") | .version')
          if [ "$tag" != "$ver" ]; then echo "tag v$tag does not match Cargo.toml version $ver"; exit 1; fi
      - name: Tests
        run: cargo test -p driftdb-lsm
      - name: Dry run
        run: cargo publish -p driftdb-lsm --dry-run
      - uses: rust-lang/crates-io-auth-action@v1
        id: auth
      - name: Publish
        run: cargo publish -p driftdb-lsm
        env:
          CARGO_REGISTRY_TOKEN: ${{ steps.auth.outputs.token }}
      - name: GitHub release from CHANGELOG
        env:
          GH_TOKEN: ${{ github.token }}
        run: |
          ver="${GITHUB_REF_NAME#v}"
          awk -v v="$ver" 'index($0, "## [" v "]") == 1 {f=1; next} /^## \[/ {if (f) exit} /^\[/ {if (f) exit} f' CHANGELOG.md > notes.md
          test -s notes.md || { echo "no CHANGELOG section for $ver"; exit 1; }
          gh release create "$GITHUB_REF_NAME" --title "driftdb-lsm $ver" --notes-file notes.md
```

- [ ] **Step 2: Validate locally**

Run: `python3 -c "import yaml,sys; yaml.safe_load(open('.github/workflows/release.yml'))"` → no error. Check the awk extraction against the real CHANGELOG: `ver=0.1.0; awk -v v="$ver" 'index($0, "## [" v "]") == 1 {f=1; next} /^## \[/ {if (f) exit} /^\[/ {if (f) exit} f' CHANGELOG.md | head` → prints the 0.1.0 section body only.

- [ ] **Step 3: Create the GitHub environment, restricted to `v*` tags**

```bash
gh api -X PUT repos/Bunty9/driftdb/environments/release \
  -F 'deployment_branch_policy[protected_branches]=false' \
  -F 'deployment_branch_policy[custom_branch_policies]=true'
gh api -X POST repos/Bunty9/driftdb/environments/release/deployment-branch-policies -f name='v*' -f type=tag
gh api repos/Bunty9/driftdb/environments/release --jq '.name, .deployment_branch_policy'
```

- [ ] **Step 4: Docs** — rewrite the "Release procedure" and "Optional: automate later releases" parts of `docs/plans/2026-09-28-publishing.md` into "Releasing (trusted publishing)": one-time crates.io setup (Trusted Publisher: owner `Bunty9`, repo `driftdb`, workflow `release.yml`, environment `release`), the `release` environment restricted to `v*` tags, per-release steps (bump `Cargo.toml` version, move CHANGELOG `[Unreleased]` items under `## [X.Y.Z] - date` + compare links, commit, `git tag -a vX.Y.Z -m …`, `git push origin vX.Y.Z`, watch `gh run watch`), and the optional hardening (crates.io "trusted publishing only", revoke local token). In `CLAUDE.md` Packaging facts, replace the release line with: releases are cut by pushing a `vX.Y.Z` tag; `.github/workflows/release.yml` publishes via trusted publishing; never publish locally.

- [ ] **Step 5: Commit**

```bash
git add .github/workflows/release.yml docs/plans/2026-09-28-publishing.md CLAUDE.md
git commit -m "ci: tag-triggered release workflow with crates.io trusted publishing"
```

---

### Task 7: Launch posts (parallel; `scratchpad/` only, never committed)

**Files:**
- Create: `scratchpad/linkedin.md`, `scratchpad/devto.md`

- [ ] **Step 1: Gather facts** — read `README.md` (Benchmarks, Durability & recovery, Design tradeoffs), `PROGRESS.md`, `CHANGELOG.md`, `docs/ARCHITECTURE.md`, `examples/jobqueue/README.md` if it exists yet, and `git log --oneline` for the bug-fix history (merge error propagation, snapshot registration race, shutdown ordering, manifest corruption handling).
- [ ] **Step 2: Write `scratchpad/linkedin.md`** — ~200 words, first-person, plain text (no markdown headers): hook ("I wanted to understand what fsync actually guarantees, so I built a storage engine"), three lessons (group commit: 400 → 93,800 writes/s just by sharing fsyncs across 1 → 1,024 concurrent writers; a k-way merge that kept draining after an I/O error would resurrect deleted keys; bounding WAL size per memtable makes recovery ~9 ms instead of "replay gigabytes"), honest caveats (Linux only, laptop numbers), links `https://crates.io/crates/driftdb-lsm` and `https://github.com/Bunty9/driftdb`, 3–5 hashtags (#rustlang #databases #systemsprogramming #opensource).
- [ ] **Step 3: Write `scratchpad/devto.md`** — front matter:

```yaml
---
title: "I built an LSM-tree storage engine in Rust to understand fsync"
published: false
description: "Group commit, MVCC snapshots, leveled compaction and crash recovery — what I learned building driftdb-lsm, and the bugs review caught."
tags: rust, database, systems, opensource
---
```

Body sections (1,800–2,500 words): Why; Architecture (ASCII diagram adapted from README); The write path and what an ack means (group commit, fdatasync vs fsync, fsyncgate poisoning); Reads and MVCC snapshots (seqno visibility, why reads register their seq); Compaction and GC (leveled, the GC rule `visible(filter(x,S),s) == visible(x,s)`, proptest); Crash recovery (one WAL per memtable, torn tails, manifest final-frame rule, kill -9 test); The bugs review caught (merge error resurrecting deletes, snapshot GC race, shutdown hang ordering, manifest read error wiping state) — concrete, each with the fix; Numbers (README table verbatim + hardware caveat); Using it (Cargo line, a 15-line snippet using `Db::open`, `write_batch`, `snapshot`, `scan`; pointer to `examples/jobqueue` and its patterns: key design, record+index in one batch, serialize read-modify-write); Limits and what's next (Linux only, no transactions/CAS, no RocksDB comparison yet, macOS F_FULLFSYNC, shared block cache). Every number must appear in README/PROGRESS; every code snippet must compile against the public API (verify by pasting it into a temporary `examples/jobqueue/examples/post_check.rs`, `cargo build -p jobqueue --example post_check`, then delete the file).
- [ ] **Step 4: Verify not tracked** — `git status --short scratchpad` prints nothing (`/scratchpad/` is in `.gitignore`).

---

### Task 8: Review, release 0.1.1, verify

- [ ] **Step 1: Opus review** of `examples/jobqueue` (correctness of the RMW locking, batch atomicity, error mapping, shutdown, crash-demo validity), `release.yml` (permissions, tag check, awk extraction), and the posts' factual accuracy against the repo. Fix confirmed findings (Tasks' gates re-run).
- [ ] **Step 2: Version bump** — root `Cargo.toml` `version = "0.1.1"`; `CHANGELOG.md`: move `[Unreleased]` items to `## [0.1.1] - <today>` (Added: jobqueue example, release workflow; Changed: package `include` narrowed), add `[0.1.1]: https://github.com/Bunty9/driftdb/compare/v0.1.0...v0.1.1` and update `[Unreleased]` to compare from `v0.1.1`. Run `cargo publish -p driftdb-lsm --dry-run`. Commit `chore: release 0.1.1`, push `main`, wait for CI green (`gh run watch`).
- [ ] **Step 3: Owner gate** — confirm with the owner that the crates.io Trusted Publisher is configured (owner `Bunty9`, repo `driftdb`, workflow `release.yml`, environment `release`). Do not push the tag before that.
- [ ] **Step 4: Tag** — `git tag -a v0.1.1 -m "driftdb-lsm 0.1.1" && git push origin v0.1.1`; `gh run watch` the `release` run. On failure at the auth step: trusted publisher config mismatch — report the exact error to the owner; do not fall back to a local token publish without the owner's explicit go-ahead.
- [ ] **Step 5: Verify** — `curl -s https://crates.io/api/v1/crates/driftdb-lsm | jq '.crate.max_version'` → `"0.1.1"`; the version's `published_by`/trustpub info shows GitHub Actions (`curl -s https://crates.io/api/v1/crates/driftdb-lsm/0.1.1 | jq '.version.trustpub_data // .version.published_by'`); `https://docs.rs/crate/driftdb-lsm/0.1.1/status.json` → `doc_status: true` (may take minutes); GitHub release `v0.1.1` exists with CHANGELOG notes; scratch consumer project with `driftdb-lsm = "=0.1.1"` builds and runs a put/get.
- [ ] **Step 6: Wrap-up** — update `PROGRESS.md` (Released: 0.1.1 via trusted publishing; remove the trusted-publishing item from Next), memory note, final summary to owner including the optional hardening steps (trusted-publishing-only toggle, revoke local token).
