# jobqueue: a durable job queue on driftdb-lsm

A reference integration for [`driftdb-lsm`](../../README.md): a small job queue
(enqueue, claim with a lease, complete, fail with retries, dead-letter; a lease that
expires counts as a failed attempt, so a job whose worker keeps crashing ends up dead) with a typed
store, a secondary index, snapshot reads, an HTTP API and a crash test. Read
`src/store.rs` first; it is the part to copy.

## What this is

```bash
cargo run -p jobqueue -- demo --jobs 300     # scripted end-to-end run
cargo run -p jobqueue -- crash-demo          # SIGKILL a writer, verify every acked job survived
cargo run -p jobqueue -- serve --dir ./jobqueue-data --addr 127.0.0.1:3000   # HTTP API
```

`demo` and `crash-demo` use a temp directory that they delete afterwards. With `--dir D` the
directory must be empty or not exist, and it is never deleted (the data is left in place for
you to inspect); a non-empty `--dir` is refused.

Sample `demo` output (a real run; ids, seqs and temp paths vary, and the mid-run counts in
step 4 depend on timing):

```text
[1] open /tmp/jobqueue-demo-1254035 with small Options so flushes/compactions happen

[2] a worker claims one job with a 1s lease and then 'crashes' (never finishes it)
    job 1 is running with nobody working on it

[3] 4 producers enqueue 300 jobs while 8 workers drain the queue

[4] snapshot report while the workers are still writing (consistent point-in-time view)
    at seq 2667: {"dead": 24, "done": 219, "pending": 54, "running": 4}
    workers completed 270 jobs

[5] the crashed worker's lease expires; requeue_expired puts its job back
    the crashed worker's late complete() was rejected: LeaseLost (fencing token)
    requeued 1 job(s); job 1 finished by another worker
    final: {"dead": 30, "done": 271, "pending": 0, "running": 0}

[6] online backup: export every job from a snapshot to JSONL
    wrote 301 jobs to /tmp/jobqueue-demo-1254035.jsonl

[7] purge done jobs, then flush + full compaction to drop their tombstones
    purged 271
    before: files per level [1], 155737 SST bytes, write amp 1.72x
    after:  files per level [0, 0, 0, 0, 0, 0, 1], 127712 SST bytes, write amp 4.51x

[8] close, reopen, and check nothing changed (recovery)
    identical after reopen: {"dead": 30, "done": 0, "pending": 0, "running": 0}

demo ok
```

`crash-demo` re-execs the binary as a child that enqueues jobs and prints `acked <id>` only
after `enqueue` returned (that is, after `fdatasync`). The parent SIGKILLs it after about a
second, reopens the directory and checks that every acked id is present.

## Using driftdb in your project

```toml
[dependencies]
driftdb-lsm = "0.1"   # imported as `driftdb`
```

Linux only (the WAL uses `fdatasync`, the directory lock uses `flock`). Rust 1.85+ for the
library. This example crate itself is not published and uses a `path` dependency.

## Feature to code map

| driftdb feature | Where to look |
|---|---|
| `Options`, `Db::open` and its errors (`Locked`) | `JobStore::open` in `src/store.rs` |
| Atomic multi-key write (`WriteBatch`) | `enqueue`, `move_batch` |
| Secondary index and prefix scan | `src/keys.rs`, `list` |
| Read-modify-write safety | the `write` mutex in `JobStore` |
| Snapshots (consistent reads) | `report_at`, `export` |
| Deletes, tombstones, compaction | `purge`, `maintenance` |
| Stats | `StatsView`, `GET /stats` |
| Graceful shutdown | `serve` in `src/main.rs` + `JobStore::close` |
| Crash safety | `crash-demo` |

## Patterns and pitfalls

a. **Design keys for byte order.** The engine sorts keys as bytes. Encode ids fixed-width (here 16
   lowercase hex digits, `job/<id>`) or big-endian so range and prefix scans return them in numeric order.
b. **Put a record and its index entries in one `WriteBatch`.** The batch is atomic, so the
   record and its index can never disagree after a crash.
c. **There is no compare-and-swap.** Serialize read-modify-write behind a lock (or shard
   locks by key); never read then write unguarded. The lock only protects one process, which
   is fine because a directory has one owner. A lock does not protect against slow
   *workers*, so claims also carry a **fencing token**: every `claim` increments the job's
   `claim_token`, and `complete`/`fail` must present the token they were given. Without
   it, a worker whose lease expired (it stalled, or was paused) could wake up and finish a
   job that `requeue_expired` already handed to someone else, overwriting the new owner's
   state. With it, the late worker gets `StoreError::LeaseLost` (HTTP 409), as in demo step 5.
d. **`Snapshot::get` and `Snapshot::scan` are synchronous.** Call them from
   `tokio::task::spawn_blocking` (see `report_at` and `export`), not directly on the async
   executor.
e. **`scan` returns a `Vec`.** The whole range is materialized. `limit` caps the response, not
   the scan — bound the key range itself.
f. **One process per directory.** A second `open` fails with `Error::Locked`; `JobStore::open`
   turns that into a readable message.
g. **Call `close().await` on shutdown.** It drains the writer and flushes memtables.
   Dropping the last handle also works, but it blocks the thread doing the drop.
h. **Deletes are tombstones until compaction.** `purge` frees nothing by itself; compaction
   reclaims them, so total SST bytes drop after purge plus `maintenance` (flush + full
   compaction), as demo step 7 prints. Write amp is cumulative over the engine's lifetime, so
   it only goes up: the full compaction rewrites data and raises it.

### Known limitations

- Each `claim` scans the whole pending index to take its first entry (marked `ponytail` in
  `store.rs`); fine for thousands of pending jobs, not millions.
- There is one global write lock, so each state transition pays its own `fdatasync`, one at
  a time. These operations do not benefit from driftdb's group commit; shard the lock or
  batch transitions if write throughput matters.
- Linux only, like the library.

## HTTP API

| Route | Body | Result |
|---|---|---|
| `POST /jobs` | `{"kind": "...", "payload": {...}, "max_attempts": 3}` | 201 and the job |
| `GET /jobs?status=pending&limit=50` | | list of jobs (status: pending, running, done, dead) |
| `GET /jobs/{id}` | | the job, or 404 |
| `POST /jobs/claim` | `{"lease_ms": 30000}` | the job (now running, with a new `claim_token`), or 204 if none |
| `POST /jobs/{id}/complete` | `{"claim_token": N}` | the job; 409 for a stale token or a job that is not running |
| `POST /jobs/{id}/fail` | `{"claim_token": N, "error": "..."}` | the job (retried, or dead after `max_attempts`); 409 as above |
| `GET /report` | | counts per status from one snapshot |
| `GET /stats` | | engine stats (levels, write amplification) |
| `POST /admin/maintenance` | | flush + full compaction, stats before and after |

Every error response is `{"error": "..."}`.

```bash
curl -s -XPOST localhost:3000/jobs -H 'content-type: application/json' \
  -d '{"kind":"email","payload":{"to":"a@example.com"}}'
curl -s -XPOST localhost:3000/jobs/claim -H 'content-type: application/json' -d '{"lease_ms":30000}'
# the claim response includes "claim_token": 1
curl -s -XPOST localhost:3000/jobs/1/complete -H 'content-type: application/json' -d '{"claim_token":1}'
curl -s 'localhost:3000/jobs?status=done&limit=10'
curl -s localhost:3000/report
```

`serve` also requeues jobs whose lease expired, every 5 seconds. Stop it with Ctrl-C or SIGTERM; it
shuts down gracefully.
