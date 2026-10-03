//! `JobStore`: the integration pattern this example exists to show.
//!
//! * One `driftdb::Db`, cloned freely (it is an `Arc` handle) and shared by every task.
//! * Every state change is ONE `WriteBatch` (record + index + counter), so a crash can never
//!   leave a job half-moved. driftdb acks a batch only after `fdatasync`.
//! * `claim` bumps a per-job `claim_token`; `complete`/`fail` must present it, so a worker whose
//!   lease expired cannot finish or fail a job that someone else has re-claimed.
//! * driftdb has no compare-and-swap or transactions. Anything that reads state and writes
//!   based on it (allocate an id, claim, complete, fail, requeue) runs under one async
//!   mutex; plain reads take no lock. Without it two workers could claim the same job.
//!   ponytail: one global write lock caps write concurrency at "one RMW at a time"; shard
//!   the lock by key (e.g. per queue) if that ever becomes the bottleneck.

use crate::keys::{
    id_from_key, job_key, prefix_range, status_key, status_prefix, JOB_PREFIX, NEXT_ID_KEY,
};
use crate::model::{Job, JobStatus, NewJob, Report, StatsView};
use driftdb::{Db, Options, Snapshot, WriteBatch};
use std::collections::BTreeMap;
use std::io::Write;
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
    InvalidState {
        id: u64,
        status: JobStatus,
        expected: JobStatus,
    },
    #[error("job {id} was re-claimed by another worker (stale claim_token)")]
    LeaseLost { id: u64 },
    #[error("job record is {size} bytes; the limit is {limit}")]
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
                bytes
                    .as_slice()
                    .try_into()
                    .map_err(|_| StoreError::CorruptIndex("meta/next_id".into()))?,
            ),
            None => 1,
        };
        Ok(Self {
            db,
            write: Arc::new(Mutex::new(next_id)),
        })
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
            claim_token: 0,
            max_attempts: new.max_attempts.unwrap_or(DEFAULT_MAX_ATTEMPTS).max(1),
            lease_until: None,
            created_at: now,
            updated_at: now,
            last_error: None,
        };
        let value = serde_json::to_vec(&job)?;
        let limit = MAX_PAYLOAD_BYTES.min(driftdb::MAX_VALUE_LEN);
        if value.len() > limit {
            return Err(StoreError::PayloadTooLarge {
                size: value.len(),
                limit,
            });
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
        let pending = self
            .db
            .scan(prefix_range(&status_prefix(JobStatus::Pending)))
            .await?;
        let Some((key, _)) = pending.first() else {
            return Ok(None);
        };
        let id = id_from_key(key)
            .ok_or_else(|| StoreError::CorruptIndex(String::from_utf8_lossy(key).into()))?;
        let mut job = self.get(id).await?.ok_or(StoreError::NotFound(id))?;
        job.status = JobStatus::Running;
        job.claim_token += 1;
        job.lease_until = Some(now.saturating_add(lease_ms));
        job.updated_at = now;
        self.db
            .write_batch(move_batch(WriteBatch::new(), JobStatus::Pending, &job)?)
            .await?;
        Ok(Some(job))
    }

    pub async fn complete(&self, id: u64, claim_token: u64, now: u64) -> Result<Job> {
        let _guard = self.write.lock().await;
        let mut job = self.owned_running_job(id, claim_token).await?;
        job.status = JobStatus::Done;
        job.lease_until = None;
        job.updated_at = now;
        self.db
            .write_batch(move_batch(WriteBatch::new(), JobStatus::Running, &job)?)
            .await?;
        Ok(job)
    }

    pub async fn fail(&self, id: u64, claim_token: u64, error: String, now: u64) -> Result<Job> {
        let _guard = self.write.lock().await;
        let mut job = self.owned_running_job(id, claim_token).await?;
        job.attempts += 1;
        job.status = if job.attempts >= job.max_attempts {
            JobStatus::Dead
        } else {
            JobStatus::Pending
        };
        job.lease_until = None;
        job.last_error = Some(error);
        job.updated_at = now;
        self.db
            .write_batch(move_batch(WriteBatch::new(), JobStatus::Running, &job)?)
            .await?;
        Ok(job)
    }

    /// Move `running` jobs whose lease ended at or before `now` back to `pending` (their worker
    /// died), or to `dead` once the expiry exhausts `max_attempts`. All moves go in one batch.
    pub async fn requeue_expired(&self, now: u64) -> Result<usize> {
        let _guard = self.write.lock().await;
        let running = self
            .db
            .scan(prefix_range(&status_prefix(JobStatus::Running)))
            .await?;
        let mut batch = WriteBatch::new();
        let mut moved = 0;
        for (key, _) in running {
            let id = id_from_key(&key)
                .ok_or_else(|| StoreError::CorruptIndex(String::from_utf8_lossy(&key).into()))?;
            let Some(mut job) = self.get(id).await? else {
                continue;
            };
            if job.lease_until.is_some_and(|until| until <= now) {
                // A lease that expires is a failed attempt, so a crash-looping job dead-letters.
                job.attempts += 1;
                job.status = if job.attempts >= job.max_attempts {
                    JobStatus::Dead
                } else {
                    JobStatus::Pending
                };
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
            let Some(id) = id_from_key(&key) else {
                continue;
            };
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
            Ok(Report {
                counts,
                oldest_pending,
                snapshot_seq: snap.seq(),
            })
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
            out.into_inner().map_err(|e| e.into_error())?.sync_all()?;
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
            let Some(id) = id_from_key(&key) else {
                continue;
            };
            let Some(job) = self.get(id).await? else {
                continue;
            };
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

    async fn owned_running_job(&self, id: u64, claim_token: u64) -> Result<Job> {
        let job = self.get(id).await?.ok_or(StoreError::NotFound(id))?;
        if job.claim_token != claim_token {
            return Err(StoreError::LeaseLost { id });
        }
        if job.status != JobStatus::Running {
            return Err(StoreError::InvalidState {
                id,
                status: job.status,
                expected: JobStatus::Running,
            });
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
