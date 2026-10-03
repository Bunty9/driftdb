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

use crate::keys::{id_from_key, job_key, prefix_range, status_key, status_prefix, NEXT_ID_KEY};
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
    InvalidState {
        id: u64,
        status: JobStatus,
        expected: JobStatus,
    },
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
        job.lease_until = Some(now + lease_ms);
        job.updated_at = now;
        self.db
            .write_batch(move_batch(WriteBatch::new(), JobStatus::Pending, &job)?)
            .await?;
        Ok(Some(job))
    }

    pub async fn complete(&self, id: u64, now: u64) -> Result<Job> {
        let _guard = self.write.lock().await;
        let mut job = self.running_job(id).await?;
        job.status = JobStatus::Done;
        job.lease_until = None;
        job.updated_at = now;
        self.db
            .write_batch(move_batch(WriteBatch::new(), JobStatus::Running, &job)?)
            .await?;
        Ok(job)
    }

    pub async fn fail(&self, id: u64, error: String, now: u64) -> Result<Job> {
        let _guard = self.write.lock().await;
        let mut job = self.running_job(id).await?;
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

    /// Return `running` jobs whose lease ended at or before `now` to `pending` (their worker
    /// died). All moves go in one batch.
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

    async fn running_job(&self, id: u64) -> Result<Job> {
        let job = self.get(id).await?.ok_or(StoreError::NotFound(id))?;
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
