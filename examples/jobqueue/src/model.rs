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
    pub const ALL: [JobStatus; 4] = [
        JobStatus::Pending,
        JobStatus::Running,
        JobStatus::Done,
        JobStatus::Dead,
    ];

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
    /// Incremented on every claim; fences off workers whose lease expired.
    #[serde(default)]
    pub claim_token: u64,
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
