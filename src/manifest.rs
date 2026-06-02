//! Append-only manifest log.
//!
//! The manifest is the durable record of which SSTables belong to which level and which WAL
//! file is currently active. On open the engine replays the manifest to rebuild the in-memory
//! level metadata, then opens the current WAL and replays any records past the last flush.
//!
//! Records are length-prefixed bincode-encoded `ManifestRecord` values:
//!
//! ```text
//!   [u32 BE len][bincode payload]
//! ```
//!
//! Atomic update: writers append, then `fdatasync`. A successful append is what makes a
//! compaction visible; crash before the append → the compactor output SST is orphaned and
//! GC'd at next open.

use crate::error::{Error, Result};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

/// One entry in the manifest log. New variants must be appended (not reordered) — bincode is
/// position-indexed for enums.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ManifestRecord {
    /// A flush or compaction produced a new SSTable at `level`.
    SstAdded { level: u8, path: PathBuf },
    /// A compaction consumed an SSTable; reader handles holding the old file may finish their
    /// in-flight reads before the file is unlinked from disk.
    SstDeleted { level: u8, path: PathBuf },
    /// WAL rotation. `seq` is the first seqno that will appear in the new file.
    NewWal { path: PathBuf, seq: u64 },
}

/// Append-only manifest log. Owns an open file handle for appends + a replayed snapshot of the
/// log on construction.
#[derive(Debug)]
pub struct Manifest {
    _dir: PathBuf,
    // Populated in Phase 2:
    //   file: std::fs::File,
    //   records: Vec<ManifestRecord>,
}

impl Manifest {
    /// Open (or create) the manifest in `dir`. Replays the log to rebuild in-memory state.
    pub fn open(dir: &Path) -> Result<Self> {
        // Phase 2: open/create `dir/MANIFEST`, replay length-prefixed records, store handle.
        std::fs::create_dir_all(dir)?;
        Ok(Self {
            _dir: dir.to_path_buf(),
        })
    }

    /// Append one record + fdatasync. Returns once the record is durable.
    pub fn append(&mut self, _record: ManifestRecord) -> Result<()> {
        // Phase 2: bincode::serialize → write [len][bytes] → fdatasync.
        Ok(())
    }

    /// All records seen so far, in append order. Used by `Db::open` to rebuild level metadata
    /// and discover orphaned SST files from a crashed compaction.
    pub fn replay(&self) -> Vec<ManifestRecord> {
        Vec::new()
    }
}

/// Serialize one record to bytes. Pulled out for testability + so the writer + replayer
/// agree on the framing.
pub fn encode(record: &ManifestRecord) -> Result<Vec<u8>> {
    let payload = bincode::serialize(record)?;
    let mut out = Vec::with_capacity(4 + payload.len());
    out.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    out.extend_from_slice(&payload);
    Ok(out)
}

/// Deserialize one record from a `[u32 BE len][bytes]` framed slice. Returns the record + the
/// number of bytes consumed.
pub fn decode(buf: &[u8]) -> Result<(ManifestRecord, usize)> {
    if buf.len() < 4 {
        return Err(Error::ManifestCorrupt("truncated length prefix".into()));
    }
    let len = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
    if buf.len() < 4 + len {
        return Err(Error::ManifestCorrupt("truncated payload".into()));
    }
    let record: ManifestRecord = bincode::deserialize(&buf[4..4 + len])?;
    Ok((record, 4 + len))
}
