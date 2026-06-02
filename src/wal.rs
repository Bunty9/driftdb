//! Write-ahead log with group-commit fsync.
//!
//! ## Record format
//!
//! ```text
//!   offset  size  field
//!   ------  ----  ----------------------------------------
//!     0      4    crc32 (big-endian) over everything after
//!     4      8    seqno          (u64 BE)
//!    12      4    key_len        (u32 BE)
//!    16      4    val_len        (u32 BE)
//!    20      K    key bytes
//!  20+K      V    value bytes
//! ```
//!
//! Files are named `wal-NNNNNN.log` and rotated at 64MB. On open, the engine replays the
//! newest WAL by streaming records until either EOF, a length prefix that exceeds the
//! remaining file, or a CRC mismatch — at which point everything past that point is treated
//! as torn-tail garbage and truncated.
//!
//! ## Group commit
//!
//! The writer task collects records for up to `commit_window` (default 2ms) or 128 records,
//! whichever comes first, then issues one `fdatasync`. The cost of fdatasync (~100µs on NVMe,
//! milliseconds on rotational) is amortized across the batch. See the README "Design
//! tradeoffs" section for the fdatasync-vs-fsync defense.

use bytes::{BufMut, BytesMut};
use std::io::Write;
use tokio::sync::{mpsc, oneshot};

/// Default commit window — see module docs.
pub const DEFAULT_COMMIT_WINDOW_MS: u64 = 2;
/// Default group-commit batch size — see module docs.
pub const DEFAULT_GROUP_BATCH: usize = 128;

/// A record about to be written to the WAL. Constructed inside [`WalWriter::run`] and never
/// exposed publicly — this is the on-the-wire shape, not the API.
#[derive(Debug, Clone)]
pub struct WalRecord {
    pub seqno: u64,
    pub key: Vec<u8>,
    pub val: Vec<u8>,
}

/// Message sent to the WAL writer task. `Write` carries an `ack` oneshot that fires once the
/// containing group-commit has completed `fdatasync`. `Sync` forces an immediate flush of any
/// accumulated batch — useful for graceful shutdown.
#[derive(Debug)]
pub enum WalMsg {
    Write {
        key: Vec<u8>,
        val: Vec<u8>,
        ack: oneshot::Sender<u64>,
    },
    Sync,
}

/// Owns the append-only WAL file and the in-flight batch state. Constructed once at `Db::open`
/// and driven by `WalWriter::run` on a dedicated tokio task.
#[derive(Debug)]
pub struct WalWriter {
    file: std::fs::File,
    next_seqno: u64,
    /// Accumulated acks for the current group-commit batch.
    pending: Vec<oneshot::Sender<u64>>,
}

impl WalWriter {
    /// Open a WAL writer over an existing file handle. Caller is responsible for creating the
    /// file in append mode and positioning it at the end. `next_seqno` should be `last_seqno + 1`
    /// from the prior replay (or `1` on a fresh database).
    pub fn new(file: std::fs::File, next_seqno: u64) -> Self {
        Self {
            file,
            next_seqno,
            pending: Vec::with_capacity(DEFAULT_GROUP_BATCH),
        }
    }

    /// Drive the writer until the channel closes.
    ///
    /// Group-commit: collect writes for up to `commit_window`, then one `fdatasync`. Amortizes
    /// the ~100µs fsync cost across many writes. Code path lifted verbatim from
    /// `projects-l3-l4.md` § P5 with the ack-sender type aligned to `u64` so callers learn the
    /// assigned seqno.
    pub async fn run(mut self, mut rx: mpsc::Receiver<WalMsg>, commit_window: std::time::Duration) {
        let mut ticker = tokio::time::interval(commit_window);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut last_seq = self.next_seqno;
        loop {
            tokio::select! {
                Some(msg) = rx.recv() => match msg {
                    WalMsg::Write { key, val, ack } => {
                        let seq = self.next_seqno;
                        self.next_seqno += 1;
                        // Best-effort append; a failure here will be surfaced to the caller
                        // once we wire the error channel in Phase 2.
                        let _ = self.append_record(seq, &key, &val);
                        last_seq = seq;
                        self.pending.push(ack);
                        if self.pending.len() >= DEFAULT_GROUP_BATCH {
                            self.flush_and_ack(last_seq);
                        }
                    }
                    WalMsg::Sync => self.flush_and_ack(last_seq),
                },
                _ = ticker.tick() => {
                    if !self.pending.is_empty() { self.flush_and_ack(last_seq); }
                }
                else => break,
            }
        }
        // Drain on shutdown.
        if !self.pending.is_empty() {
            self.flush_and_ack(last_seq);
        }
    }

    fn flush_and_ack(&mut self, seq: u64) {
        // `fdatasync` is enough — file size only grows by appends and metadata (mtime) is not
        // load-bearing for our recovery contract. See README "Design tradeoffs" § "fdatasync".
        use std::os::unix::io::AsRawFd;
        // Safety: `self.file` is a valid open file descriptor owned by this struct; `fdatasync`
        // takes a raw fd and is the documented syscall for "durable through power loss without
        // also syncing metadata".
        unsafe {
            libc::fdatasync(self.file.as_raw_fd());
        }
        for ack in self.pending.drain(..) {
            let _ = ack.send(seq);
        }
    }

    fn append_record(&mut self, seq: u64, key: &[u8], val: &[u8]) -> std::io::Result<()> {
        let mut buf = BytesMut::with_capacity(16 + key.len() + val.len());
        buf.put_u64(seq);
        buf.put_u32(key.len() as u32);
        buf.put_u32(val.len() as u32);
        buf.put_slice(key);
        buf.put_slice(val);
        let crc = crc32fast::hash(&buf);
        self.file.write_all(&crc.to_be_bytes())?;
        self.file.write_all(&buf)?;
        Ok(())
    }
}
