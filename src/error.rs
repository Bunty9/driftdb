//! Error type and shared `Result` alias for the public API.
//!
//! The crate uses `thiserror` for the library surface so callers can pattern-match on
//! variants. The boundary contract: I/O errors (`std::io::Error`) and bincode serialization
//! errors (`bincode::Error`) are wrapped through `#[from]`. Anything that indicates on-disk
//! corruption — a bad WAL CRC or a malformed manifest record — surfaces as a `WalCorrupt` or
//! `ManifestCorrupt` variant with a human-readable message.

use thiserror::Error;

/// Crate-wide error type. See module docs for the full taxonomy.
#[derive(Debug, Error)]
pub enum Error {
    /// Underlying filesystem error from `std::io` or `tokio::fs`.
    #[error("io: {0}")]
    Io(#[from] std::io::Error),

    /// WAL record failed checksum validation, ran off the end of the file mid-record, or had
    /// a length prefix that exceeded the configured limit.
    #[error("wal corrupt: {0}")]
    WalCorrupt(String),

    /// Manifest log replay encountered an unknown record kind, a truncated record, or a
    /// version mismatch.
    #[error("manifest corrupt: {0}")]
    ManifestCorrupt(String),

    /// bincode (de)serialization failed — used by the manifest log and the SSTable footer.
    #[error("bincode: {0}")]
    Bincode(#[from] bincode::Error),

    /// SSTable file failed validation on open or during a block read: truncated file, bad
    /// magic/footer offsets, a block that failed its CRC check, or a malformed index/bloom.
    #[error("sst corrupt: {0}")]
    SstCorrupt(String),

    /// A caller-supplied argument violates a documented limit — e.g. a key longer than
    /// [`crate::wal::MAX_KEY_LEN`] or a value longer than [`crate::wal::MAX_VALUE_LEN`]. Checked
    /// before the op ever reaches the writer thread, so the engine and any other pending writes
    /// are unaffected.
    #[error("invalid argument: {0}")]
    InvalidArgument(String),

    /// `Db::open`/`open_with` found `<dir>/LOCK` already held by another live `Db` (in this
    /// process or another). Only one open `Db` per directory is supported: two writers sharing
    /// a WAL/manifest would corrupt each other's state, and mmap-based WAL replay racing a live
    /// writer could SIGBUS. Close/drop the other `Db` first.
    #[error("driftdb: {0} is already open by another Db instance (directory lock held)")]
    Locked(std::path::PathBuf),
}

/// Crate-wide `Result` alias.
pub type Result<T> = std::result::Result<T, Error>;
