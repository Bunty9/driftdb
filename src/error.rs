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
}

/// Crate-wide `Result` alias.
pub type Result<T> = std::result::Result<T, Error>;
