//! Error type and shared `Result` alias for the public API.
//!
//! The crate uses `thiserror` for the library surface so callers can pattern-match on
//! variants. The boundary contract: only `std::io::Error` is wrapped through `#[from]` — no
//! third-party error type appears in the public API (in particular, bincode's `Error` type is
//! never exposed; a bincode failure is always mapped to `ManifestCorrupt` or `SstCorrupt` at
//! the call site, since bincode is only ever used to decode the manifest log or an SST's
//! index/bloom region). WAL corruption is *not* one of those variants: a bad CRC, an invalid
//! kind byte, or a header/body that runs off the end of the file is always treated as a torn
//! tail from a mid-write crash (see `wal.rs`'s module docs) and silently truncated away on
//! replay rather than surfaced as an error. Manifest corruption gets the stricter treatment —
//! a `ManifestCorrupt` variant — because only a torn *tail* frame is dropped that way; the same
//! corruption in the middle of the log is a real error (see `manifest.rs`'s module docs).

use thiserror::Error;

/// Crate-wide error type. See module docs for the full taxonomy.
///
/// `#[non_exhaustive]`: new variants may be added in a minor (0.x) release; match with a
/// wildcard arm (`_ => ...`) rather than exhaustively.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum Error {
    /// Underlying filesystem error from `std::io`.
    #[error("io: {0}")]
    Io(#[from] std::io::Error),

    /// Manifest log replay encountered an unknown record kind or a truncated/corrupt record.
    /// A format-version mismatch is reported as [`Error::UnsupportedFormat`] instead.
    #[error("manifest corrupt: {0}")]
    ManifestCorrupt(String),

    /// SSTable file failed validation on open or during a block read: truncated file, bad
    /// magic/footer offsets, a block that failed its CRC check, or a malformed index/bloom. A
    /// format-version mismatch is reported as [`Error::UnsupportedFormat`] instead.
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

    /// `Db::open`/`open_with` found a manifest (or SST) written by an on-disk format version
    /// this build doesn't support. Checked before recovery touches any WAL or SST file, so the
    /// directory is left exactly as found — nothing is deleted or rewritten. Open it with a
    /// driftdb version that supports `found` instead.
    #[error("driftdb: on-disk format version {found} is not supported (this build supports {supported})")]
    UnsupportedFormat {
        /// The format version recorded on disk.
        found: u32,
        /// The format version this build supports.
        supported: u32,
    },
}

/// Crate-wide `Result` alias.
pub type Result<T> = std::result::Result<T, Error>;
