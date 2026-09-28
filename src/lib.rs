//! # driftdb
//!
//! Embeddable LSM-tree key-value engine. Write path:
//! `put → WAL (group-commit fsync) → memtable → freeze → flush → L0 SST → compactor → L1+`.
//! Read path: memtable → frozen memtable(s) → L0 (newest first) → L1+ (binary search,
//! bloom-filtered inside each SST). Keys carry a `(user_key, seqno)` pair for MVCC snapshot
//! reads; a single dedicated writer thread group-commits batches to the WAL, and a single
//! background thread owns flush + leveled compaction.
//!
//! Internals (threads, locks, recovery, invariants) are documented in
//! [ARCHITECTURE.md](https://github.com/Bunty9/driftdb/blob/main/docs/ARCHITECTURE.md);
//! on-disk formats in the [README](https://github.com/Bunty9/driftdb#on-disk-formats).
//!
//! ## Installation
//!
//! The crate is published as `driftdb-lsm` (the `driftdb` name on crates.io belongs to an
//! unrelated project), but the library is imported as `driftdb`:
//!
//! ```toml
//! [dependencies]
//! driftdb-lsm = "0.1"
//! ```
//!
//! Linux only for now: the WAL uses `fdatasync(2)` and the directory lock uses `flock(2)`.
//!
//! ## Quick start
//!
//! ```no_run
//! use driftdb::{Db, Result};
//!
//! # async fn run() -> Result<()> {
//! let db = Db::open("/tmp/driftdb-demo").await?;
//! db.put(b"hello", b"world").await?;
//! assert_eq!(db.get(b"hello").await?.as_deref(), Some(&b"world"[..]));
//! db.flush().await?; // force a flush to L0, just to demonstrate it's there
//! assert_eq!(db.get(b"hello").await?.as_deref(), Some(&b"world"[..]));
//! # Ok(())
//! # }
//! ```

#![deny(rust_2018_idioms)]
#![warn(missing_debug_implementations)]
#![warn(missing_docs)]

// The WAL relies on `fdatasync(2)` and the directory lock on `flock(2)`. macOS has no
// `fdatasync` (it would need `fcntl(F_FULLFSYNC)`) and Windows has neither, so fail with a
// readable message instead of an unresolved-import error from deep inside `libc`.
#[cfg(not(target_os = "linux"))]
compile_error!("driftdb currently supports Linux only (it needs fdatasync(2) and flock(2)).");

// Internal modules. Not `pub`: the public surface is the re-exports below plus `__bench`. A
// private `mod` declared at the crate root is still reachable from every other module in the
// crate (privacy is scoped to the declaring module and its descendants, and every module is a
// descendant of the root) -- it just isn't reachable from outside the crate, which is the point.
mod compaction;
mod db;
mod error;
mod iter;
mod manifest;
mod memtable;
mod sstable;
mod wal;

pub use crate::db::{Db, Options, Snapshot, Stats, WriteBatch};
pub use crate::error::{Error, Result};
pub use crate::wal::{MAX_KEY_LEN, MAX_VALUE_LEN};

/// On-disk format version shared by the manifest log and the SSTable footer. Bump this whenever
/// the WAL, SST, or manifest byte layout changes in a way an older or newer build can't read;
/// [`Db::open`] refuses a directory written by an unsupported version (see
/// [`Error::UnsupportedFormat`]) instead of misreading it.
pub(crate) const FORMAT_VERSION: u32 = 1;

/// Not part of the public API; no semver guarantees; for benches only.
///
/// `benches/report.rs`'s raw WAL-replay bench needs to drive `WalFile`/`replay` directly
/// (bypassing `Db` entirely) to isolate the replay decode path from the cost of flushing to an
/// SST on recovery. Everything else should go through [`Db`].
#[doc(hidden)]
pub mod __bench {
    pub use crate::memtable::Value;
    pub use crate::wal::{replay, wal_path, WalFile};
}
