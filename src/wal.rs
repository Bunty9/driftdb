//! Write-ahead log: record codec, per-file writer, and crash-recovery replay.
//!
//! ## Record format
//!
//! ```text
//!   offset  size  field
//!   ------  ----  ----------------------------------------
//!     0      4    crc32 (big-endian) over everything after
//!     4      8    seqno          (u64 BE)
//!    12      1    kind           (1 = Put, 0 = Delete)
//!    13      4    key_len        (u32 BE)
//!    17      4    val_len        (u32 BE, always 0 for Delete)
//!    21      K    key bytes
//!  21+K      V    value bytes
//! ```
//!
//! Files are named `wal-NNNNNN.log`, one per memtable generation (see `db.rs`'s freeze/flush
//! cycle). `WalFile::append` only buffers records in memory; `WalFile::sync` writes the buffer
//! out and issues one `fdatasync` per call, so callers control the fsync granularity (typically
//! once per group-commit batch).
//!
//! ## Torn-tail policy
//!
//! On replay we stream records until EOF, a header/body that runs off the end of the file, an
//! invalid `kind` byte, or a CRC mismatch. **Any of those is treated as "this is where the crash
//! landed"** and everything from that offset onward is truncated away — including a CRC mismatch
//! that might in principle be a genuinely corrupted (not torn) record deep inside the file. This
//! is the LevelDB/RocksDB tradeoff: a mid-file bit flip is indistinguishable from a torn tail
//! without per-record sync points, and treating both as "stop here" is what makes replay total
//! (it never returns a corruption error) at the cost of silently dropping everything after a rare
//! non-tail corruption instead of surfacing it.

use crate::memtable::Value;
use crate::Result;
use std::io::Write;
use std::os::unix::io::AsRawFd;
use std::path::{Path, PathBuf};

/// Fixed record header size: 4 (crc) + 8 (seq) + 1 (kind) + 4 (klen) + 4 (vlen).
const HEADER_LEN: usize = 4 + 8 + 1 + 4 + 4;

const KIND_DELETE: u8 = 0;
const KIND_PUT: u8 = 1;

/// Largest key `WalFile::append` (and the rest of the on-disk formats) will encode. `Db`
/// rejects larger inputs before they ever reach here — this module only `debug_assert`s it.
pub const MAX_KEY_LEN: usize = 65_535;
/// Largest value `WalFile::append` (and the rest of the on-disk formats) will encode. `Db`
/// rejects larger inputs before they ever reach here — this module only `debug_assert`s it.
pub const MAX_VALUE_LEN: usize = 256 * 1024 * 1024;

/// Path for WAL generation `number` inside `dir` — `dir/wal-NNNNNN.log`.
pub fn wal_path(dir: &Path, number: u64) -> PathBuf {
    dir.join(format!("wal-{number:06}.log"))
}

/// List every `wal-NNNNNN.log` file in `dir`, sorted by generation number ascending. Entries
/// that don't match the naming scheme (junk files, `MANIFEST`, `*.sst`, ...) are ignored.
pub fn list_wal_files(dir: &Path) -> Result<Vec<(u64, PathBuf)>> {
    let mut out = Vec::new();
    for entry in std::fs::read_dir(dir)? {
        let path = entry?.path();
        let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
            continue;
        };
        if let Some(number) = parse_wal_name(name) {
            out.push((number, path));
        }
    }
    out.sort_by_key(|(number, _)| *number);
    Ok(out)
}

/// Parse `wal-NNNNNN.log` -> generation number; `None` for anything else.
fn parse_wal_name(name: &str) -> Option<u64> {
    let digits = name.strip_prefix("wal-")?.strip_suffix(".log")?;
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    digits.parse().ok()
}

/// Fsync a directory — needed after creating/renaming/unlinking a file in it so the directory
/// entry itself is durable, not just the file's contents.
pub fn sync_dir(dir: &Path) -> std::io::Result<()> {
    std::fs::File::open(dir)?.sync_all()
}

/// One open WAL file. `append` buffers records in memory; `sync` is the only call that touches
/// disk (`write_all` of the buffer, then `fdatasync`).
#[derive(Debug)]
pub struct WalFile {
    file: std::fs::File,
    number: u64,
    buf: Vec<u8>,
    /// Bytes already durably written (synced) to `file`.
    synced_len: u64,
    /// Set once a `sync` call's `fdatasync` fails. A failed fsync can't be safely retried — the
    /// kernel does not guarantee the dirty pages are still around to retry against — so once
    /// poisoned every subsequent `sync` fails immediately too (fsyncgate).
    poisoned: bool,
}

impl WalFile {
    /// Create a brand-new WAL file for generation `number` in `dir`. Uses `create_new` so it
    /// fails if the file already exists (generations are never reused), then `fsync`s the
    /// directory so the new file's entry survives a crash right after creation.
    pub fn create(dir: &Path, number: u64) -> Result<Self> {
        let path = wal_path(dir, number);
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&path)?;
        sync_dir(dir)?;
        Ok(Self {
            file,
            number,
            buf: Vec::new(),
            synced_len: 0,
            poisoned: false,
        })
    }

    /// This file's generation number.
    pub fn number(&self) -> u64 {
        self.number
    }

    /// Encode one record and append it to the in-memory buffer. Not durable until `sync`.
    ///
    /// Debug-only sanity check against [`MAX_KEY_LEN`]/[`MAX_VALUE_LEN`] — enforcement lives in
    /// `Db`, which rejects oversized inputs before they get here.
    ///
    /// ponytail: writes straight into `self.buf` (reserving space for the CRC, then hashing the
    /// body slice in place) instead of building a separate `Vec` per record and copying it in --
    /// one fewer allocation and one fewer memcpy per write on the hot path.
    pub fn append(&mut self, seq: u64, key: &[u8], val: &Value) {
        let (kind, val_bytes): (u8, &[u8]) = match val {
            Value::Put(v) => (KIND_PUT, v.as_slice()),
            Value::Delete => (KIND_DELETE, &[]),
        };
        debug_assert!(
            key.len() <= MAX_KEY_LEN,
            "WalFile::append: key length {} exceeds MAX_KEY_LEN ({MAX_KEY_LEN}); Db must reject this before it reaches the WAL",
            key.len()
        );
        debug_assert!(
            val_bytes.len() <= MAX_VALUE_LEN,
            "WalFile::append: value length {} exceeds MAX_VALUE_LEN ({MAX_VALUE_LEN}); Db must reject this before it reaches the WAL",
            val_bytes.len()
        );
        let record_len = HEADER_LEN + key.len() + val_bytes.len();
        self.buf.reserve(record_len);
        let crc_pos = self.buf.len();
        self.buf.extend_from_slice(&[0u8; 4]); // placeholder, patched below
        let body_start = self.buf.len();
        self.buf.extend_from_slice(&seq.to_be_bytes());
        self.buf.push(kind);
        self.buf
            .extend_from_slice(&(key.len() as u32).to_be_bytes());
        self.buf
            .extend_from_slice(&(val_bytes.len() as u32).to_be_bytes());
        self.buf.extend_from_slice(key);
        self.buf.extend_from_slice(val_bytes);

        let crc = crc32fast::hash(&self.buf[body_start..]);
        self.buf[crc_pos..crc_pos + 4].copy_from_slice(&crc.to_be_bytes());
    }

    /// Write the buffered records and `fdatasync` the file. Checks the syscall's return value
    /// directly (rather than trusting `std`'s wrapper) per the plan's fsyncgate contract: on
    /// failure the file is poisoned and every future `sync` fails too.
    pub fn sync(&mut self) -> std::io::Result<()> {
        if self.poisoned {
            return Err(std::io::Error::other(
                "wal poisoned: a previous fdatasync failed",
            ));
        }
        if let Err(e) = self.file.write_all(&self.buf) {
            self.poisoned = true;
            return Err(e);
        }
        // Safety: `self.file` is a valid, open file descriptor owned by this struct for the
        // duration of this call.
        let ret = unsafe { libc::fdatasync(self.file.as_raw_fd()) };
        if ret == -1 {
            self.poisoned = true;
            return Err(std::io::Error::last_os_error());
        }
        self.synced_len += self.buf.len() as u64;
        self.buf.clear();
        Ok(())
    }

    /// Logical size of the file: bytes already synced plus whatever is still buffered.
    pub fn size(&self) -> u64 {
        self.synced_len + self.buf.len() as u64
    }

    /// Bytes currently buffered, not yet synced -- i.e. exactly what the next `sync()` call will
    /// write. Used by the caller to attribute those bytes to write-amplification accounting
    /// right before calling `sync`.
    pub fn pending_len(&self) -> u64 {
        self.buf.len() as u64
    }
}

/// Replay every valid record in `path`, calling `f(seq, key, val)` for each in file order.
///
/// Stops at the first record whose header or body runs past the end of the file, whose `kind`
/// byte is invalid, or whose CRC doesn't match (see the module docs' torn-tail policy). If
/// anything was dropped, the file is truncated to the last good offset and fsynced so a future
/// `append` starts clean. Returns the maximum seqno seen, or `0` if the file was empty (or every
/// record was torn).
///
/// ponytail: reads the file via `mmap` (like `SstReader`) instead of `read_to_end` into a fresh
/// `Vec` -- one less full-file copy off the page cache before decoding even starts. The
/// per-record `key`/`value` copies below stay: the memtable owns its keys/values, so something
/// has to allocate them eventually.
pub fn replay(path: &Path, mut f: impl FnMut(u64, Vec<u8>, Value)) -> Result<u64> {
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)?;
    let total_len = file.metadata()?.len() as usize;
    if total_len == 0 {
        return Ok(0);
    }
    // Safety: this file was just opened by this call and nothing else concurrently writes to a
    // WAL file mid-replay (replay only ever runs during recovery, before the writer thread for
    // this generation exists).
    let mmap = unsafe { memmap2::Mmap::map(&file)? };
    let data: &[u8] = &mmap;

    let mut offset = 0usize;
    let mut max_seq = 0u64;

    loop {
        if data.len() - offset < HEADER_LEN {
            break; // clean EOF, or a header that got cut off mid-write.
        }
        let rec = &data[offset..];
        let crc_stored = u32::from_be_bytes(rec[0..4].try_into().unwrap());
        let seq = u64::from_be_bytes(rec[4..12].try_into().unwrap());
        let kind = rec[12];
        let klen = u32::from_be_bytes(rec[13..17].try_into().unwrap()) as usize;
        let vlen = u32::from_be_bytes(rec[17..21].try_into().unwrap()) as usize;

        // Bounds-check the length prefixes against what's actually left in the file before
        // trusting them for slicing — guards against an absurd/corrupt klen or vlen without
        // ever allocating that many bytes.
        let after_header = data.len() - offset - HEADER_LEN;
        if klen > after_header {
            break;
        }
        let after_key = after_header - klen;
        if vlen > after_key {
            break;
        }
        if kind != KIND_PUT && kind != KIND_DELETE {
            break;
        }

        let key_start = offset + HEADER_LEN;
        let val_start = key_start + klen;
        let val_end = val_start + vlen;

        let crc_computed = crc32fast::hash(&data[offset + 4..val_end]);
        if crc_computed != crc_stored {
            break;
        }

        let value = if kind == KIND_PUT {
            Value::Put(data[val_start..val_end].to_vec())
        } else {
            Value::Delete
        };
        let key = data[key_start..val_start].to_vec();

        max_seq = max_seq.max(seq);
        f(seq, key, value);

        offset = val_end;
    }

    let need_truncate = offset < data.len();
    drop(mmap); // must not touch `data` (borrowed from it) past this point.
    if need_truncate {
        file.set_len(offset as u64)?;
        file.sync_all()?;
    }

    Ok(max_seq)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    /// `memtable::Value` doesn't derive `PartialEq` (out of scope for this module), so tests
    /// compare by hand.
    fn value_eq(a: &Value, b: &Value) -> bool {
        match (a, b) {
            (Value::Put(x), Value::Put(y)) => x == y,
            (Value::Delete, Value::Delete) => true,
            _ => false,
        }
    }

    fn write_and_sync(records: &[(u64, &[u8], Value)]) -> (tempfile::TempDir, PathBuf) {
        let dir = tempdir().expect("tempdir");
        let mut wal = WalFile::create(dir.path(), 1).expect("create");
        for (seq, key, val) in records {
            wal.append(*seq, key, val);
        }
        wal.sync().expect("sync");
        let path = wal_path(dir.path(), 1);
        (dir, path)
    }

    #[test]
    fn round_trip_puts_and_deletes() {
        let records: Vec<(u64, &[u8], Value)> = vec![
            (1, b"a", Value::Put(b"1".to_vec())),
            (2, b"b", Value::Put(b"2".to_vec())),
            (3, b"a", Value::Delete),
            (4, b"c", Value::Put(b"long value ".repeat(100))),
        ];
        let (_dir, path) = write_and_sync(&records);

        let mut got = Vec::new();
        let max_seq = replay(&path, |seq, key, val| got.push((seq, key, val))).expect("replay");

        assert_eq!(max_seq, 4);
        assert_eq!(got.len(), records.len());
        for ((seq, key, val), (exp_seq, exp_key, exp_val)) in got.iter().zip(records.iter()) {
            assert_eq!(seq, exp_seq);
            assert_eq!(key, exp_key);
            assert!(value_eq(val, exp_val));
        }
    }

    #[test]
    fn empty_value_put_distinguished_from_delete() {
        let records: Vec<(u64, &[u8], Value)> = vec![
            (1, b"empty-put", Value::Put(Vec::new())),
            (2, b"deleted", Value::Delete),
        ];
        let (_dir, path) = write_and_sync(&records);

        let mut got = Vec::new();
        replay(&path, |seq, key, val| got.push((seq, key, val))).expect("replay");

        assert!(value_eq(&got[0].2, &Value::Put(Vec::new())));
        assert!(value_eq(&got[1].2, &Value::Delete));
    }

    /// On-disk length of one record for `key`/`val`, matching `WalFile::append`'s encoding.
    fn record_len(key: &[u8], val: &Value) -> u64 {
        let val_len = match val {
            Value::Put(v) => v.len(),
            Value::Delete => 0,
        };
        (HEADER_LEN + key.len() + val_len) as u64
    }

    #[test]
    fn torn_tail_at_several_cut_points_recovers_complete_prefix_and_truncates() {
        let records: Vec<(u64, &[u8], Value)> = vec![
            (1, b"a", Value::Put(b"aaa".to_vec())),
            (2, b"b", Value::Put(b"bbb".to_vec())),
            (3, b"c", Value::Put(b"ccc".to_vec())),
        ];
        let (_dir, path) = write_and_sync(&records);
        let full_len = std::fs::metadata(&path).unwrap().len();
        let full_bytes = std::fs::read(&path).unwrap();

        // Cumulative byte offset after each record.
        let mut boundaries = Vec::new();
        let mut running = 0u64;
        for (_, key, val) in &records {
            running += record_len(key, val);
            boundaries.push(running);
        }

        // Chop off 1..full_len bytes and confirm replay recovers exactly the complete prefix,
        // and the file on disk is truncated down to that prefix's boundary.
        for cut in 1..full_len {
            let good_len = full_len - cut;
            std::fs::write(&path, &full_bytes[..good_len as usize]).unwrap();

            let mut got = Vec::new();
            let max_seq = replay(&path, |seq, key, val| got.push((seq, key, val))).expect("replay");

            // `boundaries` is sorted ascending, so the count of entries `<= good_len` is also
            // the index of the last one (0 if none qualify).
            let expected_complete = boundaries.iter().filter(|&&b| b <= good_len).count();
            let expected_len = expected_complete
                .checked_sub(1)
                .map_or(0, |i| boundaries[i]);

            assert_eq!(
                got.len(),
                expected_complete,
                "cut={cut} good_len={good_len} expected {expected_complete} complete records"
            );
            assert_eq!(
                std::fs::metadata(&path).unwrap().len(),
                expected_len,
                "file should be truncated to the last good record boundary"
            );
            assert_eq!(
                max_seq,
                if expected_complete > 0 {
                    records[expected_complete - 1].0
                } else {
                    0
                }
            );
        }
    }

    #[test]
    fn corrupted_byte_in_middle_record_stops_there() {
        let records: Vec<(u64, &[u8], Value)> = vec![
            (1, b"a", Value::Put(b"aaa".to_vec())),
            (2, b"b", Value::Put(b"bbb".to_vec())),
            (3, b"c", Value::Put(b"ccc".to_vec())),
        ];
        let (_dir, path) = write_and_sync(&records);
        let first_len = record_len(records[0].1, &records[0].2);

        let mut bytes = std::fs::read(&path).unwrap();
        let flip_at = first_len as usize + 5; // inside the second record's header/body
        bytes[flip_at] ^= 0xFF;
        std::fs::write(&path, &bytes).unwrap();

        let mut got = Vec::new();
        let max_seq = replay(&path, |seq, key, val| got.push((seq, key, val))).expect("replay");

        assert_eq!(got.len(), 1);
        assert_eq!(got[0].0, 1);
        assert_eq!(max_seq, 1);
        assert_eq!(std::fs::metadata(&path).unwrap().len(), first_len);
    }

    #[test]
    fn list_wal_files_orders_and_ignores_junk() {
        let dir = tempdir().expect("tempdir");
        for name in ["wal-000003.log", "wal-000001.log", "wal-000002.log"] {
            std::fs::write(dir.path().join(name), b"").unwrap();
        }
        for name in [
            "MANIFEST",
            "000001.sst",
            "wal-abc.log",
            "notawal-000001.log",
        ] {
            std::fs::write(dir.path().join(name), b"").unwrap();
        }

        let files = list_wal_files(dir.path()).expect("list");
        let numbers: Vec<u64> = files.iter().map(|(n, _)| *n).collect();
        assert_eq!(numbers, vec![1, 2, 3]);
    }

    #[test]
    #[should_panic(expected = "exceeds MAX_KEY_LEN")]
    fn append_debug_asserts_against_oversized_key() {
        let dir = tempdir().expect("tempdir");
        let mut wal = WalFile::create(dir.path(), 1).expect("create");
        let oversized_key = vec![0u8; MAX_KEY_LEN + 1];
        wal.append(1, &oversized_key, &Value::Put(b"v".to_vec()));
    }

    #[test]
    #[should_panic(expected = "exceeds MAX_VALUE_LEN")]
    fn append_debug_asserts_against_oversized_value() {
        let dir = tempdir().expect("tempdir");
        let mut wal = WalFile::create(dir.path(), 1).expect("create");
        let oversized_val = vec![0u8; MAX_VALUE_LEN + 1];
        wal.append(1, b"k", &Value::Put(oversized_val));
    }

    #[test]
    fn replay_of_empty_file() {
        let dir = tempdir().expect("tempdir");
        let wal = WalFile::create(dir.path(), 1).expect("create");
        drop(wal); // no append/sync — file exists but is empty.
        let path = wal_path(dir.path(), 1);

        let mut got = Vec::new();
        let max_seq = replay(&path, |seq, key, val| got.push((seq, key, val))).expect("replay");

        assert!(got.is_empty());
        assert_eq!(max_seq, 0);
    }

    #[test]
    fn sync_failure_poisons_subsequent_syncs() {
        let dir = tempdir().expect("tempdir");
        let mut wal = WalFile::create(dir.path(), 1).expect("create");
        wal.append(1, b"a", &Value::Put(b"1".to_vec()));
        wal.poisoned = true; // simulate a prior fdatasync failure directly.

        let err = wal.sync().expect_err("poisoned wal must fail sync");
        assert!(err.to_string().contains("poisoned"));
    }
}
