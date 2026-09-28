//! Append-only manifest log.
//!
//! The manifest is the durable record of which SSTables belong to which level, which WAL file
//! has been fully flushed, and the next free SST file number. On open the engine replays the
//! manifest to rebuild [`ManifestState`], then opens the current WAL and replays any records
//! past the last flush.
//!
//! ## Framing
//!
//! Each edit (a batch of [`ManifestRecord`]s that must apply atomically, e.g. a compaction's
//! adds + deletes) is written as one frame:
//!
//! ```text
//!   [u32 BE len][u32 BE crc32(payload)][bincode(Vec<ManifestRecord>) — `payload`, `len` bytes]
//! ```
//!
//! A torn or CRC-mismatched frame is only ever the tail of a write that crashed mid-fsync when
//! it really is the last thing in the file — then it's silently dropped by replay (not an
//! error). The same goes for an all-zero `[len=0][crc=0]` frame, which is what a sparse,
//! zero-filled extension after a crash looks like (`crc32(b"") == 0`). If either happens with
//! more bytes still following it, that's not a torn tail — it's corruption in the middle of the
//! log — and replay reports [`Error::ManifestCorrupt`] instead of silently dropping everything
//! after it. A CRC-valid non-empty frame that fails to bincode-decode is always a real
//! [`Error::ManifestCorrupt`], since the CRC proves the bytes are intact.
//!
//! ## Compaction on open
//!
//! `Manifest::open` replays the log, then immediately rewrites `MANIFEST` as a single snapshot
//! edit (`MANIFEST.tmp` → `sync_all` → rename → fsync the directory) so the log never grows
//! unbounded with historical edits — only the live state plus enough bookkeeping
//! ([`ManifestRecord::NextFileNumber`]) to resume file-number allocation.
//!
//! Atomic update: `append` writes one frame then `sync_data`s. A successful append is what
//! makes a flush/compaction visible; a crash before the append leaves the new SST(s) orphaned,
//! to be GC'd at next open.
//!
//! ## Format version
//!
//! Every snapshot rewrite appends a [`ManifestRecord::FormatVersion`] record carrying
//! [`crate::FORMAT_VERSION`]. `Manifest::open` checks the version *as it replays each frame* —
//! not only after the whole log has been read — and refuses with [`Error::UnsupportedFormat`]
//! the instant a too-new version is seen, before decoding any later frame or touching the
//! directory in any way (no rewrite, no WAL replay, no deletions). A manifest with no
//! `FormatVersion` record at all (written before this field existed) is treated as format
//! version 1.
//!
//! **Forward-compatibility contract:** a future, incompatible format change must write a
//! standalone `[FormatVersion(n)]` frame *first* — before any frame containing a record this
//! build might not understand. That ordering is what lets an old build recognize the format is
//! too new and stop immediately, rather than pressing on into a later frame that doesn't even
//! bincode-decode as a valid `ManifestRecord` and getting the misleading `ManifestCorrupt`
//! instead of `UnsupportedFormat`.

use crate::error::{Error, Result};
use serde::{Deserialize, Serialize};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::Path;

const MANIFEST_FILE: &str = "MANIFEST";
const MANIFEST_TMP: &str = "MANIFEST.tmp";

/// Metadata for one on-disk SSTable, as recorded in the manifest.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SstMeta {
    pub number: u64,
    pub smallest: Vec<u8>,
    pub largest: Vec<u8>,
    pub size: u64,
    pub max_seq: u64,
}

/// One entry in a manifest edit. New variants must be appended at the END (not reordered or
/// inserted) — bincode is position-indexed for enums, and `MANIFEST` files already on disk
/// depend on the existing ordinals.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ManifestRecord {
    /// A flush or compaction produced a new SSTable at `level`.
    SstAdded { level: u8, meta: SstMeta },
    /// A compaction (or GC) removed SST `number` from `level`. No-op if absent.
    SstDeleted { level: u8, number: u64 },
    /// WAL `number` has been fully flushed to L0; `last_seq` is the max seqno it contained.
    WalFlushed { number: u64, last_seq: u64 },
    /// Persists the file-number allocator across a manifest rewrite. A file number can be
    /// allocated and then deleted (e.g. a compaction output later superseded) without ever
    /// appearing in a live `SstAdded`, so `next_file_number` can't always be recovered from the
    /// live SST set alone — the snapshot rewrite emits one of these to carry it forward.
    NextFileNumber(u64),
    /// The on-disk format version this manifest (and the SSTs/WALs it describes) was written
    /// under. Appended by every snapshot rewrite (see [`ManifestState::snapshot_edit`]). Added
    /// after `NextFileNumber` — new variants must always go at the end (see the enum's docs) —
    /// so a manifest predating this variant simply never applies one, which `Manifest::open`
    /// treats as format version 1 (see its docs).
    FormatVersion(u32),
}

/// In-memory replay of the manifest log: the live SST set per level plus the durability/
/// allocation counters recovery needs.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ManifestState {
    /// `levels[0]` = L0, kept sorted by `number` ASC; `levels[n >= 1]` kept sorted by
    /// `smallest` ASC.
    pub levels: Vec<Vec<SstMeta>>,
    pub last_flushed_wal: u64,
    pub last_seq: u64,
    /// Strictly greater than every SST number seen so far; always >= 1.
    pub next_file_number: u64,
    /// The format version recorded by the last `FormatVersion` record replayed, or `0` if none
    /// was ever seen (a manifest written before this field existed). `Manifest::open` treats `0`
    /// here as format version 1 — see its docs.
    pub format_version: u32,
}

impl ManifestState {
    /// Fold one manifest record into the state. See module docs for the framing this is fed
    /// from; callers apply every record of an edit in order.
    pub fn apply(&mut self, rec: &ManifestRecord) {
        match rec {
            ManifestRecord::SstAdded { level, meta } => {
                let level = *level as usize;
                if self.levels.len() <= level {
                    self.levels.resize(level + 1, Vec::new());
                }
                self.levels[level].push(meta.clone());
                if level == 0 {
                    self.levels[level].sort_by_key(|m| m.number);
                } else {
                    self.levels[level].sort_by(|a, b| a.smallest.cmp(&b.smallest));
                }
                self.next_file_number = self.next_file_number.max(meta.number + 1).max(1);
                self.last_seq = self.last_seq.max(meta.max_seq);
            }
            ManifestRecord::SstDeleted { level, number } => {
                if let Some(files) = self.levels.get_mut(*level as usize) {
                    files.retain(|m| m.number != *number);
                }
            }
            ManifestRecord::WalFlushed { number, last_seq } => {
                self.last_flushed_wal = self.last_flushed_wal.max(*number);
                self.last_seq = self.last_seq.max(*last_seq);
            }
            ManifestRecord::NextFileNumber(n) => {
                self.next_file_number = self.next_file_number.max(*n).max(1);
            }
            ManifestRecord::FormatVersion(v) => {
                self.format_version = self.format_version.max(*v);
            }
        }
    }

    /// One edit that, replayed from a fresh (default) state, reproduces this state exactly:
    /// an `SstAdded` for every live file, then a `WalFlushed` and a `NextFileNumber` carrying
    /// the counters forward. Used to compact the on-disk log to a single snapshot frame.
    pub fn snapshot_edit(&self) -> Vec<ManifestRecord> {
        let mut edit = Vec::new();
        for (level, files) in self.levels.iter().enumerate() {
            for meta in files {
                edit.push(ManifestRecord::SstAdded {
                    level: level as u8,
                    meta: meta.clone(),
                });
            }
        }
        edit.push(ManifestRecord::WalFlushed {
            number: self.last_flushed_wal,
            last_seq: self.last_seq,
        });
        edit.push(ManifestRecord::NextFileNumber(self.next_file_number));
        edit.push(ManifestRecord::FormatVersion(self.format_version));
        edit
    }
}

/// Append-only manifest log. Owns an open file handle positioned for appends.
#[derive(Debug)]
pub struct Manifest {
    file: File,
    /// Set once an `append` fails partway through (write or sync). A failed write/sync can
    /// leave a partial frame on disk that a later, successfully-acknowledged append would come
    /// after — replay would never reach that later frame, so once poisoned every subsequent
    /// `append` fails immediately too, same rationale as `WalFile`'s fsyncgate handling.
    poisoned: bool,
}

impl Manifest {
    /// Open (or create) `dir/MANIFEST`, replay it into a [`ManifestState`], then rewrite the
    /// file as a single snapshot edit and reopen it for append. See module docs.
    ///
    /// A stale `MANIFEST.tmp` left by a crash mid-rewrite is never read — only overwritten by
    /// the fresh rewrite below — so it's implicitly ignored.
    pub fn open(dir: &Path) -> Result<(Manifest, ManifestState)> {
        fs::create_dir_all(dir)?;
        let manifest_path = dir.join(MANIFEST_FILE);
        let tmp_path = dir.join(MANIFEST_TMP);

        let mut state = ManifestState::default();
        match fs::read(&manifest_path) {
            Ok(bytes) => {
                let mut offset = 0usize;
                loop {
                    match decode_frame(&bytes[offset..])? {
                        Frame::Complete(edit, consumed) => {
                            for rec in &edit {
                                state.apply(rec);
                            }
                            offset += consumed;
                            // Bail out the instant an unsupported (too new) format version is
                            // seen, before decoding any later frame -- see the module docs'
                            // forward-compat contract. A future, incompatible format's frames
                            // after the `FormatVersion` marker aren't guaranteed to even
                            // bincode-decode as `ManifestRecord` under this build, so waiting
                            // until the whole log is replayed could surface the wrong error
                            // (`ManifestCorrupt`) instead of `UnsupportedFormat`.
                            if state.format_version > crate::FORMAT_VERSION {
                                return Err(Error::UnsupportedFormat {
                                    found: state.format_version,
                                    supported: crate::FORMAT_VERSION,
                                });
                            }
                        }
                        Frame::Torn => break,
                        Frame::Suspect { frame_len } => {
                            // Only a torn tail if nothing else follows it in the file; if more
                            // bytes come after, this is corruption in the middle of the log.
                            if offset + frame_len < bytes.len() {
                                return Err(Error::ManifestCorrupt(format!(
                                    "corrupt frame at offset {offset} ({} bytes follow it — not \
                                     the tail)",
                                    bytes.len() - offset - frame_len
                                )));
                            }
                            break;
                        }
                    }
                }
            }
            // No manifest yet: fresh state. Any other read failure (permissions, I/O error,
            // MANIFEST being a directory, ...) must propagate rather than silently starting
            // from empty state and overwriting whatever's really there.
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(e.into()),
        }
        state.next_file_number = state.next_file_number.max(1);

        // A manifest that never recorded a `FormatVersion` (0) predates this field and is
        // implicitly version 1 -- see `ManifestState::format_version`'s docs. Checked *before*
        // the snapshot rewrite below so an unsupported version is refused without touching the
        // directory at all: no rewrite, no WAL replay, no deletions.
        let found_version = if state.format_version == 0 {
            1
        } else {
            state.format_version
        };
        if found_version != crate::FORMAT_VERSION {
            return Err(Error::UnsupportedFormat {
                found: found_version,
                supported: crate::FORMAT_VERSION,
            });
        }
        state.format_version = crate::FORMAT_VERSION;

        let frame = encode_edit(&state.snapshot_edit())?;
        {
            let mut tmp = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&tmp_path)?;
            tmp.write_all(&frame)?;
            tmp.sync_all()?;
        }
        fs::rename(&tmp_path, &manifest_path)?;
        sync_dir(dir)?;

        let file = OpenOptions::new().append(true).open(&manifest_path)?;
        Ok((
            Manifest {
                file,
                poisoned: false,
            },
            state,
        ))
    }

    /// Append one edit (one frame) + `sync_data`. Returns once durable.
    pub fn append(&mut self, edit: &[ManifestRecord]) -> Result<()> {
        if self.poisoned {
            return Err(Error::ManifestCorrupt(
                "manifest poisoned: a previous append failed partway through".into(),
            ));
        }
        let frame = encode_edit(edit)?;
        if let Err(e) = self.file.write_all(&frame) {
            self.poisoned = true;
            return Err(e.into());
        }
        if let Err(e) = self.file.sync_data() {
            self.poisoned = true;
            return Err(e.into());
        }
        Ok(())
    }
}

/// Encode one edit as `[u32 BE len][u32 BE crc32(payload)][payload]`.
pub fn encode_edit(edit: &[ManifestRecord]) -> Result<Vec<u8>> {
    let payload = bincode::serialize(edit)
        .map_err(|e| Error::ManifestCorrupt(format!("failed to encode edit: {e}")))?;
    let crc = crc32fast::hash(&payload);
    let mut out = Vec::with_capacity(8 + payload.len());
    out.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    out.extend_from_slice(&crc.to_be_bytes());
    out.extend_from_slice(&payload);
    Ok(out)
}

/// Outcome of decoding one candidate frame from the front of a buffer. See module docs for the
/// torn-tail-vs-corruption policy this feeds.
enum Frame {
    /// A complete, valid, non-empty frame: the decoded edit plus bytes consumed.
    Complete(Vec<ManifestRecord>, usize),
    /// Not enough bytes buffered for even a header, or for the header's declared payload —
    /// unambiguously a torn tail, since there isn't enough data left in the file for anything
    /// else to fit after it either.
    Torn,
    /// The header's declared length fits in what's buffered, but the frame is a zero-length
    /// `[len=0][crc=0]` stand-in for a zero-filled tail, or its CRC didn't match. Ambiguous on
    /// its own — a torn tail only if nothing else follows it in the file, which the caller
    /// (which can see the rest of the buffer) decides. `frame_len` is `8 + len`.
    Suspect { frame_len: usize },
}

/// Decode one frame from the front of `buf`. See [`Frame`] for the three outcomes.
fn decode_frame(buf: &[u8]) -> Result<Frame> {
    if buf.len() < 8 {
        return Ok(Frame::Torn);
    }
    let len = u32::from_be_bytes(buf[0..4].try_into().unwrap()) as usize;
    let crc = u32::from_be_bytes(buf[4..8].try_into().unwrap());
    if buf.len() < 8 + len {
        return Ok(Frame::Torn);
    }
    let frame_len = 8 + len;
    // A zero-length frame is indistinguishable from a genuine (if pointless) empty edit by CRC
    // alone (crc32(b"") == 0), and is exactly what a sparse, zero-filled extension after a
    // crash looks like — treat it the same as a CRC mismatch rather than trying to bincode
    // decode it (that would fail anyway, since `Vec<ManifestRecord>` never serializes to zero
    // bytes).
    if len == 0 {
        return Ok(Frame::Suspect { frame_len });
    }
    let payload = &buf[8..frame_len];
    if crc32fast::hash(payload) != crc {
        return Ok(Frame::Suspect { frame_len });
    }
    let edit: Vec<ManifestRecord> = bincode::deserialize(payload)
        .map_err(|e| Error::ManifestCorrupt(format!("bad frame payload: {e}")))?;
    Ok(Frame::Complete(edit, frame_len))
}

/// fsync a directory so a preceding create/rename/unlink within it is durable.
///
/// ponytail: duplicated here rather than shared — `wal.rs` will grow the public version this
/// delegates to once that module is rewritten; dedup then.
fn sync_dir(dir: &Path) -> io::Result<()> {
    File::open(dir)?.sync_all()
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn meta(number: u64) -> SstMeta {
        SstMeta {
            number,
            smallest: vec![number as u8],
            largest: vec![number as u8],
            size: 100,
            max_seq: number,
        }
    }

    #[test]
    fn apply_orders_levels_and_updates_counters() {
        let mut state = ManifestState::default();
        state.apply(&ManifestRecord::SstAdded {
            level: 0,
            meta: meta(3),
        });
        state.apply(&ManifestRecord::SstAdded {
            level: 0,
            meta: meta(1),
        });
        state.apply(&ManifestRecord::SstAdded {
            level: 0,
            meta: meta(2),
        });
        assert_eq!(
            state.levels[0].iter().map(|m| m.number).collect::<Vec<_>>(),
            vec![1, 2, 3]
        );

        state.apply(&ManifestRecord::SstAdded {
            level: 1,
            meta: SstMeta {
                number: 10,
                smallest: b"m".to_vec(),
                largest: b"z".to_vec(),
                size: 1,
                max_seq: 5,
            },
        });
        state.apply(&ManifestRecord::SstAdded {
            level: 1,
            meta: SstMeta {
                number: 11,
                smallest: b"a".to_vec(),
                largest: b"c".to_vec(),
                size: 1,
                max_seq: 6,
            },
        });
        assert_eq!(
            state.levels[1]
                .iter()
                .map(|m| m.smallest.clone())
                .collect::<Vec<_>>(),
            vec![b"a".to_vec(), b"m".to_vec()]
        );

        state.apply(&ManifestRecord::SstDeleted {
            level: 0,
            number: 2,
        });
        assert_eq!(
            state.levels[0].iter().map(|m| m.number).collect::<Vec<_>>(),
            vec![1, 3]
        );
        // Deleting an absent number, and from a level with no files, is a no-op.
        state.apply(&ManifestRecord::SstDeleted {
            level: 0,
            number: 999,
        });
        state.apply(&ManifestRecord::SstDeleted {
            level: 6,
            number: 1,
        });
        assert_eq!(state.levels[0].len(), 2);

        assert_eq!(state.next_file_number, 12);
        assert_eq!(state.last_seq, 6);

        state.apply(&ManifestRecord::WalFlushed {
            number: 4,
            last_seq: 2,
        });
        assert_eq!(state.last_flushed_wal, 4);
        assert_eq!(state.last_seq, 6); // max(6, 2) unchanged

        state.apply(&ManifestRecord::NextFileNumber(50));
        assert_eq!(state.next_file_number, 50);
        state.apply(&ManifestRecord::NextFileNumber(3));
        assert_eq!(state.next_file_number, 50); // max(50, 3) unchanged
    }

    #[test]
    fn open_empty_dir_creates_manifest() {
        let dir = tempdir().unwrap();
        let (_m, state) = Manifest::open(dir.path()).unwrap();
        assert!(dir.path().join("MANIFEST").exists());
        assert_eq!(state.next_file_number, 1);
        assert_eq!(state.last_seq, 0);
        assert_eq!(state.last_flushed_wal, 0);
        assert!(state.levels.is_empty());
    }

    #[test]
    fn append_then_reopen_matches_state() {
        let dir = tempdir().unwrap();
        let (mut m, mut expected) = Manifest::open(dir.path()).unwrap();
        let edits = vec![
            vec![ManifestRecord::SstAdded {
                level: 0,
                meta: meta(1),
            }],
            vec![
                ManifestRecord::SstAdded {
                    level: 0,
                    meta: meta(2),
                },
                ManifestRecord::WalFlushed {
                    number: 1,
                    last_seq: 20,
                },
            ],
            vec![ManifestRecord::SstDeleted {
                level: 0,
                number: 1,
            }],
        ];
        for edit in &edits {
            m.append(edit).unwrap();
            for rec in edit {
                expected.apply(rec);
            }
        }
        drop(m);

        let (_m2, state2) = Manifest::open(dir.path()).unwrap();
        assert_eq!(state2, expected);
    }

    #[test]
    fn torn_last_frame_is_dropped_and_append_still_works() {
        let dir = tempdir().unwrap();
        let manifest_path = dir.path().join("MANIFEST");
        let (mut m, _s) = Manifest::open(dir.path()).unwrap();
        let base_len = fs::metadata(&manifest_path).unwrap().len();

        m.append(&[ManifestRecord::SstAdded {
            level: 0,
            meta: meta(5),
        }])
        .unwrap();
        drop(m);

        let full = fs::read(&manifest_path).unwrap();
        let appended_len = full.len() as u64 - base_len;
        assert!(appended_len > 0);

        for trunc in 1..=appended_len {
            let keep = (full.len() as u64 - trunc) as usize;
            fs::write(&manifest_path, &full[..keep]).unwrap();

            let (mut m2, state2) = Manifest::open(dir.path()).unwrap();
            // The torn append never took effect.
            assert!(state2.levels.is_empty() || state2.levels[0].is_empty());

            // Subsequent append + reopen still works.
            m2.append(&[ManifestRecord::WalFlushed {
                number: 1,
                last_seq: 42,
            }])
            .unwrap();
            drop(m2);

            let (_m3, state3) = Manifest::open(dir.path()).unwrap();
            assert_eq!(state3.last_flushed_wal, 1);
            assert_eq!(state3.last_seq, 42);
        }
    }

    #[test]
    fn crc_mismatch_on_last_frame_is_dropped() {
        let dir = tempdir().unwrap();
        let manifest_path = dir.path().join("MANIFEST");
        let (mut m, _s) = Manifest::open(dir.path()).unwrap();
        m.append(&[ManifestRecord::WalFlushed {
            number: 3,
            last_seq: 99,
        }])
        .unwrap();
        drop(m);

        let mut bytes = fs::read(&manifest_path).unwrap();
        let last = bytes.len() - 1;
        bytes[last] ^= 0xFF; // corrupt a payload byte of the last frame
        fs::write(&manifest_path, &bytes).unwrap();

        let (_m2, state2) = Manifest::open(dir.path()).unwrap();
        assert_eq!(state2.last_flushed_wal, 0);
        assert_eq!(state2.last_seq, 0);
    }

    #[test]
    fn non_not_found_read_error_propagates_instead_of_fresh_state() {
        let dir = tempdir().unwrap();
        // Create MANIFEST as a directory so `fs::read` fails with something other than
        // NotFound. The old code swallowed *any* read error into "fresh state"; it must now
        // propagate instead of silently proceeding as if there were no manifest at all.
        fs::create_dir(dir.path().join("MANIFEST")).unwrap();
        let err = Manifest::open(dir.path()).unwrap_err();
        assert!(matches!(err, Error::Io(_)), "got {err:?}");
    }

    #[test]
    fn middle_frame_corruption_is_rejected_and_manifest_untouched() {
        let dir = tempdir().unwrap();
        let manifest_path = dir.path().join("MANIFEST");
        let (mut m, _s) = Manifest::open(dir.path()).unwrap();
        let frame0_len = fs::metadata(&manifest_path).unwrap().len();

        // Two more frames after the initial snapshot: corrupting the first (middle) one must
        // not be mistaken for a torn tail, since a real frame follows it.
        m.append(&[ManifestRecord::SstAdded {
            level: 0,
            meta: meta(1),
        }])
        .unwrap();
        m.append(&[ManifestRecord::WalFlushed {
            number: 9,
            last_seq: 9,
        }])
        .unwrap();
        drop(m);

        let mut bytes = fs::read(&manifest_path).unwrap();
        let flip_at = frame0_len as usize + 8; // first payload byte of the middle frame
        bytes[flip_at] ^= 0xFF;
        fs::write(&manifest_path, &bytes).unwrap();

        let before = fs::read(&manifest_path).unwrap();
        let err = Manifest::open(dir.path()).unwrap_err();
        assert!(matches!(err, Error::ManifestCorrupt(_)), "got {err:?}");
        let after = fs::read(&manifest_path).unwrap();
        assert_eq!(before, after, "a failed open must not rewrite MANIFEST");
        assert!(!dir.path().join("MANIFEST.tmp").exists());
    }

    #[test]
    fn unsupported_format_version_is_rejected_and_manifest_untouched() {
        let dir = tempdir().unwrap();
        let manifest_path = dir.path().join("MANIFEST");
        let (mut m, _s) = Manifest::open(dir.path()).unwrap();
        m.append(&[ManifestRecord::FormatVersion(2)]).unwrap();
        drop(m);

        let before = fs::read(&manifest_path).unwrap();
        let err = Manifest::open(dir.path()).unwrap_err();
        assert!(
            matches!(
                err,
                Error::UnsupportedFormat {
                    found: 2,
                    supported: 1,
                }
            ),
            "got {err:?}"
        );
        let after = fs::read(&manifest_path).unwrap();
        assert_eq!(
            before, after,
            "a manifest with an unsupported format version must not be rewritten"
        );
        assert!(!dir.path().join("MANIFEST.tmp").exists());
    }

    #[test]
    fn unsupported_format_version_short_circuits_before_a_later_undecodable_frame() {
        // Simulates a future, incompatible manifest: a standalone `[FormatVersion(2)]` frame
        // first, per the forward-compat contract in the module docs, followed by a frame this
        // build has no hope of bincode-decoding as `ManifestRecord` (a stand-in for a record
        // kind that doesn't exist yet). The version check must fire on the first frame and
        // return `UnsupportedFormat` without ever attempting to decode the second — if it did,
        // it would surface the wrong error (`ManifestCorrupt`) instead.
        let dir = tempdir().unwrap();
        let manifest_path = dir.path().join("MANIFEST");

        let frame0 = encode_edit(&[ManifestRecord::FormatVersion(2)]).unwrap();
        let garbage_payload = b"not a valid bincode Vec<ManifestRecord> at all, just junk";
        let mut frame1 = Vec::new();
        frame1.extend_from_slice(&(garbage_payload.len() as u32).to_be_bytes());
        frame1.extend_from_slice(&crc32fast::hash(garbage_payload).to_be_bytes());
        frame1.extend_from_slice(garbage_payload);

        let mut bytes = Vec::new();
        bytes.extend_from_slice(&frame0);
        bytes.extend_from_slice(&frame1);
        fs::write(&manifest_path, &bytes).unwrap();

        let before = fs::read(&manifest_path).unwrap();
        let err = Manifest::open(dir.path()).unwrap_err();
        assert!(
            matches!(
                err,
                Error::UnsupportedFormat {
                    found: 2,
                    supported: 1,
                }
            ),
            "got {err:?}"
        );
        let after = fs::read(&manifest_path).unwrap();
        assert_eq!(before, after, "a failed open must not rewrite MANIFEST");
        assert!(!dir.path().join("MANIFEST.tmp").exists());
    }

    #[test]
    fn absent_format_version_is_treated_as_version_one() {
        let dir = tempdir().unwrap();
        let (m, s) = Manifest::open(dir.path()).unwrap();
        // A fresh open already rewrites the snapshot with the current version stamped in.
        assert_eq!(s.format_version, crate::FORMAT_VERSION);
        drop(m);
        let (_m2, s2) = Manifest::open(dir.path()).unwrap();
        assert_eq!(s2.format_version, crate::FORMAT_VERSION);
    }

    #[test]
    fn zero_filled_tail_is_torn_and_open_succeeds_with_earlier_state() {
        let dir = tempdir().unwrap();
        let manifest_path = dir.path().join("MANIFEST");
        let (mut m, _s) = Manifest::open(dir.path()).unwrap();
        m.append(&[ManifestRecord::WalFlushed {
            number: 7,
            last_seq: 77,
        }])
        .unwrap();
        drop(m);

        // Simulate a crash that left a sparse, zero-filled extension after the last good frame:
        // [len=0][crc=0], no payload. crc32(b"") == 0, so this can't be told apart from a real
        // CRC mismatch by hash alone — it must be recognized by position (nothing follows it).
        let mut bytes = fs::read(&manifest_path).unwrap();
        bytes.extend_from_slice(&[0u8; 8]);
        fs::write(&manifest_path, &bytes).unwrap();

        let (_m2, state2) = Manifest::open(dir.path()).unwrap();
        assert_eq!(state2.last_flushed_wal, 7);
        assert_eq!(state2.last_seq, 77);
    }

    #[test]
    fn poisoned_manifest_rejects_further_appends() {
        let dir = tempdir().unwrap();
        let (mut m, _s) = Manifest::open(dir.path()).unwrap();
        m.poisoned = true; // simulate a prior append failing partway through.
        let err = m
            .append(&[ManifestRecord::WalFlushed {
                number: 1,
                last_seq: 1,
            }])
            .unwrap_err();
        assert!(matches!(err, Error::ManifestCorrupt(_)), "got {err:?}");
    }

    #[test]
    fn counters_survive_multiple_reopens() {
        let dir = tempdir().unwrap();
        let (mut m, _s) = Manifest::open(dir.path()).unwrap();
        m.append(&[
            ManifestRecord::SstAdded {
                level: 0,
                meta: meta(7),
            },
            ManifestRecord::WalFlushed {
                number: 2,
                last_seq: 99,
            },
        ])
        .unwrap();
        drop(m);

        for _ in 0..3 {
            let (m2, state2) = Manifest::open(dir.path()).unwrap();
            assert_eq!(state2.next_file_number, 8);
            assert_eq!(state2.last_seq, 99);
            assert_eq!(state2.last_flushed_wal, 2);
            drop(m2);
        }
    }

    #[test]
    fn stale_tmp_file_is_ignored_and_overwritten() {
        let dir = tempdir().unwrap();
        let (mut m, _s) = Manifest::open(dir.path()).unwrap();
        m.append(&[ManifestRecord::WalFlushed {
            number: 1,
            last_seq: 5,
        }])
        .unwrap();
        drop(m);

        fs::write(
            dir.path().join("MANIFEST.tmp"),
            b"garbage-from-a-crashed-rewrite",
        )
        .unwrap();

        let (_m2, state2) = Manifest::open(dir.path()).unwrap();
        assert_eq!(state2.last_flushed_wal, 1);
        assert_eq!(state2.last_seq, 5);
        // The rewrite overwrote + renamed the tmp file away; no stray garbage left behind.
        assert!(!dir.path().join("MANIFEST.tmp").exists());
    }
}
