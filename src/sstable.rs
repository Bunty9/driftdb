//! Sorted-string table reader + writer.
//!
//! ## File layout
//!
//! ```text
//!   +-----------------+
//!   | data block 0    |   4KiB target (uncompressed), zstd-compressed payload
//!   | data block 1    |   [u32 BE compressed_len][u32 BE crc32(compressed)][zstd bytes]
//!   | ...             |
//!   | data block N    |
//!   +-----------------+
//!   | index block     |   bincode: Vec<(last_key, block_offset)>
//!   +-----------------+
//!   | bloom block     |   bincode: GrowableBloom
//!   +-----------------+
//!   | footer (32B)    |   [u64 BE index_off][u64 BE bloom_off][u32 BE crc32(index..bloom)]
//!   |                 |   [u32 BE reserved=0][u64 BE magic=0xDEADBEEF]
//!   +-----------------+
//! ```
//!
//! Each data block entry: `[u32 BE klen][u32 BE vlen][u64 BE seq][u8 kind][key][val]`
//! where `kind` is 1 for Put and 0 for Delete (the val bytes are absent for Delete).
//!
//! The footer's `crc32` covers every byte from `index_off` to the start of the footer — i.e.
//! the index block and bloom block together — and is checked in `open` *before* either is
//! bincode-deserialized. Without it, a bit flip in the index would silently skip blocks, and a
//! bit flip in the bloom filter's bincode bytes would be handed straight to
//! `growable-bloom-filter`'s deserializer, which is not guaranteed to fail cleanly on garbage
//! input (it can panic or allocate wildly instead) — the checksum turns both into an ordinary
//! `Error::SstCorrupt` instead.
//!
//! Each [`SstReader`] keeps a tiny per-table LRU of decompressed blocks (see [`BlockCache`])
//! so a hot key under a skewed (Zipfian) read workload doesn't pay zstd-decode on every single
//! `get()`. Blocks are immutable once written, so the cache never needs invalidation.

use crate::error::Error;
use crate::memtable::{Entry, Value};
use growable_bloom_filter::GrowableBloom;
use parking_lot::Mutex;
use std::collections::VecDeque;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Target uncompressed size of one data block before flush. The block buffer flushes whenever
/// it grows past this threshold; small tail-blocks are written as-is.
pub const BLOCK_SIZE: usize = 4096;
/// Magic trailer that identifies a complete SSTable file.
pub const SSTABLE_MAGIC: u64 = 0xDEAD_BEEF;
/// Bloom false-positive rate target.
pub const BLOOM_FPR: f64 = 0.01;
/// Bloom initial capacity hint — the filter grows past this if needed.
pub const BLOOM_CAPACITY: usize = 100_000;
/// Length of the fixed footer: `index_off (8) + bloom_off (8) + crc32 (4) + reserved (4) +
/// magic (8)`.
const FOOTER_LEN: u64 = 32;
/// Length of one data-block header: `compressed_len (4) + crc32 (4)`.
const BLOCK_HEADER_LEN: usize = 8;
/// Length of one entry header inside a decompressed block: `klen(4) + vlen(4) + seq(8) + kind(1)`.
const ENTRY_HEADER_LEN: usize = 17;

/// Summary metadata produced by [`SstWriter::finish`] — everything the manifest needs to
/// record about a newly written table without reopening it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SstSummary {
    pub smallest: Vec<u8>,
    pub largest: Vec<u8>,
    pub entries: u64,
    pub file_size: u64,
    pub max_seq: u64,
}

/// Path for SSTable number `number` inside `dir`: `dir/{number:06}.sst`.
pub fn sst_path(dir: &Path, number: u64) -> PathBuf {
    dir.join(format!("{number:06}.sst"))
}

/// `Err(InvalidInput)` if `len` can't be represented in the 4-byte length prefix the on-disk
/// entry header uses for a key or value — see [`SstWriter::add`].
fn check_u32_len(len: usize, what: &str) -> std::io::Result<()> {
    if len > u32::MAX as usize {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("SstWriter::add: {what} length {len} exceeds u32::MAX"),
        ));
    }
    Ok(())
}

/// Writer for a single SSTable file. Construct with [`SstWriter::new`], call [`SstWriter::add`]
/// in ascending `(user_key, seqno DESC)` order, then [`SstWriter::finish`].
#[derive(Debug)]
pub struct SstWriter<W: Write> {
    w: W,
    block_buf: Vec<u8>,
    index: Vec<(Vec<u8>, u64)>,
    bloom: GrowableBloom,
    offset: u64,
    last_key: Vec<u8>,
    first_key: Option<Vec<u8>>,
    entries: u64,
    max_seq: u64,
    // Debug-only ordering check state.
    prev_key: Option<Vec<u8>>,
    prev_seq: u64,
}

impl<W: Write> SstWriter<W> {
    /// Wrap a writer (typically a `BufWriter<File>`) with an empty SST state.
    pub fn new(w: W) -> Self {
        Self {
            w,
            block_buf: Vec::with_capacity(BLOCK_SIZE),
            index: Vec::new(),
            bloom: GrowableBloom::new(BLOOM_FPR, BLOOM_CAPACITY),
            offset: 0,
            last_key: Vec::new(),
            first_key: None,
            entries: 0,
            max_seq: 0,
            prev_key: None,
            prev_seq: 0,
        }
    }

    /// Append one record. Caller must feed records sorted by `(user_key ASC, seqno DESC)` so
    /// the index can record `(last_key, block_offset)` for each block.
    ///
    /// Errors (`InvalidInput`) rather than truncating if `key` or `val` is longer than
    /// `u32::MAX` bytes — the on-disk entry header only has 4 bytes for each length, so silently
    /// casting with `as u32` would wrap around and corrupt the entry instead of failing loudly.
    pub fn add(&mut self, key: &[u8], seqno: u64, val: &Value) -> std::io::Result<()> {
        check_u32_len(key.len(), "key")?;
        if let Value::Put(v) = val {
            check_u32_len(v.len(), "value")?;
        }
        if let Some(prev) = &self.prev_key {
            let cmp = key.cmp(prev.as_slice());
            debug_assert!(
                cmp != std::cmp::Ordering::Less,
                "SstWriter::add: keys out of order (user_key must be ASC)"
            );
            debug_assert!(
                cmp != std::cmp::Ordering::Equal || seqno <= self.prev_seq,
                "SstWriter::add: seqno must descend within one key (seqno DESC)"
            );
        }

        self.bloom.insert(key);
        if self.first_key.is_none() {
            self.first_key = Some(key.to_vec());
        }
        self.entries += 1;
        self.max_seq = self.max_seq.max(seqno);

        let vlen = match val {
            Value::Put(v) => v.len() as u32,
            Value::Delete => 0,
        };
        let kind: u8 = match val {
            Value::Put(_) => 1,
            Value::Delete => 0,
        };
        self.block_buf
            .extend_from_slice(&(key.len() as u32).to_be_bytes());
        self.block_buf.extend_from_slice(&vlen.to_be_bytes());
        self.block_buf.extend_from_slice(&seqno.to_be_bytes());
        self.block_buf.push(kind);
        self.block_buf.extend_from_slice(key);
        if let Value::Put(v) = val {
            self.block_buf.extend_from_slice(v);
        }
        self.last_key = key.to_vec();
        self.prev_key = Some(key.to_vec());
        self.prev_seq = seqno;
        if self.block_buf.len() >= BLOCK_SIZE {
            self.flush_block()?;
        }
        Ok(())
    }

    fn flush_block(&mut self) -> std::io::Result<()> {
        if self.block_buf.is_empty() {
            return Ok(());
        }
        let compressed = zstd::encode_all(&self.block_buf[..], 3)?;
        let crc = crc32fast::hash(&compressed);
        self.w.write_all(&(compressed.len() as u32).to_be_bytes())?;
        self.w.write_all(&crc.to_be_bytes())?;
        self.w.write_all(&compressed)?;
        self.index.push((self.last_key.clone(), self.offset));
        self.offset += BLOCK_HEADER_LEN as u64 + compressed.len() as u64;
        self.block_buf.clear();
        Ok(())
    }

    /// Approximate on-disk size so far: bytes already flushed plus the pending (uncompressed)
    /// block buffer. Used by the flush thread to decide when to roll to a new SST.
    pub fn estimated_size(&self) -> u64 {
        self.offset + self.block_buf.len() as u64
    }

    /// True if no records have been added yet.
    pub fn is_empty(&self) -> bool {
        self.entries == 0
    }

    /// Finalize: flush any partial block, append the index + bloom + footer, and flush the
    /// underlying writer. Returns the writer back (so callers can e.g. `sync_all` the file)
    /// alongside a summary of what was written.
    pub fn finish(mut self) -> std::io::Result<(W, SstSummary)> {
        if !self.block_buf.is_empty() {
            self.flush_block()?;
        }
        let index_off = self.offset;
        // bincode serialization errors are reported as `InvalidData` to keep the signature
        // homogeneous with the rest of the writer.
        let index_bytes = bincode::serialize(&self.index)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        self.w.write_all(&index_bytes)?;
        let bloom_off = index_off + index_bytes.len() as u64;
        let bloom_bytes = bincode::serialize(&self.bloom)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        self.w.write_all(&bloom_bytes)?;

        // CRC over the whole index+bloom region so `open` can catch a corrupt index or bloom
        // before ever handing either to their (untrusted-input-unsafe) bincode deserializers.
        let mut region_crc = crc32fast::Hasher::new();
        region_crc.update(&index_bytes);
        region_crc.update(&bloom_bytes);
        let region_crc = region_crc.finalize();

        self.w.write_all(&index_off.to_be_bytes())?;
        self.w.write_all(&bloom_off.to_be_bytes())?;
        self.w.write_all(&region_crc.to_be_bytes())?;
        self.w.write_all(&0u32.to_be_bytes())?; // reserved
        self.w.write_all(&SSTABLE_MAGIC.to_be_bytes())?;
        self.w.flush()?;

        let file_size = bloom_off + bloom_bytes.len() as u64 + FOOTER_LEN;
        let summary = SstSummary {
            smallest: self.first_key.unwrap_or_default(),
            largest: self.last_key,
            entries: self.entries,
            file_size,
            max_seq: self.max_seq,
        };
        Ok((self.w, summary))
    }
}

/// Number of decompressed blocks kept per [`SstReader`]. Small on purpose: this is a per-table
/// cache (one of these exists per open SST), and a Zipfian-skewed read workload concentrates on
/// a handful of blocks per hot table, not the whole file.
const BLOCK_CACHE_CAPACITY: usize = 8;

/// Tiny LRU of decompressed blocks, keyed by data-region offset. Linear scan is fine at
/// [`BLOCK_CACHE_CAPACITY`]'s size; most-recently-used sits at the front.
///
/// ponytail: a per-table cache this small won't help a scan that touches every block exactly
/// once (compaction, full iteration) — it's aimed at `get()`'s repeat hits on the same hot
/// block. Widen it (or shard across tables) if profiling ever shows point-read p99 still
/// dominated by decompression with this in place.
#[derive(Debug, Default)]
struct BlockCache {
    entries: Mutex<VecDeque<(u64, Arc<Vec<Entry>>)>>,
}

impl BlockCache {
    fn get(&self, offset: u64) -> Option<Arc<Vec<Entry>>> {
        let mut entries = self.entries.lock();
        let pos = entries.iter().position(|(o, _)| *o == offset)?;
        let hit = entries.remove(pos).expect("position just found");
        let block = hit.1.clone();
        entries.push_front(hit);
        Some(block)
    }

    fn insert(&self, offset: u64, block: Arc<Vec<Entry>>) {
        let mut entries = self.entries.lock();
        if entries.iter().any(|(o, _)| *o == offset) {
            return; // lost a race with another reader decoding the same block; keep the winner.
        }
        if entries.len() >= BLOCK_CACHE_CAPACITY {
            entries.pop_back();
        }
        entries.push_front((offset, block));
    }
}

/// Read handle for an on-disk SSTable. Holds the mmap region + parsed index + bloom. Cheap to
/// clone-by-`Arc`; `open` does all the validation work up front so later reads never panic on
/// a corrupt file — they return [`Error::SstCorrupt`] instead.
pub struct SstReader {
    mmap: memmap2::Mmap,
    index: Vec<(Vec<u8>, u64)>,
    bloom: GrowableBloom,
    /// Byte length of the data-block region (== index_off from the footer).
    data_len: u64,
    path: PathBuf,
    block_cache: BlockCache,
}

impl std::fmt::Debug for SstReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SstReader")
            .field("path", &self.path)
            .field("entries_in_index", &self.index.len())
            .field("data_len", &self.data_len)
            .finish()
    }
}

// SstReader must be usable from multiple reader threads concurrently: the mmap is read-only,
// the index/bloom are immutable after `open`.
const _: fn() = || {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<SstReader>();
};

impl SstReader {
    /// Open an SSTable for reading. Parses the footer + index + bloom up-front; data blocks
    /// are read lazily through the mmap region. Any structural problem — truncated file, bad
    /// magic, out-of-bounds offsets, malformed index/bloom bincode — is reported as
    /// `Error::SstCorrupt`, never a panic.
    pub fn open(path: &Path) -> crate::Result<Self> {
        let file = std::fs::File::open(path)?;
        // Safety: the file is opened read-only and not truncated/modified for the lifetime of
        // the mmap that we control here; a concurrent external truncation is the classic mmap
        // footgun but out of scope for a single-writer LSM where SST files are immutable once
        // written.
        let mmap = unsafe { memmap2::Mmap::map(&file)? };
        let total_len = mmap.len() as u64;
        if total_len < FOOTER_LEN {
            return Err(Error::SstCorrupt(format!(
                "{}: file too short ({} bytes) to hold a footer",
                path.display(),
                total_len
            )));
        }
        let footer_start = total_len - FOOTER_LEN;
        let footer = &mmap[footer_start as usize..];
        let index_off = u64::from_be_bytes(footer[0..8].try_into().unwrap());
        let bloom_off = u64::from_be_bytes(footer[8..16].try_into().unwrap());
        let region_crc_expected = u32::from_be_bytes(footer[16..20].try_into().unwrap());
        // footer[20..24] is reserved.
        let magic = u64::from_be_bytes(footer[24..32].try_into().unwrap());
        if magic != SSTABLE_MAGIC {
            return Err(Error::SstCorrupt(format!(
                "{}: bad magic {:#x}",
                path.display(),
                magic
            )));
        }
        if !(index_off <= bloom_off && bloom_off <= footer_start) {
            return Err(Error::SstCorrupt(format!(
                "{}: footer offsets out of bounds (index_off={index_off}, bloom_off={bloom_off}, footer_start={footer_start})",
                path.display()
            )));
        }
        // Verify the index+bloom region's checksum *before* handing either to bincode: a
        // corrupt index would otherwise silently skip blocks, and a corrupt bloom filter could
        // panic or hang inside `growable-bloom-filter`'s own deserializer instead of failing
        // cleanly.
        let region = mmap
            .get(index_off as usize..footer_start as usize)
            .ok_or_else(|| {
                Error::SstCorrupt(format!(
                    "{}: index/bloom region out of bounds",
                    path.display()
                ))
            })?;
        let region_crc_actual = crc32fast::hash(region);
        if region_crc_actual != region_crc_expected {
            return Err(Error::SstCorrupt(format!(
                "{}: index/bloom region crc mismatch (expected {region_crc_expected:#x}, got {region_crc_actual:#x})",
                path.display()
            )));
        }
        let index_bytes = &region[..(bloom_off - index_off) as usize];
        let index: Vec<(Vec<u8>, u64)> = bincode::deserialize(index_bytes)
            .map_err(|e| Error::SstCorrupt(format!("{}: bad index: {e}", path.display())))?;
        let bloom_bytes = &region[(bloom_off - index_off) as usize..];
        let bloom: GrowableBloom = bincode::deserialize(bloom_bytes)
            .map_err(|e| Error::SstCorrupt(format!("{}: bad bloom: {e}", path.display())))?;

        Ok(Self {
            mmap,
            index,
            bloom,
            data_len: index_off,
            path: path.to_path_buf(),
            block_cache: BlockCache::default(),
        })
    }

    /// [`Self::decode_block`], but checks (and populates) the per-table [`BlockCache`] first —
    /// used by [`SstReader::get`], where a skewed workload repeatedly hits the same block. Not
    /// used by [`SstIter`]: a full scan touches each block exactly once, so caching it would
    /// only add an `Arc` clone with no hit-rate payoff -- see [`SstIter::next`].
    fn read_block(&self, offset: u64) -> crate::Result<Arc<Vec<Entry>>> {
        if let Some(cached) = self.block_cache.get(offset) {
            return Ok(cached);
        }
        let entries = Arc::new(self.decode_block(offset)?);
        self.block_cache.insert(offset, entries.clone());
        Ok(entries)
    }

    /// Decode + decompress the block starting at byte `offset` in the data region, returning
    /// its entries in on-disk order. Bounds- and crc-checked; corruption never panics. Does not
    /// touch the [`BlockCache`] -- see [`SstReader::read_block`].
    fn decode_block(&self, offset: u64) -> crate::Result<Vec<Entry>> {
        let data = &self.mmap[..self.data_len as usize];
        let start = offset as usize;
        let header_end = start.checked_add(BLOCK_HEADER_LEN).ok_or_else(|| {
            Error::SstCorrupt(format!(
                "{}: block header offset overflow at offset {offset}",
                self.path.display()
            ))
        })?;
        let header = data.get(start..header_end).ok_or_else(|| {
            Error::SstCorrupt(format!(
                "{}: block header out of bounds at offset {offset}",
                self.path.display()
            ))
        })?;
        let clen = u32::from_be_bytes(header[0..4].try_into().unwrap()) as usize;
        let crc_expected = u32::from_be_bytes(header[4..8].try_into().unwrap());
        let payload_start = header_end;
        let payload_end = payload_start.checked_add(clen).ok_or_else(|| {
            Error::SstCorrupt(format!(
                "{}: block length overflow at offset {offset}",
                self.path.display()
            ))
        })?;
        let payload = data.get(payload_start..payload_end).ok_or_else(|| {
            Error::SstCorrupt(format!(
                "{}: block payload out of bounds at offset {offset}",
                self.path.display()
            ))
        })?;
        let crc_actual = crc32fast::hash(payload);
        if crc_actual != crc_expected {
            return Err(Error::SstCorrupt(format!(
                "{}: crc mismatch at block offset {offset} (expected {crc_expected:#x}, got {crc_actual:#x})",
                self.path.display()
            )));
        }
        let decompressed = zstd::decode_all(payload).map_err(|e| {
            Error::SstCorrupt(format!(
                "{}: zstd decode failed at block offset {offset}: {e}",
                self.path.display()
            ))
        })?;
        Self::parse_block(&decompressed, &self.path)
    }

    fn parse_block(buf: &[u8], path: &Path) -> crate::Result<Vec<Entry>> {
        let overflow =
            |path: &Path| Error::SstCorrupt(format!("{}: entry offset overflow", path.display()));
        let mut out = Vec::new();
        let mut pos = 0usize;
        while pos < buf.len() {
            let header_end = pos
                .checked_add(ENTRY_HEADER_LEN)
                .ok_or_else(|| overflow(path))?;
            let header = buf.get(pos..header_end).ok_or_else(|| {
                Error::SstCorrupt(format!("{}: truncated entry header", path.display()))
            })?;
            let klen = u32::from_be_bytes(header[0..4].try_into().unwrap()) as usize;
            let vlen = u32::from_be_bytes(header[4..8].try_into().unwrap()) as usize;
            let seq = u64::from_be_bytes(header[8..16].try_into().unwrap());
            let kind = header[16];
            pos = header_end;

            let key_end = pos.checked_add(klen).ok_or_else(|| overflow(path))?;
            let key = buf
                .get(pos..key_end)
                .ok_or_else(|| {
                    Error::SstCorrupt(format!("{}: truncated entry key", path.display()))
                })?
                .to_vec();
            pos = key_end;

            let val = match kind {
                1 => {
                    let val_end = pos.checked_add(vlen).ok_or_else(|| overflow(path))?;
                    let v = buf.get(pos..val_end).ok_or_else(|| {
                        Error::SstCorrupt(format!("{}: truncated entry value", path.display()))
                    })?;
                    pos = val_end;
                    Value::Put(v.to_vec())
                }
                0 => Value::Delete,
                other => {
                    return Err(Error::SstCorrupt(format!(
                        "{}: bad entry kind byte {other}",
                        path.display()
                    )))
                }
            };
            out.push((key, seq, val));
        }
        Ok(out)
    }

    /// Point lookup. Returns `Ok(None)` on bloom miss or true absence. `Some(Value::Delete)`
    /// means the newest version at or below `snapshot_seq` is a tombstone.
    pub fn get(&self, user_key: &[u8], snapshot_seq: u64) -> crate::Result<Option<Value>> {
        if self.index.is_empty() || !self.bloom.contains(user_key) {
            return Ok(None);
        }
        // First block whose last_key >= user_key; if user_key is present at all, its first
        // (newest) version starts in this block.
        let start_block = self.index.partition_point(|(k, _)| k.as_slice() < user_key);

        for (_, offset) in self.index.iter().skip(start_block) {
            let entries = self.read_block(*offset)?;
            for (k, seq, val) in entries.iter() {
                match k.as_slice().cmp(user_key) {
                    std::cmp::Ordering::Less => continue,
                    std::cmp::Ordering::Greater => return Ok(None),
                    std::cmp::Ordering::Equal => {
                        if *seq <= snapshot_seq {
                            // Entries for one key are seq DESC, so the first qualifying
                            // version we see is the newest visible one.
                            return Ok(Some(val.clone()));
                        }
                    }
                }
            }
            // Block ended still inside this key's version run (or before it even started, on
            // the very first candidate block) — keep going into the next block.
        }
        Ok(None)
    }

    /// Owning forward iterator over all entries with `user_key >= start`, in file order.
    pub fn iter_from(self: &Arc<Self>, start: &[u8]) -> SstIter {
        let next_block = if start.is_empty() {
            0
        } else {
            self.index.partition_point(|(k, _)| k.as_slice() < start)
        };
        SstIter {
            reader: Arc::clone(self),
            next_block,
            current: Vec::new().into_iter(),
            start_key: start.to_vec(),
            done: false,
        }
    }

    /// Owning forward iterator over every entry in the table.
    pub fn iter(self: &Arc<Self>) -> SstIter {
        self.iter_from(&[])
    }
}

/// Forward iterator returned by [`SstReader::iter`] / [`SstReader::iter_from`]. Holds an
/// `Arc<SstReader>` so it outlives the reader it was created from. Yields entries in on-disk
/// order (`user_key ASC, seqno DESC`); an `Err` is yielded once and ends the stream.
#[derive(Debug)]
pub struct SstIter {
    reader: Arc<SstReader>,
    next_block: usize,
    current: std::vec::IntoIter<Entry>,
    start_key: Vec<u8>,
    done: bool,
}

const _: fn() = || {
    fn assert_send<T: Send>() {}
    assert_send::<SstIter>();
};

impl Iterator for SstIter {
    type Item = crate::Result<Entry>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        loop {
            if let Some((k, seq, v)) = self.current.next() {
                if k.as_slice() < self.start_key.as_slice() {
                    continue;
                }
                return Some(Ok((k, seq, v)));
            }
            if self.next_block >= self.reader.index.len() {
                self.done = true;
                return None;
            }
            let offset = self.reader.index[self.next_block].1;
            self.next_block += 1;
            // `decode_block`, not the cached `read_block`: a full scan touches each block
            // exactly once, so bypassing the cache keeps this a plain owning move (no `Arc`,
            // no clone) -- exactly as before the cache existed.
            match self.reader.decode_block(offset) {
                Ok(entries) => self.current = entries.into_iter(),
                Err(e) => {
                    self.done = true;
                    return Some(Err(e));
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memtable::Value;
    use proptest::prelude::*;
    use std::collections::BTreeMap;
    use std::io::BufWriter;

    fn write_sst(path: &Path, entries: &[Entry]) -> SstSummary {
        let file = std::fs::File::create(path).unwrap();
        let mut w = SstWriter::new(BufWriter::new(file));
        for (k, seq, v) in entries {
            w.add(k, *seq, v).unwrap();
        }
        let (_, summary) = w.finish().unwrap();
        summary
    }

    fn open(path: &Path) -> Arc<SstReader> {
        Arc::new(SstReader::open(path).unwrap())
    }

    #[test]
    fn round_trip_many_blocks() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.sst");
        let entries: Vec<Entry> = (0..10_000u32)
            .map(|i| {
                let k = format!("key-{i:06}").into_bytes();
                (k, 1, Value::Put(format!("value-{i}").into_bytes()))
            })
            .collect();
        let summary = write_sst(&path, &entries);
        assert_eq!(summary.entries, 10_000);
        assert_eq!(summary.smallest, entries[0].0);
        assert_eq!(summary.largest, entries[9_999].0);

        let reader = open(&path);
        for (k, seq, v) in &entries {
            assert_eq!(reader.get(k, *seq).unwrap().as_ref(), Some(v));
        }
        let collected: Vec<Entry> = reader.iter().map(|r| r.unwrap()).collect();
        assert_eq!(collected, entries);
    }

    #[test]
    fn multi_version_spans_block_boundary() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.sst");
        // Large values force each version of "k" into its own block, so the run of versions
        // spans multiple consecutive blocks.
        let big = vec![b'x'; BLOCK_SIZE];
        let entries: Vec<Entry> = (0..20u64)
            .rev()
            .map(|seq| (b"k".to_vec(), seq, Value::Put(big.clone())))
            .collect(); // seq DESC: 19, 18, ..., 0
        let path2 = path.clone();
        write_sst(&path2, &entries);
        let reader = open(&path);

        // Newest <= 15 should be seq 15.
        let got = reader.get(b"k", 15).unwrap().unwrap();
        assert_eq!(got, Value::Put(big.clone()));
        // Verify it's specifically seq 15's slot by checking iteration order/seq count.
        let all: Vec<Entry> = reader.iter().map(|r| r.unwrap()).collect();
        assert_eq!(all.len(), 20);
        let seqs: Vec<u64> = all.iter().map(|(_, s, _)| *s).collect();
        assert_eq!(seqs, (0..20u64).rev().collect::<Vec<_>>());
    }

    #[test]
    fn snapshot_visibility_and_tombstones() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.sst");
        let entries: Vec<Entry> = vec![
            (b"a".to_vec(), 5, Value::Put(b"v5".to_vec())),
            (b"a".to_vec(), 3, Value::Put(b"v3".to_vec())),
            (b"a".to_vec(), 1, Value::Delete),
        ];
        write_sst(&path, &entries);
        let reader = open(&path);

        assert_eq!(
            reader.get(b"a", 10).unwrap(),
            Some(Value::Put(b"v5".to_vec()))
        );
        assert_eq!(
            reader.get(b"a", 4).unwrap(),
            Some(Value::Put(b"v3".to_vec()))
        );
        assert_eq!(reader.get(b"a", 1).unwrap(), Some(Value::Delete));
        assert_eq!(reader.get(b"a", 0).unwrap(), None);
    }

    #[test]
    fn bloom_negative_for_absent_key() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.sst");
        let entries: Vec<Entry> = vec![(b"present".to_vec(), 1, Value::Put(b"v".to_vec()))];
        write_sst(&path, &entries);
        let reader = open(&path);
        assert_eq!(
            reader.get(b"present", 1).unwrap(),
            Some(Value::Put(b"v".to_vec()))
        );
        assert_eq!(reader.get(b"absent", 1).unwrap(), None);
    }

    #[test]
    fn add_rejects_key_or_value_length_exceeding_u32_max() {
        // `SstWriter::add` takes real slices, so the length check itself is exercised here
        // through the same private helper it calls — driving it through `add` for real would
        // require actually allocating a >4GiB buffer, which isn't a reasonable thing for a unit
        // test to do.
        assert!(check_u32_len(u32::MAX as usize, "key").is_ok());
        let err = check_u32_len(u32::MAX as usize + 1, "key").unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
    }

    #[test]
    fn corrupted_index_region_errors_without_panic() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.sst");
        let entries: Vec<Entry> = (0..50u32)
            .map(|i| {
                (
                    format!("k{i:04}").into_bytes(),
                    1,
                    Value::Put(vec![0u8; 64]),
                )
            })
            .collect();
        write_sst(&path, &entries);

        let mut bytes = std::fs::read(&path).unwrap();
        let footer_start = bytes.len() - FOOTER_LEN as usize;
        let index_off =
            u64::from_be_bytes(bytes[footer_start..footer_start + 8].try_into().unwrap());
        let bloom_off = u64::from_be_bytes(
            bytes[footer_start + 8..footer_start + 16]
                .try_into()
                .unwrap(),
        );
        assert!(
            bloom_off > index_off,
            "index region must be non-empty for this test"
        );
        bytes[index_off as usize] ^= 0xFF;
        std::fs::write(&path, &bytes).unwrap();

        let err = SstReader::open(&path).unwrap_err();
        assert!(matches!(err, Error::SstCorrupt(_)), "got {err:?}");
    }

    #[test]
    fn corrupted_bloom_region_errors_without_panic() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.sst");
        let entries: Vec<Entry> = (0..50u32)
            .map(|i| {
                (
                    format!("k{i:04}").into_bytes(),
                    1,
                    Value::Put(vec![0u8; 64]),
                )
            })
            .collect();
        write_sst(&path, &entries);

        let mut bytes = std::fs::read(&path).unwrap();
        let footer_start = bytes.len() - FOOTER_LEN as usize;
        let bloom_off = u64::from_be_bytes(
            bytes[footer_start + 8..footer_start + 16]
                .try_into()
                .unwrap(),
        );
        assert!(
            (footer_start as u64) > bloom_off,
            "bloom region must be non-empty for this test"
        );
        bytes[bloom_off as usize] ^= 0xFF;
        std::fs::write(&path, &bytes).unwrap();

        // Must fail cleanly with SstCorrupt, never panic/hang trying to deserialize a bogus
        // bloom filter.
        let err = SstReader::open(&path).unwrap_err();
        assert!(matches!(err, Error::SstCorrupt(_)), "got {err:?}");
    }

    #[test]
    fn iter_from_matches_filtered_source() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.sst");
        let entries: Vec<Entry> = (0..500u32)
            .map(|i| (format!("k{i:04}").into_bytes(), 1, Value::Put(vec![0u8; 8])))
            .collect();
        write_sst(&path, &entries);
        let reader = open(&path);

        for start in [&b""[..], b"k0000", b"k0250", b"k0499", b"k9999", b"zzzz"] {
            let expected: Vec<Entry> = entries
                .iter()
                .filter(|(k, _, _)| k.as_slice() >= start)
                .cloned()
                .collect();
            let got: Vec<Entry> = reader.iter_from(start).map(|r| r.unwrap()).collect();
            assert_eq!(got, expected, "start={start:?}");
        }
    }

    #[test]
    fn corrupted_crc_errors() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.sst");
        write_sst(&path, &[(b"a".to_vec(), 1, Value::Put(b"v".to_vec()))]);

        let mut bytes = std::fs::read(&path).unwrap();
        // Flip a byte inside the first block's compressed payload (right after the 8-byte
        // block header).
        bytes[BLOCK_HEADER_LEN] ^= 0xFF;
        std::fs::write(&path, &bytes).unwrap();

        let reader = open(&path);
        let err = reader.get(b"a", 1).unwrap_err();
        assert!(matches!(err, Error::SstCorrupt(_)), "got {err:?}");
    }

    #[test]
    fn truncated_file_errors_not_panics() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.sst");
        write_sst(&path, &[(b"a".to_vec(), 1, Value::Put(b"v".to_vec()))]);

        let bytes = std::fs::read(&path).unwrap();
        for cut in [0, 1, 5, bytes.len() / 2, bytes.len() - 1] {
            let truncated = &bytes[..cut];
            let tpath = dir.path().join(format!("trunc-{cut}.sst"));
            std::fs::write(&tpath, truncated).unwrap();
            match SstReader::open(&tpath) {
                Ok(r) => {
                    // Opened despite truncation (footer accidentally aligned) — reads must
                    // still never panic.
                    let _ = r.get(b"a", 1);
                }
                Err(e) => assert!(matches!(e, Error::SstCorrupt(_)), "got {e:?}"),
            }
        }
    }

    #[test]
    fn empty_sst_opens_and_returns_none() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("empty.sst");
        let file = std::fs::File::create(&path).unwrap();
        let w: SstWriter<BufWriter<std::fs::File>> = SstWriter::new(BufWriter::new(file));
        assert!(w.is_empty());
        let (_, summary) = w.finish().unwrap();
        assert_eq!(summary.entries, 0);

        let reader = open(&path);
        assert_eq!(reader.get(b"anything", u64::MAX).unwrap(), None);
        assert_eq!(reader.iter().count(), 0);
    }

    #[test]
    fn bloom_hashes_slice_consistently_between_writer_and_reader() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.sst");
        let keys: Vec<Vec<u8>> = (0..200u32)
            .map(|i| format!("bk{i:04}").into_bytes())
            .collect();
        let entries: Vec<Entry> = keys
            .iter()
            .cloned()
            .map(|k| (k, 1, Value::Put(b"v".to_vec())))
            .collect();
        write_sst(&path, &entries);
        let reader = open(&path);
        for k in &keys {
            assert!(
                reader.bloom.contains(k.as_slice()),
                "bloom missed inserted key {k:?}"
            );
        }
    }

    fn sorted_dedup_entries() -> impl Strategy<Value = Vec<Entry>> {
        proptest::collection::vec(
            (
                proptest::collection::vec(any::<u8>(), 1..6),
                any::<u64>(),
                proptest::option::of(proptest::collection::vec(any::<u8>(), 0..12)),
            ),
            0..200,
        )
        .prop_map(|mut raw| {
            // Dedup identical (key, seq) pairs (last write wins) then sort by (key ASC, seq DESC).
            let mut map: BTreeMap<(Vec<u8>, std::cmp::Reverse<u64>), Value> = BTreeMap::new();
            for (k, seq, v) in raw.drain(..) {
                let val = match v {
                    Some(bytes) => Value::Put(bytes),
                    None => Value::Delete,
                };
                map.insert((k, std::cmp::Reverse(seq)), val);
            }
            map.into_iter()
                .map(|((k, std::cmp::Reverse(seq)), v)| (k, seq, v))
                .collect()
        })
    }

    proptest! {
        #[test]
        fn iter_and_get_match_model(entries in sorted_dedup_entries()) {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("t.sst");
            write_sst(&path, &entries);
            let reader = open(&path);

            let collected: Vec<Entry> = reader.iter().map(|r| r.unwrap()).collect();
            prop_assert_eq!(&collected, &entries);

            // BTreeMap model: for each user_key, newest version <= snapshot_seq.
            let mut model: BTreeMap<Vec<u8>, Vec<(u64, Value)>> = BTreeMap::new();
            for (k, seq, v) in &entries {
                model.entry(k.clone()).or_default().push((*seq, v.clone()));
            }
            for (k, versions) in &model {
                for &(seq, _) in versions {
                    let expected = versions
                        .iter()
                        .filter(|(s, _)| *s <= seq)
                        .max_by_key(|(s, _)| *s)
                        .map(|(_, v)| v.clone());
                    let got = reader.get(k, seq).unwrap();
                    prop_assert_eq!(got, expected);
                }
            }
            // Absent key never found.
            prop_assert_eq!(reader.get(b"__definitely_absent__", u64::MAX).unwrap(), None);
        }
    }
}
