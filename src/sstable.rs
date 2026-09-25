//! Sorted-string table reader + writer.
//!
//! ## File layout
//!
//! ```text
//!   +-----------------+
//!   | data block 0    |   4KiB target, zstd-compressed payload
//!   | data block 1    |   [u32 BE compressed_len][compressed bytes]
//!   | ...             |
//!   | data block N    |
//!   +-----------------+
//!   | index block     |   bincode: Vec<(last_key, block_offset)>
//!   +-----------------+
//!   | bloom block     |   bincode: GrowableBloom
//!   +-----------------+
//!   | footer (24B)    |   [u64 BE index_off][u64 BE bloom_off][u64 BE magic=0xDEADBEEF]
//!   +-----------------+
//! ```
//!
//! Each data block entry: `[u32 BE klen][u32 BE vlen][u64 BE seq][u8 kind][key][val]`
//! where `kind` is 1 for Put and 0 for Delete (the val bytes are absent for Delete).

use crate::memtable::Value;
use growable_bloom_filter::GrowableBloom;
use std::io::Write;
use std::path::Path;

/// Target uncompressed size of one data block before flush. The block buffer flushes whenever
/// it grows past this threshold; small tail-blocks are written as-is.
pub const BLOCK_SIZE: usize = 4096;
/// Magic trailer that identifies a complete SSTable file.
pub const SSTABLE_MAGIC: u64 = 0xDEAD_BEEF;
/// Bloom false-positive rate target.
pub const BLOOM_FPR: f64 = 0.01;
/// Bloom initial capacity hint — the filter grows past this if needed.
pub const BLOOM_CAPACITY: usize = 100_000;

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
        }
    }

    /// Append one record. Caller must feed records sorted by `(user_key ASC, seqno DESC)` so
    /// the index can record `(last_key, block_offset)` for each block.
    pub fn add(&mut self, key: &[u8], seqno: u64, val: &Value) -> std::io::Result<()> {
        self.bloom.insert(&key);
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
        if self.block_buf.len() >= BLOCK_SIZE {
            self.flush_block()?;
        }
        Ok(())
    }

    fn flush_block(&mut self) -> std::io::Result<()> {
        let compressed = zstd::encode_all(&self.block_buf[..], 3)?;
        self.w.write_all(&(compressed.len() as u32).to_be_bytes())?;
        self.w.write_all(&compressed)?;
        self.index.push((self.last_key.clone(), self.offset));
        self.offset += 4 + compressed.len() as u64;
        self.block_buf.clear();
        Ok(())
    }

    /// Finalize: flush any partial block, append the index + bloom + footer.
    pub fn finish(mut self) -> std::io::Result<()> {
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
        self.w.write_all(&index_off.to_be_bytes())?;
        self.w.write_all(&bloom_off.to_be_bytes())?;
        self.w.write_all(&SSTABLE_MAGIC.to_be_bytes())?;
        Ok(())
    }
}

/// Read handle for an on-disk SSTable. Holds the mmap region + parsed index + bloom.
///
/// **Phase 1 status:** types and signatures are wired so callers compile; bodies are stubs
/// that `todo!()` and will land in Phase 2 alongside the flush thread and compactor.
#[derive(Debug)]
pub struct SstReader {
    // Populated in Phase 2:
    //   mmap: memmap2::Mmap,
    //   index: Vec<(Vec<u8>, u64)>,
    //   bloom: GrowableBloom,
    //   data_end: u64,
    _path: std::path::PathBuf,
}

impl SstReader {
    /// Open an SSTable for reading. Parses the footer + index + bloom up-front; data blocks
    /// are read lazily through the mmap region.
    pub fn open(path: &Path) -> std::io::Result<Self> {
        // Phase 2: open file, mmap, parse footer at end, deserialize index + bloom.
        Ok(Self {
            _path: path.to_path_buf(),
        })
    }

    /// Point lookup. Returns `Ok(None)` on bloom miss or true absence.
    pub fn get(&self, _user_key: &[u8], _snapshot_seq: u64) -> std::io::Result<Option<Value>> {
        // Phase 2: bloom check → binary-search index → decompress block → linear scan.
        todo!("SstReader::get lands in Phase 2 alongside the flush thread")
    }

    /// Forward iterator over all `(user_key, seqno, Value)` triples in the table.
    pub fn iter(&self) -> SstIter<'_> {
        // Phase 2: build a `SstIter` that walks blocks in order.
        todo!("SstReader::iter lands in Phase 2 alongside compaction merge")
    }
}

/// Forward iterator returned by [`SstReader::iter`]. Phase-1 stub.
#[derive(Debug)]
pub struct SstIter<'a> {
    _reader: &'a SstReader,
}

impl<'a> Iterator for SstIter<'a> {
    type Item = (Vec<u8>, u64, Value);
    fn next(&mut self) -> Option<Self::Item> {
        todo!("SstIter::next lands in Phase 2")
    }
}
