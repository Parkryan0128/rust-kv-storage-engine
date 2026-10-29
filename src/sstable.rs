#[cfg(test)]
use crate::cache::Cache;
use crate::{
    block::{indexed_records, BlockCursor, ReadBlock, SHARE_LARGE_VALUE_AT},
    bloom::Bloom,
    cache::{BlockCache, CacheKey, Counters},
    codec::*,
    error::corrupt,
    fault,
    memtable::Record,
    Result,
};
use std::{
    borrow::Borrow,
    fs::{self, File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    os::unix::fs::FileExt,
    path::Path,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, OnceLock,
    },
};
const LEGACY_MAGIC: &[u8; 8] = b"RKVSST01";
const PAGED_MAGIC: &[u8; 8] = b"RKVSST02";
const INDEXED_MAGIC: &[u8; 8] = b"RKVSST03";
const MAGIC: &[u8; 8] = b"RKVSST04";
const LEGACY_FOOTER: usize = 24;
const FOOTER: usize = 28;
const INDEX_PAGE_TARGET: usize = 1024 * 1024;
#[cfg(test)]
type Block = Vec<(Vec<u8>, Record)>;
#[path = "scan_table.rs"]
mod scan_table;
pub(crate) use scan_table::TableScan;
#[path = "compaction_cursor.rs"]
mod compaction_cursor;
pub(crate) use compaction_cursor::TableCursor;
#[path = "table_writer.rs"]
mod table_writer;
pub(crate) use table_writer::TableWriter;
#[cfg(test)]
#[path = "read_profile.rs"]
mod read_profile;
struct Index {
    first: Vec<u8>,
    offset: u64,
    len: u32,
    validated: AtomicU64,
}
pub(crate) struct Table {
    pub id: u64,
    file: File,
    index: Vec<Index>,
    bloom: Bloom,
    pub count: u64,
    pub file_bytes: u64,
    pub max_seq: u64,
    pub compaction_buffer_bytes: usize,
    cacheable: AtomicBool,
    indexed: bool,
    // Version 04 persists the maximum in checksummed metadata. Older files
    // learn it lazily after their last block is read, without eager data I/O.
    last_key: OnceLock<Vec<u8>>,
}
fn at(file: &File, offset: u64, len: usize) -> Result<Vec<u8>> {
    let mut b = vec![0; len];
    file.read_exact_at(&mut b, offset)?;
    Ok(b)
}
impl Table {
    pub fn write<K: AsRef<[u8]>, R: Borrow<Record>>(
        path: &Path,
        id: u64,
        records: impl Iterator<Item = Result<(K, R)>>,
        estimate: u64,
        block_size: usize,
        bloom_bits: usize,
    ) -> Result<Self> {
        // Flushes know the exact count, so do not retain hashes for a rebuild.
        // An overestimated iterator still has the bounded readback fallback.
        let mut writer = TableWriter::new(path, estimate, block_size, bloom_bits, false)?;
        for item in records {
            let (key, record) = item?;
            let record = record.borrow();
            writer.push(key.as_ref(), record.seq, record.value.as_deref())?;
        }
        writer.finish(path, id, bloom_bits)
    }
    pub(crate) fn write_with(
        path: &Path,
        id: u64,
        estimate: u64,
        block_size: usize,
        bloom_bits: usize,
        fill: impl FnOnce(&mut TableWriter) -> Result<()>,
    ) -> Result<Self> {
        let mut writer = TableWriter::new(path, estimate, block_size, bloom_bits, true)?;
        fill(&mut writer)?;
        writer.finish(path, id, bloom_bits)
    }
    pub fn open(path: &Path, id: u64) -> Result<Self> {
        let file = File::open(path)?;
        let len = file.metadata()?.len();
        if len < (8 + LEGACY_FOOTER) as u64 {
            return Err(corrupt("short SST"));
        }
        let magic = at(&file, 0, 8)?;
        let legacy = magic == LEGACY_MAGIC;
        let indexed = magic == INDEXED_MAGIC || magic == MAGIC;
        let footer_size = if legacy {
            LEGACY_FOOTER
        } else if indexed || magic == PAGED_MAGIC {
            FOOTER
        } else {
            return Err(corrupt("SST version"));
        };
        if len < (8 + footer_size) as u64 {
            return Err(corrupt("short SST"));
        }
        let footer = at(&file, len - footer_size as u64, footer_size)?;
        let checksum_at = footer_size - 4;
        if &footer[..8] != magic.as_slice()
            || crc32fast::hash(&footer[..checksum_at])
                != u32::from_le_bytes(footer[checksum_at..].try_into().unwrap())
        {
            return Err(corrupt("SST footer checksum/version"));
        }
        let mut c = Cursor {
            b: &footer[8..checksum_at],
        };
        let offset = c.u64()?;
        let size = if legacy { c.u32()? as u64 } else { c.u64()? };
        if (legacy && size > (MAX_FRAME + HEADER) as u64)
            || offset < 8
            || offset.checked_add(size) != Some(len - footer_size as u64)
        {
            return Err(corrupt("SST metadata bounds"));
        }
        let mut input = file.try_clone()?;
        input.seek(SeekFrom::Start(offset))?;
        let mut raw = input.take(size);
        let payload =
            read_frame(&mut raw, false)?.ok_or_else(|| corrupt("missing SST metadata"))?;
        let mut c = Cursor { b: &payload };
        let count = c.u64()?;
        let max_seq = c.u64()?;
        let bloom = Bloom::decode(&mut c)?;
        let n = if legacy { c.u32()? as u64 } else { c.u64()? };
        let maximum = if magic == MAGIC {
            let len = c.u32()? as usize;
            if len > MAX_RECORD - 17 {
                return Err(corrupt("SST maximum key length"));
            }
            Some(c.take(len)?)
        } else {
            None
        };
        // Every stored record needs at least its 17-byte header. This is a
        // conservative bound (frames/directories also consume data bytes), but
        // prevents a checksummed, impossible count from entering stats and
        // compaction estimates where multiple counts are added together.
        if count > (offset - 8) / 17 || n > count || n > size / 16 {
            return Err(corrupt("SST index length"));
        }
        // Grow only as verified entries are decoded, not from an untrusted count.
        let mut index = Vec::new();
        let mut end = 8;
        if legacy {
            if n > (c.b.len() / 16) as u64 || raw.limit() != 0 {
                return Err(corrupt("SST metadata frame length"));
            }
            for _ in 0..n {
                read_index(&mut c, &mut index, &mut end, offset)?;
            }
            c.done()?;
        } else {
            c.done()?;
            while let Some(page) = read_frame(&mut raw, false)? {
                if page.is_empty() {
                    return Err(corrupt("empty SST index page"));
                }
                let mut c = Cursor { b: &page };
                while !c.b.is_empty() {
                    if index.len() as u64 >= n {
                        return Err(corrupt("extra SST index entries"));
                    }
                    read_index(&mut c, &mut index, &mut end, offset)?;
                }
            }
        }
        if index.len() as u64 != n
            || end != offset
            || (count == 0) != (n == 0)
            || (max_seq == 0) != (count == 0)
        {
            return Err(corrupt("SST index/count mismatch"));
        }
        let last_key = OnceLock::new();
        if let Some(maximum) = maximum {
            if let Some(last) = index.last() {
                if maximum < last.first.as_slice()
                    || (count == n && maximum != last.first.as_slice())
                {
                    return Err(corrupt("SST maximum key bounds"));
                }
                let _ = last_key.set(maximum.to_vec());
            } else if !maximum.is_empty() {
                return Err(corrupt("nonempty maximum in empty SST"));
            }
        }
        let frame = index.iter().map(|i| i.len as usize).max().unwrap_or(0);
        let offsets = if indexed {
            0
        } else {
            frame.saturating_sub(HEADER) / 17 * std::mem::size_of::<u32>()
        };
        // A reused frame/index may retain up to twice the current requirement.
        let compaction_buffer_bytes = (frame + offsets) * 2 + std::mem::size_of::<ReadBlock>();
        Ok(Self {
            id,
            file,
            index,
            bloom,
            count,
            file_bytes: len,
            max_seq,
            compaction_buffer_bytes,
            cacheable: AtomicBool::new(true),
            indexed,
            last_key,
        })
    }
    #[cfg(test)]
    pub fn block(&self, n: usize) -> Result<Block> {
        let i = &self.index[n];
        let bytes = at(&self.file, i.offset, i.len as usize)?;
        let block = self.decode(n, bytes, Vec::new())?;
        (0..block.record_count())
            .map(|n| {
                let record = block.record_at(n)?;
                Ok((record.key.to_vec(), record.to_owned()))
            })
            .collect()
    }

    // Sequential readers share buffer reuse and validation without admitting
    // blocks to the point-read cache. Callers account for their own read stats.
    fn read_stream_block(&self, n: usize, previous: Option<ReadBlock>) -> Result<ReadBlock> {
        let entry = &self.index[n];
        let (mut bytes, offsets) = previous.map(ReadBlock::into_buffers).unwrap_or_default();
        let len = entry.len as usize;
        if bytes.capacity() > len.saturating_mul(2) {
            bytes = Vec::new();
        }
        resize_frame(&mut bytes, len);
        self.file.read_exact_at(&mut bytes, entry.offset)?;
        self.decode(n, bytes, offsets)
    }

    fn decode(&self, n: usize, bytes: Vec<u8>, offsets: Vec<u32>) -> Result<ReadBlock> {
        let i = &self.index[n];
        let next = self.index.get(n + 1).map(|next| next.first.as_slice());
        let block = if self.indexed {
            let (block, fingerprint) = ReadBlock::decode_indexed(
                bytes,
                &i.first,
                next,
                self.max_seq,
                i.validated.load(Ordering::Relaxed),
            )?;
            i.validated.store(fingerprint, Ordering::Relaxed);
            block
        } else {
            ReadBlock::decode_reusing(bytes, offsets, &i.first, next, self.max_seq)?
        };
        if n + 1 == self.index.len() {
            let last = block.key_at(block.record_count() - 1)?;
            if let Some(maximum) = self.last_key.get() {
                if maximum.as_slice() != last {
                    return Err(corrupt("SST maximum key disagrees with data"));
                }
            } else {
                let _ = self.last_key.set(last.to_vec());
            }
        }
        Ok(block)
    }
    #[cfg(test)]
    pub fn get(
        &self,
        key: &[u8],
        cache: &BlockCache,
        counters: &Counters,
    ) -> Result<Option<Record>> {
        self.get_hashed(key, &mut None, cache, counters)
    }
    pub fn get_hashed(
        &self,
        key: &[u8],
        hash: &mut Option<u64>,
        cache: &BlockCache,
        counters: &Counters,
    ) -> Result<Option<Record>> {
        if self.index.first().is_none_or(|i| key < i.first.as_slice()) {
            return Ok(None);
        }
        if self
            .last_key
            .get()
            .is_some_and(|last| key > last.as_slice())
        {
            return Ok(None);
        }
        if !self
            .bloom
            .contains_hash(*hash.get_or_insert_with(|| crate::bloom::hash(key)))
        {
            counters.bloom_negatives.fetch_add(1, Ordering::Relaxed);
            return Ok(None);
        }
        let p = self.index.partition_point(|i| i.first.as_slice() <= key);
        if p == 0 {
            return Ok(None);
        }
        let n = p - 1;
        if cache.is_disabled() {
            counters.reads.fetch_add(1, Ordering::Relaxed);
            let block = self.read_stream_block(n, None)?;
            return if block.frame_capacity() >= SHARE_LARGE_VALUE_AT && block.record_count() == 1 {
                Arc::new(block).lookup_shared_large(key)
            } else {
                block.lookup(key)
            };
        }
        // Drop the cache lock before reading from disk.
        let i = &self.index[n];
        let cache_key = CacheKey::from((self.id, n));
        let (cached, reusable, budget) = {
            let mut shard = cache.lock_for_frame(cache_key, i.len as usize, self.indexed);
            let cached = shard.get(cache_key);
            let reusable = if cached.is_none()
                && cache.is_regular(i.len as usize, self.indexed)
                && self.cacheable.load(Ordering::Acquire)
            {
                shard.take_reusable(i.len as usize, self.indexed)
            } else {
                None
            };
            (cached, reusable, shard.block_budget())
        };
        let block = if let Some(b) = cached {
            b
        } else {
            counters.reads.fetch_add(1, Ordering::Relaxed);
            let (mut bytes, offsets) = reusable.map(ReadBlock::into_buffers).unwrap_or_default();
            resize_frame(&mut bytes, i.len as usize);
            self.file.read_exact_at(&mut bytes, i.offset)?;
            let mut block = self.decode(n, bytes, offsets)?;
            block.trim_to_budget(budget);
            let b = Arc::new(block);
            #[cfg(test)]
            crate::test_hooks::pause("cache_before_admission");
            cache.insert(
                cache_key,
                i.len as usize,
                self.indexed,
                b.clone(),
                &self.cacheable,
            );
            b
        };
        if i.len as usize >= SHARE_LARGE_VALUE_AT && block.record_count() == 1 {
            block.lookup_shared_large(key)
        } else {
            block.lookup(key)
        }
    }
    // Mark before purging all cache shards. Once each shard's purge finishes,
    // an old snapshot can still read the file but cannot refill that shard.
    pub fn retire(&self) {
        self.cacheable.store(false, Ordering::Release);
    }
}
fn resize_frame(bytes: &mut Vec<u8>, len: usize) {
    if bytes.capacity() < len {
        // Vec::resize alone grows geometrically; recycled frames close to the
        // cache budget could otherwise become too large to admit.
        bytes.reserve_exact(len - bytes.len());
    }
    bytes.resize(len, 0);
}
fn write_metadata(
    f: &mut impl Write,
    count: u64,
    max_seq: u64,
    bloom: &Bloom,
    index: &[Index],
    maximum: Option<&[u8]>,
) -> Result<u64> {
    let mut page = vec![];
    put_u64(&mut page, count);
    put_u64(&mut page, max_seq);
    bloom.encode(&mut page);
    put_u64(&mut page, index.len() as u64);
    if let Some(maximum) = maximum {
        put_u32(&mut page, maximum.len() as u32);
        page.extend_from_slice(maximum);
    }
    let mut bytes = write_frame(f, &page)?;
    page.clear();
    for i in index {
        // A single large key can exceed the target, but fits within MAX_FRAME.
        if !page.is_empty() && page.len() + 16 + i.first.len() > INDEX_PAGE_TARGET {
            bytes += write_frame(f, &page)?;
            page.clear();
        }
        put_u32(&mut page, i.first.len() as u32);
        page.extend(&i.first);
        put_u64(&mut page, i.offset);
        put_u32(&mut page, i.len);
    }
    if !page.is_empty() {
        bytes += write_frame(f, &page)?;
    }
    Ok(bytes)
}
fn read_index(
    c: &mut Cursor<'_>,
    index: &mut Vec<Index>,
    end: &mut u64,
    metadata_offset: u64,
) -> Result<()> {
    let kl = c.u32()? as usize;
    let first = c.take(kl)?.to_vec();
    let off = c.u64()?;
    let bl = c.u32()?;
    if off != *end
        || bl as usize > MAX_FRAME + HEADER
        || bl < HEADER as u32
        || off
            .checked_add(bl as u64)
            .is_none_or(|e| e > metadata_offset)
        || index.last().is_some_and(|p| p.first >= first)
    {
        return Err(corrupt("SST index bounds/order"));
    }
    *end = off + bl as u64;
    index.push(Index {
        first,
        offset: off,
        len: bl,
        validated: AtomicU64::new(0),
    });
    Ok(())
}
#[cfg(test)]
fn write_block(f: &mut File, b: &mut Vec<u8>, first: &[u8], index: &mut Vec<Index>) -> Result<()> {
    let offset = f.stream_position()?;
    let len = write_frame(f, b)? as u32;
    index.push(Index {
        first: first.to_vec(),
        offset,
        len,
        validated: AtomicU64::new(0),
    });
    b.clear();
    Ok(())
}
pub(crate) fn sync_dir(path: &Path) -> Result<()> {
    File::open(path)?.sync_all()?;
    Ok(())
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::{manifest::Manifest, Engine};
    use bytes::Bytes;

    #[test]
    fn borrowed_writer_preserves_bytes_and_rejects_unsorted_input() {
        // The borrowed writer must not require cloning input keys or records.
        struct Key<'a>(&'a [u8]);
        impl AsRef<[u8]> for Key<'_> {
            fn as_ref(&self) -> &[u8] {
                self.0
            }
        }
        let records = [
            (
                vec![],
                Record {
                    seq: 1,
                    value: Some(Bytes::new()),
                },
            ),
            (
                b"a".to_vec(),
                Record {
                    seq: 2,
                    value: None,
                },
            ),
            (
                b"b".to_vec(),
                Record {
                    seq: 3,
                    value: Some(Bytes::from(vec![42; 8192])),
                },
            ),
            (
                b"z".to_vec(),
                Record {
                    seq: 4,
                    value: Some(Bytes::from_static(b"last")),
                },
            ),
        ];
        let dir = tempfile::tempdir().unwrap();
        for block_size in [64, 16384] {
            for count in [0, records.len()] {
                let owned = dir.path().join(format!("owned-{block_size}-{count}.sst"));
                let borrowed = dir
                    .path()
                    .join(format!("borrowed-{block_size}-{count}.sst"));
                Table::write(
                    &owned,
                    1,
                    records.iter().take(count).cloned().map(Ok),
                    1000,
                    block_size,
                    10,
                )
                .unwrap();
                let table = Table::write(
                    &borrowed,
                    2,
                    records.iter().take(count).map(|(k, r)| Ok((Key(k), r))),
                    1000,
                    block_size,
                    10,
                )
                .unwrap();
                assert_eq!(table.count, count as u64);
                assert_eq!(fs::read(owned).unwrap(), fs::read(borrowed).unwrap());
            }
        }
        for (i, order) in [[0, 0], [1, 0]].into_iter().enumerate() {
            let path = dir.path().join(format!("unsorted-{i}.sst"));
            let input = order
                .into_iter()
                .map(|n| Ok((Key(&records[n].0), &records[n].1)));
            assert!(matches!(
                Table::write(&path, 3, input, 2, 64, 10),
                Err(crate::EngineError::Corruption(_))
            ));
            assert!(!path.exists());
        }
    }

    #[test]
    fn impossible_checksummed_record_counts_are_rejected_at_open() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("table.sst");
        drop(
            Table::write(
                &path,
                1,
                std::iter::once(Ok((
                    b"k".to_vec(),
                    Record {
                        seq: 1,
                        value: Some(Bytes::new()),
                    },
                ))),
                1,
                64,
                10,
            )
            .unwrap(),
        );
        let original = fs::read(&path).unwrap();
        let footer = original.len() - FOOTER;
        let offset =
            u64::from_le_bytes(original[footer + 8..footer + 16].try_into().unwrap()) as usize;
        let mut input = &original[offset..footer];
        let summary = read_frame(&mut input, false).unwrap().unwrap();
        let bound = (offset as u64 - 8) / 17;
        for count in [bound + 1, u64::MAX / 2 + 1, u64::MAX] {
            let mut payload = summary.clone();
            payload[..8].copy_from_slice(&count.to_le_bytes());
            let mut changed = original[..offset].to_vec();
            write_frame(&mut changed, &payload).unwrap();
            changed.extend_from_slice(input);
            changed.extend_from_slice(&original[footer..]);
            assert_eq!(changed.len(), original.len());
            fs::write(&path, changed).unwrap();
            assert!(
                matches!(
                    Table::open(&path, 1),
                    Err(crate::EngineError::Corruption(_))
                ),
                "count {count}"
            );
        }
        fs::write(&path, original).unwrap();
        assert_eq!(Table::open(&path, 1).unwrap().count, 1);
    }

    #[test]
    fn larger_recycled_frame_remains_cacheable_near_the_budget() {
        let dir = tempfile::tempdir().unwrap();
        let table = Table::write(
            &dir.path().join("table.sst"),
            1,
            [(b'a', 920), (b'b', 970)]
                .into_iter()
                .enumerate()
                .map(|(i, (key, len))| {
                    Ok((
                        vec![key],
                        Record {
                            seq: i as u64 + 1,
                            value: Some(Bytes::from(vec![key; len])),
                        },
                    ))
                }),
            2,
            64,
            10,
        )
        .unwrap();
        assert_eq!(table.index[0].len, 958);
        assert_eq!(table.index[1].len, 1008);
        let cache = BlockCache::new(1200);
        let counters = Counters::default();
        assert_eq!(table.get(b"a", &cache, &counters).unwrap().unwrap().seq, 1);
        assert_eq!(table.get(b"b", &cache, &counters).unwrap().unwrap().seq, 2);
        assert_eq!(counters.reads.load(Ordering::Relaxed), 2);
        assert_eq!(table.get(b"b", &cache, &counters).unwrap().unwrap().seq, 2);
        assert_eq!(counters.reads.load(Ordering::Relaxed), 2);
        assert_eq!(cache.stats().1, 1);
        assert!(cache.bytes() <= 1200);
    }

    #[test]
    fn packing_uses_encoded_lengths_and_allows_one_oversized_record() {
        let dir = tempfile::tempdir().unwrap();
        let table = Table::write(
            &dir.path().join("packed.sst"),
            1,
            (b'a'..=b'f').enumerate().map(|(i, key)| {
                Ok((
                    vec![key],
                    Record {
                        seq: i as u64 + 1,
                        value: Some(Bytes::new()),
                    },
                ))
            }),
            6,
            64,
            10,
        )
        .unwrap();
        assert_eq!(table.index.len(), 3);
        for n in 0..3 {
            assert_eq!(table.index[n].len as usize, 2 * 18 + 2 * 4 + 4 + HEADER);
            assert_eq!(table.block(n).unwrap().len(), 2);
        }
        let large = Table::write(
            &dir.path().join("large.sst"),
            2,
            std::iter::once(Ok((
                b"key".to_vec(),
                Record {
                    seq: 7,
                    value: Some(Bytes::from(vec![42; 1024])),
                },
            ))),
            1,
            64,
            10,
        )
        .unwrap();
        assert_eq!(large.index.len(), 1);
        assert_eq!(
            large.block(0).unwrap()[0].1.value.as_ref().unwrap().len(),
            1024
        );
    }

    #[test]
    fn output_count_controls_bloom_size_after_heavy_deduplication() {
        let dir = tempfile::tempdir().unwrap();
        for count in [0u64, 17] {
            let records = || {
                (0..count).map(|key| {
                    Ok((
                        key.to_be_bytes().to_vec(),
                        Record {
                            seq: key + 1,
                            value: Some(Bytes::from_static(b"value")),
                        },
                    ))
                })
            };
            let actual = Table::write(
                &dir.path().join(format!("actual-{count}.sst")),
                1,
                records(),
                1_000_000,
                64,
                10,
            )
            .unwrap();
            let expected = Table::write(
                &dir.path().join(format!("expected-{count}.sst")),
                2,
                records(),
                count,
                64,
                10,
            )
            .unwrap();
            assert_eq!(actual.file_bytes, expected.file_bytes);
            if count == 0 {
                // Version 04 adds the empty maximum-key length to the summary.
                assert_eq!(actual.file_bytes, 92);
            }
            let cache = BlockCache::new(0);
            let counters = Counters::default();
            for key in 0..count {
                assert_eq!(
                    actual
                        .get(&key.to_be_bytes(), &cache, &counters)
                        .unwrap()
                        .unwrap()
                        .seq,
                    key + 1
                );
            }
        }
    }

    #[test]
    fn retired_snapshot_reads_do_not_repopulate_or_evict_live_cache_entries() {
        let dir = tempfile::tempdir().unwrap();
        let table = Table::write(
            &dir.path().join("retired.sst"),
            1,
            std::iter::once(Ok((
                b"key".to_vec(),
                Record {
                    seq: 1,
                    value: Some(Bytes::from_static(b"value")),
                },
            ))),
            1,
            64,
            10,
        )
        .unwrap();
        let cache = BlockCache::new(512);
        let counters = Counters::default();
        table.get(b"key", &cache, &counters).unwrap();
        let capacity = cache.bytes();
        let held = cache.lock((1, 0)).get((1, 0)).unwrap();
        table.retire();
        cache.remove_tables(&std::collections::HashSet::from([1]));
        {
            let mut cache = cache.lock((1, 0));
            assert_eq!(cache.bytes(), 0);
            *cache = Cache::new(capacity);
            cache.insert((2, 0), held.clone());
        }
        let live_bytes = cache.bytes();
        // A retired file stays usable through its open handle after unlink.
        fs::remove_file(dir.path().join("retired.sst")).unwrap();
        assert_eq!(
            table.get(b"key", &cache, &counters).unwrap().unwrap().seq,
            1
        );
        assert_eq!(cache.bytes(), live_bytes);
        assert!(cache.lock((1, 0)).get((1, 0)).is_none());
        assert!(cache.lock((1, 0)).get((2, 0)).is_some());
        assert_eq!(held.get(b"key").unwrap().value.unwrap(), "value");
    }

    #[test]
    fn failed_large_block_read_preserves_warm_small_entries() {
        let dir = tempfile::tempdir().unwrap();
        let make = |name: &str, id, size| {
            Table::write(
                &dir.path().join(name),
                id,
                std::iter::once(Ok((
                    b"key".to_vec(),
                    Record {
                        seq: 1,
                        value: Some(Bytes::from(vec![42; size])),
                    },
                ))),
                1,
                16384,
                10,
            )
            .unwrap()
        };
        let small = make("small.sst", 1, 128);
        let large = make("large.sst", 2, 4 * 1024 * 1024);
        let cache = BlockCache::with_block_size(8 * 1024 * 1024, 16384);
        let counters = Counters::default();
        small.get(b"key", &cache, &counters).unwrap();
        let before = cache.stats();
        let mut damaged = OpenOptions::new()
            .write(true)
            .open(dir.path().join("large.sst"))
            .unwrap();
        damaged
            .seek(SeekFrom::Start(large.index[0].offset + HEADER as u64))
            .unwrap();
        damaged.write_all(&[255]).unwrap();
        damaged.sync_all().unwrap();
        assert!(large.get(b"key", &cache, &counters).is_err());
        assert_eq!(cache.stats(), before);
        let reads = counters.reads.load(Ordering::Relaxed);
        assert_eq!(
            small
                .get(b"key", &cache, &counters)
                .unwrap()
                .unwrap()
                .value
                .unwrap()
                .len(),
            128
        );
        assert_eq!(counters.reads.load(Ordering::Relaxed), reads);
        assert_eq!(cache.stats().1, before.1 + 1);
    }

    #[test]
    fn retirement_between_disk_read_and_admission_cannot_refill_cache() {
        use std::{sync::mpsc::sync_channel, thread, time::Duration};
        for value_len in [64, 4 * 1024 * 1024] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("old.sst");
            let table = Arc::new(
                Table::write(
                    &path,
                    1,
                    std::iter::once(Ok((
                        b"key".to_vec(),
                        Record {
                            seq: 1,
                            value: Some(Bytes::from(vec![42; value_len])),
                        },
                    ))),
                    1,
                    64,
                    10,
                )
                .unwrap(),
            );
            let cache = Arc::new(BlockCache::new(8 * 1024 * 1024));
            let (entered_tx, entered_rx) = sync_channel(1);
            let (resume_tx, resume_rx) = sync_channel(1);
            let reader_table = table.clone();
            let reader_cache = cache.clone();
            let reader = thread::spawn(move || {
                crate::test_hooks::arm("cache_before_admission", entered_tx, resume_rx);
                reader_table.get(b"key", &reader_cache, &Counters::default())
            });
            entered_rx.recv_timeout(Duration::from_secs(10)).unwrap();
            table.retire();
            cache.remove_tables(&std::collections::HashSet::from([1]));
            fs::remove_file(path).unwrap();
            resume_tx.send(()).unwrap();
            assert_eq!(
                reader
                    .join()
                    .unwrap()
                    .unwrap()
                    .unwrap()
                    .value
                    .unwrap()
                    .len(),
                value_len
            );
            assert_eq!(cache.bytes(), 0);
        }
    }

    #[test]
    fn retirement_purges_every_shard_while_pinned_files_remain_readable() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("retired-shards.sst");
        let table = Table::write(
            &path,
            7,
            (0..128u64).map(|key| {
                Ok((
                    key.to_be_bytes(),
                    Record {
                        seq: key + 1,
                        value: Some(Bytes::from(vec![42; 128])),
                    },
                ))
            }),
            128,
            64,
            10,
        )
        .unwrap();
        let cache = BlockCache::new(8 * 1024 * 1024);
        let counters = Counters::default();
        for key in 0..128u64 {
            table.get(&key.to_be_bytes(), &cache, &counters).unwrap();
        }
        assert!(cache.bytes() > 0);
        table.retire();
        cache.remove_tables(&std::collections::HashSet::from([7]));
        assert_eq!(cache.bytes(), 0);
        fs::remove_file(path).unwrap();
        for key in 0..128u64 {
            assert_eq!(
                table
                    .get(&key.to_be_bytes(), &cache, &counters)
                    .unwrap()
                    .unwrap()
                    .seq,
                key + 1
            );
        }
        assert_eq!(cache.bytes(), 0);
        assert_eq!(counters.reads.load(Ordering::Relaxed), 256);
    }

    #[test]
    fn version_two_and_current_tables_remain_readable_together() {
        let dir = tempfile::tempdir().unwrap();
        drop(Engine::open(dir.path()).unwrap());
        let path = dir.path().join("sst/00000000000000000002.sst");
        let mut file = File::create(path).unwrap();
        file.write_all(PAGED_MAGIC).unwrap();
        let mut bytes = Vec::new();
        encode_record(
            b"old",
            &Record {
                seq: 1,
                value: Some(Bytes::from_static(b"v2")),
            },
            &mut bytes,
        );
        let mut index = Vec::new();
        write_block(&mut file, &mut bytes, b"old", &mut index).unwrap();
        let mut bloom = Bloom::new(1, 10);
        bloom.insert(b"old");
        let offset = file.stream_position().unwrap();
        let size = write_metadata(&mut file, 1, 1, &bloom, &index, None).unwrap();
        let mut footer = PAGED_MAGIC.to_vec();
        put_u64(&mut footer, offset);
        put_u64(&mut footer, size);
        let checksum = crc32fast::hash(&footer);
        put_u32(&mut footer, checksum);
        file.write_all(&footer).unwrap();
        file.sync_all().unwrap();
        drop(file);
        Manifest {
            wal_floor: 0,
            max_seq: 1,
            tables: vec![2],
        }
        .save(dir.path())
        .unwrap();
        let db = Engine::open(dir.path()).unwrap();
        db.put(b"new", b"v3").unwrap();
        db.flush().unwrap();
        assert_eq!(db.get(b"old").unwrap().unwrap(), "v2");
        assert_eq!(db.get(b"new").unwrap().unwrap(), "v3");
        assert_eq!(
            db.scan(b"new".as_slice()..=b"old".as_slice())
                .unwrap()
                .collect::<Result<Vec<_>>>()
                .unwrap(),
            [
                (b"new".to_vec(), Bytes::from_static(b"v3")),
                (b"old".to_vec(), Bytes::from_static(b"v2"))
            ]
        );
        db.compact().unwrap();
        drop(db);
        let db = Engine::open(dir.path()).unwrap();
        assert_eq!(db.get(b"old").unwrap().unwrap(), "v2");
        assert_eq!(db.get(b"new").unwrap().unwrap(), "v3");
    }

    #[test]
    fn legacy_sst_can_be_read_and_compacted_with_new_tables() {
        for singleton_first in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            drop(Engine::open(dir.path()).unwrap());
            let path = dir.path().join("sst/00000000000000000002.sst");
            let mut f = File::create(&path).unwrap();
            f.write_all(LEGACY_MAGIC).unwrap();
            let mut index = vec![];
            let mut bloom = Bloom::new(2, 10);
            for (key, seq, value) in [
                (&b"a"[..], 1, Some(Bytes::from_static(b"old"))),
                (&b"deleted"[..], 2, None),
            ] {
                let mut block = vec![];
                encode_record(key, &Record { seq, value }, &mut block);
                write_block(&mut f, &mut block, key, &mut index).unwrap();
                bloom.insert(key);
            }
            let offset = f.stream_position().unwrap();
            let mut meta = vec![];
            put_u64(&mut meta, 2);
            put_u64(&mut meta, 2);
            bloom.encode(&mut meta);
            put_u32(&mut meta, index.len() as u32);
            for i in index {
                put_u32(&mut meta, i.first.len() as u32);
                meta.extend(i.first);
                put_u64(&mut meta, i.offset);
                put_u32(&mut meta, i.len);
            }
            let size = write_frame(&mut f, &meta).unwrap();
            let mut footer = LEGACY_MAGIC.to_vec();
            put_u64(&mut footer, offset);
            put_u32(&mut footer, size as u32);
            let checksum = crc32fast::hash(&footer);
            put_u32(&mut footer, checksum);
            f.write_all(&footer).unwrap();
            f.sync_all().unwrap();
            drop(f);
            Manifest {
                wal_floor: 0,
                max_seq: 2,
                tables: vec![2],
            }
            .save(dir.path())
            .unwrap();

            let db = Engine::open(dir.path()).unwrap();
            assert_eq!(db.get(b"a").unwrap().as_deref(), Some(&b"old"[..]));
            assert_eq!(db.get(b"deleted").unwrap(), None);
            if singleton_first {
                db.compact().unwrap();
                assert_eq!(db.stats().sst_records, 1);
                assert_eq!(db.stats().sequence, 2);
                let id = db.inspect().unwrap().tables[0].id;
                let encoded = fs::read(dir.path().join(format!("sst/{id:020}.sst"))).unwrap();
                assert_eq!(&encoded[..8], MAGIC);
                db.compact().unwrap();
                assert_eq!(db.stats().compactions, 1);
            }
            db.put(b"b", b"new").unwrap();
            db.flush().unwrap();
            assert_eq!(db.stats().sst_files, 2);
            assert_eq!(
                db.scan(..).unwrap().collect::<Result<Vec<_>>>().unwrap(),
                [
                    (b"a".to_vec(), Bytes::from_static(b"old")),
                    (b"b".to_vec(), Bytes::from_static(b"new"))
                ]
            );
            db.compact().unwrap();
            drop(db);
            let db = Engine::open(dir.path()).unwrap();
            assert_eq!(db.get(b"a").unwrap().as_deref(), Some(&b"old"[..]));
            assert_eq!(db.get(b"b").unwrap().as_deref(), Some(&b"new"[..]));
            assert_eq!(db.get(b"deleted").unwrap(), None);
        }
    }

    #[test]
    fn paged_index_checks_counts_order_and_page_integrity() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("table.sst");
        let records = (1..=2).map(|seq| {
            Ok((
                vec![seq as u8; INDEX_PAGE_TARGET / 2],
                Record {
                    seq,
                    value: Some(Bytes::from_static(b"value")),
                },
            ))
        });
        let table = Table::write(&path, 1, records, 2, 64, 10).unwrap();
        assert_eq!(table.index.len(), 2);
        drop(table);
        let original = fs::read(&path).unwrap();
        let footer_start = original.len() - FOOTER;
        let offset = u64::from_le_bytes(
            original[footer_start + 8..footer_start + 16]
                .try_into()
                .unwrap(),
        ) as usize;
        let mut raw = &original[offset..footer_start];
        let mut pages = vec![];
        while let Some(page) = read_frame(&mut raw, false).unwrap() {
            pages.push(page);
        }
        // Summary plus two index pages; neither entry is split across frames.
        assert_eq!(pages.len(), 3);
        for case in 0..6 {
            let mut changed = pages.clone();
            match case {
                0 => {
                    changed.pop();
                }
                1 => changed.push(pages[2].clone()),
                2 => changed.push(vec![]),
                3 => changed.swap(1, 2),
                4 => {
                    let n = changed[0].len();
                    changed[0][n - 8..].copy_from_slice(&1u64.to_le_bytes());
                }
                _ => {
                    // Valid checksums cannot disguise an index key out of order.
                    changed[2][4..4 + INDEX_PAGE_TARGET / 2].fill(1);
                }
            }
            let mut data = original[..offset].to_vec();
            for page in changed {
                write_frame(&mut data, &page).unwrap();
            }
            let size = data.len() - offset;
            let mut footer = MAGIC.to_vec();
            put_u64(&mut footer, offset as u64);
            put_u64(&mut footer, size as u64);
            let checksum = crc32fast::hash(&footer);
            put_u32(&mut footer, checksum);
            data.extend(footer);
            fs::write(&path, data).unwrap();
            assert!(Table::open(&path, 1).is_err(), "case {case}");
        }
        // Damage a later page without fixing its checksum.
        let mut damaged = original.clone();
        damaged[footer_start - 1] ^= 1;
        fs::write(&path, damaged).unwrap();
        assert!(Table::open(&path, 1).is_err());
        fs::write(&path, original).unwrap();
        assert_eq!(Table::open(&path, 1).unwrap().count, 2);
    }
}

#[cfg(test)]
#[path = "read_diagnostics.rs"]
mod read_diagnostics;

#[cfg(test)]
#[path = "sstable_bounds_tests.rs"]
mod bounds_tests;
