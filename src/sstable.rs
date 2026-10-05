use crate::{
    block::ReadBlock,
    bloom::Bloom,
    cache::{Block, Cache, Counters},
    codec::*,
    error::corrupt,
    fault,
    memtable::Record,
    Result,
};
use parking_lot::Mutex;
use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    os::unix::fs::FileExt,
    path::Path,
    sync::{atomic::Ordering, Arc},
};
const LEGACY_MAGIC: &[u8; 8] = b"RKVSST01";
const MAGIC: &[u8; 8] = b"RKVSST02";
const LEGACY_FOOTER: usize = 24;
const FOOTER: usize = 28;
const INDEX_PAGE_TARGET: usize = 1024 * 1024;
#[cfg(test)]
#[path = "read_profile.rs"]
mod read_profile;
struct Index {
    first: Vec<u8>,
    offset: u64,
    len: u32,
}
pub(crate) struct Table {
    pub id: u64,
    file: File,
    index: Vec<Index>,
    bloom: Bloom,
    pub count: u64,
    pub file_bytes: u64,
    pub max_seq: u64,
}
fn at(file: &File, offset: u64, len: usize) -> Result<Vec<u8>> {
    let mut b = vec![0; len];
    file.read_exact_at(&mut b, offset)?;
    Ok(b)
}
impl Table {
    pub fn write(
        path: &Path,
        id: u64,
        records: impl Iterator<Item = Result<(Vec<u8>, Record)>>,
        estimate: u64,
        block_size: usize,
        bloom_bits: usize,
    ) -> Result<Self> {
        let tmp = path.with_extension("tmp");
        let mut f = OpenOptions::new().create_new(true).write(true).open(&tmp)?;
        f.write_all(MAGIC)?;
        let mut index = vec![];
        let mut bloom = Bloom::new(estimate, bloom_bits);
        let mut block = vec![];
        let mut first = vec![];
        let mut previous: Option<Vec<u8>> = None;
        let mut count = 0;
        let mut max_seq = 0;
        for item in records {
            let (key, r) = item?;
            if previous.as_ref().is_some_and(|p| p >= &key) {
                return Err(corrupt("unsorted SST input"));
            }
            if !block.is_empty() && block.len() + r.size(&key) > block_size {
                write_block(&mut f, &mut block, &first, &mut index)?;
            }
            if block.is_empty() {
                first = key.clone();
            }
            bloom.insert(&key);
            encode_record(&key, &r, &mut block);
            max_seq = max_seq.max(r.seq);
            count += 1;
            previous = Some(key);
        }
        if !block.is_empty() {
            write_block(&mut f, &mut block, &first, &mut index)?;
        }
        let meta_offset = f.stream_position()?;
        let meta_len = write_metadata(&mut f, count, max_seq, &bloom, &index)?;
        let mut footer = vec![];
        footer.extend(MAGIC);
        put_u64(&mut footer, meta_offset);
        put_u64(&mut footer, meta_len);
        let checksum = crc32fast::hash(&footer);
        put_u32(&mut footer, checksum);
        f.write_all(&footer)?;
        fault::hit("sst_before_sync")?;
        f.sync_all()?;
        fault::hit("sst_after_sync")?;
        fs::rename(&tmp, path)?;
        sync_dir(path.parent().unwrap())?;
        fault::hit("sst_after_rename")?;
        // Opening rebuilds these structures; do not retain a second full index.
        drop(index);
        drop(bloom);
        Self::open(path, id)
    }
    pub fn open(path: &Path, id: u64) -> Result<Self> {
        let file = File::open(path)?;
        let len = file.metadata()?.len();
        if len < (8 + LEGACY_FOOTER) as u64 {
            return Err(corrupt("short SST"));
        }
        let magic = at(&file, 0, 8)?;
        let legacy = magic == LEGACY_MAGIC;
        let footer_size = if legacy {
            LEGACY_FOOTER
        } else if magic == MAGIC {
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
        if n > count || n > size / 16 {
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
        Ok(Self {
            id,
            file,
            index,
            bloom,
            count,
            file_bytes: len,
            max_seq,
        })
    }
    pub fn block(&self, n: usize) -> Result<Block> {
        let i = &self.index[n];
        let bytes = at(&self.file, i.offset, i.len as usize)?;
        let mut raw = &bytes[..];
        let payload = read_frame(&mut raw, false)?.ok_or_else(|| corrupt("empty data frame"))?;
        if !raw.is_empty() {
            return Err(corrupt("data frame length"));
        }
        let mut c = Cursor { b: &payload };
        let mut block: Block = vec![];
        while !c.b.is_empty() {
            let item = decode_record(&mut c)?;
            if block.last().is_some_and(|p| p.0 >= item.0)
                || item.1.seq > self.max_seq
                || self
                    .index
                    .get(n + 1)
                    .is_some_and(|next| item.0 >= next.first)
            {
                return Err(corrupt("SST record order/sequence"));
            }
            block.push(item);
        }
        if block.first().is_none_or(|x| x.0 != i.first) {
            return Err(corrupt("SST first key mismatch"));
        }
        Ok(block)
    }
    pub fn get(
        &self,
        key: &[u8],
        cache: &Mutex<Cache>,
        counters: &Counters,
    ) -> Result<Option<Record>> {
        if self.index.first().is_none_or(|i| key < i.first.as_slice()) {
            return Ok(None);
        }
        if !self.bloom.contains(key) {
            counters.bloom_negatives.fetch_add(1, Ordering::Relaxed);
            return Ok(None);
        }
        let p = self.index.partition_point(|i| i.first.as_slice() <= key);
        if p == 0 {
            return Ok(None);
        }
        let n = p - 1;
        // Drop the cache lock before reading from disk.
        let i = &self.index[n];
        let (cached, reusable) = {
            let mut cache = cache.lock();
            let cached = cache.get((self.id, n), counters);
            let reusable = if cached.is_none() {
                cache.take_reusable(i.len as usize)
            } else {
                None
            };
            (cached, reusable)
        };
        let block = if let Some(b) = cached {
            b
        } else {
            counters.reads.fetch_add(1, Ordering::Relaxed);
            let (mut bytes, offsets) = reusable.map(ReadBlock::into_buffers).unwrap_or_default();
            bytes.resize(i.len as usize, 0);
            self.file.read_exact_at(&mut bytes, i.offset)?;
            let b = Arc::new(ReadBlock::decode_reusing(
                bytes,
                offsets,
                &i.first,
                self.index.get(n + 1).map(|next| next.first.as_slice()),
                self.max_seq,
            )?);
            cache.lock().insert((self.id, n), b.clone());
            b
        };
        Ok(block.get(key))
    }
    pub fn iter(self: &Arc<Self>) -> TableIter {
        TableIter {
            table: self.clone(),
            next_block: 0,
            block: vec![].into_iter(),
            failed: false,
        }
    }
}
fn write_metadata(
    f: &mut impl Write,
    count: u64,
    max_seq: u64,
    bloom: &Bloom,
    index: &[Index],
) -> Result<u64> {
    let mut page = vec![];
    put_u64(&mut page, count);
    put_u64(&mut page, max_seq);
    bloom.encode(&mut page);
    put_u64(&mut page, index.len() as u64);
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
    });
    Ok(())
}
fn write_block(f: &mut File, b: &mut Vec<u8>, first: &[u8], index: &mut Vec<Index>) -> Result<()> {
    let offset = f.stream_position()?;
    let len = write_frame(f, b)? as u32;
    index.push(Index {
        first: first.to_vec(),
        offset,
        len,
    });
    b.clear();
    Ok(())
}
pub(crate) fn sync_dir(path: &Path) -> Result<()> {
    File::open(path)?.sync_all()?;
    Ok(())
}
pub(crate) struct TableIter {
    table: Arc<Table>,
    next_block: usize,
    block: std::vec::IntoIter<(Vec<u8>, Record)>,
    failed: bool,
}
impl Iterator for TableIter {
    type Item = Result<(Vec<u8>, Record)>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.failed {
            return None;
        }
        if let Some(x) = self.block.next() {
            return Some(Ok(x));
        }
        if self.next_block == self.table.index.len() {
            return None;
        }
        match self.table.block(self.next_block) {
            Ok(b) => {
                self.next_block += 1;
                self.block = b.into_iter();
                self.next()
            }
            Err(e) => {
                self.failed = true;
                Some(Err(e))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{manifest::Manifest, Engine};
    use bytes::Bytes;

    #[test]
    fn legacy_sst_can_be_read_and_compacted_with_new_tables() {
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
        db.put(b"b", b"new").unwrap();
        db.flush().unwrap();
        assert_eq!(db.stats().sst_files, 2);
        db.compact().unwrap();
        drop(db);
        let db = Engine::open(dir.path()).unwrap();
        assert_eq!(db.get(b"a").unwrap().as_deref(), Some(&b"old"[..]));
        assert_eq!(db.get(b"b").unwrap().as_deref(), Some(&b"new"[..]));
        assert_eq!(db.get(b"deleted").unwrap(), None);
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
