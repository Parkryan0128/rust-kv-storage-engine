use crate::{
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
    io::{Seek, Write},
    os::unix::fs::FileExt,
    path::Path,
    sync::{atomic::Ordering, Arc},
};
const MAGIC: &[u8; 8] = b"RKVSST01";
const FOOTER: usize = 24;
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
        let mut meta = vec![];
        put_u64(&mut meta, count);
        put_u64(&mut meta, max_seq);
        bloom.encode(&mut meta);
        put_u32(&mut meta, index.len() as u32);
        for i in &index {
            put_u32(&mut meta, i.first.len() as u32);
            meta.extend(&i.first);
            put_u64(&mut meta, i.offset);
            put_u32(&mut meta, i.len);
        }
        let meta_len = write_frame(&mut f, &meta)?;
        let mut footer = vec![];
        footer.extend(MAGIC);
        put_u64(&mut footer, meta_offset);
        put_u32(&mut footer, meta_len as u32);
        let checksum = crc32fast::hash(&footer);
        put_u32(&mut footer, checksum);
        f.write_all(&footer)?;
        fault::hit("sst_before_sync")?;
        f.sync_all()?;
        fault::hit("sst_after_sync")?;
        fs::rename(&tmp, path)?;
        sync_dir(path.parent().unwrap())?;
        fault::hit("sst_after_rename")?;
        Self::open(path, id)
    }
    pub fn open(path: &Path, id: u64) -> Result<Self> {
        let file = File::open(path)?;
        let len = file.metadata()?.len();
        if len < (8 + FOOTER) as u64 {
            return Err(corrupt("short SST"));
        }
        if at(&file, 0, 8)? != MAGIC {
            return Err(corrupt("SST version"));
        }
        let footer = at(&file, len - FOOTER as u64, FOOTER)?;
        if &footer[..8] != MAGIC
            || crc32fast::hash(&footer[..20])
                != u32::from_le_bytes(footer[20..].try_into().unwrap())
        {
            return Err(corrupt("SST footer checksum/version"));
        }
        let mut c = Cursor { b: &footer[8..20] };
        let offset = c.u64()?;
        let size = c.u32()? as usize;
        if size > MAX_FRAME + HEADER
            || offset < 8
            || offset.checked_add(size as u64) != Some(len - FOOTER as u64)
        {
            return Err(corrupt("SST metadata bounds"));
        }
        let meta = at(&file, offset, size)?;
        let mut raw = &meta[..];
        let payload =
            read_frame(&mut raw, false)?.ok_or_else(|| corrupt("missing SST metadata"))?;
        if !raw.is_empty() {
            return Err(corrupt("SST metadata frame length"));
        }
        let mut c = Cursor { b: &payload };
        let count = c.u64()?;
        let max_seq = c.u64()?;
        let bloom = Bloom::decode(&mut c)?;
        let n = c.u32()? as usize;
        if n > c.b.len() / 16 {
            return Err(corrupt("SST index length"));
        }
        let mut index: Vec<Index> = Vec::with_capacity(n);
        let mut end = 8;
        for _ in 0..n {
            let kl = c.u32()? as usize;
            let first = c.take(kl)?.to_vec();
            let off = c.u64()?;
            let bl = c.u32()?;
            if off != end
                || bl as usize > MAX_FRAME + HEADER
                || bl < HEADER as u32
                || off.checked_add(bl as u64).is_none_or(|e| e > offset)
                || index.last().is_some_and(|p| p.first >= first)
            {
                return Err(corrupt("SST index bounds/order"));
            }
            end = off + bl as u64;
            index.push(Index {
                first,
                offset: off,
                len: bl,
            });
        }
        c.done()?;
        if end != offset || (count == 0) != (n == 0) || (max_seq == 0) != (count == 0) {
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
        let cached = { cache.lock().get((self.id, n), counters) };
        let block = if let Some(b) = cached {
            b
        } else {
            counters.reads.fetch_add(1, Ordering::Relaxed);
            let b = Arc::new(self.block(n)?);
            cache.lock().insert((self.id, n), b.clone());
            b
        };
        Ok(block
            .binary_search_by(|(k, _)| k.as_slice().cmp(key))
            .ok()
            .map(|i| block[i].1.clone()))
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
