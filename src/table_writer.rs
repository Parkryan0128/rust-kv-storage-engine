use super::*;
use std::ops::Range;

// Both frozen memory and a borrowed merge stream through the same publisher.
// The last key stays in the encoded block, avoiding an extra key allocation.
pub(crate) struct TableWriter {
    file: File,
    index: Vec<Index>,
    bloom: Bloom,
    block: Vec<u8>,
    offsets: Vec<u32>,
    first: Vec<u8>,
    last_key: Option<Range<usize>>,
    position: u64,
    count: u64,
    max_seq: u64,
    block_size: usize,
}

impl TableWriter {
    pub(super) fn new(
        path: &Path,
        estimate: u64,
        block_size: usize,
        bloom_bits: usize,
    ) -> Result<Self> {
        let mut file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(path.with_extension("tmp"))?;
        file.write_all(MAGIC)?;
        Ok(Self {
            file,
            index: Vec::new(),
            bloom: Bloom::new(estimate, bloom_bits),
            block: vec![0; HEADER],
            offsets: Vec::new(),
            first: Vec::new(),
            last_key: None,
            position: MAGIC.len() as u64,
            count: 0,
            max_seq: 0,
            block_size,
        })
    }

    pub fn push(&mut self, key: &[u8], seq: u64, value: Option<&[u8]>) -> Result<()> {
        // Check before a possible flush clears the previous block.
        if self
            .last_key
            .as_ref()
            .is_some_and(|r| &self.block[r.clone()] >= key)
        {
            return Err(corrupt("unsorted SST input"));
        }
        let record_len = 17 + key.len() + value.map_or(0, <[u8]>::len);
        if !self.offsets.is_empty()
            && self.block.len() - HEADER + record_len + (self.offsets.len() + 1) * 4 + 4
                > self.block_size
        {
            self.flush_block()?;
        }
        if self.offsets.is_empty() {
            self.first = key.to_vec();
        }
        self.bloom.insert(key);
        self.offsets.push((self.block.len() - HEADER) as u32);
        let start = self.block.len() + 17;
        encode_record_parts(key, seq, value, &mut self.block);
        self.last_key = Some(start..start + key.len());
        self.max_seq = self.max_seq.max(seq);
        self.count += 1;
        Ok(())
    }

    fn flush_block(&mut self) -> Result<()> {
        if self.offsets.is_empty() {
            return Ok(());
        }
        for &offset in &self.offsets {
            put_u32(&mut self.block, offset);
        }
        put_u32(&mut self.block, self.offsets.len() as u32);
        let header = frame_header(&self.block[HEADER..])?;
        self.block[..HEADER].copy_from_slice(&header);
        // One contiguous frame, including retries for short/interrupted writes.
        // All writes are sequential; pread during Bloom rebuilding does not
        // change this tracked file position.
        self.file.write_all(&self.block)?;
        self.index.push(Index {
            first: std::mem::take(&mut self.first),
            offset: self.position,
            len: self.block.len() as u32,
            validated: AtomicU64::new(0),
        });
        self.position += self.block.len() as u64;
        self.block.truncate(HEADER);
        self.offsets.clear();
        self.last_key = None;
        Ok(())
    }

    pub(super) fn finish(mut self, path: &Path, id: u64, bloom_bits: usize) -> Result<Table> {
        self.flush_block()?;
        if self.bloom.needs_rebuild(self.count, bloom_bits) {
            self.bloom = Bloom::new(self.count, bloom_bits);
            for i in &self.index {
                resize_frame(&mut self.block, i.len as usize);
                fault::hit("bloom_rebuild_read")?;
                self.file.read_exact_at(&mut self.block, i.offset)?;
                let mut c = Cursor {
                    b: indexed_records(frame_payload(&self.block)?)?,
                };
                while !c.b.is_empty() {
                    self.bloom.insert(decode_record_ref(&mut c)?.key);
                }
            }
        }
        let meta_len = write_metadata(
            &mut self.file,
            self.count,
            self.max_seq,
            &self.bloom,
            &self.index,
        )?;
        let mut footer = Vec::new();
        footer.extend(MAGIC);
        put_u64(&mut footer, self.position);
        put_u64(&mut footer, meta_len);
        let checksum = crc32fast::hash(&footer);
        put_u32(&mut footer, checksum);
        self.file.write_all(&footer)?;
        fault::hit("sst_before_sync")?;
        self.file.sync_all()?;
        fault::hit("sst_after_sync")?;
        fs::rename(path.with_extension("tmp"), path)?;
        sync_dir(path.parent().unwrap())?;
        fault::hit("sst_after_rename")?;
        // Release the buffers and metadata before reopening the published table.
        drop(self);
        Table::open(path, id)
    }
}
