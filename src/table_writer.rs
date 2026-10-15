use super::*;
use std::io::BufWriter;
use std::ops::Range;

const OUTPUT_BUFFER_BYTES: usize = 256 * 1024;
const MAX_BLOOM_HASHES: usize = 64 * 1024 / std::mem::size_of::<u64>();

// Both frozen memory and a borrowed merge stream through the same publisher.
// The last key stays in the encoded block, avoiding an extra key allocation.
pub(crate) struct TableWriter {
    file: BufWriter<File>,
    index: Vec<Index>,
    bloom: Bloom,
    // A bounded compaction-only shortcut. None uses output readback if needed.
    bloom_hashes: Option<Vec<u64>>,
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
        retain_hashes: bool,
    ) -> Result<Self> {
        let file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(path.with_extension("tmp"))?;
        let mut file = BufWriter::with_capacity(OUTPUT_BUFFER_BYTES, file);
        file.write_all(MAGIC)?;
        Ok(Self {
            file,
            index: Vec::new(),
            bloom: Bloom::new(estimate, bloom_bits),
            bloom_hashes: retain_hashes.then(Vec::new),
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
        let hash = crate::bloom::hash(key);
        self.bloom.insert_hash(hash);
        if let Some(hashes) = &mut self.bloom_hashes {
            if hashes.len() == MAX_BLOOM_HASHES {
                // Release the allocation on overflow, not just its contents.
                self.bloom_hashes = None;
            } else {
                hashes.push(hash);
            }
        }
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
        // Coalesce small frames without changing their on-disk boundaries.
        // Position counts buffered bytes too; pread never changes it.
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
            fault::hit("bloom_rebuild")?;
            self.bloom = Bloom::new(self.count, bloom_bits);
            if let Some(hashes) = self.bloom_hashes.take() {
                debug_assert_eq!(hashes.len() as u64, self.count);
                for hash in hashes {
                    self.bloom.insert_hash(hash);
                }
            } else {
                // Positional reads must see every buffered output block.
                self.file.flush()?;
                for i in &self.index {
                    resize_frame(&mut self.block, i.len as usize);
                    fault::hit("bloom_rebuild_read")?;
                    self.file
                        .get_ref()
                        .read_exact_at(&mut self.block, i.offset)?;
                    let mut c = Cursor {
                        b: indexed_records(frame_payload(&self.block)?)?,
                    };
                    while !c.b.is_empty() {
                        self.bloom.insert(decode_record_ref(&mut c)?.key);
                    }
                }
            }
        }
        self.bloom_hashes = None;
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
        // Propagate buffered write errors before syncing or publishing the SST.
        self.file.flush()?;
        fault::hit("sst_before_sync")?;
        self.file.get_ref().sync_all()?;
        fault::hit("sst_after_sync")?;
        fs::rename(path.with_extension("tmp"), path)?;
        sync_dir(path.parent().unwrap())?;
        fault::hit("sst_after_rename")?;
        // Release the buffers and metadata before reopening the published table.
        drop(self);
        Table::open(path, id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retained_hashes_match_readback_at_cap_and_after_overflow() {
        let dir = tempfile::tempdir().unwrap();
        for count in [0, 1, MAX_BLOOM_HASHES, MAX_BLOOM_HASHES + 1] {
            let mut outputs = Vec::new();
            for retain in [false, true] {
                let path = dir.path().join(format!("hash-{count}-{retain}.sst"));
                let mut writer =
                    TableWriter::new(&path, (count as u64 + 100) * 3, 1024, 10, retain).unwrap();
                for n in 0..count as u64 {
                    let value = (n % 7 != 0).then_some([42; 32]);
                    writer
                        .push(
                            &n.to_be_bytes(),
                            n + 1,
                            value.as_ref().map(|v| v.as_slice()),
                        )
                        .unwrap();
                }
                assert!(writer.bloom.needs_rebuild(count as u64, 10));
                assert_eq!(
                    writer.bloom_hashes.is_some(),
                    retain && count <= MAX_BLOOM_HASHES
                );
                if let Some(hashes) = &writer.bloom_hashes {
                    assert_eq!(hashes.len(), count);
                    assert!(hashes.capacity() <= MAX_BLOOM_HASHES);
                }
                let table = writer.finish(&path, 1, 10).unwrap();
                assert_eq!(table.count, count as u64);
                for n in 0..count as u64 {
                    assert!(table.bloom.contains(&n.to_be_bytes()));
                }
                outputs.push(fs::read(path).unwrap());
            }
            assert_eq!(outputs[0], outputs[1], "count={count}");
        }
    }

    #[test]
    fn buffering_preserves_bytes_with_bloom_rebuild_empty_and_oversized_blocks() {
        let dir = tempfile::tempdir().unwrap();
        for sizes in [
            vec![],
            vec![128; 100],
            vec![128, OUTPUT_BUFFER_BYTES + 17, 128],
        ] {
            let mut outputs = Vec::new();
            for buffered in [false, true] {
                let path = dir.path().join(format!("{}-{buffered}.sst", sizes.len()));
                let mut writer = TableWriter::new(&path, 1000, 1024, 10, false).unwrap();
                if !buffered {
                    writer.file = BufWriter::with_capacity(0, writer.file.into_inner().unwrap());
                }
                for (n, &len) in sizes.iter().enumerate() {
                    writer
                        .push(
                            &(n as u64).to_be_bytes(),
                            n as u64 + 1,
                            Some(&vec![42; len]),
                        )
                        .unwrap();
                }
                writer.flush_block().unwrap();
                if buffered && sizes.iter().sum::<usize>() < OUTPUT_BUFFER_BYTES / 2 {
                    assert_eq!(writer.file.get_ref().metadata().unwrap().len(), 0);
                    assert_eq!(writer.file.buffer().len() as u64, writer.position);
                }
                // The large estimate forces Bloom rebuilding to read buffered data.
                let table = Arc::new(writer.finish(&path, 1, 10).unwrap());
                let mut cursor = TableCursor::new(table).unwrap();
                for (n, &len) in sizes.iter().enumerate() {
                    let record = cursor.current().unwrap().unwrap();
                    assert_eq!(record.key, (n as u64).to_be_bytes());
                    assert_eq!(record.value.unwrap(), vec![42; len]);
                    cursor.advance().unwrap();
                }
                assert!(cursor.current().unwrap().is_none());
                outputs.push(fs::read(path).unwrap());
            }
            assert_eq!(outputs[0], outputs[1]);
        }
    }
}
