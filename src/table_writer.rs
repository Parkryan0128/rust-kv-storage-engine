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
    bloom_hasher: crate::bloom::PrefixHasher,
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
    small_blocks: usize,
    prefix_block: Vec<u8>,
    prefix_encoded: bool,
    expanded_frame_limit: usize,
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
            bloom_hasher: crate::bloom::PrefixHasher::default(),
            bloom_hashes: retain_hashes.then(Vec::new),
            block: vec![0; HEADER],
            offsets: Vec::new(),
            first: Vec::new(),
            last_key: None,
            position: MAGIC.len() as u64,
            count: 0,
            max_seq: 0,
            block_size,
            small_blocks: 0,
            prefix_block: Vec::new(),
            prefix_encoded: false,
            expanded_frame_limit: 0,
        })
    }

    pub fn push(&mut self, key: &[u8], seq: u64, value: Option<&[u8]>) -> Result<()> {
        if seq == 0
            || key
                .len()
                .saturating_add(value.map_or(0, <[u8]>::len))
                .saturating_add(17)
                > MAX_RECORD
        {
            return Err(corrupt("invalid SST input record"));
        }
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
        let hash = self.bloom_hasher.hash(key);
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
        self.expanded_frame_limit = self.expanded_frame_limit.max(self.block.len());
        let compressed = encode_prefix_frame(
            &self.block,
            &self.first,
            &self.block[self.last_key.as_ref().unwrap().clone()],
            &mut self.offsets,
            &mut self.prefix_block,
        )?;
        let published = if compressed {
            self.prefix_encoded = true;
            &self.prefix_block
        } else {
            let header = frame_header(&self.block[HEADER..])?;
            self.block[..HEADER].copy_from_slice(&header);
            &self.block
        };
        // Coalesce frames while retaining the same logical block boundaries.
        self.file.write_all(published)?;
        let published_len = published.len();
        self.index.push(Index {
            first: std::mem::take(&mut self.first),
            offset: self.position,
            len: published_len as u32,
            validated: AtomicU64::new(0),
        });
        self.position += published_len as u64;
        // A large record should not pin scratch space for the rest of a long merge.
        // Consecutive large records keep their reusable buffer.
        let target = self.block_size.max(4096);
        if self.block.capacity() > target.saturating_mul(4) && self.block.len() <= target * 2 {
            self.small_blocks += 1;
        } else {
            self.small_blocks = 0;
        }
        self.block.truncate(HEADER);
        if self.small_blocks >= 4 {
            self.block.shrink_to(target);
            self.small_blocks = 0;
        }
        // Prefix scratch is bounded too; isolated oversized blocks must not
        // retain it for the rest of a long publication.
        if !compressed || self.prefix_block.capacity() > target.saturating_mul(4) {
            self.prefix_block = Vec::new();
        }
        self.offsets.clear();
        self.last_key = None;
        Ok(())
    }

    pub(super) fn finish(mut self, path: &Path, id: u64, bloom_bits: usize) -> Result<Table> {
        let last_key = OnceLock::new();
        if let Some(range) = &self.last_key {
            let _ = last_key.set(self.block[range.clone()].to_vec());
        }
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
                for (n, i) in self.index.iter().enumerate() {
                    resize_frame(&mut self.block, i.len as usize);
                    fault::hit("bloom_rebuild_read")?;
                    self.file
                        .get_ref()
                        .read_exact_at(&mut self.block, i.offset)?;
                    let next = self.index.get(n + 1).map(|entry| entry.first.as_slice());
                    let bytes = std::mem::take(&mut self.block);
                    let (block, _) = if self.prefix_encoded {
                        ReadBlock::decode_prefix_indexed(
                            bytes,
                            &i.first,
                            next,
                            self.max_seq,
                            0,
                            self.expanded_frame_limit,
                        )?
                    } else {
                        ReadBlock::decode_indexed(bytes, &i.first, next, self.max_seq, 0)?
                    };
                    block.visit_key_hashes(&mut self.bloom_hasher, |hash| {
                        self.bloom.insert_hash(hash)
                    })?;
                    (self.block, _) = block.into_buffers();
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
            Some(last_key.get().map_or(&[], Vec::as_slice)),
            self.prefix_encoded.then_some(self.expanded_frame_limit),
        )?;
        let mut footer = Vec::new();
        let magic = if self.prefix_encoded {
            PREFIX_MAGIC
        } else {
            MAGIC
        };
        footer.extend(magic);
        put_u64(&mut footer, self.position);
        put_u64(&mut footer, meta_len);
        let checksum = crc32fast::hash(&footer);
        put_u32(&mut footer, checksum);
        self.file.write_all(&footer)?;
        // Propagate buffered write errors before syncing or publishing the SST.
        self.file.flush()?;
        if self.prefix_encoded {
            self.file.get_ref().write_all_at(PREFIX_MAGIC, 0)?;
        }
        fault::hit("sst_before_sync")?;
        self.file.get_ref().sync_all()?;
        fault::hit("sst_after_sync")?;
        fs::rename(path.with_extension("tmp"), path)?;
        fault::hit("sst_before_dir_sync")?;
        sync_dir(path.parent().unwrap())?;
        fault::hit("sst_after_rename")?;
        // The writer validated ordering/record limits while streaming. Transfer
        // its metadata after durable publication instead of reading it back.
        let frame = self.index.iter().map(|i| i.len as usize).max().unwrap_or(0);
        let file = self.file.into_inner().map_err(|e| e.into_error())?;
        let read_prefix_hash = ReadPrefixHash::new(
            self.index
                .first()
                .map_or(&[], |entry| entry.first.as_slice()),
        );
        Ok(Table {
            id,
            file,
            index: self.index,
            bloom: self.bloom,
            read_prefix_hash,
            count: self.count,
            max_seq: self.max_seq,
            file_bytes: self.position + meta_len + FOOTER as u64,
            compaction_buffer_bytes: frame * 2
                + if self.prefix_encoded {
                    self.expanded_frame_limit
                } else {
                    0
                }
                + std::mem::size_of::<ReadBlock>(),
            cacheable: AtomicBool::new(true),
            indexed: true,
            expanded_frame_limit: self.prefix_encoded.then_some(self.expanded_frame_limit),
            last_key,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[test]
    fn alternating_large_and_small_output_preserves_records_and_reclaims_scratch() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("bursts.sst");
        let mut writer = TableWriter::new(&path, 48, 4096, 10, false).unwrap();
        let giant = vec![5; 1024 * 1024];
        for n in 0..48u64 {
            writer
                .push(
                    &n.to_be_bytes(),
                    n + 1,
                    Some(if n % 8 == 0 {
                        giant.as_slice()
                    } else {
                        &[6; 2048]
                    }),
                )
                .unwrap();
        }
        assert!(writer.block.capacity() <= 8192);
        let table = Arc::new(writer.finish(&path, 1, 10).unwrap());
        let mut cursor = TableCursor::new(table).unwrap();
        for n in 0..48u64 {
            let row = cursor.current().unwrap();
            assert_eq!(row.key, n.to_be_bytes());
            assert_eq!(
                row.value.unwrap(),
                if n % 8 == 0 {
                    giant.as_slice()
                } else {
                    &[6; 2048]
                }
            );
            cursor.advance().unwrap();
        }
        assert!(cursor.current().is_none());
    }

    #[test]
    fn output_scratch_shrinks_after_small_blocks_but_reuses_consecutive_giants() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("scratch.sst");
        let mut writer = TableWriter::new(&path, 40, 4096, 10, false).unwrap();
        let large = vec![7; 1024 * 1024];
        writer.push(&0u64.to_be_bytes(), 1, Some(&large)).unwrap();
        writer.push(&1u64.to_be_bytes(), 2, Some(&large)).unwrap();
        let capacity = writer.block.capacity();
        writer.push(&2u64.to_be_bytes(), 3, Some(&large)).unwrap();
        assert_eq!(writer.block.capacity(), capacity);
        for n in 3..40u64 {
            writer
                .push(&n.to_be_bytes(), n + 1, Some(&[9; 2048]))
                .unwrap();
        }
        assert!(writer.block.capacity() <= 8192);
        let table = Arc::new(writer.finish(&path, 1, 10).unwrap());
        let mut cursor = TableCursor::new(table).unwrap();
        for n in 0..40u64 {
            let row = cursor.current().unwrap();
            assert_eq!(row.key, n.to_be_bytes());
            assert_eq!(
                row.value.unwrap().len(),
                if n < 3 { large.len() } else { 2048 }
            );
            cursor.advance().unwrap();
        }
        assert!(cursor.current().is_none());
    }

    #[test]
    fn prefix_tables_reopen_rebuild_bloom_and_mix_plain_blocks() {
        for retain_hashes in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("prefix.sst");
            // Overestimate enough to exercise both retained-hash and readback
            // Bloom rebuilding after prefix frames have been written.
            let mut writer = TableWriter::new(&path, 100_000, 4096, 10, retain_hashes).unwrap();
            let mut expected = Vec::new();
            for n in 0..300u64 {
                let mut key = vec![b'x'; 128];
                key.extend_from_slice(&n.to_be_bytes());
                let value = (n % 7 != 0).then(|| vec![n as u8; n as usize % 31]);
                writer.push(&key, n + 1, value.as_deref()).unwrap();
                expected.push((
                    key,
                    Record {
                        seq: n + 1,
                        value: value.map(Bytes::from),
                    },
                ));
            }
            // A lone large value and unrelated short keys retain plain frames.
            writer.push(b"y", 1000, Some(&vec![9; 64 * 1024])).unwrap();
            expected.push((
                b"y".to_vec(),
                Record {
                    seq: 1000,
                    value: Some(Bytes::from(vec![9; 64 * 1024])),
                },
            ));
            writer.push(b"z", 1001, Some(&[])).unwrap();
            expected.push((
                b"z".to_vec(),
                Record {
                    seq: 1001,
                    value: Some(Bytes::new()),
                },
            ));
            let published = writer.finish(&path, 1, 10).unwrap();
            assert_eq!(&fs::read(&path).unwrap()[..8], PREFIX_MAGIC);
            let reopened = Table::open(&path, 1).unwrap();
            assert_eq!(
                published.compaction_buffer_bytes,
                reopened.compaction_buffer_bytes
            );
            assert_eq!(
                published.expanded_frame_limit,
                reopened.expanded_frame_limit
            );
            for table in [published, reopened] {
                for capacity in [0, 8192, 1024 * 1024] {
                    let cache = BlockCache::new(capacity);
                    let counters = Counters::default();
                    for (key, record) in expected.iter().rev() {
                        assert_eq!(
                            table.get(key, &cache, &counters).unwrap(),
                            Some(record.clone())
                        );
                    }
                }
                let table = Arc::new(table);
                for excluded in [false, true] {
                    let start = if excluded {
                        std::ops::Bound::Excluded(expected[13].0.clone())
                    } else {
                        std::ops::Bound::Included(expected[13].0.clone())
                    };
                    let range = Arc::new(crate::scan::KeyRange {
                        start,
                        end: std::ops::Bound::Included(expected[225].0.clone()),
                    });
                    let mut scan = table.scan(range);
                    let counters = Counters::default();
                    let mut rows = Vec::new();
                    while let Some(row) = scan
                        .advance(&counters, |r| (r.key.to_vec(), r.to_owned()))
                        .unwrap()
                    {
                        rows.push(row);
                    }
                    assert_eq!(rows, expected[13 + usize::from(excluded)..=225]);
                }
                let mut cursor = TableCursor::new(table).unwrap();
                for (key, record) in &expected {
                    let got = cursor.current().unwrap();
                    assert_eq!(got.key, key);
                    assert_eq!(got.to_owned(), *record);
                    cursor.advance().unwrap();
                }
                assert!(cursor.current().is_none());
            }
        }
    }

    #[test]
    fn published_metadata_and_bounds_match_reopened_table() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("metadata.sst");
        let mut writer = TableWriter::new(&path, 300, 512, 10, false).unwrap();
        assert!(writer.push(b"invalid", 0, None).is_err());
        for n in 0..300u64 {
            writer
                .push(&n.to_be_bytes(), n + 1, Some(&[42; 32]))
                .unwrap();
        }
        let published = writer.finish(&path, 1, 10).unwrap();
        let reopened = Table::open(&path, 1).unwrap();
        assert_eq!(
            (
                published.count,
                published.max_seq,
                published.file_bytes,
                published.compaction_buffer_bytes
            ),
            (
                reopened.count,
                reopened.max_seq,
                reopened.file_bytes,
                reopened.compaction_buffer_bytes
            )
        );
        assert_eq!(published.index.len(), reopened.index.len());
        for (a, b) in published.index.iter().zip(&reopened.index) {
            assert_eq!((&a.first, a.offset, a.len), (&b.first, b.offset, b.len));
        }
        assert_eq!(reopened.last_key.get(), published.last_key.get());
        let cache = BlockCache::new(0);
        let counters = Counters::default();
        for n in 0..300u64 {
            assert_eq!(
                published.get(&n.to_be_bytes(), &cache, &counters).unwrap(),
                reopened.get(&n.to_be_bytes(), &cache, &counters).unwrap()
            );
        }
        assert_eq!(reopened.last_key.get(), published.last_key.get());
    }

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
    fn long_prefix_hashes_survive_bloom_rebuild_and_reopen() {
        let dir = tempfile::tempdir().unwrap();
        for count in [MAX_BLOOM_HASHES, MAX_BLOOM_HASHES + 1] {
            let mut expected = Bloom::new(count as u64, 10);
            let keys: Vec<_> = (0..count as u64)
                .map(|n| {
                    let mut key = vec![(n / 4096) as u8; 128];
                    key.extend(n.to_be_bytes());
                    expected.insert(&key);
                    key
                })
                .collect();
            let mut expected_bits = Vec::new();
            expected.encode(&mut expected_bits);
            let mut outputs = Vec::new();
            for retain in [false, true] {
                let path = dir.path().join(format!("prefix-{count}-{retain}.sst"));
                let mut writer =
                    TableWriter::new(&path, count as u64 * 4, 4096, 10, retain).unwrap();
                for (n, key) in keys.iter().enumerate() {
                    writer.push(key, n as u64 + 1, Some(&[42; 32])).unwrap();
                }
                assert!(writer.bloom.needs_rebuild(count as u64, 10));
                writer.finish(&path, 1, 10).unwrap();
                let reopened = Table::open(&path, 1).unwrap();
                let mut actual_bits = Vec::new();
                reopened.bloom.encode(&mut actual_bits);
                assert_eq!(actual_bits, expected_bits);
                let cache = BlockCache::new(0);
                let counters = Counters::default();
                for key in &keys {
                    assert_eq!(
                        reopened.get(key, &cache, &counters).unwrap().unwrap().value,
                        Some(bytes::Bytes::from_static(&[42; 32]))
                    );
                }
                outputs.push(fs::read(&path).unwrap());
            }
            assert_eq!(outputs[0], outputs[1]);
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
                if buffered && sizes.iter().sum::<usize>() < OUTPUT_BUFFER_BYTES / 2 {
                    assert_eq!(writer.file.get_ref().metadata().unwrap().len(), 0);
                    assert_eq!(writer.file.buffer().len() as u64, writer.position);
                }
                // The large estimate forces Bloom rebuilding to read buffered data.
                let table = Arc::new(writer.finish(&path, 1, 10).unwrap());
                let mut cursor = TableCursor::new(table).unwrap();
                for (n, &len) in sizes.iter().enumerate() {
                    let record = cursor.current().unwrap();
                    assert_eq!(record.key, (n as u64).to_be_bytes());
                    assert_eq!(record.value.unwrap(), vec![42; len]);
                    cursor.advance().unwrap();
                }
                assert!(cursor.current().is_none());
                outputs.push(fs::read(path).unwrap());
            }
            assert_eq!(outputs[0], outputs[1]);
        }
    }
}
