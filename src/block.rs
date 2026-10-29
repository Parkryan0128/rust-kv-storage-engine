use crate::{codec::*, error::corrupt, memtable::Record, Result};

// The frame and offsets stay immutable after validation. Only the selected
// value is copied out, so a caller retaining a small value cannot pin a block.
pub(crate) struct ReadBlock {
    bytes: Vec<u8>,
    offsets: Vec<u32>,
    // Zero for legacy blocks; otherwise the persisted offset directory starts here.
    index_start: usize,
}

impl ReadBlock {
    #[cfg(test)]
    pub fn decode(bytes: Vec<u8>, first: &[u8], next: Option<&[u8]>, max_seq: u64) -> Result<Self> {
        Self::decode_reusing(bytes, Vec::new(), first, next, max_seq)
    }

    pub fn decode_reusing(
        bytes: Vec<u8>,
        mut offsets: Vec<u32>,
        first: &[u8],
        next: Option<&[u8]>,
        max_seq: u64,
    ) -> Result<Self> {
        let payload = frame_payload(&bytes)?;
        let mut c = Cursor { b: payload };
        let mut previous: Option<&[u8]> = None;
        offsets.clear();
        while !c.b.is_empty() {
            let offset = bytes.len() - c.b.len();
            let r = decode_record_ref(&mut c)?;
            if previous.is_some_and(|p| p >= r.key) || r.seq > max_seq {
                return Err(corrupt("SST record order/sequence"));
            }
            if previous.is_none() && r.key != first {
                return Err(corrupt("SST first key mismatch"));
            }
            if previous.is_none() && offsets.capacity() == 0 {
                // A bounded hint from an already validated record avoids repeated
                // small reallocations without trusting an on-disk record count.
                let record_bytes = bytes.len() - c.b.len() - offset;
                offsets.reserve((payload.len() / record_bytes).min(256));
            }
            // Frames are limited to MAX_FRAME + HEADER, well below u32::MAX.
            offsets.push(offset as u32);
            previous = Some(r.key);
        }
        let last = previous.ok_or_else(|| corrupt("SST first key mismatch"))?;
        // Strict ordering above makes the last key an upper bound for every
        // record. Check the next block boundary once, not once per record.
        if next.is_some_and(|key| last >= key) {
            return Err(corrupt("SST record order/sequence"));
        }
        // A recycled dense block must not carry a large index into a sparse one.
        if offsets.capacity() > offsets.len().max(4).saturating_mul(2) {
            offsets.shrink_to_fit();
        }
        Ok(Self {
            bytes,
            offsets,
            index_start: 0,
        })
    }

    pub fn decode_indexed(
        bytes: Vec<u8>,
        first: &[u8],
        next: Option<&[u8]>,
        max_seq: u64,
        validated: u64,
    ) -> Result<(Self, u64)> {
        let payload = frame_payload(&bytes)?;
        let records = indexed_records(payload)?;
        let index_start = HEADER + records.len();
        // The extra bit distinguishes an unvalidated block from a valid zero CRC.
        let fingerprint =
            (1u64 << 32) | u32::from_le_bytes(bytes[8..12].try_into().unwrap()) as u64;
        if fingerprint != validated {
            let mut cursor = Cursor { b: records };
            let mut previous: Option<&[u8]> = None;
            for offset in payload[records.len()..payload.len() - 4].chunks_exact(4) {
                let offset = u32::from_le_bytes(offset.try_into().unwrap()) as usize;
                if offset != records.len() - cursor.b.len() {
                    return Err(corrupt("SST record offset"));
                }
                let record = decode_record_ref(&mut cursor)?;
                if record.seq > max_seq || previous.is_some_and(|p| p >= record.key) {
                    return Err(corrupt("SST record order/sequence"));
                }
                if previous.is_none() && record.key != first {
                    return Err(corrupt("SST first key mismatch"));
                }
                previous = Some(record.key);
            }
            cursor.done()?;
            let last = previous.ok_or_else(|| corrupt("empty SST block"))?;
            if next.is_some_and(|key| last >= key) {
                return Err(corrupt("SST record order/sequence"));
            }
        }
        Ok((
            Self {
                bytes,
                offsets: Vec::new(),
                index_start,
            },
            fingerprint,
        ))
    }

    pub fn into_buffers(self) -> (Vec<u8>, Vec<u32>) {
        (self.bytes, self.offsets)
    }

    pub fn frame_capacity(&self) -> usize {
        self.bytes.capacity()
    }

    pub fn trim_to_budget(&mut self, budget: usize) {
        let minimum = self.bytes.len()
            + self.offsets.len() * std::mem::size_of::<u32>()
            + std::mem::size_of::<Self>();
        if minimum <= budget && self.allocated_bytes() > budget {
            self.bytes.shrink_to_fit();
            self.offsets.shrink_to_fit();
        }
    }

    #[cfg(test)]
    pub fn buffer_addresses(&self) -> (*const u8, *const u32) {
        (self.bytes.as_ptr(), self.offsets.as_ptr())
    }

    fn key(&self, offset: u32) -> &[u8] {
        let offset = offset as usize;
        // These lengths and boundaries were checked before publishing the block.
        let len = u32::from_le_bytes(self.bytes[offset + 9..offset + 13].try_into().unwrap());
        &self.bytes[offset + 17..offset + 17 + len as usize]
    }

    #[cfg(test)]
    pub fn get(&self, key: &[u8]) -> Option<Record> {
        self.lookup(key).expect("validated block")
    }

    pub fn lookup(&self, key: &[u8]) -> Result<Option<Record>> {
        if self.index_start != 0 {
            let (mut left, mut right) = (0, self.record_count());
            while left < right {
                let middle = left + (right - left) / 2;
                match self.key_at(middle)?.cmp(key) {
                    std::cmp::Ordering::Less => left = middle + 1,
                    std::cmp::Ordering::Greater => right = middle,
                    std::cmp::Ordering::Equal => {
                        return Ok(Some(self.record_at(middle)?.to_owned()))
                    }
                }
            }
            return Ok(None);
        }
        let index = self
            .offsets
            .binary_search_by(|&offset| self.key(offset).cmp(key))
            .ok();
        let Some(index) = index else { return Ok(None) };
        let mut c = Cursor {
            b: &self.bytes[self.offsets[index] as usize..],
        };
        Ok(Some(
            decode_record_ref(&mut c)
                .expect("immutable validated record")
                .to_owned(),
        ))
    }

    pub fn record_count(&self) -> usize {
        if self.index_start == 0 {
            self.offsets.len()
        } else {
            (self.bytes.len() - self.index_start - 4) / 4
        }
    }

    // Decode only the comparison key after block validation. Keep bounds checks
    // here too: a stale validation fingerprint must never permit a slice panic.
    #[inline]
    pub fn key_at(&self, index: usize) -> Result<&[u8]> {
        let bytes = self.record_bytes(index)?;
        let length = bytes.get(9..13).ok_or_else(|| corrupt("SST key header"))?;
        let length = u32::from_le_bytes(length.try_into().unwrap()) as usize;
        let end = 17usize
            .checked_add(length)
            .ok_or_else(|| corrupt("SST key length"))?;
        bytes.get(17..end).ok_or_else(|| corrupt("SST key bounds"))
    }

    #[inline]
    pub fn record_at(&self, index: usize) -> Result<RecordRef<'_>> {
        decode_record_ref(&mut Cursor {
            b: self.record_bytes(index)?,
        })
    }

    #[inline]
    fn record_bytes(&self, index: usize) -> Result<&[u8]> {
        if index >= self.record_count() {
            return Err(corrupt("SST record index"));
        }
        let offset = if self.index_start == 0 {
            self.offsets[index] as usize
        } else {
            let start = self.index_start + index * 4;
            HEADER + u32::from_le_bytes(self.bytes[start..start + 4].try_into().unwrap()) as usize
        };
        let end = if self.index_start == 0 {
            self.bytes.len()
        } else {
            self.index_start
        };
        self.bytes
            .get(offset..end)
            .ok_or_else(|| corrupt("SST record offset"))
    }

    pub fn allocated_bytes(&self) -> usize {
        self.bytes.capacity()
            + self.offsets.capacity() * std::mem::size_of::<u32>()
            + std::mem::size_of::<Self>()
    }
}

// Own the immutable block and decoded head offsets together. No self-reference
// or unchecked slice is needed, and repeated current() calls never decode again.
pub(crate) struct BlockCursor {
    block: ReadBlock,
    head: Option<RecordPosition>,
}
struct RecordPosition {
    key_start: usize,
    key_end: usize,
    end: usize,
    seq: u64,
    has_value: bool,
}
impl BlockCursor {
    pub fn new(block: ReadBlock, index: usize) -> Result<Self> {
        let offset = if index == block.record_count() {
            None
        } else {
            let end = if block.index_start == 0 {
                block.bytes.len()
            } else {
                block.index_start
            };
            Some(end - block.record_bytes(index)?.len())
        };
        let mut out = Self { block, head: None };
        if let Some(offset) = offset {
            out.decode_at(offset)?;
        }
        Ok(out)
    }
    fn data_end(&self) -> usize {
        if self.block.index_start == 0 {
            self.block.bytes.len()
        } else {
            self.block.index_start
        }
    }
    fn decode_at(&mut self, offset: usize) -> Result<()> {
        let bytes = self
            .block
            .bytes
            .get(offset..self.data_end())
            .ok_or_else(|| corrupt("SST cursor offset"))?;
        let mut c = Cursor { b: bytes };
        let record = decode_record_ref(&mut c)?;
        let key_start = offset + 17;
        let key_end = key_start + record.key.len();
        self.head = Some(RecordPosition {
            key_start,
            key_end,
            end: self.data_end() - c.b.len(),
            seq: record.seq,
            has_value: record.value.is_some(),
        });
        Ok(())
    }
    pub fn current(&self) -> Option<RecordRef<'_>> {
        let head = self.head.as_ref()?;
        Some(RecordRef {
            key: &self.block.bytes[head.key_start..head.key_end],
            seq: head.seq,
            value: head
                .has_value
                .then(|| &self.block.bytes[head.key_end..head.end]),
        })
    }
    pub fn advance(&mut self) -> Result<()> {
        let Some(head) = self.head.take() else {
            return Ok(());
        };
        if head.end != self.data_end() {
            self.decode_at(head.end)?;
        }
        Ok(())
    }
    #[cfg(test)]
    pub fn buffer_addresses(&self) -> (*const u8, *const u32) {
        self.block.buffer_addresses()
    }
    pub fn into_block(self) -> ReadBlock {
        self.block
    }
    #[cfg(test)]
    pub fn frame_capacity(&self) -> usize {
        self.block.frame_capacity()
    }
}

pub(crate) fn indexed_records(payload: &[u8]) -> Result<&[u8]> {
    let footer = payload
        .get(payload.len().saturating_sub(4)..)
        .filter(|footer| footer.len() == 4)
        .ok_or_else(|| corrupt("SST offset footer"))?;
    let count = u32::from_le_bytes(footer.try_into().unwrap()) as usize;
    if count == 0 || count > (payload.len() - 4) / 21 {
        return Err(corrupt("SST offset count"));
    }
    Ok(&payload[..payload.len() - 4 - count * 4])
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    fn indexed_frame(records: &[TestRecord<'_>]) -> Vec<u8> {
        let mut payload = Vec::new();
        let mut offsets = Vec::new();
        for &(key, seq, value) in records {
            offsets.push(payload.len() as u32);
            encode_record(
                key,
                &Record {
                    seq,
                    value: value.map(Bytes::copy_from_slice),
                },
                &mut payload,
            );
        }
        for &offset in &offsets {
            put_u32(&mut payload, offset);
        }
        put_u32(&mut payload, offsets.len() as u32);
        framed(&payload)
    }

    #[test]
    fn indexed_lookup_matches_ordered_model_at_every_search_boundary() {
        for include_empty in [false, true] {
            for count in [
                1usize, 2, 3, 4, 5, 7, 8, 9, 15, 16, 17, 31, 32, 33, 63, 64, 65, 127, 128, 129,
                255, 256, 257,
            ] {
                // Prefixes, embedded zeros and non-power-of-two directory lengths.
                let mut model = std::collections::BTreeMap::new();
                model.insert(
                    if include_empty {
                        Vec::new()
                    } else {
                        b"first".to_vec()
                    },
                    (1, Some(vec![])),
                );
                for n in 1..count {
                    let mut key = b"shared-prefix/".to_vec();
                    key.extend_from_slice(&(n as u32 * 2).to_be_bytes());
                    if n % 3 == 0 {
                        key.push(0);
                    }
                    model.insert(
                        key,
                        (n as u64 + 1, (n % 5 != 0).then(|| vec![n as u8; n % 31])),
                    );
                }
                let rows: Vec<_> = model
                    .iter()
                    .map(|(k, (seq, v))| (k.as_slice(), *seq, v.as_deref()))
                    .collect();
                let (block, _) = ReadBlock::decode_indexed(
                    indexed_frame(&rows),
                    rows[0].0,
                    None,
                    count as u64,
                    0,
                )
                .unwrap();
                let mut probes = vec![vec![0], vec![255], b"shared-prefix/".to_vec()];
                for key in model.keys() {
                    probes.push(key.clone());
                    let mut after = key.clone();
                    after.push(0);
                    probes.push(after);
                    if !key.is_empty() {
                        probes.push(key[..key.len() - 1].to_vec());
                    }
                }
                for n in 0..=count {
                    let mut key = b"shared-prefix/".to_vec();
                    key.extend_from_slice(&(n as u32 * 2 + 1).to_be_bytes());
                    probes.push(key);
                }
                for key in probes {
                    let actual = block.lookup(&key).unwrap();
                    let expected = model.get(&key).map(|(seq, v)| Record {
                        seq: *seq,
                        value: v.as_deref().map(Bytes::copy_from_slice),
                    });
                    assert_eq!(
                        actual, expected,
                        "count={count}, include_empty={include_empty}, key={key:?}"
                    );
                }
            }
        }
    }

    #[test]
    fn persisted_offsets_validate_once_and_revalidate_changed_frames() {
        let bytes = indexed_frame(&[
            (b"", 1, Some(b"")),
            (b"b", 2, None),
            (b"z", 3, Some(b"last")),
        ]);
        let (block, fingerprint) =
            ReadBlock::decode_indexed(bytes.clone(), b"", None, 3, 0).unwrap();
        assert_eq!(block.record_count(), 3);
        assert_eq!(block.lookup(b"z").unwrap().unwrap().value.unwrap(), "last");
        assert_eq!(block.lookup(b"b").unwrap().unwrap().value, None);
        assert_eq!(block.lookup(b"a").unwrap(), None);
        assert!(block.offsets.is_empty());
        let (again, cached) =
            ReadBlock::decode_indexed(bytes.clone(), b"", None, 3, fingerprint).unwrap();
        assert_eq!(cached, fingerprint);
        assert_eq!(again.lookup(b"").unwrap().unwrap().value.unwrap().len(), 0);
        // A fresh, valid CRC is not enough to bypass structural checks after a change.
        let mut payload = frame_payload(&bytes).unwrap().to_vec();
        payload[17..25].fill(0); // second record's sequence
        assert!(ReadBlock::decode_indexed(framed(&payload), b"", None, 3, fingerprint).is_err());
        for at in 0..bytes.len() {
            let mut changed = bytes.clone();
            changed[at] ^= 1;
            assert!(ReadBlock::decode_indexed(changed, b"", None, 3, fingerprint).is_err());
        }
    }

    #[test]
    fn persisted_directory_rejects_checksum_valid_bad_offsets_counts_and_order() {
        let original = indexed_frame(&[(b"a", 1, None), (b"b", 2, None)]);
        let payload = frame_payload(&original).unwrap();
        for case in 0..6 {
            let mut changed = payload.to_vec();
            match case {
                0 => changed[36..40].copy_from_slice(&1u32.to_le_bytes()),
                1 => changed[40..44].copy_from_slice(&u32::MAX.to_le_bytes()),
                2 => changed[44..48].copy_from_slice(&0u32.to_le_bytes()),
                3 => changed[44..48].copy_from_slice(&u32::MAX.to_le_bytes()),
                4 => changed[35] = b'a',
                _ => changed[18..26].copy_from_slice(&3u64.to_le_bytes()),
            }
            assert!(ReadBlock::decode_indexed(framed(&changed), b"a", None, 2, 0).is_err());
        }
        assert!(ReadBlock::decode_indexed(original, b"a", Some(b"b"), 2, 0).is_err());
    }

    type TestRecord<'a> = (&'a [u8], u64, Option<&'a [u8]>);

    #[test]
    fn sequential_cursor_reuses_heads_and_checks_record_boundaries() {
        let records: &[TestRecord<'_>] = &[
            (b"", 1, None),
            (b"a", 2, Some(b"")),
            (b"b", 3, Some(b"value")),
        ];
        for indexed in [false, true] {
            let make = || {
                if indexed {
                    ReadBlock::decode_indexed(indexed_frame(records), b"", None, 3, 0)
                        .unwrap()
                        .0
                } else {
                    ReadBlock::decode(frame(records), b"", None, 3).unwrap()
                }
            };
            for begin in 0..=records.len() {
                let mut cursor = BlockCursor::new(make(), begin).unwrap();
                for &(key, seq, value) in &records[begin..] {
                    for _ in 0..4 {
                        let head = cursor.current().unwrap();
                        assert_eq!((head.key, head.seq, head.value), (key, seq, value));
                    }
                    cursor.advance().unwrap();
                }
                assert!(cursor.current().is_none());
                cursor.advance().unwrap();
                assert!(cursor.current().is_none());
            }
            assert!(BlockCursor::new(make(), records.len() + 1).is_err());
            let mut cursor = BlockCursor::new(make(), 0).unwrap();
            let next = cursor.head.as_ref().unwrap().end;
            cursor.block.bytes[next + 8] = 2;
            assert!(cursor.advance().is_err());
            assert!(cursor.current().is_none());
        }
    }

    #[test]
    fn key_only_probes_match_records_and_reject_invalid_slices() {
        let records: &[TestRecord<'_>] = &[
            (b"", 1, None),
            (b"shared-prefix", 2, Some(b"value")),
            (b"shared-prefix\0", 3, Some(b"")),
            (b"shared-prefix-long", 4, None),
        ];
        for indexed in [false, true] {
            let mut block = if indexed {
                ReadBlock::decode_indexed(indexed_frame(records), b"", None, 4, 0)
                    .unwrap()
                    .0
            } else {
                ReadBlock::decode(frame(records), b"", None, 4).unwrap()
            };
            for (i, &(k, seq, v)) in records.iter().enumerate() {
                assert_eq!(block.key_at(i).unwrap(), k);
                let r = block.lookup(k).unwrap().unwrap();
                assert_eq!((r.seq, r.value.as_deref()), (seq, v));
            }
            assert!(block.key_at(records.len()).is_err());
            // Defensive slices must remain safe even if a previously validated
            // in-memory frame is inconsistent (no unchecked offset arithmetic).
            block.bytes[HEADER + 9..HEADER + 13].copy_from_slice(&u32::MAX.to_le_bytes());
            assert!(block.key_at(0).is_err());
            if indexed {
                block.bytes[block.index_start..block.index_start + 4]
                    .copy_from_slice(&u32::MAX.to_le_bytes());
                assert!(block.key_at(0).is_err());
            }
        }
    }

    fn frame(records: &[TestRecord<'_>]) -> Vec<u8> {
        let mut payload = Vec::new();
        for &(key, seq, value) in records {
            encode_record(
                key,
                &Record {
                    seq,
                    value: value.map(Bytes::copy_from_slice),
                },
                &mut payload,
            );
        }
        framed(&payload)
    }

    fn framed(payload: &[u8]) -> Vec<u8> {
        let mut bytes = Vec::new();
        write_frame(&mut bytes, payload).unwrap();
        bytes
    }

    #[test]
    fn lookup_handles_empty_binary_keys_values_tombstones_and_missing_keys() {
        let records: &[TestRecord<'_>] = &[
            (b"", 1, Some(b"")),
            (b"\0", 3, Some(b"binary\0value")),
            (b"a", 4, None),
            (b"aa", 2, Some(b"prefix")),
            (b"\xff", 5, Some(b"last")),
        ];
        let block = ReadBlock::decode(frame(records), b"", None, 5).unwrap();
        for &(key, seq, value) in records {
            let actual = block.get(key).unwrap();
            assert_eq!(actual.seq, seq);
            assert_eq!(actual.value.as_deref(), value);
        }
        for key in [&b"\0\0"[..], b"ab", b"\xff\0"] {
            assert!(block.get(key).is_none());
        }
        let value = block.get(b"aa").unwrap().value.unwrap();
        let address = value.as_ptr() as usize;
        let start = block.bytes.as_ptr() as usize;
        assert!(!(start..start + block.bytes.len()).contains(&address));
        drop(block);
        assert_eq!(value, "prefix");
    }

    #[test]
    fn every_frame_truncation_and_byte_flip_is_rejected() {
        let original = frame(&[(b"a", 1, Some(b"value")), (b"b", 2, None)]);
        for cut in 0..original.len() {
            assert!(ReadBlock::decode(original[..cut].to_vec(), b"a", None, 2).is_err());
        }
        for i in 0..original.len() {
            let mut bytes = original.clone();
            bytes[i] ^= 1;
            assert!(ReadBlock::decode(bytes, b"a", None, 2).is_err());
        }
        let mut extra = original;
        extra.push(0);
        assert!(ReadBlock::decode(extra, b"a", None, 2).is_err());
    }

    #[test]
    fn valid_checksums_do_not_hide_bad_headers_order_or_boundaries() {
        let original = frame(&[(b"a", 1, Some(b"one")), (b"b", 2, Some(b"two"))]);
        let payload = frame_payload(&original).unwrap();
        // Corrupt the second record: looking up the first must still reject it.
        let second = 17 + 1 + 3;
        for case in 0..8 {
            let mut changed = payload.to_vec();
            match case {
                0 => changed[second..second + 8].fill(0),
                1 => changed[second..second + 8].copy_from_slice(&3u64.to_le_bytes()),
                2 => changed[second + 8] = 2,
                3 => changed[second + 8] = 0,
                4 => changed[second + 9..second + 13].copy_from_slice(&u32::MAX.to_le_bytes()),
                5 => changed[second + 13..second + 17].copy_from_slice(&u32::MAX.to_le_bytes()),
                6 => changed[second + 17] = b'a',
                _ => changed.push(0),
            }
            assert!(ReadBlock::decode(framed(&changed), b"a", None, 2).is_err());
        }
        assert!(ReadBlock::decode(original.clone(), b"wrong", None, 2).is_err());
        assert!(ReadBlock::decode(original.clone(), b"a", Some(b"b"), 2).is_err());
        assert!(ReadBlock::decode(framed(b""), b"", None, 2).is_err());
        let reversed = frame(&[(b"b", 1, None), (b"a", 2, None)]);
        assert!(ReadBlock::decode(reversed, b"b", None, 2).is_err());
        for cut in 1..payload.len() {
            if cut != second {
                assert!(ReadBlock::decode(framed(&payload[..cut]), b"a", None, 2).is_err());
            }
        }
    }

    #[test]
    fn sparse_reused_blocks_release_large_offset_buffers() {
        let bytes = frame(&[(b"a", 1, Some(b"value"))]);
        let offsets = Vec::with_capacity(4096);
        let block = ReadBlock::decode_reusing(bytes, offsets, b"a", None, 1).unwrap();
        assert!(block.offsets.capacity() <= 8);
        assert_eq!(block.get(b"a").unwrap().value.unwrap(), "value");
    }

    #[test]
    fn checking_only_the_last_upper_bound_still_rejects_cross_block_keys() {
        let bytes = frame(&[(b"a", 1, None), (b"m", 2, None), (b"z", 3, None)]);
        for next in [b"b", b"m", b"z"] {
            assert!(ReadBlock::decode(bytes.clone(), b"a", Some(next), 3).is_err());
        }
        assert!(ReadBlock::decode(bytes, b"a", Some(b"zz"), 3).is_ok());
    }
}
