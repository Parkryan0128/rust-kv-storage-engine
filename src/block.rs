use crate::{codec::*, error::corrupt, memtable::Record, Result};

// The frame and offsets stay immutable after validation. Only the selected
// value is copied out, so a caller retaining a small value cannot pin a block.
pub(crate) struct ReadBlock {
    bytes: Vec<u8>,
    offsets: Vec<u32>,
    // Zero for legacy blocks; otherwise the persisted offset directory starts here.
    index_start: u32,
    // Zero for reconstructed legacy offsets, two or four for persisted offsets.
    offset_width: u32,
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
            offset_width: 0,
        })
    }

    pub fn decode_indexed(
        bytes: Vec<u8>,
        first: &[u8],
        next: Option<&[u8]>,
        max_seq: u64,
        validated: u64,
        compact: bool,
    ) -> Result<(Self, u64)> {
        let payload = frame_payload(&bytes)?;
        let (records, width) = indexed_layout(payload, compact)?;
        let index_start = (HEADER + records.len()) as u32;
        // The extra bit distinguishes an unvalidated block from a valid zero CRC.
        let fingerprint =
            (1u64 << 32) | u32::from_le_bytes(bytes[8..12].try_into().unwrap()) as u64;
        if fingerprint != validated {
            let mut cursor = Cursor { b: records };
            let mut previous: Option<&[u8]> = None;
            for offset in payload[records.len()..payload.len() - 4].chunks_exact(width) {
                let offset = if width == 2 {
                    u16::from_le_bytes(offset.try_into().unwrap()) as usize
                } else {
                    u32::from_le_bytes(offset.try_into().unwrap()) as usize
                };
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
                offset_width: width as u32,
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
        // Specialize once per lookup, avoiding width dispatch at each probe.
        match self.offset_width {
            2 => return self.lookup_indexed::<2>(key),
            4 => return self.lookup_indexed::<4>(key),
            _ => {}
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

    fn lookup_indexed<const WIDTH: usize>(&self, key: &[u8]) -> Result<Option<Record>> {
        let (mut left, mut right) = (
            0,
            (self.bytes.len() - self.index_start as usize - 4) / WIDTH,
        );
        while left < right {
            let middle = left + (right - left) / 2;
            let record = self.record_indexed::<WIDTH>(middle)?;
            match record.key.cmp(key) {
                std::cmp::Ordering::Less => left = middle + 1,
                std::cmp::Ordering::Greater => right = middle,
                std::cmp::Ordering::Equal => return Ok(Some(record.to_owned())),
            }
        }
        Ok(None)
    }

    pub fn record_count(&self) -> usize {
        match self.offset_width {
            2 => (self.bytes.len() - self.index_start as usize - 4) / 2,
            4 => (self.bytes.len() - self.index_start as usize - 4) / 4,
            _ => self.offsets.len(),
        }
    }

    pub fn record_at(&self, index: usize) -> Result<RecordRef<'_>> {
        if index >= self.record_count() {
            return Err(corrupt("SST record index"));
        }
        match self.offset_width {
            2 => self.record_indexed::<2>(index),
            4 => self.record_indexed::<4>(index),
            _ => decode_record_ref(&mut Cursor {
                b: &self.bytes[self.offsets[index] as usize..],
            }),
        }
    }

    #[inline]
    fn record_indexed<const WIDTH: usize>(&self, index: usize) -> Result<RecordRef<'_>> {
        let end = self.index_start as usize;
        let start = end + index * WIDTH;
        let offset = HEADER
            + if WIDTH == 2 {
                u16::from_le_bytes(self.bytes[start..start + 2].try_into().unwrap()) as usize
            } else {
                u32::from_le_bytes(self.bytes[start..start + 4].try_into().unwrap()) as usize
            };
        let mut cursor = Cursor {
            b: self
                .bytes
                .get(offset..end)
                .ok_or_else(|| corrupt("SST record offset"))?,
        };
        decode_record_ref(&mut cursor)
    }

    pub fn allocated_bytes(&self) -> usize {
        self.bytes.capacity()
            + self.offsets.capacity() * std::mem::size_of::<u32>()
            + std::mem::size_of::<Self>()
    }
}

pub(crate) fn indexed_records(payload: &[u8], compact: bool) -> Result<&[u8]> {
    Ok(indexed_layout(payload, compact)?.0)
}

fn indexed_layout(payload: &[u8], compact: bool) -> Result<(&[u8], usize)> {
    let footer = payload
        .get(payload.len().saturating_sub(4)..)
        .filter(|footer| footer.len() == 4)
        .ok_or_else(|| corrupt("SST offset footer"))?;
    let trailer = u32::from_le_bytes(footer.try_into().unwrap());
    let width = if compact && trailer & (1 << 31) == 0 {
        2
    } else {
        4
    };
    let count = if compact {
        trailer & !(1 << 31)
    } else {
        trailer
    } as usize;
    if count == 0 || count > (payload.len() - 4) / (17 + width) {
        return Err(corrupt("SST offset count"));
    }
    Ok((&payload[..payload.len() - 4 - count * width], width))
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    fn indexed_frame(records: &[TestRecord<'_>]) -> Vec<u8> {
        indexed_frame_with_width(records, false)
    }

    fn indexed_frame_with_width(records: &[TestRecord<'_>], compact: bool) -> Vec<u8> {
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
        let narrow = compact
            && offsets
                .last()
                .is_none_or(|&offset| offset <= u16::MAX as u32);
        for &offset in &offsets {
            if narrow {
                payload.extend((offset as u16).to_le_bytes());
            } else {
                put_u32(&mut payload, offset);
            }
        }
        let wide_flag = if compact && !narrow { 1 << 31 } else { 0 };
        put_u32(&mut payload, offsets.len() as u32 | wide_flag);
        framed(&payload)
    }

    #[test]
    fn compact_offsets_preserve_lookup_validation_and_resident_metadata_size() {
        // Packing the start and width into two u32s keeps ReadBlock's old size.
        assert_eq!(
            std::mem::size_of::<ReadBlock>(),
            2 * std::mem::size_of::<Vec<u8>>() + 8
        );
        let records: &[TestRecord<'_>] = &[
            (b"", 1, Some(b"")),
            (b"\0", 2, None),
            (b"z", 3, Some(b"last")),
        ];
        let bytes = indexed_frame_with_width(records, true);
        assert_eq!(
            indexed_frame(records).len() - bytes.len(),
            records.len() * 2
        );
        let (block, fingerprint) =
            ReadBlock::decode_indexed(bytes.clone(), b"", None, 3, 0, true).unwrap();
        assert_eq!(block.offset_width, 2);
        assert!(block.record_at(3).is_err());
        let (again, cached) =
            ReadBlock::decode_indexed(bytes.clone(), b"", None, 3, fingerprint, true).unwrap();
        assert_eq!(cached, fingerprint);
        for &(key, seq, value) in records {
            let actual = again.lookup(key).unwrap().unwrap();
            assert_eq!(actual.seq, seq);
            assert_eq!(actual.value.as_deref(), value);
        }
        assert!(again.lookup(b"a").unwrap().is_none());
        for cut in 0..bytes.len() {
            assert!(
                ReadBlock::decode_indexed(bytes[..cut].to_vec(), b"", None, 3, 0, true).is_err()
            );
        }
        for at in 0..bytes.len() {
            let mut changed = bytes.clone();
            changed[at] ^= 1;
            assert!(ReadBlock::decode_indexed(changed, b"", None, 3, fingerprint, true).is_err());
        }
        let payload = frame_payload(&bytes).unwrap();
        let directory = indexed_records(payload, true).unwrap().len();
        for case in 0..7 {
            let mut changed = payload.to_vec();
            let tail = changed.len() - 4;
            match case {
                0 => changed[directory..directory + 2].copy_from_slice(&1u16.to_le_bytes()),
                1 => changed[directory + 2..directory + 4].copy_from_slice(&u16::MAX.to_le_bytes()),
                2 => changed[tail..].copy_from_slice(&0u32.to_le_bytes()),
                3 => changed[tail..].copy_from_slice(&u32::MAX.to_le_bytes()),
                4 => changed[tail + 3] |= 0x80, // wrong directory width with a valid CRC
                5 => changed[17..25].fill(0),   // second sequence
                _ => changed[52] = 0,           // last key now duplicates the second
            }
            assert!(
                ReadBlock::decode_indexed(framed(&changed), b"", None, 3, fingerprint, true)
                    .is_err(),
                "case {case}"
            );
        }
        assert!(ReadBlock::decode_indexed(bytes, b"", Some(b"z"), 3, 0, true).is_err());
    }

    #[test]
    fn compact_offsets_switch_at_the_last_record_start_not_frame_length() {
        for (second_offset, width) in [(65535usize, 2), (65536, 4)] {
            let large = vec![42; second_offset - 18];
            let records: &[TestRecord<'_>] = &[(b"a", 1, Some(&large)), (b"b", 2, Some(b"last"))];
            let bytes = indexed_frame_with_width(records, true);
            let (block, fingerprint) =
                ReadBlock::decode_indexed(bytes.clone(), b"a", None, 2, 0, true).unwrap();
            assert_eq!(block.offset_width, width);
            assert_eq!(block.record_at(1).unwrap().key, b"b");
            assert_eq!(
                block.lookup(b"a").unwrap().unwrap().value.unwrap().as_ref(),
                large
            );
            let (again, _) =
                ReadBlock::decode_indexed(bytes, b"a", None, 2, fingerprint, true).unwrap();
            assert_eq!(again.lookup(b"b").unwrap().unwrap().value.unwrap(), "last");
        }
        let large = vec![42; 100_000];
        let bytes = indexed_frame_with_width(&[(b"a", 1, Some(&large))], true);
        let (block, _) = ReadBlock::decode_indexed(bytes, b"a", None, 1, 0, true).unwrap();
        assert_eq!(block.offset_width, 2);
        assert_eq!(
            block.lookup(b"a").unwrap().unwrap().value.unwrap().as_ref(),
            large
        );
    }

    #[test]
    fn persisted_offsets_validate_once_and_revalidate_changed_frames() {
        let bytes = indexed_frame(&[
            (b"", 1, Some(b"")),
            (b"b", 2, None),
            (b"z", 3, Some(b"last")),
        ]);
        let (block, fingerprint) =
            ReadBlock::decode_indexed(bytes.clone(), b"", None, 3, 0, false).unwrap();
        assert_eq!(block.record_count(), 3);
        assert_eq!(block.lookup(b"z").unwrap().unwrap().value.unwrap(), "last");
        assert_eq!(block.lookup(b"b").unwrap().unwrap().value, None);
        assert_eq!(block.lookup(b"a").unwrap(), None);
        assert!(block.offsets.is_empty());
        let (again, cached) =
            ReadBlock::decode_indexed(bytes.clone(), b"", None, 3, fingerprint, false).unwrap();
        assert_eq!(cached, fingerprint);
        assert_eq!(again.lookup(b"").unwrap().unwrap().value.unwrap().len(), 0);
        // A fresh, valid CRC is not enough to bypass structural checks after a change.
        let mut payload = frame_payload(&bytes).unwrap().to_vec();
        payload[17..25].fill(0); // second record's sequence
        assert!(
            ReadBlock::decode_indexed(framed(&payload), b"", None, 3, fingerprint, false).is_err()
        );
        for at in 0..bytes.len() {
            let mut changed = bytes.clone();
            changed[at] ^= 1;
            assert!(ReadBlock::decode_indexed(changed, b"", None, 3, fingerprint, false).is_err());
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
            assert!(ReadBlock::decode_indexed(framed(&changed), b"a", None, 2, 0, false).is_err());
        }
        assert!(ReadBlock::decode_indexed(original, b"a", Some(b"b"), 2, 0, false).is_err());
    }

    type TestRecord<'a> = (&'a [u8], u64, Option<&'a [u8]>);

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
