use crate::{codec::*, error::corrupt, memtable::Record, Result};

// The frame and offsets stay immutable after validation. Only the selected
// value is copied out, so a caller retaining a small value cannot pin a block.
pub(crate) struct ReadBlock {
    bytes: Vec<u8>,
    offsets: Vec<u32>,
}

impl ReadBlock {
    pub fn decode(
        bytes: Vec<u8>,
        first: &[u8],
        next: Option<&[u8]>,
        max_seq: u64,
    ) -> Result<Self> {
        let payload = frame_payload(&bytes)?;
        let mut c = Cursor { b: payload };
        let mut previous: Option<&[u8]> = None;
        let mut offsets = Vec::new();
        while !c.b.is_empty() {
            let offset = bytes.len() - c.b.len();
            let r = decode_record_ref(&mut c)?;
            if previous.is_some_and(|p| p >= r.key)
                || r.seq > max_seq
                || next.is_some_and(|key| r.key >= key)
            {
                return Err(corrupt("SST record order/sequence"));
            }
            if previous.is_none() && r.key != first {
                return Err(corrupt("SST first key mismatch"));
            }
            // Frames are limited to MAX_FRAME + HEADER, well below u32::MAX.
            offsets.push(offset as u32);
            previous = Some(r.key);
        }
        if offsets.is_empty() {
            return Err(corrupt("SST first key mismatch"));
        }
        Ok(Self { bytes, offsets })
    }

    fn key(&self, offset: u32) -> &[u8] {
        let offset = offset as usize;
        // These lengths and boundaries were checked before publishing the block.
        let len = u32::from_le_bytes(self.bytes[offset + 9..offset + 13].try_into().unwrap());
        &self.bytes[offset + 17..offset + 17 + len as usize]
    }

    pub fn get(&self, key: &[u8]) -> Option<Record> {
        let index = self
            .offsets
            .binary_search_by(|&offset| self.key(offset).cmp(key))
            .ok()?;
        let mut c = Cursor {
            b: &self.bytes[self.offsets[index] as usize..],
        };
        Some(
            decode_record_ref(&mut c)
                .expect("immutable validated record")
                .to_owned(),
        )
    }

    pub fn allocated_bytes(&self) -> usize {
        self.bytes.capacity()
            + self.offsets.capacity() * std::mem::size_of::<u32>()
            + std::mem::size_of::<Self>()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

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
}
