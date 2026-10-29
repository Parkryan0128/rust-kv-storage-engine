use super::*;

const PREFIX_FLAG: u32 = 1 << 31;

// Optional block-local prefix sharing. Ordinary and single-record blocks keep
// their existing bytes. Restrict it to a material saving, not tiny key prefixes.
pub(crate) fn encode_prefix_frame(
    full: &[u8],
    first: &[u8],
    last: &[u8],
    offsets: &mut [u32],
    encoded: &mut Vec<u8>,
) -> Result<bool> {
    if first.len() < 32 || offsets.len() < 2 {
        return Ok(false);
    }
    let prefix_len = first.iter().zip(last).take_while(|(a, b)| a == b).count();
    let saving = (offsets.len() - 1)
        .saturating_mul(prefix_len)
        .saturating_sub(8);
    if prefix_len < 16 || saving < (full.len() - HEADER) / 8 {
        return Ok(false);
    }
    let records = indexed_records(&full[HEADER..])?;
    encoded.clear();
    encoded.resize(HEADER, 0);
    put_u32(encoded, (full.len() - HEADER) as u32);
    put_u32(encoded, prefix_len as u32);
    encoded.extend_from_slice(&first[..prefix_len]);
    let start = encoded.len();
    // push() already validated these records. Copy their encoded headers and
    // contiguous suffix/value bodies instead of decoding and encoding again.
    for row in 0..offsets.len() {
        let begin = offsets[row] as usize;
        let end = offsets.get(row + 1).map_or(records.len(), |n| *n as usize);
        let record = records
            .get(begin..end)
            .ok_or_else(|| corrupt("SST writer offset"))?;
        let header = record
            .get(..17)
            .ok_or_else(|| corrupt("SST writer record"))?;
        let key_len = u32::from_le_bytes(header[9..13].try_into().unwrap()) as usize;
        let suffix_len = key_len
            .checked_sub(prefix_len)
            .ok_or_else(|| corrupt("SST writer prefix"))?;
        let body = record
            .get(17 + prefix_len..)
            .ok_or_else(|| corrupt("SST writer prefix body"))?;
        offsets[row] = (encoded.len() - start) as u32;
        encoded.extend_from_slice(&header[..9]);
        put_u32(encoded, suffix_len as u32);
        encoded.extend_from_slice(&header[13..]);
        encoded.extend_from_slice(body);
    }
    for offset in offsets.iter() {
        put_u32(encoded, *offset);
    }
    put_u32(encoded, offsets.len() as u32 | PREFIX_FLAG);
    let header = frame_header(&encoded[HEADER..])?;
    encoded[..HEADER].copy_from_slice(&header);
    debug_assert_eq!(full.len() - encoded.len(), saving);
    Ok(true)
}

// Independent test oracle: reconstruct ordinary records from prefix frames.
// Production readers keep these frames encoded and reconstruct cursor keys only.
// Validate counts before allocation and bound expansion by the table summary.
#[cfg(test)]
fn expand_prefix_frame(bytes: Vec<u8>, maximum: usize) -> Result<Vec<u8>> {
    if bytes.len() > maximum {
        return Err(corrupt("SST expanded block limit"));
    }
    let Some(footer) = bytes
        .get(bytes.len().saturating_sub(4)..)
        .filter(|f| f.len() == 4)
    else {
        return Err(corrupt("SST prefix footer"));
    };
    let tagged_count = u32::from_le_bytes(footer.try_into().unwrap());
    if tagged_count & PREFIX_FLAG == 0 {
        return Ok(bytes);
    }
    let payload = frame_payload(&bytes)?;
    let count = (tagged_count & !PREFIX_FLAG) as usize;
    let mut cursor = Cursor { b: payload };
    let decoded_len = cursor.u32()? as usize;
    let prefix_len = cursor.u32()? as usize;
    if count < 2
        || count > payload.len().saturating_sub(12) / 21
        || prefix_len == 0
        || prefix_len > MAX_RECORD - 17
        || decoded_len > MAX_FRAME
        || decoded_len + HEADER > maximum
    {
        return Err(corrupt("SST prefix header"));
    }
    let prefix = cursor.take(prefix_len)?;
    let expected = payload.len().checked_sub(8 + prefix_len).and_then(|n| {
        count
            .checked_mul(prefix_len)
            .and_then(|added| n.checked_add(added))
    });
    if expected != Some(decoded_len) {
        return Err(corrupt("SST prefix expansion length"));
    }
    let directory_len = count * 4 + 4;
    let record_len = cursor
        .b
        .len()
        .checked_sub(directory_len)
        .ok_or_else(|| corrupt("SST prefix directory"))?;
    let (records, directory) = cursor.b.split_at(record_len);
    let mut cursor = Cursor { b: records };
    let mut output = Vec::with_capacity(decoded_len + HEADER);
    output.resize(HEADER, 0);
    for (row, offset) in directory[..directory.len() - 4].chunks_exact(4).enumerate() {
        let offset = u32::from_le_bytes(offset.try_into().unwrap()) as usize;
        if offset != record_len - cursor.b.len() {
            return Err(corrupt("SST prefix offset"));
        }
        let record = decode_record_ref(&mut cursor)?;
        let key_len = prefix_len + record.key.len();
        if key_len
            .saturating_add(record.value.map_or(0, <[u8]>::len))
            .saturating_add(17)
            > MAX_RECORD
        {
            return Err(corrupt("SST prefix record limit"));
        }
        // The full-key record differs only in its key length and shared prefix.
        put_u64(&mut output, record.seq);
        output.push(u8::from(record.value.is_some()));
        put_u32(&mut output, key_len as u32);
        put_u32(&mut output, record.value.map_or(0, <[u8]>::len) as u32);
        output.extend_from_slice(prefix);
        output.extend_from_slice(record.key);
        if let Some(value) = record.value {
            output.extend_from_slice(value);
        }
        // Bounds above imply this before any large allocation can occur.
        if output.len() > decoded_len + HEADER - directory_len {
            return Err(corrupt("SST prefix expanded records"));
        }
        debug_assert!(row < count);
    }
    cursor.done()?;
    for (row, offset) in directory[..directory.len() - 4].chunks_exact(4).enumerate() {
        let offset = u32::from_le_bytes(offset.try_into().unwrap()) as usize;
        put_u32(&mut output, (offset + row * prefix_len) as u32);
    }
    put_u32(&mut output, count as u32);
    if output.len() != decoded_len + HEADER {
        return Err(corrupt("SST prefix decoded length"));
    }
    let header = frame_header(&output[HEADER..])?;
    output[..HEADER].copy_from_slice(&header);
    Ok(output)
}

pub(super) fn compare_key(prefix: &[u8], suffix: &[u8], key: &[u8]) -> std::cmp::Ordering {
    let shared = prefix.len().min(key.len());
    match prefix[..shared].cmp(&key[..shared]) {
        std::cmp::Ordering::Equal if key.len() < prefix.len() => std::cmp::Ordering::Greater,
        std::cmp::Ordering::Equal => suffix.cmp(&key[shared..]),
        order => order,
    }
}

impl ReadBlock {
    // Keep prefix blocks encoded in the cache. Get compares only the suffix
    // after checking the shared prefix once; cursors reconstruct one head key.
    pub(crate) fn decode_prefix_indexed(
        bytes: Vec<u8>,
        first: &[u8],
        next: Option<&[u8]>,
        max_seq: u64,
        validated: u64,
        maximum: usize,
    ) -> Result<(Self, u64)> {
        if bytes.len() > maximum {
            return Err(corrupt("SST expanded block limit"));
        }
        let footer = bytes
            .get(bytes.len().saturating_sub(4)..)
            .filter(|f| f.len() == 4)
            .ok_or_else(|| corrupt("SST prefix footer"))?;
        let tagged_count = u32::from_le_bytes(footer.try_into().unwrap());
        if tagged_count & PREFIX_FLAG == 0 {
            return Self::decode_indexed(bytes, first, next, max_seq, validated);
        }
        let payload = frame_payload(&bytes)?;
        let count = (tagged_count & !PREFIX_FLAG) as usize;
        let mut cursor = Cursor { b: payload };
        let decoded_len = cursor.u32()? as usize;
        let prefix_len = cursor.u32()? as usize;
        if count < 2
            || count > payload.len().saturating_sub(12) / 21
            || prefix_len == 0
            || prefix_len > MAX_RECORD - 17
            || decoded_len > MAX_FRAME
            || decoded_len + HEADER > maximum
        {
            return Err(corrupt("SST prefix header"));
        }
        let prefix = cursor.take(prefix_len)?;
        let expected = payload.len().checked_sub(8 + prefix_len).and_then(|n| {
            count
                .checked_mul(prefix_len)
                .and_then(|added| n.checked_add(added))
        });
        if expected != Some(decoded_len) {
            return Err(corrupt("SST prefix expansion length"));
        }
        let directory_len = count * 4 + 4;
        let record_len = cursor
            .b
            .len()
            .checked_sub(directory_len)
            .ok_or_else(|| corrupt("SST prefix directory"))?;
        let (records, directory) = cursor.b.split_at(record_len);
        let fingerprint =
            (1u64 << 32) | u32::from_le_bytes(bytes[8..12].try_into().unwrap()) as u64;
        if fingerprint != validated {
            let mut cursor = Cursor { b: records };
            let mut previous: Option<&[u8]> = None;
            for offset in directory[..directory.len() - 4].chunks_exact(4) {
                if u32::from_le_bytes(offset.try_into().unwrap()) as usize
                    != record_len - cursor.b.len()
                {
                    return Err(corrupt("SST prefix offset"));
                }
                let record = decode_record_ref(&mut cursor)?;
                if prefix_len + 17 + record.key.len() + record.value.map_or(0, <[u8]>::len)
                    > MAX_RECORD
                    || record.seq > max_seq
                    || previous.is_some_and(|key| key >= record.key)
                {
                    return Err(corrupt("SST prefix record order/sequence/length"));
                }
                if previous.is_none() && !compare_key(prefix, record.key, first).is_eq() {
                    return Err(corrupt("SST first key mismatch"));
                }
                previous = Some(record.key);
            }
            cursor.done()?;
            let last = previous.ok_or_else(|| corrupt("empty SST prefix block"))?;
            if next.is_some_and(|key| !compare_key(prefix, last, key).is_lt()) {
                return Err(corrupt("SST record order/sequence"));
            }
        }
        let record_start = HEADER + 8 + prefix_len;
        let index_start = bytes.len() - directory_len;
        Ok((
            Self {
                bytes,
                offsets: Vec::new(),
                index_start,
                record_start,
            },
            fingerprint,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture() -> (Vec<u8>, Vec<u8>, Vec<u8>, Vec<u32>) {
        let mut bytes = vec![0; HEADER];
        let mut offsets = Vec::new();
        let mut first = Vec::new();
        let mut last = Vec::new();
        for n in 0..73u64 {
            let mut key = vec![b'x'; 128];
            key.extend_from_slice(&n.to_be_bytes());
            key.resize(key.len() + n as usize % 7, 0);
            if n == 0 {
                first = key.clone();
            }
            last = key.clone();
            offsets.push((bytes.len() - HEADER) as u32);
            encode_record_parts(
                &key,
                n + 1,
                (n % 5 != 0).then_some(&[42; 17][..]),
                &mut bytes,
            );
        }
        for offset in &offsets {
            put_u32(&mut bytes, *offset);
        }
        put_u32(&mut bytes, offsets.len() as u32);
        let header = frame_header(&bytes[HEADER..]).unwrap();
        bytes[..HEADER].copy_from_slice(&header);
        (bytes, first, last, offsets)
    }

    #[test]
    fn cursor_key_capacity_stays_inside_the_expanded_frame_charge() {
        let mut first = vec![b'x'; 128];
        first.push(0);
        let mut last = vec![b'x'; 128];
        last.extend_from_slice(&[1; 129]);
        let mut full = vec![0; HEADER];
        let mut offsets = vec![0];
        encode_record_parts(&first, 1, None, &mut full);
        offsets.push((full.len() - HEADER) as u32);
        encode_record_parts(&last, 2, None, &mut full);
        for offset in &offsets {
            put_u32(&mut full, *offset);
        }
        put_u32(&mut full, 2);
        let mut encoded = Vec::new();
        assert!(encode_prefix_frame(&full, &first, &last, &mut offsets, &mut encoded).unwrap());
        let (block, _) =
            ReadBlock::decode_prefix_indexed(encoded, &first, None, 2, 0, full.len()).unwrap();
        let mut cursor = BlockCursor::new(block, 0).unwrap();
        assert_eq!(cursor.current().unwrap().key, first);
        cursor.advance().unwrap();
        assert_eq!(cursor.current().unwrap().key, last);
        assert!(cursor.prefix_key.capacity() <= full.len() - HEADER);
    }

    #[test]
    fn native_prefix_lookup_cursor_and_cached_validation_match_full_records() {
        let (bytes, first, last, mut offsets) = fixture();
        let mut encoded = Vec::new();
        encode_prefix_frame(&bytes, &first, &last, &mut offsets, &mut encoded).unwrap();
        let (plain, _) = ReadBlock::decode_indexed(bytes.clone(), &first, None, 73, 0).unwrap();
        let (native, fingerprint) =
            ReadBlock::decode_prefix_indexed(encoded.clone(), &first, None, 73, 0, bytes.len())
                .unwrap();
        assert!(native.allocated_bytes() < plain.allocated_bytes());
        let (warm, _) = ReadBlock::decode_prefix_indexed(
            encoded.clone(),
            &first,
            None,
            73,
            fingerprint,
            bytes.len(),
        )
        .unwrap();
        let mut probes = vec![vec![], vec![b'x'; 128], vec![b'w'; 140], vec![b'y'; 140]];
        for n in 0..plain.record_count() {
            let key = plain.key_at(n).unwrap();
            assert_eq!(native.full_key_at(n).unwrap(), key);
            probes.push(key.to_vec());
            let mut after = key.to_vec();
            after.push(0);
            probes.push(after);
            probes.push(key[..key.len() - 1].to_vec());
        }
        for key in probes {
            assert_eq!(native.lookup(&key).unwrap(), plain.lookup(&key).unwrap());
            assert_eq!(warm.lookup(&key).unwrap(), plain.lookup(&key).unwrap());
            for n in [0, 1, 35, 72] {
                assert_eq!(
                    native.compare_key_at(n, &key).unwrap(),
                    plain.key_at(n).unwrap().cmp(&key)
                );
            }
        }
        let mut native_hashes = Vec::new();
        let mut plain_hashes = Vec::new();
        native
            .visit_key_hashes(&mut crate::bloom::PrefixHasher::default(), |h| {
                native_hashes.push(h)
            })
            .unwrap();
        plain
            .visit_key_hashes(&mut crate::bloom::PrefixHasher::default(), |h| {
                plain_hashes.push(h)
            })
            .unwrap();
        assert_eq!(native_hashes, plain_hashes);
        for (row, hash) in native_hashes.into_iter().enumerate() {
            assert_eq!(hash, crate::bloom::hash(plain.key_at(row).unwrap()));
        }
        let mut cursor = BlockCursor::new(native, 3).unwrap();
        for n in 3..plain.record_count() {
            let row = cursor.current().unwrap();
            assert_eq!(row.key, plain.key_at(n).unwrap());
            assert_eq!(row.to_owned(), plain.record_at(n).unwrap().to_owned());
            cursor.advance().unwrap();
        }
        assert!(cursor.current().is_none());
        for index in 0..encoded.len() {
            let mut damaged = encoded.clone();
            damaged[index] ^= 1;
            assert!(ReadBlock::decode_prefix_indexed(
                damaged,
                &first,
                None,
                73,
                fingerprint,
                bytes.len()
            )
            .is_err());
        }
    }

    #[test]
    fn prefix_frame_roundtrips_exact_bytes_and_all_record_kinds() {
        let (bytes, first, last, mut offsets) = fixture();
        let mut encoded = Vec::new();
        assert!(encode_prefix_frame(&bytes, &first, &last, &mut offsets, &mut encoded).unwrap());
        assert!(encoded.len() * 3 < bytes.len());
        assert_eq!(
            expand_prefix_frame(encoded.clone(), bytes.len()).unwrap(),
            bytes
        );
        assert!(expand_prefix_frame(encoded.clone(), bytes.len() - 1).is_err());
        let decoded = expand_prefix_frame(encoded, bytes.len()).unwrap();
        let (block, _) = ReadBlock::decode_indexed(decoded, &first, None, 73, 0).unwrap();
        assert_eq!(block.record_count(), 73);
        assert_eq!(block.key_at(72).unwrap(), last);
        assert_eq!(
            expand_prefix_frame(bytes.clone(), bytes.len()).unwrap(),
            bytes
        );
    }

    #[test]
    fn invalid_prefix_headers_counts_offsets_and_crc_fail_closed() {
        let (bytes, first, last, mut offsets) = fixture();
        let mut encoded = Vec::new();
        encode_prefix_frame(&bytes, &first, &last, &mut offsets, &mut encoded).unwrap();
        for index in 0..encoded.len() {
            let mut damaged = encoded.clone();
            damaged[index] ^= 1;
            assert!(
                expand_prefix_frame(damaged, bytes.len())
                    .and_then(|b| ReadBlock::decode_indexed(b, &first, None, 73, 0))
                    .is_err(),
                "offset {index}"
            );
        }
        for (at, value) in [
            (HEADER, u32::MAX),
            (HEADER + 4, 0),
            (HEADER + 4, u32::MAX),
            (encoded.len() - 4, PREFIX_FLAG),
            (encoded.len() - 4, u32::MAX),
            (encoded.len() - 8, u32::MAX),
        ] {
            let mut damaged = encoded.clone();
            damaged[at..at + 4].copy_from_slice(&value.to_le_bytes());
            let header = frame_header(&damaged[HEADER..]).unwrap();
            damaged[..HEADER].copy_from_slice(&header);
            assert!(expand_prefix_frame(damaged, bytes.len()).is_err());
        }
    }
}
