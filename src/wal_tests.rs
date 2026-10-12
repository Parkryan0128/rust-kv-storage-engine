use super::*;
use crate::{batch::BATCH_HEADER, EngineError};
use bytes::Bytes;

fn single(key: &[u8], seq: u64) -> Record {
    Record {
        seq,
        value: Some(Bytes::copy_from_slice(key)),
    }
}

fn payload() -> Vec<u8> {
    let mut out = Vec::new();
    put_u64(&mut out, 0);
    put_u32(&mut out, BATCH_VERSION);
    put_u32(&mut out, 2);
    encode_record(b"a", &single(b"v", 2), &mut out);
    encode_record(
        b"b",
        &Record {
            seq: 3,
            value: None,
        },
        &mut out,
    );
    out
}

#[test]
fn malformed_batch_payloads_never_apply_a_prefix() {
    let good = payload();
    let second = BATCH_HEADER + 19;
    let mut bad = Vec::new();
    for len in 0..good.len() {
        bad.push(good[..len].to_vec());
    }
    for (offset, value) in [(8, 2u32), (12, 0), (12, 1), (12, 3), (12, u32::MAX)] {
        let mut bytes = good.clone();
        bytes[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
        bad.push(bytes);
    }
    for (offset, seq) in [
        (BATCH_HEADER, 0u64),
        (BATCH_HEADER, 1),
        (second, 2),
        (second, 4),
    ] {
        let mut bytes = good.clone();
        bytes[offset..offset + 8].copy_from_slice(&seq.to_le_bytes());
        bad.push(bytes);
    }
    let mut bytes = good.clone();
    bytes[second + 8] = 2;
    bad.push(bytes);
    let mut bytes = good.clone();
    bytes.push(0);
    bad.push(bytes);
    for bytes in bad {
        let mut table = MemTable::default();
        table.insert(b"base".to_vec(), single(b"old", 1));
        let mut seq = 1;
        assert!(matches!(
            replay(&bytes, &mut table, &mut seq),
            Err(EngineError::Corruption(_))
        ));
        assert_eq!(seq, 1);
        assert_eq!(table.data.len(), 1);
        assert_eq!(
            table.get(b"base").unwrap().value.as_deref(),
            Some(&b"old"[..])
        );
    }
    let mut table = MemTable::default();
    let mut seq = 1;
    replay(&good, &mut table, &mut seq).unwrap();
    assert_eq!(seq, 3);
    assert_eq!(table.get(b"a").unwrap().value.as_deref(), Some(&b"v"[..]));
    assert!(table.get(b"b").unwrap().value.is_none());
}

#[test]
fn every_batch_frame_cut_recovers_all_or_none_and_repairs_the_tail() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("1.wal");
    let mut wal = Wal::create(&path, 1).unwrap();
    wal.append(b"base", &single(b"old", 1)).unwrap();
    let prefix = wal.bytes as usize;
    let mut batch = WriteBatch::new();
    batch.put(b"a", b"new").delete(b"base").put(b"b", b"new");
    wal.append_batch(&batch, 2, batch.encoded_len().unwrap())
        .unwrap();
    drop(wal);
    let original = std::fs::read(&path).unwrap();
    // All batch operations occupy exactly one frame, following the legacy frame.
    let mut frames = &original[8..];
    assert!(read_frame(&mut frames, false).unwrap().is_some());
    assert_eq!(
        read_frame(&mut frames, false).unwrap().unwrap()[..8],
        [0; 8]
    );
    assert!(read_frame(&mut frames, false).unwrap().is_none());
    for cut in prefix..=original.len() {
        std::fs::write(&path, &original[..cut]).unwrap();
        let mut seq = 0;
        let (mut wal, table) = Wal::recover(&path, 1, true, &mut seq).unwrap();
        let complete = cut == original.len();
        assert_eq!(seq, if complete { 4 } else { 1 }, "cut {cut}");
        assert_eq!(table.get(b"a").is_some(), complete, "cut {cut}");
        assert_eq!(table.get(b"b").is_some(), complete, "cut {cut}");
        assert_eq!(table.get(b"base").unwrap().value.is_none(), complete);
        assert_eq!(
            wal.bytes as usize,
            if complete { original.len() } else { prefix }
        );
        wal.append(b"after", &single(b"ok", seq + 1)).unwrap();
        drop(wal);
        let (_, restored) = Wal::recover(&path, 1, true, &mut 0).unwrap();
        assert_eq!(
            restored.get(b"after").unwrap().value.as_deref(),
            Some(&b"ok"[..])
        );

        if cut > prefix && !complete {
            std::fs::write(&path, &original[..cut]).unwrap();
            assert!(matches!(
                Wal::recover(&path, 1, false, &mut 0),
                Err(EngineError::Corruption(_))
            ));
        }
    }
    for at in prefix..original.len() {
        let mut broken = original.clone();
        broken[at] ^= 0x80;
        std::fs::write(&path, broken).unwrap();
        assert!(
            matches!(
                Wal::recover(&path, 1, true, &mut 0),
                Err(EngineError::Corruption(_))
            ),
            "byte {at}"
        );
    }
}

#[test]
fn sequence_limit_and_legacy_rejection_are_explicit() {
    let mut bytes = payload();
    bytes[BATCH_HEADER..BATCH_HEADER + 8].copy_from_slice(&(u64::MAX - 1).to_le_bytes());
    let second = BATCH_HEADER + 19;
    bytes[second..second + 8].copy_from_slice(&u64::MAX.to_le_bytes());
    let mut table = MemTable::default();
    let mut seq = u64::MAX - 2;
    replay(&bytes, &mut table, &mut seq).unwrap();
    assert_eq!(seq, u64::MAX);
    assert!(decode_record(&mut Cursor { b: &bytes }).is_err());
    assert!(replay(&bytes, &mut table, &mut seq).is_err());
}
