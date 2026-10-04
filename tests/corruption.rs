mod common;
use common::*;
use rust_kv_storage_engine::{Engine, EngineError, Options};
use std::fs;
fn copy_db(from: &std::path::Path, to: &std::path::Path) {
    for sub in ["wal", "sst"] {
        fs::create_dir_all(to.join(sub)).unwrap();
        for p in fs::read_dir(from.join(sub)).unwrap() {
            let p = p.unwrap().path();
            fs::copy(&p, to.join(sub).join(p.file_name().unwrap())).unwrap();
        }
    }
    fs::copy(from.join("MANIFEST"), to.join("MANIFEST")).unwrap();
}
#[test]
fn every_truncation_offset_of_last_wal_record_is_recovered_and_repaired() {
    let d = tempfile::tempdir().unwrap();
    let e = Engine::open(d.path()).unwrap();
    e.put(b"a", b"first").unwrap();
    e.put(b"b", b"second").unwrap();
    drop(e);
    let wal = files(&d.path().join("wal"), "wal").pop().unwrap();
    let prefix = fs::metadata(&wal).unwrap().len() as usize;
    let e = Engine::open(d.path()).unwrap();
    e.put(b"c", b"last-value").unwrap();
    drop(e);
    let full = fs::read(&wal).unwrap();
    for cut in prefix..=full.len() {
        let dest = tempfile::tempdir().unwrap();
        copy_db(d.path(), dest.path());
        let target = dest.path().join("wal").join(wal.file_name().unwrap());
        fs::write(&target, &full[..cut]).unwrap();
        let e = Engine::open(dest.path()).unwrap();
        assert_eq!(e.get(b"a").unwrap().unwrap(), "first");
        assert_eq!(e.get(b"b").unwrap().unwrap(), "second");
        assert_eq!(
            e.get(b"c").unwrap().is_some(),
            cut == full.len(),
            "cut {cut}"
        );
        e.put(b"d", b"after-tail").unwrap();
        drop(e);
        let e = Engine::open(dest.path()).unwrap();
        assert_eq!(e.get(b"d").unwrap().unwrap(), "after-tail");
    }
}
#[test]
fn every_single_byte_flip_in_complete_wal_is_a_controlled_error() {
    let d = tempfile::tempdir().unwrap();
    let e = Engine::open(d.path()).unwrap();
    e.put(b"key", b"some-value").unwrap();
    drop(e);
    let wal = files(&d.path().join("wal"), "wal").pop().unwrap();
    let bytes = fs::read(&wal).unwrap();
    for at in 0..bytes.len() {
        let dest = tempfile::tempdir().unwrap();
        copy_db(d.path(), dest.path());
        let mut broken = bytes.clone();
        broken[at] ^= 0x80;
        fs::write(
            dest.path().join("wal").join(wal.file_name().unwrap()),
            broken,
        )
        .unwrap();
        assert!(
            matches!(Engine::open(dest.path()), Err(EngineError::Corruption(_))),
            "byte {at}"
        );
    }
}
#[test]
fn every_manifest_byte_flip_is_rejected() {
    let d = tempfile::tempdir().unwrap();
    let e = Engine::open(d.path()).unwrap();
    e.put(b"k", b"v").unwrap();
    e.flush().unwrap();
    drop(e);
    let bytes = fs::read(d.path().join("MANIFEST")).unwrap();
    for at in 0..bytes.len() {
        let dest = tempfile::tempdir().unwrap();
        copy_db(d.path(), dest.path());
        let mut b = bytes.clone();
        b[at] ^= 1;
        fs::write(dest.path().join("MANIFEST"), b).unwrap();
        assert!(Engine::open(dest.path()).is_err(), "byte {at}");
    }
}
#[test]
fn every_sst_byte_flip_is_detected_at_open_or_read() {
    let d = tempfile::tempdir().unwrap();
    let e = Engine::open(d.path()).unwrap();
    e.put(b"key", b"value").unwrap();
    e.flush().unwrap();
    drop(e);
    let sst = files(&d.path().join("sst"), "sst").pop().unwrap();
    let bytes = fs::read(&sst).unwrap();
    for at in 0..bytes.len() {
        let dest = tempfile::tempdir().unwrap();
        copy_db(d.path(), dest.path());
        let mut b = bytes.clone();
        b[at] ^= 1;
        fs::write(dest.path().join("sst").join(sst.file_name().unwrap()), b).unwrap();
        match Engine::open_with_options(
            dest.path(),
            Options {
                block_cache_capacity: 0,
                ..Options::default()
            },
        ) {
            Err(_) => {}
            Ok(e) => assert!(e.get(b"key").is_err(), "undetected byte {at}"),
        }
    }
}
#[test]
fn truncated_sst_and_missing_referenced_sst_are_rejected() {
    let d = tempfile::tempdir().unwrap();
    let e = Engine::open(d.path()).unwrap();
    e.put(b"k", b"v").unwrap();
    e.flush().unwrap();
    drop(e);
    let sst = files(&d.path().join("sst"), "sst").pop().unwrap();
    let bytes = fs::read(&sst).unwrap();
    for cut in 0..bytes.len() {
        let dest = tempfile::tempdir().unwrap();
        copy_db(d.path(), dest.path());
        fs::write(
            dest.path().join("sst").join(sst.file_name().unwrap()),
            &bytes[..cut],
        )
        .unwrap();
        assert!(Engine::open(dest.path()).is_err(), "cut {cut}");
    }
    fs::remove_file(sst).unwrap();
    assert!(Engine::open(d.path()).is_err());
}
#[test]
fn incomplete_new_wal_header_has_no_acknowledged_data() {
    for length in 0..8 {
        let d = tempfile::tempdir().unwrap();
        let e = Engine::open(d.path()).unwrap();
        e.put(b"base", b"safe").unwrap();
        drop(e);
        fs::write(
            d.path().join("wal/00000000000000000002.wal"),
            &b"RKVWAL01"[..length],
        )
        .unwrap();
        let e = Engine::open(d.path()).unwrap();
        assert_eq!(e.get(b"base").unwrap().unwrap(), "safe");
        e.put(b"next", b"value").unwrap();
        e.flush().unwrap();
    }
}
#[test]
fn missing_manifest_is_not_silently_reinitialized() {
    let d = tempfile::tempdir().unwrap();
    let e = Engine::open(d.path()).unwrap();
    e.put(b"k", b"v").unwrap();
    drop(e);
    fs::remove_file(d.path().join("MANIFEST")).unwrap();
    assert!(matches!(
        Engine::open(d.path()),
        Err(EngineError::Corruption(_))
    ));
}

#[test]
fn checksum_valid_malformed_records_are_rejected_without_large_allocations() {
    let d = tempfile::tempdir().unwrap();
    let e = Engine::open(d.path()).unwrap();
    drop(e);
    let wal = files(&d.path().join("wal"), "wal").pop().unwrap();
    let mut rng = Rng(0x7289123);
    for _ in 0..1000 {
        let len = (rng.next() % 256) as usize;
        let mut payload = vec![0; len];
        for b in &mut payload {
            *b = rng.next() as u8;
        }
        let length = (len as u32).to_le_bytes();
        let mut data = b"RKVWAL01".to_vec();
        data.extend(length);
        data.extend(crc32fast::hash(&length).to_le_bytes());
        data.extend(crc32fast::hash(&payload).to_le_bytes());
        data.extend(payload);
        fs::write(&wal, data).unwrap();
        assert!(matches!(
            Engine::open(d.path()),
            Err(EngineError::Corruption(_))
        ));
    }
    let length = u32::MAX.to_le_bytes();
    let mut data = b"RKVWAL01".to_vec();
    data.extend(length);
    data.extend(crc32fast::hash(&length).to_le_bytes());
    data.extend([0; 4]);
    fs::write(wal, data).unwrap();
    assert!(matches!(
        Engine::open(d.path()),
        Err(EngineError::Corruption(_))
    ));
}
#[test]
fn a_torn_nonfinal_wal_is_corruption_not_silently_discarded() {
    let d = tempfile::tempdir().unwrap();
    let e = Engine::open(d.path()).unwrap();
    e.put(b"acknowledged", b"value").unwrap();
    drop(e);
    let wal = files(&d.path().join("wal"), "wal").pop().unwrap();
    let mut bytes = fs::read(&wal).unwrap();
    bytes.pop();
    fs::write(&wal, bytes).unwrap();
    fs::write(d.path().join("wal/00000000000000000002.wal"), b"RKVWAL01").unwrap();
    assert!(matches!(
        Engine::open(d.path()),
        Err(EngineError::Corruption(_))
    ));
}

#[test]
fn corrupt_compaction_input_does_not_publish_or_delete_live_files() {
    let dir = tempfile::tempdir().unwrap();
    let opts = Options {
        memtable_size_limit: 1024 * 1024,
        block_size: 64,
        block_cache_capacity: 0,
        compaction_file_threshold: 1000,
        ..Options::default()
    };
    let db = Engine::open_with_options(dir.path(), opts.clone()).unwrap();
    db.put(b"a", &[1; 64]).unwrap();
    db.put(b"b", &[2; 64]).unwrap();
    db.put(b"c", &[3; 64]).unwrap();
    db.put(b"d", &[4; 64]).unwrap();
    db.flush().unwrap();
    let damaged = files(&dir.path().join("sst"), "sst").pop().unwrap();
    db.put(b"z", b"healthy").unwrap();
    db.flush().unwrap();
    drop(db);

    // Fail after enough records to have written a data block to the temporary SST.
    let mut bytes = fs::read(&damaged).unwrap();
    let mut offset = 8;
    for _ in 0..3 {
        let payload = u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap());
        offset += 12 + payload as usize;
    }
    bytes[offset + 12] ^= 1;
    fs::write(&damaged, bytes).unwrap();
    let manifest = fs::read(dir.path().join("MANIFEST")).unwrap();
    let live = files(&dir.path().join("sst"), "sst");
    let contents: Vec<_> = live.iter().map(|p| fs::read(p).unwrap()).collect();
    let db = Engine::open_with_options(dir.path(), opts.clone()).unwrap();
    assert!(matches!(db.compact(), Err(EngineError::Corruption(_))));
    let partial = files(&dir.path().join("sst"), "tmp");
    assert_eq!(partial.len(), 1);
    assert!(fs::metadata(&partial[0]).unwrap().len() > 8);
    assert!(matches!(
        db.put(b"must-reject", b"value"),
        Err(EngineError::Background(_))
    ));
    assert_eq!(fs::read(dir.path().join("MANIFEST")).unwrap(), manifest);
    assert_eq!(files(&dir.path().join("sst"), "sst"), live);
    for (path, expected) in live.iter().zip(contents) {
        assert_eq!(fs::read(path).unwrap(), expected);
    }
    drop(db);

    let db = Engine::open_with_options(dir.path(), opts).unwrap();
    assert_eq!(db.get(b"z").unwrap().unwrap(), "healthy");
    assert_eq!(db.get(b"a").unwrap().as_deref(), Some(&[1; 64][..]));
    assert!(matches!(db.get(b"d"), Err(EngineError::Corruption(_))));
    assert_eq!(db.stats().sequence, 5);
    assert_eq!(files(&dir.path().join("sst"), "sst"), live);
    assert!(files(&dir.path().join("sst"), "tmp").is_empty());
}
