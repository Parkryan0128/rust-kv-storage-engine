use rust_kv_storage_engine::{Engine, Options};

#[test]
fn inspection_distinguishes_physical_versions_from_live_values() {
    let dir = tempfile::tempdir().unwrap();
    let db = Engine::open_with_options(
        dir.path(),
        Options {
            compaction_file_threshold: 1000,
            ..Options::default()
        },
    )
    .unwrap();
    db.put(b"user:1", b"Ryan").unwrap();
    let view = db.inspect().unwrap();
    assert_eq!(view.sequence, 1);
    assert_eq!(view.active.records, 1);
    assert_eq!(view.active.preview[0].value.as_deref(), Some(&b"Ryan"[..]));
    db.flush().unwrap();
    let first = db.inspect().unwrap().tables[0].id;
    db.delete(b"user:1").unwrap();
    assert_eq!(db.inspect().unwrap().active.preview[0].value, None);
    db.flush().unwrap();
    assert_eq!(db.get(b"user:1").unwrap(), None);
    assert_eq!(
        db.inspect_table(first).unwrap()[0].value.as_deref(),
        Some(&b"Ryan"[..])
    );
    let latest = db.inspect().unwrap().tables[1].id;
    assert_eq!(db.inspect_table(latest).unwrap()[0].value, None);
    db.compact().unwrap();
    assert!(db.inspect_table(first).is_err());
    assert_eq!(db.inspect().unwrap().tables[0].records, 0);
    assert!(db
        .inspect_table(db.inspect().unwrap().tables[0].id)
        .unwrap()
        .is_empty());
}

#[test]
fn table_preview_stops_before_the_next_block_and_keeps_full_lengths() {
    let dir = tempfile::tempdir().unwrap();
    let db = Engine::open_with_options(
        dir.path(),
        Options {
            block_size: 64,
            ..Options::default()
        },
    )
    .unwrap();
    for i in 0..33u64 {
        db.put(&i.to_be_bytes(), &[42; 8192]).unwrap();
    }
    db.flush().unwrap();
    let id = db.inspect().unwrap().tables[0].id;
    let path = dir.path().join(format!("sst/{id:020}.sst"));
    let mut bytes = std::fs::read(&path).unwrap();
    // Each oversized record occupies its own frame. Corrupt record 33, which
    // lies beyond the preview limit, without touching its index or Bloom filter.
    let mut offset = 8;
    for _ in 0..32 {
        offset += 12 + u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap()) as usize;
    }
    bytes[offset + 12] ^= 1;
    std::fs::write(path, bytes).unwrap();
    let preview = db.inspect_table(id).unwrap();
    assert_eq!(preview.len(), 32);
    for (i, record) in preview.iter().enumerate() {
        assert_eq!(record.key, (i as u64).to_be_bytes());
        assert_eq!(record.sequence, i as u64 + 1);
        assert_eq!(record.value_len, 8192);
        assert_eq!(record.value.as_deref(), Some(&[42; 128][..]));
    }
    assert!(matches!(
        db.get(&32u64.to_be_bytes()),
        Err(rust_kv_storage_engine::EngineError::Corruption(_))
    ));
}

#[test]
fn inspection_limits_preview_count_and_byte_copies() {
    let dir = tempfile::tempdir().unwrap();
    let db = Engine::open(dir.path()).unwrap();
    for i in 0..40u8 {
        let mut key = vec![i];
        key.extend([b'k'; 255]);
        db.put(&key, &[b'v'; 1024]).unwrap();
    }
    let view = db.inspect().unwrap();
    assert_eq!(view.active.records, 40);
    assert_eq!(view.active.preview.len(), 32);
    for r in view.active.preview {
        assert_eq!((r.key.len(), r.key_len), (128, 256));
        assert_eq!((r.value.unwrap().len(), r.value_len), (128, 1024));
    }
    db.flush().unwrap();
    let view = db.inspect().unwrap();
    assert_eq!(view.active.records, 0);
    let records = db.inspect_table(view.tables[0].id).unwrap();
    assert_eq!(records.len(), 32);
    assert_eq!(records[0].value_len, 1024);
}
