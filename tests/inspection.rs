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
