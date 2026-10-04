use rust_kv_storage_engine::{Engine, Options};

fn key(id: u8) -> Vec<u8> {
    let mut key = vec![0; 1024 * 1024];
    key[0] = id;
    key
}

#[test]
fn large_keys_compact_past_one_metadata_frame_and_recover() {
    let dir = tempfile::tempdir().unwrap();
    let options = Options {
        // Keep the failure deterministic: exercise one explicit full merge.
        compaction_file_threshold: 1000,
        block_cache_capacity: 0,
        ..Options::default()
    };
    let db = Engine::open_with_options(dir.path(), options.clone()).unwrap();
    for id in 0..64u8 {
        db.put(&key(id), &[id]).unwrap();
    }
    // Each first key is copied into the SST index. These valid keys alone
    // exceed the old 64 MiB metadata-frame limit when merged into one SST.
    db.compact().unwrap();
    assert_eq!(db.stats().sst_files, 1);
    assert_eq!(db.stats().sst_records, 64);
    drop(db);

    let db = Engine::open_with_options(dir.path(), options.clone()).unwrap();
    for id in 0..64u8 {
        assert_eq!(db.get(&key(id)).unwrap().as_deref(), Some(&[id][..]));
    }
    db.delete(&key(0)).unwrap();
    db.put(&key(63), b"updated").unwrap();
    db.compact().unwrap();
    drop(db);

    let db = Engine::open_with_options(dir.path(), options).unwrap();
    assert_eq!(db.get(&key(0)).unwrap(), None);
    assert_eq!(db.get(&key(63)).unwrap().as_deref(), Some(&b"updated"[..]));
    // Successful maintenance must not leave the engine in its fatal state.
    db.put(b"after-compaction", b"healthy").unwrap();
    assert_eq!(
        db.get(b"after-compaction").unwrap().as_deref(),
        Some(&b"healthy"[..])
    );
}
