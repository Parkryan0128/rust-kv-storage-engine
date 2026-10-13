mod common;
use common::*;
use rust_kv_storage_engine::{Engine, EngineError, Options, WriteBatch};
use std::{
    collections::BTreeMap,
    fs,
    sync::{Arc, Barrier},
    thread,
};

#[test]
fn ordered_batch_survives_wal_recovery_flush_and_compaction() {
    let dir = tempfile::tempdir().unwrap();
    let db = Engine::open(dir.path()).unwrap();
    db.put(b"deleted", b"old").unwrap();
    db.flush().unwrap();
    db.put(b"single", b"before").unwrap();
    let mut batch = WriteBatch::new();
    batch
        .put(b"key", b"first")
        .delete(b"key")
        .put(b"key", b"last")
        .delete(b"deleted")
        .put(b"", b"")
        .put([0, 255], [255, 0]);
    assert_eq!(batch.len(), 6);
    db.write_batch(batch).unwrap();
    db.put(b"single", b"after").unwrap();
    assert_eq!(db.stats().sequence, 9);
    drop(db);
    for action in 0..3 {
        let db = Engine::open(dir.path()).unwrap();
        assert_eq!(db.get(b"key").unwrap().unwrap(), "last");
        assert_eq!(db.get(b"deleted").unwrap(), None);
        assert_eq!(db.get(b"").unwrap().as_deref(), Some(&b""[..]));
        assert_eq!(db.get(&[0, 255]).unwrap().as_deref(), Some(&[255, 0][..]));
        assert_eq!(db.get(b"single").unwrap().unwrap(), "after");
        assert_eq!(db.stats().sequence, 9);
        if action == 0 {
            db.flush().unwrap();
        }
        if action == 1 {
            db.compact().unwrap();
        }
    }
}

#[test]
fn empty_cleared_and_volatile_batches() {
    let db = Engine::new();
    let mut batch = WriteBatch::new();
    assert!(batch.is_empty());
    batch.put(b"discard", b"value");
    batch.clear();
    assert_eq!(batch.len(), 0);
    db.write_batch(batch).unwrap();
    assert_eq!(db.stats().sequence, 0);
    let mut batch = WriteBatch::new();
    batch
        .put(b"key", b"value")
        .delete(b"key")
        .delete(b"missing");
    db.write_batch(batch).unwrap();
    assert_eq!(db.stats().active_memtable_bytes, 0);
    assert_eq!(db.stats().sequence, 3);
    assert_eq!(db.get(b"discard").unwrap(), None);
    assert_eq!(db.get(b"key").unwrap(), None);
}

#[test]
fn delete_only_batch_keeps_its_sequence_after_empty_compaction() {
    let dir = tempfile::tempdir().unwrap();
    let db = Engine::open(dir.path()).unwrap();
    db.put(b"old", b"value").unwrap();
    db.flush().unwrap();
    let mut batch = WriteBatch::new();
    batch.delete("old").delete("missing").delete("old");
    db.write_batch(batch).unwrap();
    db.compact().unwrap();
    assert_eq!(db.stats().sst_records, 0);
    drop(db);
    let db = Engine::open(dir.path()).unwrap();
    assert_eq!(db.stats().sequence, 4);
    assert!(db.get(b"old").unwrap().is_none());
    let mut batch = WriteBatch::new();
    batch.put("new", "value");
    db.write_batch(batch).unwrap();
    drop(db);
    let db = Engine::open(dir.path()).unwrap();
    assert_eq!(db.stats().sequence, 5);
    assert_eq!(db.get(b"new").unwrap().unwrap(), "value");
}

#[test]
fn invalid_later_entries_reject_the_whole_batch_without_touching_wal() {
    let dir = tempfile::tempdir().unwrap();
    let db = Engine::open_with_options(
        dir.path(),
        Options {
            max_key_size: 4,
            max_value_size: 4,
            memtable_size_limit: 1,
            ..Options::default()
        },
    )
    .unwrap();
    db.put(b"old", b"safe").unwrap();
    let wal = files(&dir.path().join("wal"), "wal")[0].clone();
    let before = fs::read(&wal).unwrap();
    for invalid_key in [false, true] {
        let mut batch = WriteBatch::new();
        batch.delete(b"old").put(b"new", b"ok");
        if invalid_key {
            batch.delete(b"too-long");
        } else {
            batch.put(b"bad", b"too-long");
        }
        assert!(matches!(
            db.write_batch(batch),
            Err(EngineError::InvalidConfig(_))
        ));
        assert_eq!(db.stats().sequence, 1);
        assert_eq!(db.get(b"old").unwrap().unwrap(), "safe");
        assert_eq!(db.get(b"new").unwrap(), None);
        assert_eq!(fs::read(&wal).unwrap(), before);
        assert_eq!(files(&dir.path().join("wal"), "wal").len(), 1);
    }
    db.write_batch(WriteBatch::new()).unwrap();
    assert_eq!(fs::read(&wal).unwrap(), before);
    db.put(b"next", b"ok").unwrap();
}

#[test]
fn batch_larger_than_memtable_target_remains_atomic_across_rotations() {
    let dir = tempfile::tempdir().unwrap();
    let opts = Options {
        memtable_size_limit: 128,
        block_size: 128,
        compaction_file_threshold: 3,
        ..Options::default()
    };
    let db = Engine::open_with_options(dir.path(), opts.clone()).unwrap();
    let mut model = BTreeMap::new();
    let mut rng = Rng(891);
    let mut sequence = 0;
    for round in 0..24 {
        let mut batch = WriteBatch::new();
        for _ in 0..8 {
            let key = (rng.next() % 16).to_be_bytes().to_vec();
            if rng.next() % 3 == 0 {
                batch.delete(&key);
                model.remove(&key);
            } else {
                let value = vec![round; 48];
                batch.put(&key, &value);
                model.insert(key, value);
            }
            sequence += 1;
        }
        db.write_batch(batch).unwrap();
        if round % 7 == 0 {
            db.flush().unwrap();
        }
        verify(&db, &model, 16);
    }
    drop(db);
    let db = Engine::open_with_options(dir.path(), opts).unwrap();
    assert_eq!(db.stats().sequence, sequence);
    verify(&db, &model, 16);
    db.compact().unwrap();
    verify(&db, &model, 16);
}

#[test]
fn concurrent_batches_single_writes_and_flushes_recover_together() {
    let dir = tempfile::tempdir().unwrap();
    let db = Engine::open_with_options(dir.path(), options()).unwrap();
    let barrier = Arc::new(Barrier::new(4));
    let workers: Vec<_> = (0..4u8)
        .map(|id| {
            let db = db.clone();
            let barrier = barrier.clone();
            thread::spawn(move || {
                barrier.wait();
                for generation in 0..12u8 {
                    let mut batch = WriteBatch::new();
                    batch
                        .put([id, 0], [generation])
                        .put([id, 1], [generation])
                        .delete([id, 2]);
                    db.write_batch(batch).unwrap();
                    db.put(&[id, 3], &[generation]).unwrap();
                    if generation % 4 == 0 {
                        db.flush().unwrap();
                    }
                }
            })
        })
        .collect();
    for worker in workers {
        worker.join().unwrap();
    }
    drop(db);
    let db = Engine::open(dir.path()).unwrap();
    assert_eq!(db.stats().sequence, 4 * 12 * 4);
    for id in 0..4u8 {
        for slot in [0, 1, 3] {
            assert_eq!(db.get(&[id, slot]).unwrap().as_deref(), Some(&[11][..]));
        }
        assert_eq!(db.get(&[id, 2]).unwrap(), None);
    }
}
