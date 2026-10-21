use super::*;

fn stop_worker(db: &Engine) {
    let core = &db.inner.core;
    core.stop.store(true, Ordering::Release);
    core.wake();
    db.inner.worker.lock().take().unwrap().join().unwrap();
    core.stop.store(false, Ordering::Release);
}

fn publish(db: &Engine) {
    let core = &db.inner.core;
    let _guard = core.maintenance.lock();
    core.freeze(&mut core.writer.lock()).unwrap();
    core.flush_one().unwrap();
}

#[test]
fn explicit_compact_merges_once_with_an_eligible_automatic_plan() {
    for style in [CompactionStyle::Full, CompactionStyle::SizeTiered] {
        for active in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let db = Engine::open_with_options(
                dir.path(),
                Options {
                    compaction_file_threshold: 4,
                    compaction_style: style,
                    ..Options::default()
                },
            )
            .unwrap();
            stop_worker(&db);
            for n in 0..4u64 {
                db.put(&n.to_be_bytes(), &[7; 128]).unwrap();
                if !active || n != 3 {
                    publish(&db);
                }
            }
            let before = db.stats();
            db.compact().unwrap();
            let after = db.stats();
            assert_eq!(after.compactions - before.compactions, 1);
            assert_eq!(after.sst_files, 1);
            assert_eq!(after.sst_records, 4);
            assert_eq!(
                after.compaction_input_bytes - before.compaction_input_bytes,
                before.sst_bytes + after.flush_bytes - before.flush_bytes
            );
            assert_eq!(
                after.compaction_output_bytes - before.compaction_output_bytes,
                after.sst_bytes
            );
            drop(db);
            let db = Engine::open(dir.path()).unwrap();
            for n in 0..4u64 {
                assert_eq!(
                    db.get(&n.to_be_bytes()).unwrap().as_deref(),
                    Some([7; 128].as_slice())
                );
            }
        }
    }
}

#[test]
fn unchanged_full_merge_preserves_file_manifest_cache_and_snapshot() {
    let dir = tempfile::tempdir().unwrap();
    let db = Engine::open(dir.path()).unwrap();
    db.put(b"key", &[7; 128]).unwrap();
    db.compact().unwrap();
    let snapshot = db.scan(..).unwrap();
    db.get(b"key").unwrap();
    let id = db.inspect().unwrap().tables[0].id;
    let manifest = fs::read(dir.path().join("MANIFEST")).unwrap();
    let sst = fs::read(sst_path(dir.path(), id)).unwrap();
    let before = db.stats();
    for _ in 0..3 {
        db.compact().unwrap();
    }
    let after = db.stats();
    assert_eq!(after.compactions, before.compactions);
    assert_eq!(
        after.compaction_output_bytes,
        before.compaction_output_bytes
    );
    assert_eq!(after.compaction_input_bytes, before.compaction_input_bytes);
    assert_eq!(after.cache_bytes, before.cache_bytes);
    assert!(after.cache_bytes > 0);
    assert_eq!(db.inspect().unwrap().tables[0].id, id);
    assert_eq!(fs::read(dir.path().join("MANIFEST")).unwrap(), manifest);
    assert_eq!(fs::read(sst_path(dir.path(), id)).unwrap(), sst);
    assert_eq!(
        snapshot.collect::<Result<Vec<_>>>().unwrap(),
        [(b"key".to_vec(), Bytes::from(vec![7; 128]))]
    );
}

#[test]
fn singleton_tombstones_and_later_publications_invalidate_the_fast_path() {
    let dir = tempfile::tempdir().unwrap();
    let db = Engine::open(dir.path()).unwrap();
    db.delete(b"key").unwrap();
    db.flush().unwrap();
    assert_eq!(db.stats().sst_records, 1);
    db.compact().unwrap();
    assert_eq!(db.stats().sst_records, 0);
    let first = db.stats().compactions;
    db.compact().unwrap();
    assert_eq!(db.stats().compactions, first);
    db.put(b"key", b"new").unwrap();
    db.compact().unwrap();
    assert_eq!(db.stats().compactions, first + 1);
    assert_eq!(db.get(b"key").unwrap().unwrap(), "new");
    db.delete(b"key").unwrap();
    db.compact().unwrap();
    assert_eq!(db.stats().sst_records, 0);
    assert_eq!(db.stats().compactions, first + 2);
    let sequence = db.stats().sequence;
    drop(db);
    let db = Engine::open(dir.path()).unwrap();
    assert_eq!(db.stats().sequence, sequence);
    assert!(db.get(b"key").unwrap().is_none());
}

#[test]
fn reopened_singleton_is_rewritten_with_current_options_once() {
    let dir = tempfile::tempdir().unwrap();
    let db = Engine::open(dir.path()).unwrap();
    for n in 0..128u64 {
        db.put(&n.to_be_bytes(), &[7; 32]).unwrap();
    }
    db.compact().unwrap();
    let old = db.inspect().unwrap().tables[0].id;
    let original = fs::read(sst_path(dir.path(), old)).unwrap();
    drop(db);
    let db = Engine::open_with_options(
        dir.path(),
        Options {
            block_size: 64,
            bloom_filter_bits_per_key: 30,
            ..Options::default()
        },
    )
    .unwrap();
    db.compact().unwrap();
    let new = db.inspect().unwrap().tables[0].id;
    assert_ne!(new, old);
    assert_eq!(db.stats().compactions, 1);
    assert_ne!(fs::read(sst_path(dir.path(), new)).unwrap(), original);
    db.compact().unwrap();
    assert_eq!(db.stats().compactions, 1);
    for n in 0..128u64 {
        assert_eq!(
            db.get(&n.to_be_bytes()).unwrap().as_deref(),
            Some([7; 32].as_slice())
        );
    }
}

#[test]
fn explicit_compaction_supersedes_partial_plan_without_a_final_rewrite() {
    let dir = tempfile::tempdir().unwrap();
    let db = Engine::open_with_options(
        dir.path(),
        Options {
            compaction_style: CompactionStyle::Full,
            compaction_file_threshold: 17,
            ..Options::default()
        },
    )
    .unwrap();
    stop_worker(&db);
    for n in 0..17u64 {
        db.put(&n.to_be_bytes(), b"value").unwrap();
        publish(&db);
    }
    assert!(db.inner.core.background_step().unwrap());
    assert_eq!(db.inner.core.maintenance.lock().pending.len(), 2);
    db.compact().unwrap();
    assert_eq!(db.stats().compactions, 2);
    assert_eq!(db.stats().sst_records, 17);
    assert!(db.inner.core.maintenance.lock().pending.is_empty());
    db.compact().unwrap();
    assert_eq!(db.stats().compactions, 2);
}

#[test]
fn failed_explicit_publication_stays_fatal_and_recovers_acknowledged_data() {
    let dir = tempfile::tempdir().unwrap();
    let db = Engine::open(dir.path()).unwrap();
    db.put(b"key", b"value").unwrap();
    db.flush().unwrap();
    crate::test_hooks::fail_once("manifest_after_rename");
    assert!(db.compact().is_err());
    assert!(matches!(db.compact(), Err(EngineError::Background(_))));
    assert!(matches!(
        db.put(b"reject", b"x"),
        Err(EngineError::Background(_))
    ));
    drop(db);
    let db = Engine::open(dir.path()).unwrap();
    assert_eq!(db.get(b"key").unwrap().unwrap(), "value");
    assert!(db.get(b"reject").unwrap().is_none());
    db.compact().unwrap();
}

#[test]
fn explicit_compaction_keeps_a_finite_goal_under_later_rotations() {
    use std::{sync::mpsc::sync_channel, time::Duration};
    let dir = tempfile::tempdir().unwrap();
    let db = Engine::open_with_options(
        dir.path(),
        Options {
            memtable_size_limit: 256,
            max_immutable_memtables: 2,
            ..Options::default()
        },
    )
    .unwrap();
    stop_worker(&db);
    db.put(b"before", &[1; 128]).unwrap();
    let (entered_tx, entered_rx) = sync_channel(1);
    let (resume_tx, resume_rx) = sync_channel(1);
    let worker_db = db.clone();
    let worker = thread::spawn(move || {
        crate::test_hooks::arm("compact_after_goal", entered_tx, resume_rx);
        worker_db.compact()
    });
    entered_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    db.put(b"later-frozen", &[2; 256]).unwrap();
    db.put(b"later-active", &[3; 256]).unwrap();
    resume_tx.send(()).unwrap();
    worker.join().unwrap().unwrap();
    assert_eq!(db.stats().immutable_memtables, 1);
    assert!(db.stats().active_memtable_bytes > 0);
    assert_eq!(db.stats().sst_records, 1);
    while db.inner.core.background_step().unwrap() {}
    db.flush().unwrap();
    drop(db);
    let db = Engine::open(dir.path()).unwrap();
    for (key, value) in [
        (b"before".as_slice(), vec![1; 128]),
        (b"later-frozen".as_slice(), vec![2; 256]),
        (b"later-active".as_slice(), vec![3; 256]),
    ] {
        assert_eq!(db.get(key).unwrap().as_deref(), Some(value.as_slice()));
    }
}
