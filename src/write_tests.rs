use super::*;
use std::{io, sync::mpsc::sync_channel, time::Duration};

#[test]
fn maintenance_failure_before_publication_rejects_single_writes_and_batches() {
    for batched in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let db = Engine::open(dir.path()).unwrap();
        db.put(b"old", b"before").unwrap();
        let writer_db = db.clone();
        let (entered_tx, entered_rx) = sync_channel(1);
        let (resume_tx, resume_rx) = sync_channel(1);
        let worker = thread::spawn(move || {
            crate::test_hooks::arm("write_before_publish", entered_tx, resume_rx);
            if batched {
                let mut batch = WriteBatch::new();
                batch.put("new", "after").delete("old");
                writer_db.write_batch(batch)
            } else {
                writer_db.put(b"new", b"after")
            }
        });
        entered_rx.recv_timeout(Duration::from_secs(10)).unwrap();
        // Deterministically emulate the maintenance worker recording an I/O
        // failure after the foreground WAL sync, before memory publication.
        db.inner
            .core
            .poison(&io::Error::other("maintenance failed").into());
        resume_tx.send(()).unwrap();
        assert!(matches!(
            worker.join().unwrap(),
            Err(EngineError::Background(_))
        ));
        {
            let state = db.inner.core.state.read();
            assert_eq!(state.sequence, 1);
            assert!(state.mem.get(b"new").is_none());
            assert_eq!(state.mem.get(b"old").unwrap().value.unwrap(), "before");
        }
        assert!(matches!(db.get(b"old"), Err(EngineError::Background(_))));
        drop(db);
        // An error after sync is uncertain to the caller, but recovery must
        // still contain the complete synced write, including a batch deletion.
        let db = Engine::open(dir.path()).unwrap();
        assert_eq!(db.get(b"new").unwrap().unwrap(), "after");
        assert_eq!(db.get(b"old").unwrap().is_none(), batched);
        assert_eq!(db.stats().sequence, if batched { 3 } else { 2 });
    }
}

#[test]
fn exhausted_writes_leave_the_wal_and_memtable_in_place() {
    let dir = tempfile::tempdir().unwrap();
    let db = Engine::open_with_options(
        dir.path(),
        Options {
            memtable_size_limit: 1,
            ..Options::default()
        },
    )
    .unwrap();
    db.put(b"old", b"safe").unwrap();
    db.inner.core.state.write().sequence = u64::MAX;
    let (id, bytes) = {
        let writer = db.inner.core.writer.lock();
        let wal = writer.as_ref().unwrap();
        (wal.id, wal.bytes)
    };
    let original = fs::read(wal_path(dir.path(), id)).unwrap();
    for kind in 0..3 {
        let result = match kind {
            0 => db.put(b"new", b"value"),
            1 => db.delete(b"old"),
            _ => {
                let mut batch = WriteBatch::new();
                batch.delete("old").put("new", "value");
                db.write_batch(batch)
            }
        };
        assert!(matches!(result, Err(EngineError::SequenceExhausted)));
        let writer = db.inner.core.writer.lock();
        let wal = writer.as_ref().unwrap();
        assert_eq!((wal.id, wal.bytes), (id, bytes));
        assert_eq!(fs::read(wal_path(dir.path(), id)).unwrap(), original);
        assert_eq!(ids(&dir.path().join("wal"), "wal").unwrap(), [id]);
        assert_eq!(db.stats().immutable_memtables, 0);
        assert_eq!(db.get(b"old").unwrap().unwrap(), "safe");
        assert!(db.get(b"new").unwrap().is_none());
    }
}

#[test]
fn full_frozen_queue_allows_active_writes_but_blocks_the_next_rotation() {
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
    let guard = db.inner.core.maintenance.lock();
    for key in [b"a", b"b", b"c", b"d", b"e"] {
        db.put(key, &[7; 128]).unwrap();
    }
    assert_eq!(db.stats().immutable_memtables, 2);
    assert!(db.stats().active_memtable_bytes < 256);
    let (active_tx, active_rx) = sync_channel(1);
    let (entered_tx, entered_rx) = sync_channel(1);
    let (resume_tx, resume_rx) = sync_channel(1);
    let writer_db = db.clone();
    let writer = thread::spawn(move || {
        let active = writer_db.put(b"f", &[7; 128]);
        active_tx.send(active.is_ok()).unwrap();
        active?;
        crate::test_hooks::arm("write_backpressure", entered_tx, resume_rx);
        writer_db.put(b"g", &[7; 128])
    });
    let active = active_rx.recv_timeout(Duration::from_secs(5));
    // Release maintenance even when testing a regressed implementation, so the
    // worker cannot remain blocked while Engine::drop joins it during unwinding.
    if active != Ok(true) {
        drop(guard);
        drop(resume_tx);
        let _ = writer.join();
        panic!("active write was blocked by a full frozen queue: {active:?}");
    }
    let blocked = entered_rx.recv_timeout(Duration::from_secs(5));
    let stats = db.stats();
    drop(guard);
    let _ = resume_tx.send(());
    writer.join().unwrap().unwrap();
    assert_eq!(blocked, Ok(()));
    assert_eq!(stats.sequence, 6);
    assert_eq!(stats.immutable_memtables, 2);
    assert!(stats.active_memtable_bytes >= 256);
    db.flush().unwrap();
    drop(db);
    let db = Engine::open(dir.path()).unwrap();
    for key in [b"a", b"b", b"c", b"d", b"e", b"f", b"g"] {
        assert_eq!(db.get(key).unwrap().as_deref(), Some(&[7; 128][..]));
    }
}

#[test]
fn maintenance_alternates_urgent_flush_and_compaction_and_respects_flush_goal() {
    let dir = tempfile::tempdir().unwrap();
    let db = Engine::open_with_options(
        dir.path(),
        Options {
            max_immutable_memtables: 2,
            compaction_file_threshold: 2,
            compaction_style: CompactionStyle::Full,
            ..Options::default()
        },
    )
    .unwrap();
    let core = &db.inner.core;
    let mut schedule = core.maintenance.lock();
    for key in [b"a", b"b"] {
        db.put(key, &[7; 128]).unwrap();
        core.freeze(&mut core.writer.lock()).unwrap();
        core.flush_one().unwrap();
    }
    for key in [b"c", b"d"] {
        db.put(key, &[7; 128]).unwrap();
        core.freeze(&mut core.writer.lock()).unwrap();
    }
    assert_eq!(db.stats().sst_files, 2);
    assert_eq!(db.stats().immutable_memtables, 2);
    assert!(core.maintenance_step(true, &mut schedule).unwrap());
    assert_eq!(db.stats().sst_files, 3);
    assert_eq!(db.stats().immutable_memtables, 1);
    assert_eq!(db.stats().compactions, 0);
    // Replenish the queue as an active writer would. The next job must still
    // compact, otherwise perpetual pressure can accumulate SSTs without bound.
    db.put(b"e", &[7; 128]).unwrap();
    core.freeze(&mut core.writer.lock()).unwrap();
    assert!(core.maintenance_step(true, &mut schedule).unwrap());
    assert_eq!(db.stats().immutable_memtables, 2);
    assert_eq!(db.stats().compactions, 1);
    assert_eq!(db.stats().sst_files, 1);
    assert!(!core.maintenance_step(false, &mut schedule).unwrap());
    assert_eq!(db.stats().immutable_memtables, 2);
    assert!(core.maintenance_step(true, &mut schedule).unwrap());
    assert_eq!(db.stats().immutable_memtables, 1);
    drop(schedule);
    db.flush().unwrap();
    drop(db);
    let db = Engine::open(dir.path()).unwrap();
    for key in [b"a", b"b", b"c", b"d", b"e"] {
        assert_eq!(db.get(key).unwrap().as_deref(), Some(&[7; 128][..]));
    }
}
