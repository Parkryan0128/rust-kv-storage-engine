use super::*;
use std::{sync::mpsc::sync_channel, time::Duration};

// Drive the production background step deterministically, without a second
// worker racing the scheduling assertions.
fn fixture() -> (tempfile::TempDir, Engine) {
    let dir = tempfile::tempdir().unwrap();
    let db = Engine::open_with_options(
        dir.path(),
        Options {
            memtable_size_limit: 256,
            max_immutable_memtables: 1,
            compaction_style: CompactionStyle::Full,
            compaction_file_threshold: 17,
            ..Options::default()
        },
    )
    .unwrap();
    let core = &db.inner.core;
    core.stop.store(true, Ordering::Release);
    core.wake();
    db.inner.worker.lock().take().unwrap().join().unwrap();
    core.stop.store(false, Ordering::Release);
    let guard = core.maintenance.lock();
    for i in 0..17u8 {
        db.put(&[i], &[i]).unwrap();
        db.put(b"hot", &[i]).unwrap();
        if i == 0 {
            db.put(b"deleted", b"old").unwrap();
        } else if i == 16 {
            db.delete(b"deleted").unwrap();
        }
        core.freeze(&mut core.writer.lock()).unwrap();
        core.flush_one().unwrap();
    }
    drop(guard);
    (dir, db)
}

fn verify(db: &Engine) {
    for i in 0..17u8 {
        assert_eq!(db.get(&[i]).unwrap().as_deref(), Some([i].as_slice()));
    }
    assert_eq!(db.get(b"hot").unwrap().as_deref(), Some([16].as_slice()));
    assert!(db.get(b"deleted").unwrap().is_none());
}

#[test]
fn background_plan_continues_below_threshold_and_preserves_deletions() {
    let (dir, db) = fixture();
    let core = &db.inner.core;
    assert!(core.background_step().unwrap());
    assert_eq!(db.stats().compactions, 1);
    assert_eq!(db.stats().sst_files, 2);
    assert_eq!(core.maintenance.lock().pending.len(), 2);
    verify(&db);
    assert!(core.background_step().unwrap());
    assert_eq!(db.stats().compactions, 2);
    assert_eq!(db.stats().sst_files, 1);
    assert!(core.maintenance.lock().pending.is_empty());
    assert!(!core.background_step().unwrap());
    assert_eq!(db.stats().sst_records, 18);
    drop(db);
    verify(&Engine::open(dir.path()).unwrap());
}

#[test]
fn urgent_flush_releases_a_blocked_writer_between_merge_groups() {
    let (dir, db) = fixture();
    let core = &db.inner.core;
    assert!(core.background_step().unwrap());
    db.put(b"frozen", &[7; 256]).unwrap();
    core.freeze(&mut core.writer.lock()).unwrap();
    db.put(b"active", &[8; 256]).unwrap();
    let (entered_tx, entered_rx) = sync_channel(1);
    let (resume_tx, resume_rx) = sync_channel(1);
    let (done_tx, done_rx) = sync_channel(1);
    let writer_db = db.clone();
    let writer = thread::spawn(move || {
        crate::test_hooks::arm("write_backpressure", entered_tx, resume_rx);
        done_tx.send(writer_db.put(b"after", b"yes")).unwrap();
    });
    entered_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    resume_tx.send(()).unwrap();
    assert!(core.background_step().unwrap());
    // One urgent flush, with the second compaction group still pending.
    assert_eq!(db.stats().compactions, 1);
    let done = done_rx.recv_timeout(Duration::from_secs(5));
    if done.is_err() {
        core.poison(&std::io::Error::other("test writer timeout").into());
    }
    writer.join().unwrap();
    done.unwrap().unwrap();
    assert_eq!(core.maintenance.lock().pending.len(), 2);
    assert!(core.background_step().unwrap());
    assert_eq!(db.stats().compactions, 2);
    verify(&db);
    // The interleaved flush was not part of the original plan. Do not drop
    // partial-merge tombstones, nor accidentally consume this new SST.
    assert_eq!(db.stats().sst_files, 2);
    assert!(core.maintenance.lock().pending.is_empty());
    while core.background_step().unwrap() {}
    db.compact().unwrap();
    drop(db);
    let db = Engine::open(dir.path()).unwrap();
    verify(&db);
    assert_eq!(db.get(b"after").unwrap().unwrap(), "yes");
    assert_eq!(
        db.get(b"frozen").unwrap().as_deref(),
        Some([7; 256].as_slice())
    );
    assert_eq!(
        db.get(b"active").unwrap().as_deref(),
        Some([8; 256].as_slice())
    );
}

#[test]
fn stopped_or_failed_background_plan_leaves_a_recoverable_published_prefix() {
    for fail in [false, true] {
        let (dir, db) = fixture();
        let core = &db.inner.core;
        assert!(core.background_step().unwrap());
        let manifest = fs::read(dir.path().join("MANIFEST")).unwrap();
        let ids = core.state.read().manifest.tables.clone();
        if fail {
            crate::test_hooks::fail_once("sst_before_dir_sync");
            let error = core.background_step().unwrap_err();
            core.poison(&error);
            assert!(matches!(
                db.put(b"reject", b"x"),
                Err(EngineError::Background(_))
            ));
            assert!(!core.background_step().unwrap());
        } else {
            core.stop.store(true, Ordering::Release);
            assert!(!core.background_step().unwrap());
            verify(&db);
        }
        assert_eq!(db.stats().compactions, 1);
        assert_eq!(fs::read(dir.path().join("MANIFEST")).unwrap(), manifest);
        for id in ids {
            assert!(sst_path(dir.path(), id).exists());
        }
        drop(db);
        let db = Engine::open(dir.path()).unwrap();
        verify(&db);
        assert!(db.get(b"reject").unwrap().is_none());
        db.compact().unwrap();
        assert_eq!(db.stats().sst_files, 1);
        verify(&db);
    }
}

#[test]
fn explicit_flush_and_compact_drain_an_incomplete_background_plan() {
    for compact in [false, true] {
        let (dir, db) = fixture();
        assert!(db.inner.core.background_step().unwrap());
        if compact {
            db.compact().unwrap();
        } else {
            db.flush().unwrap();
        }
        assert!(db.inner.core.maintenance.lock().pending.is_empty());
        assert_eq!(db.stats().sst_files, 1);
        verify(&db);
        drop(db);
        verify(&Engine::open(dir.path()).unwrap());
    }
}
