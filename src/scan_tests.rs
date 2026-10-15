use super::*;
use std::{io, sync::mpsc::sync_channel, time::Duration};

#[test]
fn single_source_scan_keeps_bounds_deletions_and_snapshot() {
    for disk in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let db = Engine::open(dir.path()).unwrap();
        let mut batch = WriteBatch::new();
        batch
            .put("a", "first")
            .put("b", "deleted")
            .put("c", "last")
            .put("z", "excluded")
            .delete("b");
        db.write_batch(batch).unwrap();
        if disk {
            db.flush().unwrap();
        }
        let mut scan = db.scan(b"a".as_slice()..=b"c".as_slice()).unwrap();
        db.put(b"c", b"later").unwrap();
        assert_eq!(
            scan.next().unwrap().unwrap(),
            (b"a".to_vec(), Bytes::from_static(b"first"))
        );
        assert_eq!(
            scan.next().unwrap().unwrap(),
            (b"c".to_vec(), Bytes::from_static(b"last"))
        );
        assert!(scan.next().is_none());
        assert!(scan.next().is_none());
    }
}

#[test]
fn disk_scan_materializes_only_live_winners() {
    let dir = tempfile::tempdir().unwrap();
    let db = Engine::open_with_options(
        dir.path(),
        Options {
            compaction_file_threshold: 1000,
            block_size: 4096,
            ..Options::default()
        },
    )
    .unwrap();
    for generation in 0..3u8 {
        let mut batch = WriteBatch::new();
        for key in 0..8u8 {
            if generation == 2 && key % 2 == 0 {
                batch.delete([key]);
            } else {
                batch.put([key], vec![generation; 8192]);
            }
        }
        db.write_batch(batch).unwrap();
        db.flush().unwrap();
    }
    let mut scan = db.scan(..).unwrap();
    crate::codec::take_materialized_values();
    let first = scan.next().unwrap().unwrap();
    assert_eq!(first, (vec![1], Bytes::from(vec![2; 8192])));
    assert_eq!(crate::codec::take_materialized_values(), 1);
    let rest = scan.collect::<Result<Vec<_>>>().unwrap();
    assert_eq!(
        rest.iter().map(|(k, _)| k[0]).collect::<Vec<_>>(),
        [3, 5, 7]
    );
    assert!(rest.iter().all(|(_, v)| v.as_ref() == vec![2; 8192]));
    assert_eq!(crate::codec::take_materialized_values(), 3);
    assert_eq!(db.stats().cache_bytes, 0);
}

#[test]
fn frozen_snapshot_merge_releases_locks_and_survives_concurrent_publication() {
    let dir = tempfile::tempdir().unwrap();
    let db = Engine::open(dir.path()).unwrap();
    db.put(b"f", b"disk").unwrap();
    db.flush().unwrap();
    let maintenance = db.inner.core.maintenance.lock();
    let mut batch = WriteBatch::new();
    batch.put("a", "old").put("b", "old").put("c", "old");
    db.write_batch(batch).unwrap();
    db.inner
        .core
        .freeze(&mut db.inner.core.writer.lock())
        .unwrap();
    let mut batch = WriteBatch::new();
    batch
        .put("a", "frozen")
        .delete("b")
        .put("c", "frozen")
        .put("e", "");
    db.write_batch(batch).unwrap();
    db.inner
        .core
        .freeze(&mut db.inner.core.writer.lock())
        .unwrap();
    let mut batch = WriteBatch::new();
    batch
        .put("a", "active")
        .delete("c")
        .put("d", "active")
        .put("z", "excluded");
    db.write_batch(batch).unwrap();

    let reader_db = db.clone();
    let (entered_tx, entered_rx) = sync_channel(1);
    let (resume_tx, resume_rx) = sync_channel(1);
    let reader = thread::spawn(move || {
        crate::test_hooks::arm("scan_before_frozen_merge", entered_tx, resume_rx);
        reader_db
            .scan(b"a".as_slice()..b"z".as_slice())?
            .collect::<Result<Vec<_>>>()
    });
    entered_rx.recv_timeout(Duration::from_secs(10)).unwrap();
    let state_available = db.inner.core.state.try_write().is_some();
    let writer_available = db.inner.core.writer.try_lock().is_some();
    drop(maintenance);
    if !state_available || !writer_available {
        resume_tx.send(()).unwrap();
        reader.join().unwrap().unwrap();
        panic!("frozen snapshot merge retained a database lock");
    }
    let mut batch = WriteBatch::new();
    batch
        .put("a", "later")
        .delete("d")
        .delete("e")
        .put("f", "later");
    db.write_batch(batch).unwrap();
    db.compact().unwrap();
    resume_tx.send(()).unwrap();
    assert_eq!(
        reader.join().unwrap().unwrap(),
        [
            (b"a".to_vec(), Bytes::from_static(b"active")),
            (b"d".to_vec(), Bytes::from_static(b"active")),
            (b"e".to_vec(), Bytes::new()),
            (b"f".to_vec(), Bytes::from_static(b"disk")),
        ]
    );
    assert_eq!(db.stats().immutable_memtables, 0);
}

#[test]
fn snapshot_includes_frozen_memory_and_active_overrides() {
    let dir = tempfile::tempdir().unwrap();
    let db = Engine::open(dir.path()).unwrap();
    let mut batch = WriteBatch::new();
    batch.put("a", "disk").put("b", "disk").put("z", "disk");
    db.write_batch(batch).unwrap();
    db.flush().unwrap();
    let maintenance = db.inner.core.maintenance.lock();
    let mut batch = WriteBatch::new();
    batch.put("a", "frozen").put("c", "frozen").delete("z");
    db.write_batch(batch).unwrap();
    db.inner
        .core
        .freeze(&mut db.inner.core.writer.lock())
        .unwrap();
    let mut batch = WriteBatch::new();
    batch.put("a", "active").delete("b").put("d", "active");
    db.write_batch(batch).unwrap();
    assert_eq!(db.stats().immutable_memtables, 1);
    let snapshot = db.scan(..).unwrap();
    drop(maintenance);
    db.compact().unwrap();
    let rows = snapshot.collect::<Result<Vec<_>>>().unwrap();
    assert_eq!(
        rows,
        vec![
            (b"a".to_vec(), Bytes::from_static(b"active")),
            (b"c".to_vec(), Bytes::from_static(b"frozen")),
            (b"d".to_vec(), Bytes::from_static(b"active"))
        ]
    );
}

#[test]
fn scan_disk_io_holds_no_state_or_writer_lock() {
    let dir = tempfile::tempdir().unwrap();
    let db = Engine::open(dir.path()).unwrap();
    db.put(b"a", b"before").unwrap();
    db.flush().unwrap();
    let snapshot = db.scan(..).unwrap();
    let (entered_tx, entered_rx) = sync_channel(1);
    let (resume_tx, resume_rx) = sync_channel(1);
    let reader = thread::spawn(move || {
        crate::test_hooks::arm("scan_before_block_read", entered_tx, resume_rx);
        snapshot.collect::<Result<Vec<_>>>()
    });
    entered_rx.recv_timeout(Duration::from_secs(10)).unwrap();
    let state_available = db.inner.core.state.try_write().is_some();
    let writer_available = db.inner.core.writer.try_lock().is_some();
    if !state_available || !writer_available {
        resume_tx.send(()).unwrap();
        reader.join().unwrap().unwrap();
        panic!("scan retained a database lock during disk I/O");
    }
    db.delete(b"a").unwrap();
    db.put(b"b", b"after").unwrap();
    // This unlinks the snapshot's input SST while its read is paused.
    db.compact().unwrap();
    resume_tx.send(()).unwrap();
    assert_eq!(
        reader.join().unwrap().unwrap(),
        [(b"a".to_vec(), Bytes::from_static(b"before"))]
    );
}

#[test]
fn scans_check_health_even_when_the_range_is_empty() {
    let db = Engine::new();
    db.put(b"a", b"before").unwrap();
    let snapshot = db.scan(..).unwrap();
    db.inner
        .core
        .poison(&io::Error::other("maintenance failed").into());
    assert!(matches!(db.scan(..), Err(EngineError::Background(_))));
    assert!(matches!(
        db.scan(b"a".as_slice()..b"a".as_slice()),
        Err(EngineError::Background(_))
    ));
    // A successfully captured snapshot is independent of later engine health.
    assert_eq!(
        snapshot.collect::<Result<Vec<_>>>().unwrap(),
        [(b"a".to_vec(), Bytes::from_static(b"before"))]
    );
}
