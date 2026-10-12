use super::*;
use std::{sync::mpsc::sync_channel, time::Duration};

#[test]
fn sequence_exhaustion_rejects_the_whole_batch() {
    let db = Engine::new();
    db.inner.core.state.write().sequence = u64::MAX - 1;
    let mut batch = WriteBatch::new();
    batch.put("a", "first").put("b", "second");
    assert!(matches!(
        db.write_batch(batch),
        Err(EngineError::SequenceExhausted)
    ));
    assert_eq!(db.stats().sequence, u64::MAX - 1);
    assert!(db.get(b"a").unwrap().is_none());
    let mut batch = WriteBatch::new();
    batch.put("a", "last");
    db.write_batch(batch).unwrap();
    assert_eq!(db.stats().sequence, u64::MAX);
    let mut batch = WriteBatch::new();
    batch.delete("a");
    assert!(matches!(
        db.write_batch(batch),
        Err(EngineError::SequenceExhausted)
    ));
    assert_eq!(db.get(b"a").unwrap().unwrap(), "last");
    db.write_batch(WriteBatch::new()).unwrap();
}

#[test]
fn oversized_batch_is_rejected_before_wal_or_memory_changes() {
    let dir = tempfile::tempdir().unwrap();
    let db = Engine::open(dir.path()).unwrap();
    let before = db.inner.core.writer.lock().as_ref().unwrap().bytes;
    // Share one backing buffer so this boundary test allocates only 16 MiB.
    let value = Bytes::from(vec![7; 16 * 1024 * 1024]);
    let mut batch = WriteBatch {
        entries: (0..4)
            .map(|i| crate::batch::Entry {
                key: vec![i],
                value: Some(value.clone()),
            })
            .collect(),
    };
    let overhead = crate::batch::BATCH_HEADER + 4 * 18;
    batch.entries[3].value = Some(value.slice(..value.len() - overhead));
    assert_eq!(batch.encoded_len().unwrap(), crate::codec::MAX_FRAME);
    batch.entries[3].value = Some(value);
    assert!(matches!(
        db.write_batch(batch),
        Err(EngineError::InvalidConfig(_))
    ));
    assert_eq!(db.inner.core.writer.lock().as_ref().unwrap().bytes, before);
    assert_eq!(db.stats().sequence, 0);
    assert_eq!(db.stats().active_memtable_bytes, 0);
    db.put(b"still", b"healthy").unwrap();
}

#[test]
fn batch_holds_reader_and_rotation_barriers_until_all_operations_are_applied() {
    let db = Engine::new();
    db.put(b"a", b"old").unwrap();
    let (entered_tx, entered_rx) = sync_channel(1);
    let (resume_tx, resume_rx) = sync_channel(1);
    let worker_db = db.clone();
    let worker = thread::spawn(move || {
        crate::test_hooks::arm("batch_after_first_apply", entered_tx, resume_rx);
        let mut batch = WriteBatch::new();
        batch
            .put("a", "intermediate")
            .put("b", "new")
            .put("a", "final");
        worker_db.write_batch(batch)
    });
    entered_rx.recv_timeout(Duration::from_secs(10)).unwrap();
    let readers_blocked = db.inner.core.state.try_read().is_none();
    let rotation_blocked = db.inner.core.writer.try_lock().is_none();
    resume_tx.send(()).unwrap();
    worker.join().unwrap().unwrap();
    assert!(readers_blocked);
    assert!(rotation_blocked);
    assert_eq!(db.get(b"a").unwrap().unwrap(), "final");
    assert_eq!(db.get(b"b").unwrap().unwrap(), "new");
    assert_eq!(db.stats().sequence, 4);
}

#[test]
fn even_empty_batches_reject_a_poisoned_engine() {
    let db = Engine::new();
    db.inner.core.state.write().fatal = Some("test failure".into());
    assert!(matches!(
        db.write_batch(WriteBatch::new()),
        Err(EngineError::Background(_))
    ));
}
