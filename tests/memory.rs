use rust_kv_storage_engine::Engine;

#[test]
fn volatile_deletes_reclaim_keys_and_do_not_accumulate_tombstones() {
    let engine = Engine::new();
    let clone = engine.clone();
    engine.put(b"keep", b"alive").unwrap();
    let baseline = engine.stats().active_memtable_bytes;
    for i in 0..1000u64 {
        let key = i.to_be_bytes();
        engine.put(&key, &[42; 128]).unwrap();
        clone.delete(&key).unwrap();
        assert_eq!(engine.get(&key).unwrap(), None);
        assert_eq!(engine.stats().active_memtable_bytes, baseline);
        clone.delete(&key).unwrap();
        assert_eq!(engine.stats().active_memtable_bytes, baseline);
    }
    assert_eq!(engine.inspect().unwrap().active.records, 1);
    assert_eq!(engine.stats().sequence, 3001);
    clone.delete(b"keep").unwrap();
    engine.flush().unwrap();
    engine.compact().unwrap();
    assert_eq!(engine.stats().active_memtable_bytes, 0);
    assert_eq!(engine.inspect().unwrap().active.records, 0);
    engine.put(b"keep", b"again").unwrap();
    assert_eq!(clone.get(b"keep").unwrap().unwrap(), "again");
}

#[test]
fn lightweight_activity_matches_detailed_stats_without_memory_estimates() {
    let db = Engine::new();
    for n in 0..10_000u64 {
        db.put(&n.to_be_bytes(), &[1; 32]).unwrap();
    }
    for n in 0..5000u64 {
        db.delete(&n.to_be_bytes()).unwrap();
    }
    let a = db.activity_stats();
    let detailed = db.stats();
    assert_eq!(a.sequence, detailed.sequence);
    assert_eq!(a.active_memtable_bytes, detailed.active_memtable_bytes);
    assert_eq!(a.immutable_memtables, detailed.immutable_memtables);
    assert_eq!(a.cache_bytes, detailed.cache_bytes);
    assert_eq!(a.sst_bytes, detailed.sst_bytes);
    assert!(detailed.active_memtable_allocated_bytes > 0);
}

#[test]
fn polling_activity_during_writes_and_maintenance_finishes_and_keeps_data() {
    use rust_kv_storage_engine::{Options, WriteBatch};
    let dir = tempfile::tempdir().unwrap();
    let db = Engine::open_with_options(
        dir.path(),
        Options {
            memtable_size_limit: 4096,
            ..Options::default()
        },
    )
    .unwrap();
    let writer_db = db.clone();
    let worker = std::thread::spawn(move || {
        for group in 0..20u64 {
            let mut batch = WriteBatch::new();
            for key in group * 32..(group + 1) * 32 {
                batch.put(key.to_be_bytes(), [7; 128]);
            }
            writer_db.write_batch(batch).unwrap();
        }
        writer_db.flush().unwrap();
    });
    let mut sequence = 0;
    while !worker.is_finished() {
        let activity = db.activity_stats();
        assert!(activity.sequence >= sequence);
        sequence = activity.sequence;
        std::thread::yield_now();
    }
    worker.join().unwrap();
    assert_eq!(db.activity_stats().sequence, 640);
    for key in 0..640u64 {
        assert_eq!(
            db.get(&key.to_be_bytes()).unwrap().unwrap().as_ref(),
            &[7; 128]
        );
    }
}
