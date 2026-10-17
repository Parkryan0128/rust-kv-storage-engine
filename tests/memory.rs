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
