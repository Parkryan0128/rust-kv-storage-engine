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
