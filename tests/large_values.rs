use bytes::BytesMut;
use rust_kv_storage_engine::{Engine, Options};

#[test]
fn retained_large_values_survive_eviction_updates_compaction_and_engine_drop() {
    for capacity in [0, 8192, 1024 * 1024] {
        let directory = tempfile::tempdir().unwrap();
        let options = Options {
            memtable_size_limit: 4 * 1024 * 1024,
            block_cache_capacity: capacity,
            block_size: 4096,
            compaction_file_threshold: 1000,
            ..Options::default()
        };
        let value: Vec<u8> = (0..128 * 1024).map(|n| (n % 251) as u8).collect();
        let db = Engine::open_with_options(directory.path(), options.clone()).unwrap();
        for n in 0..16u64 {
            db.put(&n.to_be_bytes(), &value).unwrap();
        }
        db.flush().unwrap();
        let held = db.get(&0u64.to_be_bytes()).unwrap().unwrap();
        let slice = held.slice(17..held.len() - 11);
        let mut distinct = Vec::new();
        for n in 1..16u64 {
            distinct.push(db.get(&n.to_be_bytes()).unwrap().unwrap());
            assert!(db.stats().cache_bytes <= capacity);
        }
        if capacity != 0 && capacity >= value.len() {
            let reads = db.stats().block_reads;
            assert_eq!(
                db.get(&0u64.to_be_bytes()).unwrap().as_deref(),
                Some(value.as_slice())
            );
            assert!(db.stats().block_reads > reads);
        }
        db.put(&0u64.to_be_bytes(), b"new").unwrap();
        db.delete(&1u64.to_be_bytes()).unwrap();
        db.flush().unwrap();
        db.compact().unwrap();
        assert_eq!(
            db.get(&0u64.to_be_bytes()).unwrap().as_deref(),
            Some(&b"new"[..])
        );
        assert!(db.get(&1u64.to_be_bytes()).unwrap().is_none());
        assert!(db.stats().cache_bytes <= capacity);
        drop(db);
        // Returned values must not retain the database lock or file handles.
        let reopened = Engine::open_with_options(directory.path(), options).unwrap();
        assert_eq!(
            reopened.get(&0u64.to_be_bytes()).unwrap().as_deref(),
            Some(&b"new"[..])
        );
        assert!(reopened.get(&1u64.to_be_bytes()).unwrap().is_none());
        drop(reopened);
        let mut mutable = BytesMut::from(held.clone());
        mutable[17] ^= 255;
        assert_eq!(held.as_ref(), value);
        assert_eq!(slice.as_ref(), &value[17..value.len() - 11]);
        std::thread::spawn(move || {
            for bytes in distinct {
                assert_eq!(bytes, held);
            }
        })
        .join()
        .unwrap();
    }
}
