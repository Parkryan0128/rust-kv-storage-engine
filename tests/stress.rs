use rust_kv_storage_engine::{Engine, Options};

fn growing_keyspace(keys: u64) {
    // Opt in to a chosen local disk, always inside a fresh temporary directory.
    let d = match std::env::var_os("KV_STRESS_DIR") {
        Some(path) => tempfile::tempdir_in(path).unwrap(),
        None => tempfile::tempdir().unwrap(),
    };
    let options = Options {
        memtable_size_limit: 64 * 1024,
        block_cache_capacity: 64 * 1024,
        block_size: 4096,
        ..Options::default()
    };
    let value = |key: u64, updated: bool| {
        let mut bytes = vec![u8::from(updated); 256];
        bytes[..8].copy_from_slice(&key.to_le_bytes());
        bytes
    };
    let mut e = Engine::open_with_options(d.path(), options.clone()).unwrap();
    for batch in 0..5 {
        let start = keys * batch / 5;
        let end = keys * (batch + 1) / 5;
        for key in start..end {
            e.put(&key.to_be_bytes(), &value(key, false)).unwrap();
        }
        e.flush().unwrap();
        drop(e);
        e = Engine::open_with_options(d.path(), options.clone()).unwrap();
        for key in 0..end {
            assert_eq!(
                e.get(&key.to_be_bytes()).unwrap().as_deref(),
                Some(value(key, false).as_slice())
            );
        }
    }
    // Exercise old versions and tombstones alongside the growing live dataset.
    for key in 0..keys {
        match key % 4 {
            0 => e.delete(&key.to_be_bytes()).unwrap(),
            1 => e.put(&key.to_be_bytes(), &value(key, true)).unwrap(),
            _ => {}
        }
    }
    e.compact().unwrap();
    assert_eq!(e.stats().sst_records, keys - keys.div_ceil(4));
    drop(e);
    let e = Engine::open_with_options(d.path(), options).unwrap();
    for key in 0..keys {
        let expected = (key % 4 != 0).then(|| value(key, key % 4 == 1));
        assert_eq!(
            e.get(&key.to_be_bytes()).unwrap().as_deref(),
            expected.as_deref()
        );
    }
}

#[test]
fn growing_keyspace_survives_reopen_updates_and_compaction() {
    growing_keyspace(20_000);
}

#[test]
#[ignore = "extended acceptance: one million distinct durable keys"]
fn million_distinct_keys_survive_growth_and_recovery() {
    growing_keyspace(1_000_000);
}

#[test]
#[ignore = "extended acceptance: five million fsync writes"]
fn five_million_overwrites_compact_to_one_record_per_live_key() {
    let d = tempfile::tempdir().unwrap();
    let e = Engine::open_with_options(
        d.path(),
        Options {
            memtable_size_limit: 64 * 1024,
            block_size: 4096,
            ..Options::default()
        },
    )
    .unwrap();
    for generation in 0..50000u64 {
        for key in 0..100u64 {
            e.put(&key.to_be_bytes(), &generation.to_le_bytes())
                .unwrap();
        }
        if generation % 5000 == 4999 {
            eprintln!("completed {} durable mutations", (generation + 1) * 100);
        }
    }
    e.compact().unwrap();
    assert_eq!(e.stats().sst_records, 100);
    assert_eq!(e.stats().sst_files, 1);
    drop(e);
    let e = Engine::open(d.path()).unwrap();
    for key in 0..100u64 {
        assert_eq!(
            e.get(&key.to_be_bytes()).unwrap().as_deref(),
            Some(&49999u64.to_le_bytes()[..])
        );
    }
    for key in 0..100u64 {
        e.delete(&key.to_be_bytes()).unwrap();
    }
    e.compact().unwrap();
    assert_eq!(e.stats().sst_records, 0);
    drop(e);
    let e = Engine::open(d.path()).unwrap();
    for key in 0..100u64 {
        assert_eq!(e.get(&key.to_be_bytes()).unwrap(), None);
    }
}
