use rust_kv_storage_engine::{Engine, Options};
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
