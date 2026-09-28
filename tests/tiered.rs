mod common;
use common::*;
use rust_kv_storage_engine::{CompactionStyle, Engine, Options};
use std::fs;

fn opts() -> Options {
    Options {
        memtable_size_limit: 8 * 1024 * 1024,
        block_size: 1024,
        compaction_file_threshold: 3,
        ..Options::default()
    }
}

fn seed(path: &std::path::Path) {
    let mut o = opts();
    o.compaction_file_threshold = 10000;
    let e = Engine::open_with_options(path, o).unwrap();
    for k in 0..512u64 {
        e.put(&k.to_be_bytes(), &[1; 128]).unwrap();
    }
    e.flush().unwrap();
    for generation in 2..5u8 {
        e.delete(&0u64.to_be_bytes()).unwrap();
        for k in 1..16u64 {
            e.put(&k.to_be_bytes(), &[generation; 128]).unwrap();
        }
        e.flush().unwrap();
    }
    assert_eq!(e.stats().sst_files, 4);
}

fn verify_seed(e: &Engine) {
    assert_eq!(e.get(&0u64.to_be_bytes()).unwrap(), None);
    for k in 1..512u64 {
        let value = if k < 16 { 4 } else { 1 };
        assert_eq!(
            e.get(&k.to_be_bytes()).unwrap().as_deref(),
            Some(&[value; 128][..])
        );
    }
}

#[test]
fn partial_merge_preserves_cold_file_and_tombstone_across_reopen() {
    let d = tempfile::tempdir().unwrap();
    seed(d.path());
    let cold = files(&d.path().join("sst"), "sst")
        .into_iter()
        .max_by_key(|p| fs::metadata(p).unwrap().len())
        .unwrap();
    let original = fs::read(&cold).unwrap();
    let e = Engine::open_with_options(d.path(), opts()).unwrap();
    e.flush().unwrap();
    assert_eq!(e.stats().sst_files, 2);
    assert_eq!(e.stats().compactions, 1);
    assert!(e.stats().compaction_input_bytes < original.len() as u64);
    assert_eq!(fs::read(&cold).unwrap(), original);
    verify_seed(&e);
    drop(e);
    let e = Engine::open_with_options(d.path(), opts()).unwrap();
    verify_seed(&e);
    e.compact().unwrap();
    assert_eq!(e.stats().sst_files, 1);
    assert_eq!(e.stats().sst_records, 511);
    assert!(!cold.exists());
    drop(e);
    let e = Engine::open(d.path()).unwrap();
    verify_seed(&e);
}

#[test]
fn policy_can_change_on_existing_files_without_format_migration() {
    let d = tempfile::tempdir().unwrap();
    seed(d.path());
    let mut o = opts();
    o.compaction_style = CompactionStyle::Full;
    let e = Engine::open_with_options(d.path(), o).unwrap();
    e.flush().unwrap();
    assert_eq!(e.stats().sst_files, 1);
    assert_eq!(e.stats().sst_records, 511);
    verify_seed(&e);
    drop(e);
    let e = Engine::open_with_options(d.path(), opts()).unwrap();
    verify_seed(&e);
    assert_eq!(e.stats().compactions, 0);
    assert_eq!(e.stats().flush_bytes, 0);
}

#[test]
fn tiered_cascades_and_reopens_match_model_for_mixed_record_sizes() {
    use std::collections::BTreeMap;
    for seed in [1, 17, 931] {
        let d = tempfile::tempdir().unwrap();
        let mut o = opts();
        o.memtable_size_limit = 8192;
        o.compaction_file_threshold = 2;
        let mut e = Engine::open_with_options(d.path(), o.clone()).unwrap();
        let mut model = BTreeMap::new();
        let mut rng = Rng(seed);
        for round in 0..60 {
            for _ in 0..80 {
                let key = (rng.next() % 192).to_be_bytes().to_vec();
                if rng.next() % 4 == 0 {
                    e.delete(&key).unwrap();
                    model.remove(&key);
                } else {
                    let value = vec![(round + 1) as u8; 1 + (rng.next() % 1024) as usize];
                    e.put(&key, &value).unwrap();
                    model.insert(key, value);
                }
            }
            e.flush().unwrap();
            verify(&e, &model, 192);
            if round % 7 == 0 {
                drop(e);
                e = Engine::open_with_options(d.path(), o.clone()).unwrap();
                verify(&e, &model, 192);
            }
        }
        e.compact().unwrap();
        assert_eq!(e.stats().sst_records, model.len() as u64);
        drop(e);
        let e = Engine::open(d.path()).unwrap();
        verify(&e, &model, 192);
    }
}
