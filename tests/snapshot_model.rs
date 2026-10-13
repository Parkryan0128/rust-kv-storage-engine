mod common;
use common::Rng;
use rust_kv_storage_engine::{Engine, Options, Result, Scan, WriteBatch};
use std::collections::BTreeMap;

type Rows = Vec<(Vec<u8>, Vec<u8>)>;
fn rows(scan: Scan) -> Rows {
    scan.map(|r| r.map(|(k, v)| (k, v.to_vec())))
        .collect::<Result<_>>()
        .unwrap()
}

#[test]
fn historical_snapshots_match_a_model_through_rotation_compaction_and_recovery() {
    for seed in 1..=4 {
        let dir = tempfile::tempdir().unwrap();
        let opts = Options {
            memtable_size_limit: 1024,
            block_size: 64,
            block_cache_capacity: if seed % 2 == 0 { 0 } else { 4096 },
            compaction_file_threshold: 3,
            ..Options::default()
        };
        let mut db = Engine::open_with_options(dir.path(), opts.clone()).unwrap();
        let mut model = BTreeMap::new();
        let mut rng = Rng(seed * 7919);
        let mut saved: Vec<(Scan, Rows)> = Vec::new();
        for round in 0..36 {
            let mut batch = WriteBatch::new();
            for _ in 0..5 {
                let n = (rng.next() % 16) as u8;
                let key = if n == 0 {
                    vec![]
                } else {
                    vec![n / 4, 0, n, 255]
                };
                if rng.next() % 3 == 0 {
                    batch.delete(&key);
                    model.remove(&key);
                } else {
                    let size = [0, 1, 63, 65, 512, 8193][rng.next() as usize % 6];
                    let value = vec![round; size];
                    batch.put(&key, &value);
                    model.insert(key, value);
                }
            }
            db.write_batch(batch).unwrap();
            let expected: Rows = model.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
            assert_eq!(rows(db.scan(..).unwrap()), expected);
            if round % 3 == 0 {
                saved.push((db.scan(..).unwrap(), expected));
            }
            if round % 4 == 0 {
                db.flush().unwrap();
            }
            if round % 7 == 0 {
                db.compact().unwrap();
            }
            if round % 11 == 10 {
                for (snapshot, historical) in saved.drain(..) {
                    assert_eq!(rows(snapshot), historical);
                }
                drop(db);
                db = Engine::open_with_options(dir.path(), opts.clone()).unwrap();
                assert_eq!(
                    rows(db.scan(..).unwrap()),
                    model
                        .iter()
                        .map(|(k, v)| (k.clone(), v.clone()))
                        .collect::<Rows>()
                );
            }
        }
        db.compact().unwrap();
        for (snapshot, historical) in saved {
            assert_eq!(rows(snapshot), historical);
        }
        // Compaction to an empty SST must not resurrect values in a range scan.
        let mut batch = WriteBatch::new();
        for key in model.keys() {
            batch.delete(key);
        }
        db.write_batch(batch).unwrap();
        db.compact().unwrap();
        drop(db);
        assert!(Engine::open(dir.path())
            .unwrap()
            .scan(..)
            .unwrap()
            .next()
            .is_none());
    }
}
