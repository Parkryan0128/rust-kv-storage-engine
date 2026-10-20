use rust_kv_storage_engine::{Engine, Options, Result};
use std::collections::BTreeMap;

fn rows(db: &Engine) -> BTreeMap<Vec<u8>, Vec<u8>> {
    db.scan(..)
        .unwrap()
        .map(|r| r.map(|(k, v)| (k, v.to_vec())))
        .collect::<Result<_>>()
        .unwrap()
}

#[test]
fn borrowed_single_writes_preserve_snapshots_accounting_and_recovery() {
    let keys = [
        vec![],
        vec![0],
        vec![0, 255, 0],
        vec![7; 65536],
        vec![8; 1024 * 1024],
    ];
    for persistent in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let opts = Options {
            memtable_size_limit: 64 * 1024 * 1024,
            ..Options::default()
        };
        let db = if persistent {
            Engine::open_with_options(dir.path(), opts.clone()).unwrap()
        } else {
            Engine::new()
        };
        let mut model = BTreeMap::new();
        for key in &keys {
            db.put(key, b"old").unwrap();
            model.insert(key.clone(), b"old".to_vec());
        }
        let old = db.scan(..).unwrap();
        let expected_old = model.clone();
        for pass in 0..4usize {
            for (i, key) in keys.iter().enumerate() {
                if (i + pass) % 3 == 0 {
                    db.delete(key).unwrap();
                    model.remove(key);
                } else {
                    let value = vec![pass as u8; [0, 1, 1024, 17][pass]];
                    db.put(key, &value).unwrap();
                    model.insert(key.clone(), value);
                }
            }
            assert_eq!(rows(&db), model);
            let charge: usize = if persistent {
                keys.iter()
                    .map(|key| 48 + key.len() + model.get(key).map_or(0, Vec::len))
                    .sum()
            } else {
                model
                    .iter()
                    .map(|(key, value)| 48 + key.len() + value.len())
                    .sum()
            };
            assert_eq!(db.stats().active_memtable_bytes, charge);
        }
        assert_eq!(
            old.map(|r| r.map(|(k, v)| (k, v.to_vec())))
                .collect::<Result<BTreeMap<_, _>>>()
                .unwrap(),
            expected_old
        );
        if persistent {
            drop(db);
            let db = Engine::open_with_options(dir.path(), opts).unwrap();
            assert_eq!(rows(&db), model);
            db.flush().unwrap();
            db.compact().unwrap();
            drop(db);
            assert_eq!(rows(&Engine::open(dir.path()).unwrap()), model);
        }
    }
}
