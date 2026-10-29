mod common;
use rust_kv_storage_engine::{Engine, Options, Result, Scan, WriteBatch};
use std::{collections::BTreeMap, ops::Bound};

type Rows = Vec<(Vec<u8>, Vec<u8>)>;
fn rows(scan: Scan) -> Rows {
    scan.map(|r| r.map(|(k, v)| (k, v.to_vec())))
        .collect::<Result<_>>()
        .unwrap()
}

fn key(n: u64) -> Vec<u8> {
    let mut key = vec![0xfe; 128];
    key.extend_from_slice(&n.to_be_bytes());
    key.resize(key.len() + n as usize % 17, 0);
    key
}

fn expected(model: &BTreeMap<Vec<u8>, Vec<u8>>) -> Rows {
    model.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
}

#[test]
fn prefix_tables_preserve_bounds_versions_and_deletions_across_reopen() {
    for cache in [0, 4096] {
        let dir = tempfile::tempdir().unwrap();
        let opts = Options {
            block_size: 4096,
            block_cache_capacity: cache,
            compaction_file_threshold: 10000,
            ..Options::default()
        };
        let mut db = Engine::open_with_options(dir.path(), opts.clone()).unwrap();
        let mut model = BTreeMap::new();
        let mut historical = Vec::new();
        for generation in 0..6u8 {
            let mut batch = WriteBatch::new();
            for n in 0..256u64 {
                let k = key(n);
                if generation != 0 && (n + generation as u64) % 5 == 0 {
                    batch.delete(&k);
                    model.remove(&k);
                } else {
                    let value = vec![generation; n as usize % 47];
                    batch.put(&k, &value);
                    model.insert(k, value);
                }
            }
            // Mixed plain frames: empty/binary keys and one oversized value.
            for (k, v) in [
                (vec![], vec![generation]),
                (vec![255], vec![generation; 8192]),
            ] {
                batch.put(&k, &v);
                model.insert(k, v);
            }
            db.write_batch(batch).unwrap();
            historical.push((db.scan(..).unwrap(), expected(&model)));
            db.flush().unwrap();
            assert!(common::files(dir.path(), "sst")
                .iter()
                .any(|p| { std::fs::read(p).unwrap().starts_with(b"RKVSST05") }));
            if generation % 2 != 0 {
                db.compact().unwrap();
            }
            if generation % 2 != 0 {
                for (snapshot, wanted) in historical.drain(..) {
                    assert_eq!(rows(snapshot), wanted);
                }
                drop(db);
                db = Engine::open_with_options(dir.path(), opts.clone()).unwrap();
            }
            assert_eq!(rows(db.scan(..).unwrap()), expected(&model));
            for n in 0..256 {
                let k = key(n);
                assert_eq!(
                    db.get(&k).unwrap().as_deref(),
                    model.get(&k).map(Vec::as_slice)
                );
            }
            for n in [0, 1, 15, 16, 127, 128, 255] {
                let mut probes = vec![key(n), key(n)[..128].to_vec()];
                let mut after = key(n);
                after.push(255);
                probes.push(after);
                for probe in probes {
                    for inclusive in [false, true] {
                        let lower = if inclusive {
                            Bound::Included(probe.as_slice())
                        } else {
                            Bound::Excluded(probe.as_slice())
                        };
                        let wanted: Rows = model
                            .iter()
                            .filter(|(k, _)| {
                                if inclusive {
                                    k.as_slice() >= probe.as_slice()
                                } else {
                                    k.as_slice() > probe.as_slice()
                                }
                            })
                            .map(|(k, v)| (k.clone(), v.clone()))
                            .collect();
                        assert_eq!(rows(db.scan((lower, Bound::Unbounded)).unwrap()), wanted);
                        let upper = if inclusive {
                            Bound::Included(probe.as_slice())
                        } else {
                            Bound::Excluded(probe.as_slice())
                        };
                        let wanted: Rows = model
                            .iter()
                            .filter(|(k, _)| {
                                if inclusive {
                                    k.as_slice() <= probe.as_slice()
                                } else {
                                    k.as_slice() < probe.as_slice()
                                }
                            })
                            .map(|(k, v)| (k.clone(), v.clone()))
                            .collect();
                        assert_eq!(rows(db.scan((Bound::Unbounded, upper)).unwrap()), wanted);
                    }
                }
            }
        }
        for (snapshot, wanted) in historical {
            assert_eq!(rows(snapshot), wanted);
        }
        let mut delete = WriteBatch::new();
        for k in model.keys() {
            delete.delete(k);
        }
        db.write_batch(delete).unwrap();
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

#[cfg(feature = "fault-injection")]
#[test]
fn prefix_publication_crashes_preserve_acknowledged_batches() {
    use std::{process::Command, time::Duration};
    for compact in [false, true] {
        for io_error in [false, true] {
            for point in [
                "sst_before_sync",
                "sst_after_sync",
                "sst_before_dir_sync",
                "sst_after_rename",
                "manifest_before_sync",
                "manifest_after_rename",
                "manifest_after_dir_sync",
            ] {
                let dir = tempfile::tempdir().unwrap();
                let opts = Options {
                    compaction_file_threshold: 10000,
                    ..Options::default()
                };
                let db = Engine::open_with_options(dir.path(), opts).unwrap();
                let mut model = BTreeMap::new();
                for generation in 0..3u8 {
                    let mut batch = WriteBatch::new();
                    for n in 0..128 {
                        let k = key(n);
                        if generation == 2 && n % 3 == 0 {
                            batch.delete(&k);
                            model.remove(&k);
                        } else {
                            let v = vec![generation; 32];
                            batch.put(&k, &v);
                            model.insert(k, v);
                        }
                    }
                    db.write_batch(batch).unwrap();
                    if generation < 2 || compact {
                        db.flush().unwrap();
                    }
                }
                assert!(common::files(dir.path(), "sst")
                    .iter()
                    .all(|p| { std::fs::read(p).unwrap().starts_with(b"RKVSST05") }));
                drop(db);
                let mode = match (compact, io_error) {
                    (false, false) => "flush-only",
                    (false, true) => "error",
                    (true, false) => "compact",
                    (true, true) => "compact-error",
                };
                let status = common::run_with_deadline(
                    Command::new(env!("CARGO_BIN_EXE_crash_worker"))
                        .arg(dir.path())
                        .arg(mode)
                        .arg(point),
                    Duration::from_secs(10),
                )
                .unwrap();
                assert_eq!(
                    status.code(),
                    Some(if io_error { 0 } else { 86 }),
                    "{mode}/{point}"
                );
                let db = Engine::open(dir.path()).unwrap();
                assert_eq!(
                    rows(db.scan(..).unwrap()),
                    expected(&model),
                    "{mode}/{point}"
                );
                db.compact().unwrap();
                drop(db);
                assert_eq!(
                    rows(Engine::open(dir.path()).unwrap().scan(..).unwrap()),
                    expected(&model)
                );
            }
        }
    }
}
