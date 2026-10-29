mod common;
use common::{files, Rng};
use rust_kv_storage_engine::{Engine, EngineError, Options, Result, Scan, WriteBatch};
use std::{
    collections::BTreeMap,
    fs,
    ops::Bound::{self, *},
    sync::{Arc, Barrier},
    thread,
};

fn rows(scan: Scan) -> Vec<(Vec<u8>, Vec<u8>)> {
    scan.map(|row| row.map(|(key, value)| (key, value.to_vec())))
        .collect::<Result<_>>()
        .unwrap()
}

fn keys(scan: Scan) -> Vec<Vec<u8>> {
    rows(scan).into_iter().map(|row| row.0).collect()
}

#[test]
fn bytewise_bounds_include_empty_binary_and_prefix_keys() {
    let db = Engine::new();
    let expected = vec![
        vec![],
        vec![0],
        b"a".to_vec(),
        b"aa".to_vec(),
        b"b".to_vec(),
        vec![255],
    ];
    for key in &expected {
        db.put(key, key).unwrap();
    }
    assert_eq!(keys(db.scan(..).unwrap()), expected);
    assert_eq!(
        keys(db.scan(b"a".as_slice()..b"b".as_slice()).unwrap()),
        [b"a".to_vec(), b"aa".to_vec()]
    );
    assert_eq!(
        keys(db.scan(b"a".as_slice()..=b"b".as_slice()).unwrap()),
        [b"a".to_vec(), b"aa".to_vec(), b"b".to_vec()]
    );
    assert_eq!(
        keys(db.scan(b"b".as_slice()..).unwrap()),
        [b"b".to_vec(), vec![255]]
    );
    assert_eq!(keys(db.scan(..b"a".as_slice()).unwrap()), [vec![], vec![0]]);
    assert_eq!(keys(db.scan(..=b"".as_slice()).unwrap()), [vec![]]);
    assert_eq!(
        keys(
            db.scan((Excluded(b"a".as_slice()), Excluded(b"b".as_slice())))
                .unwrap()
        ),
        [b"aa".to_vec()]
    );
    assert_eq!(
        keys(db.scan(b"a".as_slice()..=b"a".as_slice()).unwrap()),
        [b"a".to_vec()]
    );
    for range in [
        (Included(b"a".as_slice()), Excluded(b"a".as_slice())),
        (Excluded(b"a".as_slice()), Included(b"a".as_slice())),
        (Excluded(b"a".as_slice()), Excluded(b"a".as_slice())),
        (Unbounded, Excluded(b"".as_slice())),
    ] {
        let mut scan = db.scan(range).unwrap();
        assert!(scan.next().is_none());
        assert!(scan.next().is_none());
    }
    assert!(matches!(
        db.scan(b"z".as_slice()..b"a".as_slice()),
        Err(EngineError::InvalidConfig(_))
    ));
    assert!(db
        .scan(b"zz".as_slice()..b"zzz".as_slice())
        .unwrap()
        .next()
        .is_none());
}

#[test]
fn snapshot_merges_versions_deletes_and_empty_values_across_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let opts = Options {
        block_size: 64,
        compaction_file_threshold: 1000,
        ..Options::default()
    };
    let db = Engine::open_with_options(dir.path(), opts.clone()).unwrap();
    db.put(b"a", b"old").unwrap();
    db.put(b"b", b"gone").unwrap();
    db.flush().unwrap();
    db.put(b"a", b"middle").unwrap();
    db.delete(b"b").unwrap();
    db.put(b"c", b"disk").unwrap();
    db.flush().unwrap();
    let mut batch = WriteBatch::new();
    batch
        .put("a", "latest")
        .delete("c")
        .put("d", "")
        .put("e", "memory");
    db.write_batch(batch).unwrap();
    let expected = vec![
        (b"a".to_vec(), b"latest".to_vec()),
        (b"d".to_vec(), vec![]),
        (b"e".to_vec(), b"memory".to_vec()),
    ];
    assert_eq!(rows(db.scan(..).unwrap()), expected);
    drop(db);
    let db = Engine::open_with_options(dir.path(), opts).unwrap();
    assert_eq!(rows(db.scan(..).unwrap()), expected);
    let before = db.scan(..).unwrap();
    db.compact().unwrap();
    assert_eq!(rows(before), expected);
    assert_eq!(rows(db.scan(..).unwrap()), expected);
}

#[test]
fn owned_scan_survives_writes_compaction_and_engine_drop() {
    let dir = tempfile::tempdir().unwrap();
    let db = Engine::open(dir.path()).unwrap();
    db.put(b"a", b"disk").unwrap();
    db.put(b"b", b"old").unwrap();
    db.flush().unwrap();
    db.put(b"m", b"memory").unwrap();
    let mut snapshot = {
        let lo = b"a".to_vec();
        let hi = b"z".to_vec();
        db.scan(lo.as_slice()..hi.as_slice()).unwrap()
    };
    let mut batch = WriteBatch::new();
    batch
        .put("a", "new")
        .delete("b")
        .put("c", "new")
        .delete("m");
    db.write_batch(batch).unwrap();
    db.compact().unwrap();
    assert_eq!(keys(db.scan(..).unwrap()), [b"a".to_vec(), b"c".to_vec()]);
    drop(db);
    assert!(matches!(Engine::open(dir.path()), Err(EngineError::Locked)));
    let actual: Vec<_> = snapshot
        .by_ref()
        .map(|r| {
            let (k, v) = r.unwrap();
            (k, v.to_vec())
        })
        .collect();
    assert_eq!(
        actual,
        [
            (b"a".to_vec(), b"disk".to_vec()),
            (b"b".to_vec(), b"old".to_vec()),
            (b"m".to_vec(), b"memory".to_vec())
        ]
    );
    assert!(snapshot.next().is_none());
    let db = Engine::open(dir.path()).unwrap();
    assert_eq!(keys(db.scan(..).unwrap()), [b"a".to_vec(), b"c".to_vec()]);
}

#[test]
fn dropping_an_unread_scan_releases_its_database_lock() {
    let dir = tempfile::tempdir().unwrap();
    let db = Engine::open(dir.path()).unwrap();
    db.put(b"a", b"value").unwrap();
    let scan = db.scan(..).unwrap();
    drop(db);
    assert!(matches!(Engine::open(dir.path()), Err(EngineError::Locked)));
    drop(scan);
    let db = Engine::open(dir.path()).unwrap();
    let empty = db.scan(b"a".as_slice()..b"a".as_slice()).unwrap();
    drop(db);
    assert!(Engine::open(dir.path()).is_ok());
    drop(empty);
}

fn disk_fixture() -> (
    tempfile::TempDir,
    Engine,
    std::path::PathBuf,
    Vec<u8>,
    Vec<usize>,
) {
    let dir = tempfile::tempdir().unwrap();
    let db = Engine::open_with_options(
        dir.path(),
        Options {
            block_size: 64,
            ..Options::default()
        },
    )
    .unwrap();
    let mut batch = WriteBatch::new();
    for key in [b"a", b"b", b"c", b"d"] {
        batch.put(key, [9; 64]);
    }
    db.write_batch(batch).unwrap();
    db.flush().unwrap();
    let path = files(&dir.path().join("sst"), "sst").pop().unwrap();
    let bytes = fs::read(&path).unwrap();
    let mut pos = 8;
    let offsets = (0..4)
        .map(|_| {
            let start = pos;
            let len = u32::from_le_bytes(bytes[pos..pos + 4].try_into().unwrap()) as usize;
            pos += 12 + len;
            start
        })
        .collect();
    (dir, db, path, bytes, offsets)
}

#[test]
fn seeking_skips_unrelated_blocks_and_preserves_the_point_cache() {
    let (_dir, db, path, mut bytes, offsets) = disk_fixture();
    db.get(b"b").unwrap();
    let before = db.stats();
    // Corrupt blocks outside [b, d). Neither should be visited by this scan.
    bytes[offsets[0] + 12] ^= 1;
    bytes[offsets[3] + 12] ^= 1;
    fs::write(path, bytes).unwrap();
    let scan = db.scan(b"b".as_slice()..b"d".as_slice()).unwrap();
    assert_eq!(db.stats().block_reads, before.block_reads);
    assert_eq!(keys(scan), [b"b".to_vec(), b"c".to_vec()]);
    let after = db.stats();
    assert_eq!(after.block_reads - before.block_reads, 2);
    assert_eq!(
        (after.cache_bytes, after.cache_hits),
        (before.cache_bytes, before.cache_hits)
    );
    assert!(db.scan(..).unwrap().next().unwrap().is_err());
}

#[test]
fn late_corruption_is_an_error_item_without_prefetch_or_silent_eof() {
    let (dir, db, path, mut bytes, offsets) = disk_fixture();
    bytes[offsets[1] + 12] ^= 1;
    fs::write(path, bytes).unwrap();
    let before = db.stats().block_reads;
    let first = db
        .scan(..)
        .unwrap()
        .take(1)
        .collect::<Result<Vec<_>>>()
        .unwrap();
    assert_eq!(first[0].0, b"a");
    assert_eq!(db.stats().block_reads - before, 1);
    let mut scan = db.scan(..).unwrap();
    assert_eq!(scan.next().unwrap().unwrap().0, b"a");
    assert!(matches!(scan.next(), Some(Err(EngineError::Corruption(_)))));
    assert!(scan.next().is_none());
    assert!(scan.next().is_none());
    drop(db);
    // A terminal iterator no longer pins the database lock.
    assert!(Engine::open(dir.path()).is_ok());
}

#[test]
fn truncated_sst_returns_an_io_error_and_then_terminates() {
    let (_dir, db, path, _, _) = disk_fixture();
    fs::OpenOptions::new()
        .write(true)
        .open(path)
        .unwrap()
        .set_len(8)
        .unwrap();
    let mut scan = db.scan(..).unwrap();
    assert!(matches!(scan.next(), Some(Err(EngineError::Io(_)))));
    assert!(scan.next().is_none());
}

fn inside(key: &[u8], start: &Bound<Vec<u8>>, end: &Bound<Vec<u8>>) -> bool {
    let after = match start {
        Included(v) => key >= v.as_slice(),
        Excluded(v) => key > v.as_slice(),
        Unbounded => true,
    };
    let before = match end {
        Included(v) => key <= v.as_slice(),
        Excluded(v) => key < v.as_slice(),
        Unbounded => true,
    };
    after && before
}

#[test]
fn persistent_bounds_match_a_filtered_model_with_and_without_cache() {
    for cache in [0, 4096] {
        let dir = tempfile::tempdir().unwrap();
        let db = Engine::open_with_options(
            dir.path(),
            Options {
                block_size: 64,
                block_cache_capacity: cache,
                ..Options::default()
            },
        )
        .unwrap();
        let model: BTreeMap<_, _> = [
            vec![],
            vec![0],
            b"a".to_vec(),
            b"aa".to_vec(),
            b"b".to_vec(),
            vec![255],
        ]
        .into_iter()
        .map(|k| (k, vec![7; 20]))
        .collect();
        let mut batch = WriteBatch::new();
        for (k, v) in &model {
            batch.put(k, v);
        }
        db.write_batch(batch).unwrap();
        db.flush().unwrap();
        let mut bounds = vec![Unbounded];
        for key in [
            vec![],
            vec![0],
            b"a".to_vec(),
            b"ab".to_vec(),
            b"z".to_vec(),
            vec![255],
            vec![255, 255],
        ] {
            bounds.push(Included(key.clone()));
            bounds.push(Excluded(key));
        }
        for start in &bounds {
            for end in &bounds {
                if let (Included(a) | Excluded(a), Included(b) | Excluded(b)) = (start, end) {
                    if a > b {
                        continue;
                    }
                }
                let expected: Vec<_> = model
                    .iter()
                    .filter(|(k, _)| inside(k, start, end))
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect();
                let range = (
                    start.as_ref().map(Vec::as_slice),
                    end.as_ref().map(Vec::as_slice),
                );
                assert_eq!(
                    rows(db.scan(range).unwrap()),
                    expected,
                    "{start:?}..{end:?}"
                );
            }
        }
    }
}

#[test]
fn randomized_batches_flushes_and_reopens_match_a_model() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = Engine::open_with_options(dir.path(), common::options()).unwrap();
    let mut model = BTreeMap::new();
    let mut rng = Rng(0x8e32_469a_ef87);
    for round in 0..120 {
        let mut batch = WriteBatch::new();
        for _ in 0..4 {
            let key = vec![(rng.next() % 32) as u8];
            if rng.next() % 3 == 0 {
                batch.delete(&key);
                model.remove(&key);
            } else {
                let value = vec![round as u8; (rng.next() % 80) as usize];
                batch.put(&key, &value);
                model.insert(key, value);
            }
        }
        db.write_batch(batch).unwrap();
        if round % 11 == 0 {
            db.flush().unwrap();
        }
        if round % 23 == 0 {
            db.compact().unwrap();
        }
        if round % 29 == 0 {
            drop(db);
            db = Engine::open_with_options(dir.path(), common::options()).unwrap();
        }
        let a = (rng.next() % 34) as u8;
        let b = (rng.next() % 34) as u8;
        let lo = a.min(b);
        let hi = a.max(b);
        let start = match rng.next() % 3 {
            0 => Unbounded,
            1 => Included(vec![lo]),
            _ => Excluded(vec![lo]),
        };
        let end = match rng.next() % 3 {
            0 => Unbounded,
            1 => Included(vec![hi]),
            _ => Excluded(vec![hi]),
        };
        let expected: Vec<_> = model
            .iter()
            .filter(|(k, _)| inside(k, &start, &end))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        assert_eq!(
            rows(
                db.scan((
                    start.as_ref().map(Vec::as_slice),
                    end.as_ref().map(Vec::as_slice)
                ))
                .unwrap()
            ),
            expected,
            "round {round}"
        );
        assert_eq!(
            rows(db.scan(..).unwrap()),
            model
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect::<Vec<_>>()
        );
    }
}

#[test]
fn concurrent_scans_never_observe_half_a_batch() {
    let db = Engine::new();
    let mut initial = WriteBatch::new();
    initial.put("a", [0]).put("b", [0]);
    db.write_batch(initial).unwrap();
    let barrier = Arc::new(Barrier::new(2));
    let writer_db = db.clone();
    let writer_barrier = barrier.clone();
    let writer = thread::spawn(move || {
        writer_barrier.wait();
        for n in 0u64..1000 {
            let mut batch = WriteBatch::new();
            batch.put("a", n.to_le_bytes()).put("b", n.to_le_bytes());
            writer_db.write_batch(batch).unwrap();
            thread::yield_now();
        }
    });
    barrier.wait();
    for _ in 0..1000 {
        let snapshot = rows(db.scan(..).unwrap());
        assert_eq!(snapshot.len(), 2);
        assert_eq!(snapshot[0].1, snapshot[1].1);
        thread::yield_now();
    }
    writer.join().unwrap();
}

#[test]
fn bounds_proven_empty_scans_release_the_lock_and_remain_empty() {
    for mode in 0..4 {
        let dir = tempfile::tempdir().unwrap();
        let db = Engine::open(dir.path()).unwrap();
        if mode != 0 {
            db.put(b"a", b"old").unwrap();
        }
        if mode >= 2 {
            db.flush().unwrap();
        }
        let db = if mode == 3 {
            drop(db);
            Engine::open(dir.path()).unwrap()
        } else {
            db
        };
        let before = db.stats().block_reads;
        let mut empty = db.scan(b"z".as_slice()..).unwrap();
        assert_eq!(db.stats().block_reads, before);
        drop(db);
        let reopened = Engine::open(dir.path()).unwrap();
        reopened.put(b"z", b"new").unwrap();
        assert!(empty.next().is_none());
        assert!(empty.next().is_none());
        assert_eq!(reopened.get(b"z").unwrap().unwrap().as_ref(), b"new");
    }
}
