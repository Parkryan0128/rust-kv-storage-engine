mod common;
use common::*;
use rust_kv_storage_engine::{Engine, EngineError, Options};
use std::{
    collections::BTreeMap,
    fs,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Barrier,
    },
    thread,
    time::{Duration, Instant},
};
#[test]
fn volatile_api_and_shared_clones() {
    let e = Engine::new();
    let clone = e.clone();
    assert_eq!(e.get(b"missing").unwrap(), None);
    e.put(b"", b"").unwrap();
    assert_eq!(clone.get(b"").unwrap().as_deref(), Some(&b""[..]));
    e.put(&[0, 255, 0], &[255, 0, 1]).unwrap();
    clone.delete(&[0, 255, 0]).unwrap();
    assert_eq!(e.get(&[0, 255, 0]).unwrap(), None);
    e.delete(b"missing").unwrap();
    e.put(b"missing", b"resurrect").unwrap();
    assert_eq!(clone.get(b"missing").unwrap().unwrap(), "resurrect");
    e.flush().unwrap();
    e.compact().unwrap();
}
#[test]
fn durable_binary_roundtrip_and_repeated_reopen() {
    let d = tempfile::tempdir().unwrap();
    for round in 0..12u8 {
        let e = Engine::open_with_options(d.path(), options()).unwrap();
        for key in 0..round {
            assert_eq!(
                e.get(&[key, 0, 255]).unwrap().as_deref(),
                Some(&[key; 1024][..])
            );
        }
        e.put(&[round, 0, 255], &[round; 1024]).unwrap();
        if round % 3 == 0 {
            e.flush().unwrap();
        }
        if round % 4 == 0 {
            e.compact().unwrap();
        }
    }
}
#[test]
fn newest_value_and_tombstone_across_every_layer() {
    let d = tempfile::tempdir().unwrap();
    let mut o = options();
    o.compaction_file_threshold = 100;
    let e = Engine::open_with_options(d.path(), o.clone()).unwrap();
    for generation in 0..12u8 {
        e.put(b"k", &[generation]).unwrap();
        e.flush().unwrap();
        assert_eq!(e.get(b"k").unwrap().as_deref(), Some(&[generation][..]));
    }
    e.delete(b"k").unwrap();
    assert_eq!(e.get(b"k").unwrap(), None);
    e.flush().unwrap();
    assert_eq!(e.get(b"k").unwrap(), None);
    e.compact().unwrap();
    assert_eq!(e.stats().sst_records, 0);
    drop(e);
    let e = Engine::open_with_options(d.path(), o).unwrap();
    assert_eq!(e.get(b"k").unwrap(), None);
    e.put(b"k", b"again").unwrap();
    e.flush().unwrap();
    assert_eq!(e.get(b"k").unwrap().unwrap(), "again");
}
#[test]
fn directory_lock_lives_until_last_clone_drops() {
    let d = tempfile::tempdir().unwrap();
    let e = Engine::open(d.path()).unwrap();
    let e2 = e.clone();
    assert!(matches!(Engine::open(d.path()), Err(EngineError::Locked)));
    drop(e);
    assert!(matches!(Engine::open(d.path()), Err(EngineError::Locked)));
    drop(e2);
    Engine::open(d.path()).unwrap();
}
#[test]
fn invalid_options_and_record_limits_do_not_mutate_state() {
    let d = tempfile::tempdir().unwrap();
    let bad = [
        Options {
            memtable_size_limit: 0,
            ..options()
        },
        Options {
            max_immutable_memtables: 0,
            ..options()
        },
        Options {
            block_size: 0,
            ..options()
        },
        Options {
            bloom_filter_bits_per_key: 0,
            ..options()
        },
        Options {
            compaction_file_threshold: 1,
            ..options()
        },
        Options {
            max_value_size: usize::MAX,
            ..options()
        },
    ];
    for o in bad {
        assert!(matches!(
            Engine::open_with_options(d.path(), o),
            Err(EngineError::InvalidConfig(_))
        ));
    }
    let e = Engine::open_with_options(
        d.path(),
        Options {
            max_key_size: 8,
            max_value_size: 16,
            ..options()
        },
    )
    .unwrap();
    assert!(e.put(&[0; 9], b"v").is_err());
    assert!(e.put(b"k", &[0; 17]).is_err());
    assert!(e.delete(&[0; 9]).is_err());
    assert_eq!(e.stats().sequence, 0);
    e.put(&[0; 8], &[1; 16]).unwrap();
    e.flush().unwrap();
    assert_eq!(e.get(&[0; 8]).unwrap().unwrap().len(), 16);
}
#[test]
fn large_record_crosses_block_and_memtable_limits() {
    let d = tempfile::tempdir().unwrap();
    let e = Engine::open_with_options(d.path(), options()).unwrap();
    let value = vec![71; 1024 * 1024];
    e.put(b"huge", &value).unwrap();
    e.put(b"next", b"small").unwrap();
    e.flush().unwrap();
    e.compact().unwrap();
    drop(e);
    let e = Engine::open(d.path()).unwrap();
    assert_eq!(e.get(b"huge").unwrap().as_deref(), Some(value.as_slice()));
}
#[test]
fn bloom_skips_reads_and_cache_reuses_blocks_with_capacity_bound() {
    let d = tempfile::tempdir().unwrap();
    let mut o = options();
    o.memtable_size_limit = 1024 * 1024;
    o.compaction_file_threshold = 100;
    let e = Engine::open_with_options(d.path(), o.clone()).unwrap();
    for i in 0..2000u64 {
        e.put(&i.to_be_bytes(), b"value").unwrap();
    }
    e.flush().unwrap();
    drop(e);
    let e = Engine::open_with_options(d.path(), o).unwrap();
    for i in 10000..15000u64 {
        assert_eq!(e.get(&i.to_be_bytes()).unwrap(), None);
    }
    let s = e.stats();
    assert!(s.bloom_negatives > 4800, "{s:?}");
    assert!(s.block_reads < 200, "{s:?}");
    e.get(&999u64.to_be_bytes()).unwrap();
    let before = e.stats();
    for _ in 0..100 {
        e.get(&999u64.to_be_bytes()).unwrap();
    }
    let after = e.stats();
    assert_eq!(after.block_reads, before.block_reads);
    assert!(after.cache_hits >= before.cache_hits + 100);
    assert!(after.cache_bytes <= 8192);
}
#[test]
fn disabled_cache_reads_each_time() {
    let d = tempfile::tempdir().unwrap();
    let e = Engine::open_with_options(
        d.path(),
        Options {
            block_cache_capacity: 0,
            ..options()
        },
    )
    .unwrap();
    e.put(b"k", b"v").unwrap();
    e.flush().unwrap();
    for _ in 0..5 {
        assert_eq!(e.get(b"k").unwrap().unwrap(), "v");
    }
    assert_eq!(e.stats().block_reads, 5);
    assert_eq!(e.stats().cache_bytes, 0);
}
#[test]
fn differential_100000_operations_with_reopen_and_compaction() {
    let d = tempfile::tempdir().unwrap();
    let mut e = Engine::open_with_options(d.path(), options()).unwrap();
    let mut model = BTreeMap::new();
    let mut rng = Rng(0xace123);
    let keys = 257;
    for i in 0..100000 {
        let k = (rng.next() % keys as u64).to_be_bytes().to_vec();
        match rng.next() % 10 {
            0..=4 => {
                let v = vec![(rng.next() % 256) as u8; (rng.next() % 120) as usize];
                e.put(&k, &v).unwrap();
                model.insert(k, v);
            }
            5..=6 => {
                e.delete(&k).unwrap();
                model.remove(&k);
            }
            _ => assert_eq!(
                e.get(&k).unwrap().as_deref(),
                model.get(&k).map(Vec::as_slice)
            ),
        }
        if i % 2000 == 1999 {
            verify(&e, &model, keys);
            e.compact().unwrap();
            drop(e);
            e = Engine::open_with_options(d.path(), options()).unwrap();
            verify(&e, &model, keys);
        }
    }
    e.compact().unwrap();
    assert_eq!(e.stats().sst_records, model.len() as u64);
    verify(&e, &model, keys);
}
#[test]
fn ten_writers_ten_readers_with_background_compaction_and_recovery() {
    let d = tempfile::tempdir().unwrap();
    let e = Engine::open_with_options(d.path(), options()).unwrap();
    let start = Arc::new(Barrier::new(21));
    let done = Arc::new(AtomicBool::new(false));
    let mut writers = vec![];
    let mut readers = vec![];
    for t in 0..10u64 {
        let e = e.clone();
        let start = start.clone();
        writers.push(thread::spawn(move || {
            start.wait();
            for i in 0..500u64 {
                let key = (t * 500 + i).to_be_bytes();
                e.put(&key, &key).unwrap();
                if i % 3 == 0 {
                    e.delete(&key).unwrap();
                }
                if i % 7 == 0 {
                    e.put(&key, &key).unwrap();
                }
            }
        }));
    }
    for t in 0..10u64 {
        let e = e.clone();
        let start = start.clone();
        let done = done.clone();
        readers.push(thread::spawn(move || {
            let mut r = Rng(t + 1);
            start.wait();
            while !done.load(Ordering::Acquire) {
                let k = (r.next() % 5000).to_be_bytes();
                if let Some(v) = e.get(&k).unwrap() {
                    assert_eq!(&v[..], &k);
                }
            }
        }));
    }
    start.wait();
    for w in writers {
        w.join().unwrap();
    }
    done.store(true, Ordering::Release);
    for r in readers {
        r.join().unwrap();
    }
    e.compact().unwrap();
    drop(e);
    let e = Engine::open(d.path()).unwrap();
    for t in 0..10u64 {
        for i in 0..500u64 {
            let k = (t * 500 + i).to_be_bytes();
            let absent = i % 3 == 0 && i % 7 != 0;
            assert_eq!(
                e.get(&k).unwrap().as_deref(),
                if absent { None } else { Some(&k[..]) }
            );
        }
    }
}
#[test]
fn contended_key_returns_complete_values_and_final_barrier_wins() {
    let d = tempfile::tempdir().unwrap();
    let e = Engine::open_with_options(d.path(), options()).unwrap();
    let mut jobs = vec![];
    for t in 0..8u8 {
        let e = e.clone();
        jobs.push(thread::spawn(move || {
            for _ in 0..300 {
                e.put(b"hot", &[t; 256]).unwrap();
                if let Some(v) = e.get(b"hot").unwrap() {
                    assert_eq!(v.len(), 256);
                    assert!(v.iter().all(|b| *b == v[0]));
                }
                e.delete(b"hot").unwrap();
            }
        }));
    }
    for j in jobs {
        j.join().unwrap();
    }
    e.put(b"hot", b"final").unwrap();
    e.compact().unwrap();
    drop(e);
    assert_eq!(
        Engine::open(d.path())
            .unwrap()
            .get(b"hot")
            .unwrap()
            .unwrap(),
        "final"
    );
}
#[test]
fn concurrent_manual_flush_and_compact_do_not_deadlock_or_lose_data() {
    let d = tempfile::tempdir().unwrap();
    let e = Engine::open_with_options(d.path(), options()).unwrap();
    let mut jobs = vec![];
    for t in 0..3u64 {
        let e = e.clone();
        jobs.push(thread::spawn(move || {
            for i in 0..400u64 {
                e.put(&(t * 400 + i).to_be_bytes(), b"ok").unwrap();
                if i % 29 == 0 {
                    e.flush().unwrap();
                }
            }
        }));
    }
    let c = e.clone();
    let compact = thread::spawn(move || {
        for _ in 0..20 {
            c.compact().unwrap();
        }
    });
    for j in jobs {
        j.join().unwrap();
    }
    compact.join().unwrap();
    e.flush().unwrap();
    for i in 0..1200u64 {
        assert_eq!(e.get(&i.to_be_bytes()).unwrap().unwrap(), "ok");
    }
}

#[test]
fn flush_completes_while_a_producer_keeps_writing() {
    if std::env::var_os("KV_FLUSH_CHILD").is_none() {
        let mut command = std::process::Command::new(std::env::current_exe().unwrap());
        command
            .args([
                "--exact",
                "flush_completes_while_a_producer_keeps_writing",
                "--nocapture",
            ])
            .env("KV_FLUSH_CHILD", "1");
        assert!(run_with_deadline(&mut command, Duration::from_secs(30))
            .expect("flush subprocess exceeded deadline and was killed")
            .success());
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let db = Engine::open_with_options(dir.path(), options()).unwrap();
    let stop = Arc::new(AtomicBool::new(false));
    let (ready_tx, ready_rx) = std::sync::mpsc::channel();
    let writer_db = db.clone();
    let writer_stop = stop.clone();
    let writer = thread::spawn(move || {
        let mut generation = 0u64;
        while !writer_stop.load(Ordering::Acquire) {
            writer_db
                .put(&(generation % 16).to_be_bytes(), &[42; 2048])
                .unwrap();
            generation += 1;
            if generation == 16 {
                ready_tx.send(()).unwrap();
            }
        }
    });
    let ready = ready_rx.recv_timeout(Duration::from_secs(10));
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let flush_db = db.clone();
    let flush = thread::spawn(move || {
        done_tx.send(flush_db.flush()).unwrap();
    });
    let completed = done_rx.recv_timeout(Duration::from_secs(10));
    // Stop and join even on timeout, so a failed assertion leaves no writer.
    stop.store(true, Ordering::Release);
    writer.join().unwrap();
    flush.join().unwrap();
    ready.unwrap();
    completed
        .expect("flush waited for the producer to stop")
        .unwrap();
    drop(db);
    let db = Engine::open(dir.path()).unwrap();
    for key in 0..16u64 {
        assert_eq!(db.get(&key.to_be_bytes()).unwrap().unwrap().len(), 2048);
    }
}

#[test]
fn liveness_harness_terminates_a_stalled_child() {
    if std::env::var_os("KV_STALLED_CHILD").is_some() {
        loop {
            thread::park_timeout(Duration::from_secs(1));
        }
    }
    let mut command = std::process::Command::new(std::env::current_exe().unwrap());
    command
        .args(["--exact", "liveness_harness_terminates_a_stalled_child"])
        .env("KV_STALLED_CHILD", "1");
    assert!(run_with_deadline(&mut command, Duration::from_millis(200)).is_none());
}

#[test]
fn full_compaction_bounds_fan_in_and_preserves_deletions_across_rounds() {
    let dir = tempfile::tempdir().unwrap();
    let options = Options {
        compaction_file_threshold: 1000,
        ..Options::default()
    };
    let db = Engine::open_with_options(dir.path(), options.clone()).unwrap();
    for generation in 0..11u8 {
        db.put(&[generation], &vec![generation; 128 * 1024])
            .unwrap();
        db.put(b"hot", &[generation]).unwrap();
        if generation == 0 {
            db.put(b"deleted", b"old").unwrap();
        }
        if generation == 5 {
            db.delete(b"deleted").unwrap();
        }
        db.flush().unwrap();
    }
    assert_eq!(db.stats().sst_files, 11);
    db.compact().unwrap();
    assert_eq!(db.stats().sst_files, 1);
    assert_eq!(db.stats().max_compaction_inputs, 4);
    assert!(db.stats().compactions > 1);
    assert_eq!(db.get(b"deleted").unwrap(), None);
    drop(db);
    let db = Engine::open_with_options(dir.path(), options).unwrap();
    for generation in 0..11u8 {
        assert_eq!(
            db.get(&[generation]).unwrap().unwrap().as_ref(),
            vec![generation; 128 * 1024]
        );
    }
    assert_eq!(db.get(b"hot").unwrap().unwrap().as_ref(), &[10]);
    assert_eq!(db.get(b"deleted").unwrap(), None);
}

#[test]
fn deleting_all_keys_reclaims_cached_blocks_and_empty_table_metadata() {
    let dir = tempfile::tempdir().unwrap();
    let db = Engine::open_with_options(
        dir.path(),
        Options {
            compaction_file_threshold: 1000,
            ..Options::default()
        },
    )
    .unwrap();
    for key in 0..256u64 {
        db.put(&key.to_be_bytes(), b"value").unwrap();
    }
    db.flush().unwrap();
    let held = db.get(&0u64.to_be_bytes()).unwrap().unwrap();
    assert!(db.stats().cache_bytes > 0);
    for key in 0..256u64 {
        db.delete(&key.to_be_bytes()).unwrap();
    }
    db.compact().unwrap();
    assert_eq!(db.stats().sst_records, 0);
    assert_eq!(db.stats().cache_bytes, 0);
    assert_eq!(db.stats().sst_bytes, 88);
    assert_eq!(held, "value");
    drop(db);
    let db = Engine::open(dir.path()).unwrap();
    assert_eq!(db.stats().sst_bytes, 88);
    assert_eq!(db.get(&0u64.to_be_bytes()).unwrap(), None);
}

#[test]
fn background_flush_bounds_memory_and_reclaims_wals() {
    let d = tempfile::tempdir().unwrap();
    let e = Engine::open_with_options(d.path(), options()).unwrap();
    for i in 0..5000u64 {
        e.put(&i.to_be_bytes(), &[42; 100]).unwrap();
        let s = e.stats();
        assert!(s.immutable_memtables <= 2);
        assert!(s.active_memtable_bytes < 2200);
        assert!(s.immutable_memtable_bytes < 4400);
    }
    e.flush().unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    while files(&d.path().join("wal"), "wal").len() > 1 {
        assert!(Instant::now() < deadline);
        thread::yield_now();
    }
    let mut buckets = std::collections::BTreeMap::new();
    for path in files(&d.path().join("sst"), "sst") {
        let size = std::fs::metadata(path).unwrap().len();
        *buckets.entry(size.max(1).ilog2()).or_insert(0usize) += 1;
    }
    assert!(buckets
        .values()
        .all(|&n| n < options().compaction_file_threshold));
    e.compact().unwrap();
    assert_eq!(e.stats().sst_records, 5000);
}
#[test]
fn compaction_deduplicates_overwrites_and_removes_deleted_keys() {
    let d = tempfile::tempdir().unwrap();
    let e = Engine::open_with_options(d.path(), options()).unwrap();
    for generation in 0..100u64 {
        for i in 0..100u64 {
            e.put(&i.to_be_bytes(), &generation.to_le_bytes()).unwrap();
        }
    }
    for i in 0..50u64 {
        e.delete(&i.to_be_bytes()).unwrap();
    }
    e.compact().unwrap();
    assert_eq!(e.stats().sst_files, 1);
    assert_eq!(e.stats().sst_records, 50);
    drop(e);
    let e = Engine::open(d.path()).unwrap();
    let expected = 99u64.to_le_bytes();
    for i in 0..100u64 {
        assert_eq!(
            e.get(&i.to_be_bytes()).unwrap().as_deref(),
            if i < 50 { None } else { Some(&expected[..]) }
        );
    }
}
#[test]
fn orphan_sst_and_temporary_files_are_cleaned() {
    let d = tempfile::tempdir().unwrap();
    let e = Engine::open(d.path()).unwrap();
    e.put(b"k", b"v").unwrap();
    e.flush().unwrap();
    drop(e);
    fs::write(
        d.path().join("sst/00000000000000999999.sst"),
        b"uncommitted garbage",
    )
    .unwrap();
    fs::write(d.path().join("sst/unfinished.tmp"), b"partial").unwrap();
    let e = Engine::open(d.path()).unwrap();
    assert_eq!(e.get(b"k").unwrap().unwrap(), "v");
    assert!(!d.path().join("sst/unfinished.tmp").exists());
    assert_eq!(files(&d.path().join("sst"), "sst").len(), 1);
}

#[test]
fn multiple_deterministic_seeds_match_an_independent_model() {
    for seed in [1, 7, 0xdeadbeef, 0x123456789, 0xffffffffffffffff] {
        let d = tempfile::tempdir().unwrap();
        let e = Engine::open_with_options(d.path(), options()).unwrap();
        let mut model = BTreeMap::new();
        let mut rng = Rng(seed);
        for _ in 0..10000 {
            let key = (rng.next() % 128).to_be_bytes().to_vec();
            match rng.next() % 3 {
                0 => {
                    let value = rng.next().to_le_bytes().to_vec();
                    e.put(&key, &value).unwrap();
                    model.insert(key, value);
                }
                1 => {
                    e.delete(&key).unwrap();
                    model.remove(&key);
                }
                _ => assert_eq!(
                    e.get(&key).unwrap().as_deref(),
                    model.get(&key).map(Vec::as_slice)
                ),
            }
        }
        e.compact().unwrap();
        drop(e);
        let e = Engine::open(d.path()).unwrap();
        verify(&e, &model, 128);
    }
}
