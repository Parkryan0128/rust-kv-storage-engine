use super::*;
use std::{
    sync::mpsc::sync_channel,
    time::{Duration, Instant},
};

// Copied unchanged into the comparison revision. The barrier injects identical
// write pressure immediately after the first durable merge publication.
#[test]
#[ignore = "paired release maintenance scheduling diagnostic"]
fn maintenance_yield_profile() {
    for tables in [17u64, 33] {
        let dir = tempfile::tempdir().unwrap();
        let db = Engine::open_with_options(
            dir.path(),
            Options {
                memtable_size_limit: 128 * 1024,
                max_immutable_memtables: 1,
                block_size: 4096,
                compaction_style: CompactionStyle::Full,
                compaction_file_threshold: 17,
                ..Options::default()
            },
        )
        .unwrap();
        let core = &db.inner.core;
        core.stop.store(true, Ordering::Release);
        core.wake();
        db.inner.worker.lock().take().unwrap().join().unwrap();
        core.stop.store(false, Ordering::Release);
        {
            let _guard = core.maintenance.lock();
            for source in 0..tables {
                let mut batch = WriteBatch::new();
                for row in 0..64 {
                    batch.put((row * tables + source).to_be_bytes(), [source as u8; 4096]);
                }
                db.write_batch(batch).unwrap();
                core.freeze(&mut core.writer.lock()).unwrap();
                core.flush_one().unwrap();
            }
        }
        assert_eq!(db.stats().sst_files, tables as usize);
        let (entered_tx, entered_rx) = sync_channel(1);
        let (resume_tx, resume_rx) = sync_channel(1);
        let worker_db = db.clone();
        let worker = thread::spawn(move || {
            crate::test_hooks::arm("compaction_between_groups", entered_tx, resume_rx);
            let core = &worker_db.inner.core;
            loop {
                let mut schedule = core.maintenance.lock();
                let progress = core.maintenance_step(true, &mut schedule).unwrap();
                parking_lot::MutexGuard::unlock_fair(schedule);
                if !progress {
                    break;
                }
            }
        });
        entered_rx.recv_timeout(Duration::from_secs(10)).unwrap();
        db.put(b"frozen", &[7; 128 * 1024]).unwrap();
        core.freeze(&mut core.writer.lock()).unwrap();
        db.put(b"active", &[8; 128 * 1024]).unwrap();
        let (blocked_tx, blocked_rx) = sync_channel(1);
        let (release_tx, release_rx) = sync_channel(1);
        let (done_tx, done_rx) = sync_channel(1);
        let writer_db = db.clone();
        let writer = thread::spawn(move || {
            crate::test_hooks::arm("write_backpressure", blocked_tx, release_rx);
            done_tx.send(writer_db.put(b"after", b"yes")).unwrap();
        });
        blocked_rx.recv_timeout(Duration::from_secs(10)).unwrap();
        release_tx.send(()).unwrap();
        let start = Instant::now();
        resume_tx.send(()).unwrap();
        done_rx
            .recv_timeout(Duration::from_secs(10))
            .unwrap()
            .unwrap();
        let blocked_write_us = start.elapsed().as_secs_f64() * 1e6;
        writer.join().unwrap();
        worker.join().unwrap();
        let driver_join_us = start.elapsed().as_secs_f64() * 1e6;
        let (drain_us, stats) = finish_drain(&db, start);
        assert_eq!(stats.max_compaction_inputs, 16);
        drop(db);
        let db = Engine::open(dir.path()).unwrap();
        for source in 0..tables {
            for row in 0..64 {
                assert_eq!(
                    db.get(&(row * tables + source).to_be_bytes())
                        .unwrap()
                        .as_deref(),
                    Some([source as u8; 4096].as_slice())
                );
            }
        }
        assert_eq!(
            db.get(b"frozen").unwrap().as_deref(),
            Some([7; 128 * 1024].as_slice())
        );
        assert_eq!(
            db.get(b"active").unwrap().as_deref(),
            Some([8; 128 * 1024].as_slice())
        );
        assert_eq!(db.get(b"after").unwrap().unwrap(), "yes");
        println!(
            "MAINTENANCE_PROFILE {{\"tables\":{tables},\"blocked_write_us\":{blocked_write_us},\"drain_us\":{drain_us},\"driver_join_us\":{driver_join_us},\"compactions\":{},\"input_bytes\":{}}}",
            stats.compactions, stats.compaction_input_bytes,
        );
    }
}

// The driver may exit before a racing writer freezes its second table. Include
// that remaining work and the final active WAL in the completion measurement.
fn finish_drain(db: &Engine, start: Instant) -> (f64, Stats) {
    let core = &db.inner.core;
    {
        let mut schedule = core.maintenance.lock();
        while core.maintenance_step(true, &mut schedule).unwrap() {}
    }
    crate::test_hooks::pause("maintenance_before_final_flush");
    db.flush().unwrap();
    let drain_us = start.elapsed().as_secs_f64() * 1e6;
    let state = core.state.read();
    assert!(state.mem.data.is_empty());
    assert!(state.immutable.is_empty());
    drop(state);
    assert!(core.maintenance.lock().pending.is_empty());
    assert!(core.compaction_inputs().is_empty());
    (drain_us, db.stats())
}

#[test]
fn completion_timing_includes_work_after_driver_exit_and_final_active_flush() {
    for late_rotation in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let db = Engine::open(dir.path()).unwrap();
        let core = &db.inner.core;
        core.stop.store(true, Ordering::Release);
        core.wake();
        db.inner.worker.lock().take().unwrap().join().unwrap();
        core.stop.store(false, Ordering::Release);
        db.put(b"frozen", b"value").unwrap();
        if !late_rotation {
            core.freeze(&mut core.writer.lock()).unwrap();
        }
        while core.background_step().unwrap() {}
        if late_rotation {
            core.freeze(&mut core.writer.lock()).unwrap();
        }
        db.put(b"active", b"value").unwrap();
        let (entered_tx, entered_rx) = sync_channel(1);
        let (resume_tx, resume_rx) = sync_channel(1);
        let worker_db = db.clone();
        let start = Instant::now();
        let worker = thread::spawn(move || {
            crate::test_hooks::arm("maintenance_before_final_flush", entered_tx, resume_rx);
            finish_drain(&worker_db, start)
        });
        entered_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        let still_active = db.stats().active_memtable_bytes > 0;
        let earliest_completion = start.elapsed().as_secs_f64() * 1e6;
        resume_tx.send(()).unwrap();
        let (drain_us, stats) = worker.join().unwrap();
        assert!(still_active);
        assert!(drain_us >= earliest_completion);
        assert_eq!(stats.active_memtable_bytes, 0);
        assert_eq!(stats.immutable_memtables, 0);
        assert_eq!(stats.sst_records, 2);
        drop(db);
        let db = Engine::open(dir.path()).unwrap();
        for key in [b"frozen".as_slice(), b"active"] {
            assert_eq!(db.get(key).unwrap().unwrap(), "value");
        }
    }
}
