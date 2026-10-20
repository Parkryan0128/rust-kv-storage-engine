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
        let drain_us = start.elapsed().as_secs_f64() * 1e6;
        // The writer can freeze the second table after the driver sees no work.
        {
            let mut schedule = core.maintenance.lock();
            while core.maintenance_step(true, &mut schedule).unwrap() {}
        }
        db.flush().unwrap();
        let stats = db.stats();
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
            "MAINTENANCE_PROFILE {{\"tables\":{tables},\"blocked_write_us\":{blocked_write_us},\"drain_us\":{drain_us},\"compactions\":{},\"input_bytes\":{}}}",
            stats.compactions, stats.compaction_input_bytes,
        );
    }
}
