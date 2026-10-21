use super::*;
use std::time::Instant;

// Copied unchanged into baseline; only the public compact request is timed.
#[test]
#[ignore = "paired release explicit compaction diagnostic"]
fn compact_request_profile() {
    for scenario in ["automatic-empty", "automatic-active", "repeat-clean"] {
        let dir = tempfile::tempdir().unwrap();
        let db = Engine::open_with_options(
            dir.path(),
            Options {
                memtable_size_limit: 32 * 1024 * 1024,
                block_size: 4096,
                compaction_style: CompactionStyle::Full,
                compaction_file_threshold: 4,
                ..Options::default()
            },
        )
        .unwrap();
        let core = &db.inner.core;
        core.stop.store(true, Ordering::Release);
        core.wake();
        db.inner.worker.lock().take().unwrap().join().unwrap();
        core.stop.store(false, Ordering::Release);
        for source in 0..4u64 {
            let mut batch = WriteBatch::new();
            for row in 0..2048u64 {
                batch.put((row * 4 + source).to_be_bytes(), [source as u8; 1024]);
            }
            db.write_batch(batch).unwrap();
            if scenario != "automatic-active" || source != 3 {
                let _guard = core.maintenance.lock();
                core.freeze(&mut core.writer.lock()).unwrap();
                core.flush_one().unwrap();
            }
        }
        if scenario == "repeat-clean" {
            db.compact().unwrap();
        }
        let before = db.stats();
        let start = Instant::now();
        db.compact().unwrap();
        let micros = start.elapsed().as_secs_f64() * 1e6;
        let after = db.stats();
        assert_eq!(after.sst_files, 1);
        assert_eq!(after.sst_records, 8192);
        assert_eq!(after.active_memtable_bytes, 0);
        assert_eq!(after.immutable_memtables, 0);
        println!("COMPACT_REQUEST_PROFILE {{\"case\":\"{scenario}\",\"micros\":{micros},\"compactions\":{},\"input_bytes\":{},\"output_bytes\":{}}}",
            after.compactions - before.compactions,
            after.compaction_input_bytes - before.compaction_input_bytes,
            after.compaction_output_bytes - before.compaction_output_bytes);
        drop(db);
        let db = Engine::open(dir.path()).unwrap();
        for row in 0..8192u64 {
            assert_eq!(
                db.get(&row.to_be_bytes()).unwrap().as_deref(),
                Some([(row % 4) as u8; 1024].as_slice())
            );
        }
    }
}
