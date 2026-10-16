//! Identical internal benchmark copied into baseline and current builds.
use super::*;
use std::{hint::black_box, time::Instant};

#[test]
#[ignore = "bounded active/frozen snapshot benchmark"]
fn remaining_memory_scan_profile() -> Result<()> {
    let keys = 20_000u64;
    for frozen_count in [0, 2] {
        let dir = tempfile::tempdir()?;
        let db = Engine::open_with_options(
            dir.path(),
            Options {
                memtable_size_limit: 64 * 1024 * 1024,
                max_immutable_memtables: 4,
                ..Options::default()
            },
        )?;
        let maintenance = db.inner.core.maintenance.lock();
        for generation in 0..=frozen_count {
            let mut batch = WriteBatch::new();
            for k in 0..keys {
                // Frozen-only case ends with an empty active buffer.
                batch.put(k.to_be_bytes(), vec![generation as u8; 128]);
                if batch.len() == 256 {
                    db.write_batch(batch)?;
                    batch = WriteBatch::new();
                }
            }
            db.write_batch(batch)?;
            if frozen_count != 0 {
                db.inner.core.freeze(&mut db.inner.core.writer.lock())?;
            }
        }
        for trial in 1..=6 {
            let mut create_ns = 0;
            let mut first_ns = 0;
            let queries = 100;
            let start = Instant::now();
            for _ in 0..queries {
                let one = Instant::now();
                let mut scan = db.scan(..)?;
                create_ns += one.elapsed().as_nanos();
                let first = scan.next().unwrap()?;
                first_ns += one.elapsed().as_nanos();
                assert_eq!(first.0, 0u64.to_be_bytes());
                assert_eq!(first.1.as_ref(), &[frozen_count as u8; 128]);
                for k in 1..10u64 {
                    let row = scan.next().unwrap()?;
                    assert_eq!(row.0, k.to_be_bytes());
                    black_box(row);
                }
                assert!(db.inner.core.state.try_write().is_some());
            }
            let seconds = start.elapsed().as_secs_f64();
            println!(
                "MEMORY_SCAN_PROFILE {}",
                serde_json::json!({
                "case":if frozen_count == 0 {"active"} else {"frozen"},
                "keys":keys,"frozen_buffers":if frozen_count == 0 {0} else {frozen_count + 1},
                "trial":trial,"queries":queries,"consumed_rows":10,"seconds":seconds,
                "ops_per_second":queries as f64/seconds,
                "creation_mean_us":create_ns as f64/queries as f64/1000.0,
                "first_row_mean_us":first_ns as f64/queries as f64/1000.0,
                "notes":"Frozen maintenance paused in both builds; copies/merges isolated from disk flushing. No RocksDB equivalence claim."})
            );
        }
        drop(maintenance);
    }
    Ok(())
}
