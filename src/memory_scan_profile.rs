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
        let trials: Vec<u64> = std::env::var("KV_PROFILE_TRIAL")
            .ok()
            .map(|n| vec![n.parse().unwrap()])
            .unwrap_or_else(|| (1..=6).collect());
        for trial in trials {
            for consumed in [10u64, keys] {
                let mut create_ns = 0;
                let mut first_ns = 0;
                let queries = if consumed == keys { 10 } else { 100 };
                let start = Instant::now();
                for _ in 0..queries {
                    let one = Instant::now();
                    let mut scan = db.scan(..)?;
                    create_ns += one.elapsed().as_nanos();
                    let first = scan.next().unwrap()?;
                    first_ns += one.elapsed().as_nanos();
                    assert_eq!(first.0, 0u64.to_be_bytes());
                    assert_eq!(first.1.as_ref(), &[frozen_count as u8; 128]);
                    for k in 1..consumed {
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
                "trial":trial,"queries":queries,"consumed_rows":consumed,"seconds":seconds,
                "ops_per_second":queries as f64/seconds,
                "creation_mean_us":create_ns as f64/queries as f64/1000.0,
                "first_row_mean_us":first_ns as f64/queries as f64/1000.0,
                "notes":"Frozen maintenance paused in both builds; copies/merges isolated from disk flushing. No RocksDB equivalence claim."})
                );
            }
        }
        drop(maintenance);
    }
    Ok(())
}

#[test]
#[ignore = "volatile writes isolate snapshot copy cost from WAL sync"]
fn snapshot_mutation_profile() -> Result<()> {
    let trial = std::env::var("KV_PROFILE_TRIAL")
        .ok()
        .and_then(|n| n.parse::<u64>().ok())
        .unwrap_or(1);
    for retain_snapshot in [false, true] {
        let db = Engine::new();
        let keys = 20_000u64;
        let mut initial = WriteBatch::new();
        for k in 0..keys {
            initial.put(k.to_be_bytes(), [0; 128]);
        }
        db.write_batch(initial)?;
        let mut model = vec![0u8; keys as usize];
        let mut write_ns = 0;
        let queries = 400u64;
        let start = Instant::now();
        for round in 0..queries {
            let expected = model[0];
            let mut snapshot = if retain_snapshot {
                Some(db.scan(..)?)
            } else {
                None
            };
            let mut batch = WriteBatch::new();
            for j in 0..32 {
                let k = ((round * 32 + j) * 7919) % keys;
                let generation = (round % 251 + 1) as u8;
                model[k as usize] = generation;
                batch.put(k.to_be_bytes(), [generation; 128]);
            }
            let one = Instant::now();
            db.write_batch(batch)?;
            write_ns += one.elapsed().as_nanos();
            if let Some(scan) = snapshot.as_mut() {
                assert_eq!(scan.next().unwrap()?.1.as_ref(), &[expected; 128]);
            }
        }
        let seconds = start.elapsed().as_secs_f64();
        for k in 0..keys {
            assert_eq!(
                db.get(&k.to_be_bytes())?.unwrap().as_ref(),
                &[model[k as usize]; 128]
            );
        }
        println!(
            "MEMORY_WRITE_PROFILE {}",
            serde_json::json!({"trial":trial,"retain_snapshot":retain_snapshot,
            "keys":keys,"queries":queries,"batch":32,"seconds":seconds,"ops_per_second":queries as f64/seconds,
            "batch_write_mean_us":write_ns as f64/queries as f64/1000.0,
            "notes":"Volatile engine; no WAL. Total includes snapshot creation/drop; batch timer isolates writes while snapshot is retained."})
        );
    }
    Ok(())
}
