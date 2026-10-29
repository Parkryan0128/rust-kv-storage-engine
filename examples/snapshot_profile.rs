//! Production-build snapshot write diagnostic; shared unchanged with the baseline.
use rust_kv_storage_engine::{Engine, Result, WriteBatch};
use std::time::Instant;

fn main() -> Result<()> {
    let trial = std::env::var("KV_PROFILE_TRIAL")
        .ok()
        .and_then(|n| n.parse::<u64>().ok())
        .unwrap_or(1);
    for (retain_snapshot, repeated_fold) in [(false, false), (true, false), (true, true)] {
        let db = Engine::new();
        let keys = if repeated_fold {
            256u64
        } else {
            std::env::var("KV_PROFILE_KEYS")
                .ok()
                .and_then(|n| n.parse().ok())
                .unwrap_or(20_000)
        };
        let batch_size = if repeated_fold { 64 } else { 32 };
        let mut initial = WriteBatch::new();
        for k in 0..keys {
            initial.put(k.to_be_bytes(), [0; 128]);
            // Large-index diagnostics still respect the public batch payload limit.
            if k % 100_000 == 99_999 {
                db.write_batch(std::mem::replace(&mut initial, WriteBatch::new()))?;
            }
        }
        db.write_batch(initial)?;
        let mut model = vec![0u8; keys as usize];
        let mut write_ns = 0;
        let queries = std::env::var("KV_PROFILE_QUERIES")
            .ok()
            .and_then(|n| n.parse::<u64>().ok())
            .unwrap_or(40_000);
        let warmup = 2_000u64;
        let mut start = None;
        for round in 0..warmup + queries {
            if round == warmup {
                start = Some(Instant::now());
            }
            let expected = model[0];
            let mut snapshot = if retain_snapshot {
                Some(db.scan(..)?)
            } else {
                None
            };
            let mut batch = WriteBatch::new();
            for j in 0..batch_size {
                let k = ((round * batch_size + j) * 7919) % keys;
                let generation = (round % 251 + 1) as u8;
                model[k as usize] = generation;
                batch.put(k.to_be_bytes(), [generation; 128]);
            }
            let one = Instant::now();
            db.write_batch(batch)?;
            let elapsed = one.elapsed().as_nanos();
            if round >= warmup {
                write_ns += elapsed;
            }
            if let Some(scan) = snapshot.as_mut() {
                assert_eq!(scan.next().unwrap()?.1.as_ref(), &[expected; 128]);
            }
        }
        let seconds = start.unwrap().elapsed().as_secs_f64();
        for k in 0..keys {
            assert_eq!(
                db.get(&k.to_be_bytes())?.unwrap().as_ref(),
                &[model[k as usize]; 128]
            );
        }
        println!(
            "MEMORY_WRITE_PROFILE {}",
            serde_json::json!({"trial":trial,"retain_snapshot":retain_snapshot,
            "case":if repeated_fold {"repeated-fold"} else {"distributed"},
            "keys":keys,"queries":queries,"warmup":warmup,"batch":batch_size,"seconds":seconds,"ops_per_second":queries as f64/seconds,
            "batch_write_mean_us":write_ns as f64/queries as f64/1000.0,
            "notes":"Volatile engine; no WAL. Total includes snapshot creation/drop; batch timer isolates writes while snapshot is retained."})
        );
    }
    Ok(())
}
