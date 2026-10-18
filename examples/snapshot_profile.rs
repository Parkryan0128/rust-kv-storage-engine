//! Production-build snapshot write diagnostic; shared unchanged with the baseline.
use rust_kv_storage_engine::{Engine, Result, WriteBatch};
use std::time::Instant;

fn main() -> Result<()> {
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
