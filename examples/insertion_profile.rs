use rust_kv_storage_engine::{Engine, Result, WriteBatch};
use std::{hint::black_box, time::Instant};

fn apply(db: &Engine, keys: &[Vec<u8>], order: &[usize], batch: usize, value: &[u8]) -> Result<()> {
    for chunk in order.chunks(batch) {
        if batch == 1 {
            db.put(black_box(&keys[chunk[0]]), black_box(value))?;
        } else {
            let mut writes = WriteBatch::new();
            for &i in chunk {
                writes.put(black_box(&keys[i]), black_box(value));
            }
            db.write_batch(writes)?;
        }
    }
    Ok(())
}

fn main() -> Result<()> {
    let count = 100_000usize;
    for kind in ["short", "variable", "common128"] {
        let key = |i: usize| {
            let mut key = if kind == "common128" {
                vec![b'x'; 128]
            } else {
                Vec::new()
            };
            key.extend_from_slice(&(i as u64).to_be_bytes());
            if kind != "short" {
                key.resize(key.len() + i % 31, b'z');
            }
            key
        };
        let keys: Vec<_> = (0..count).map(|i| key(i * 2 + 1)).collect();
        let seeds: Vec<_> = (0..count).map(|i| key(i * 2)).collect();
        let sorted: Vec<_> = (0..count).collect();
        for shuffled in [false, true] {
            let mut order = sorted.clone();
            if shuffled {
                let mut rng = 12345u64;
                for i in (1..count).rev() {
                    rng ^= rng << 13;
                    rng ^= rng >> 7;
                    rng ^= rng << 17;
                    order.swap(i, (rng % (i + 1) as u64) as usize);
                }
            }
            for batch in [1usize, 32] {
                for retained_snapshot in [false, true] {
                    let db = Engine::new();
                    if retained_snapshot {
                        apply(&db, &seeds, &sorted, 256, &[0x11; 128])?;
                    }
                    let snapshot = retained_snapshot.then(|| db.scan(..).unwrap());
                    let start = Instant::now();
                    apply(&db, &keys, &order, batch, &[0x22; 128])?;
                    let seconds = start.elapsed().as_secs_f64();
                    let allocated = db.stats().active_memtable_allocated_bytes;
                    for key in &keys {
                        assert_eq!(db.get(key)?.as_deref(), Some([0x22; 128].as_slice()));
                    }
                    if let Some(mut old) = snapshot {
                        for key in &seeds {
                            let row = old.next().transpose()?.unwrap();
                            assert_eq!(&row.0, key);
                            assert_eq!(row.1.as_ref(), &[0x11; 128]);
                        }
                        assert!(old.next().is_none());
                    }
                    let mut rows = db.scan(..)?;
                    for i in 0..count {
                        if retained_snapshot {
                            let row = rows.next().transpose()?.unwrap();
                            assert_eq!(row.0, seeds[i]);
                            assert_eq!(row.1.as_ref(), &[0x11; 128]);
                        }
                        let row = rows.next().transpose()?.unwrap();
                        assert_eq!(row.0, keys[i]);
                        assert_eq!(row.1.as_ref(), &[0x22; 128]);
                    }
                    assert!(rows.next().is_none());
                    println!(
                        "INSERTION_PROFILE {}",
                        serde_json::json!({
                            "kind":kind,"shuffled":shuffled,"batch":batch,"retained_snapshot":retained_snapshot,
                            "keys":count,"seconds":seconds,"ops_per_second":count as f64/seconds,"allocated_bytes":allocated,
                            "measurement":"volatile production Engine::new; unique new keys; key generation/setup and exact model validation outside timer; no WAL or RocksDB comparison"
                        })
                    );
                }
            }
        }
    }
    Ok(())
}
