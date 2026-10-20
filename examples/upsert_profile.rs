use rust_kv_storage_engine::{Engine, Result};
use std::{
    hint::black_box,
    time::{Duration, Instant},
};

fn main() -> Result<()> {
    for key_bytes in [8usize, 1024, 65536] {
        for retain_snapshot in [false, true] {
            let db = Engine::new();
            let keys: Vec<_> = (0..256u64)
                .map(|i| {
                    let mut key = vec![b'x'; key_bytes];
                    key[key_bytes - 8..].copy_from_slice(&i.to_be_bytes());
                    key
                })
                .collect();
            for key in &keys {
                db.put(key, b"old")?;
            }
            let snapshot = retain_snapshot.then(|| db.scan(..).unwrap());
            let start = Instant::now();
            let mut operations = 0u64;
            while start.elapsed() < Duration::from_millis(150) {
                for key in &keys {
                    db.put(black_box(key), black_box(b"new"))?;
                    operations += 1;
                }
            }
            let seconds = start.elapsed().as_secs_f64();
            for key in &keys {
                assert_eq!(db.get(key)?.as_deref(), Some(b"new".as_slice()));
            }
            if let Some(mut snapshot) = snapshot {
                for key in &keys {
                    let (actual, value) = snapshot.next().transpose()?.unwrap();
                    assert_eq!(&actual, key);
                    assert_eq!(value.as_ref(), b"old");
                }
                assert!(snapshot.next().is_none());
            }
            println!(
                "UPSERT_PROFILE {}",
                serde_json::json!({
                    "key_bytes":key_bytes,"retain_snapshot":retain_snapshot,"operations":operations,
                    "seconds":seconds,"ops_per_second":operations as f64/seconds,
                    "measurement":"volatile Engine::new; normal release library; no WAL or RocksDB comparison"
                })
            );
        }
    }
    Ok(())
}
