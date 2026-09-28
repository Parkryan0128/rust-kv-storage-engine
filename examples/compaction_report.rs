use rust_kv_storage_engine::{CompactionStyle, Engine, Options};
use std::{collections::BTreeMap, time::Instant};

fn percentile(values: &mut [u128], percent: usize) -> f64 {
    values.sort_unstable();
    values[(values.len() - 1) * percent / 100] as f64 / 1000.0
}

fn run(workload: &str, style: CompactionStyle, trial: usize) {
    let dir = tempfile::tempdir().unwrap();
    let opts = Options {
        memtable_size_limit: 8 * 1024 * 1024,
        block_size: 4096,
        compaction_file_threshold: 4,
        compaction_style: style,
        ..Options::default()
    };
    let db = Engine::open_with_options(dir.path(), opts.clone()).unwrap();
    let mut model = BTreeMap::new();
    let mut logical_bytes = 0u64;
    for k in 0..8192u64 {
        let value = vec![1; 128];
        db.put(&k.to_be_bytes(), &value).unwrap();
        model.insert(k, value);
        logical_bytes += 136;
    }
    db.flush().unwrap();
    let mut writes = Vec::new();
    let mut flushes = Vec::new();
    let mut rng = 0xace123u64;
    let start = Instant::now();
    for batch in 0..64u64 {
        for i in 0..128u64 {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            let key = match workload {
                "append" => 8192 + batch * 128 + i,
                "hot-set" => (batch * 128 + i) % 256,
                _ => rng % 8192,
            };
            let deleting = workload == "delete-heavy" && rng % 3 == 0;
            let value = vec![(batch + 2) as u8; 128];
            let t = Instant::now();
            if deleting {
                db.delete(&key.to_be_bytes()).unwrap();
            } else {
                db.put(&key.to_be_bytes(), &value).unwrap();
            }
            writes.push(t.elapsed().as_nanos());
            logical_bytes += if deleting { 8 } else { 136 };
            if deleting {
                model.remove(&key);
            } else {
                model.insert(key, value);
            }
        }
        let t = Instant::now();
        db.flush().unwrap();
        flushes.push(t.elapsed().as_nanos());
    }
    let elapsed = start.elapsed().as_secs_f64();
    let stats = db.stats();
    let mut reads = Vec::new();
    for i in 0..4096u64 {
        let key = (i * 7919) % if workload == "append" { 16384 } else { 8192 };
        let t = Instant::now();
        let value = db.get(&key.to_be_bytes()).unwrap();
        reads.push(t.elapsed().as_nanos());
        assert_eq!(value.as_deref(), model.get(&key).map(Vec::as_slice));
    }
    drop(db);
    let db = Engine::open_with_options(dir.path(), opts).unwrap();
    for k in 0u64..if workload == "append" { 16384 } else { 8192 } {
        assert_eq!(
            db.get(&k.to_be_bytes()).unwrap().as_deref(),
            model.get(&k).map(Vec::as_slice)
        );
    }
    println!("{workload},{style:?},{trial},{logical_bytes},{},{},{},{:.4},{},{},{},{},{:.3},{:.3},{:.3},{:.3},{:.3}",
        stats.flush_bytes, stats.compaction_input_bytes, stats.compaction_output_bytes,
        (stats.flush_bytes + stats.compaction_output_bytes) as f64 / logical_bytes as f64,
        stats.compactions, stats.sst_files, stats.sst_records, stats.sst_bytes,
        8192.0 / elapsed, percentile(&mut writes, 50), percentile(&mut writes, 99),
        percentile(&mut flushes, 99), percentile(&mut reads, 99));
}

fn main() {
    let trials = std::env::args()
        .nth(1)
        .map(|s| s.parse::<usize>().unwrap())
        .unwrap_or(3);
    println!("workload,policy,trial,logical_bytes,flush_bytes,compaction_input_bytes,compaction_output_bytes,sst_write_amplification,compactions,sst_files,sst_records,sst_bytes,update_ops_s,write_p50_us,write_p99_us,flush_p99_us,read_p99_us");
    for trial in 0..trials {
        for workload in ["append", "hot-set", "delete-heavy"] {
            let policies = if trial % 2 == 0 {
                [CompactionStyle::Full, CompactionStyle::SizeTiered]
            } else {
                [CompactionStyle::SizeTiered, CompactionStyle::Full]
            };
            for style in policies {
                run(workload, style, trial);
            }
        }
    }
}
