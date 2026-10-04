mod backend;
use backend::Backend;
type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;
use serde_json::{json, Value};
use std::{
    fs,
    os::unix::fs::MetadataExt,
    path::Path,
    time::{Duration, Instant},
};

struct Timing {
    operations: usize,
    elapsed: Duration,
    samples: Vec<u64>,
}

fn measure<T>(
    operations: usize,
    mut operation: impl FnMut(usize) -> Result<T>,
    mut verify: impl FnMut(T),
) -> Result<Timing> {
    // Keep measurement memory bounded, independent of database size.
    let stride = operations.div_ceil(10_000).max(1);
    let mut samples = Vec::with_capacity(operations.div_ceil(stride));
    let start = Instant::now();
    for i in 0..operations {
        let clock = (i % stride == 0).then(Instant::now);
        let value = operation(i)?;
        if let Some(clock) = clock {
            samples.push(clock.elapsed().as_nanos() as u64);
        }
        verify(value);
    }
    Ok(Timing {
        operations,
        elapsed: start.elapsed(),
        samples,
    })
}

fn disk_bytes(path: &Path) -> std::io::Result<(u64, u64)> {
    let mut logical = 0;
    let mut allocated = 0;
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let metadata = entry.metadata()?;
        if metadata.is_dir() {
            let (l, a) = disk_bytes(&entry.path())?;
            logical += l;
            allocated += a;
        } else {
            logical += metadata.len();
            allocated += metadata.blocks() * 512;
        }
    }
    Ok((logical, allocated))
}

fn process_memory() -> Value {
    // Linux process RSS includes the engine and this bounded measurement harness.
    // It excludes filesystem cache not mapped into this process.
    let status = fs::read_to_string("/proc/self/status").unwrap_or_default();
    let kb = |name: &str| {
        status.lines().find_map(|line| {
            line.strip_prefix(name)
                .and_then(|s| s.split_whitespace().next())
                .and_then(|s| s.parse::<u64>().ok())
        })
    };
    json!({"rss_kib":kb("VmRSS:"),"peak_rss_kib":kb("VmHWM:")})
}

fn cgroup_memory() -> Result<Value> {
    let root = std::env::var("KV_BENCH_CGROUP")?;
    let root = Path::new(&root);
    let read =
        |name: &str| -> Result<u64> { Ok(fs::read_to_string(root.join(name))?.trim().parse()?) };
    let stat = fs::read_to_string(root.join("memory.stat"))?;
    let field = |name: &str| -> Option<u64> {
        stat.lines().find_map(|line| {
            let (key, value) = line.split_once(' ')?;
            (key == name).then(|| value.parse().ok()).flatten()
        })
    };
    Ok(json!({
        "current_bytes":read("memory.current")?,"peak_bytes":read("memory.peak")?,
        "anon_bytes":field("anon"),"file_bytes":field("file"),"kernel_bytes":field("kernel"),
        "swap_bytes":read("memory.swap.current")?
    }))
}

struct Run<'a> {
    dir: &'a Path,
    keys: usize,
    value_bytes: usize,
}

impl Run<'_> {
    fn report(
        &self,
        stage: &str,
        db: &Backend,
        live_keys: usize,
        mut timing: Timing,
        maintenance: Duration,
    ) -> Result<()> {
        let seconds = timing.elapsed.as_secs_f64();
        let total = seconds + maintenance.as_secs_f64();
        timing.samples.sort_unstable();
        let percentile = |p: usize| {
            if timing.samples.is_empty() {
                None
            } else {
                Some(timing.samples[(timing.samples.len() - 1) * p / 100] as f64 / 1000.0)
            }
        };
        let stats = db.stats()?;
        let (file_bytes, allocated_bytes) = disk_bytes(self.dir)?;
        println!(
            "COMPARE_REPORT {}",
            json!({
                "stage":stage,"keys":self.keys,"value_bytes":self.value_bytes,
                "live_keys":live_keys,"live_payload_bytes":live_keys as u64*(8+self.value_bytes) as u64,
                "operations":timing.operations,"operation_loop_seconds":seconds,
                "maintenance_seconds":maintenance.as_secs_f64(),"total_seconds":total,
                "ops_per_second_including_maintenance":if timing.operations>0 {Some(timing.operations as f64/total)} else {None},
                "sample_count":timing.samples.len(),"sampled_p50_us":percentile(50),"sampled_p99_us":percentile(99),
                "memory":process_memory(),"db_file_bytes":file_bytes,"db_allocated_bytes":allocated_bytes,
                "engine_stats":stats,"cgroup":cgroup_memory()?
            })
        );
        Ok(())
    }
}

fn maintenance(action: impl FnOnce() -> Result<()>) -> Result<Duration> {
    let start = Instant::now();
    action()?;
    Ok(start.elapsed())
}

fn no_operations() -> Timing {
    Timing {
        operations: 0,
        elapsed: Duration::ZERO,
        samples: vec![],
    }
}

fn value_for(buffer: &mut [u8], key: u64, updated: bool) {
    buffer[..8].copy_from_slice(&key.to_be_bytes());
    buffer[8] = u8::from(updated);
}

fn main() -> Result<()> {
    let args: Vec<_> = std::env::args().collect();
    assert_eq!(
        args.len(),
        4,
        "usage: resource_report NEW_DIR KEYS VALUE_BYTES"
    );
    let keys: usize = args[2].parse().expect("positive key count");
    let value_bytes: usize = args[3].parse().expect("value size in bytes");
    assert!(keys > 0 && keys % 4 == 0);
    assert!((9..=16 * 1024 * 1024).contains(&value_bytes));
    let dir = Path::new(&args[1]);
    assert!(
        !dir.exists(),
        "use a fresh database directory for every run"
    );
    let run = Run {
        dir,
        keys,
        value_bytes,
    };
    let db = Backend::open(dir)?;
    run.report("empty", &db, 0, no_operations(), Duration::ZERO)?;
    let mut value = vec![42; value_bytes];
    let load = measure(
        keys,
        |i| {
            value_for(&mut value, i as u64, false);
            db.put(&(i as u64).to_be_bytes(), &value)
        },
        |_| {},
    )?;
    let flush = maintenance(|| db.flush())?;
    run.report("load", &db, keys, load, flush)?;
    drop(db);

    // Fresh engine cache, but deliberately leave the OS page cache alone.
    let start = Instant::now();
    let db = Backend::open(dir)?;
    run.report("reopen", &db, keys, no_operations(), start.elapsed())?;
    for stage in ["random_read_first_pass", "random_read_repeat"] {
        let mut rng = 0xace123u64;
        let timing = measure(
            keys.min(100_000),
            |_| {
                rng ^= rng << 13;
                rng ^= rng >> 7;
                rng ^= rng << 17;
                let key = rng % keys as u64;
                Ok((key, db.get(&key.to_be_bytes())?))
            },
            |(key, actual)| {
                value_for(&mut value, key, false);
                assert_eq!(actual.as_deref(), Some(value.as_slice()));
            },
        )?;
        run.report(stage, &db, keys, timing, Duration::ZERO)?;
    }
    let update = measure(
        keys / 2,
        |i| {
            let key = (i * 2) as u64;
            value_for(&mut value, key, true);
            db.put(&key.to_be_bytes(), &value)
        },
        |_| {},
    )?;
    let flush = maintenance(|| db.flush())?;
    run.report("update_half", &db, keys, update, flush)?;
    let delete = measure(
        keys / 4,
        |i| db.delete(&((i * 4) as u64).to_be_bytes()),
        |_| {},
    )?;
    let flush = maintenance(|| db.flush())?;
    run.report("delete_quarter", &db, keys * 3 / 4, delete, flush)?;
    let compact = maintenance(|| db.compact())?;
    run.report("compact", &db, keys * 3 / 4, no_operations(), compact)?;
    drop(db);

    let db = Backend::open(dir)?;
    let verify = measure(
        keys,
        |i| Ok((i as u64, db.get(&(i as u64).to_be_bytes())?)),
        |(key, actual)| {
            if key % 4 == 0 {
                assert!(actual.is_none());
            } else {
                value_for(&mut value, key, key % 2 == 0);
                assert_eq!(actual.as_deref(), Some(value.as_slice()));
            }
        },
    )?;
    run.report(
        "verify_after_reopen",
        &db,
        keys * 3 / 4,
        verify,
        Duration::ZERO,
    )?;
    println!("COMPARE_VERIFIED keys={keys} value_bytes={value_bytes}");
    Ok(())
}
