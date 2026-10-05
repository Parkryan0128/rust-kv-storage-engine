mod backend;
use backend::Backend;
use serde_json::{json, Value};
use std::{fs, path::Path, time::Instant};
type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

fn memory() -> Value {
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

fn main() -> Result<()> {
    let args: Vec<_> = std::env::args().collect();
    assert!(
        args.len() >= 6,
        "prepare|read DIR KEYS VALUE_BYTES TABLES [QUERIES SEED]"
    );
    let path = Path::new(&args[2]);
    let keys: u64 = args[3].parse()?;
    let value_bytes: usize = args[4].parse()?;
    let tables: u64 = args[5].parse()?;
    assert!(keys > 0 && (1..=64).contains(&tables) && tables <= keys);
    assert!((9..=16 * 1024 * 1024).contains(&value_bytes));
    if args[1] == "prepare" {
        #[cfg(feature = "rocks")]
        {
            return Backend::prepare(path, keys, value_bytes, tables);
        }
        #[cfg(not(feature = "rocks"))]
        {
            return Err("engine fixtures are built by the explicit library fixture test".into());
        }
    }
    assert_eq!(args[1], "read");
    assert_eq!(args.len(), 8);
    let queries: usize = args[6].parse()?;
    let seed: u64 = args[7].parse()?;
    assert!(queries > 0 && seed > 0);
    assert!(path.is_dir(), "read requires an existing fixture");
    let db = Backend::open(path)?;
    let initial = db.stats()?;
    assert_eq!(initial["sst_files"].as_u64(), Some(tables));
    println!(
        "READ_OPEN {}",
        json!({"keys":keys,"value_bytes":value_bytes,"tables":tables,"memory":memory(),"stats":initial})
    );
    let stride = queries.div_ceil(10_000).max(1);
    let mut expected = vec![42; value_bytes];
    expected[8] = 0;
    for stage in ["random_read_first_pass", "random_read_repeat"] {
        let before = db.stats()?;
        let mut samples = Vec::with_capacity(queries.div_ceil(stride));
        let mut rng = seed;
        let start = Instant::now();
        for i in 0..queries {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            let key = (rng % keys).to_be_bytes();
            let clock = (i % stride == 0).then(Instant::now);
            let actual = db.get(&key)?;
            if let Some(clock) = clock {
                samples.push(clock.elapsed().as_nanos() as u64);
            }
            expected[..8].copy_from_slice(&key);
            assert_eq!(actual.as_deref(), Some(expected.as_slice()));
        }
        let seconds = start.elapsed().as_secs_f64();
        samples.sort_unstable();
        let percentile = |p| samples[(samples.len() - 1) * p / 100] as f64 / 1000.0;
        let after = db.stats()?;
        assert_eq!(after["sst_files"].as_u64(), Some(tables));
        println!(
            "READ_REPORT {}",
            json!({
                "stage":stage,"keys":keys,"value_bytes":value_bytes,"tables":tables,
                "operations":queries,"seed":seed,"seconds":seconds,
                "ops_per_second":queries as f64/seconds,
                "sample_count":samples.len(),"sampled_p50_us":percentile(50),
                "sampled_p99_us":percentile(99),"memory":memory(),
                "stats_before":before,"stats_after":after
            })
        );
    }
    println!("READ_VERIFIED queries={} keys={keys}", queries * 2);
    Ok(())
}
