#[path = "../workloads_backend.rs"]
mod backend;
use backend::Backend;
use serde_json::{json, Value};
use std::{
    fs,
    hint::black_box,
    path::Path,
    sync::{Arc, Barrier},
    thread,
    time::Instant,
};
type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
type Entry = (Vec<u8>, Option<Vec<u8>>);

#[derive(Clone)]
struct Config {
    keys: u64,
    value: usize,
    block: usize,
    cache: usize,
    memtable: usize,
    generations: usize,
    queries: usize,
    seed: u64,
    fixed: bool,
    churn: bool,
    batch: usize,
    threads: usize,
    span: u64,
    workload: String,
}
impl Config {
    fn parse(v: &Value) -> Self {
        let n = |k: &str, default| v[k].as_u64().unwrap_or(default);
        let c = Self {
            keys: n("keys", 20000),
            value: n("value", 128) as usize,
            block: n("block", 16384) as usize,
            cache: n("cache", 8 * 1024 * 1024) as usize,
            memtable: n("memtable", 4 * 1024 * 1024) as usize,
            generations: n("generations", 4) as usize,
            queries: n("queries", 20000) as usize,
            seed: n("seed", 7919),
            fixed: v["fixed"].as_bool().unwrap_or(true),
            churn: v["churn"].as_bool().unwrap_or(false),
            batch: n("batch", 32) as usize,
            threads: n("threads", 1) as usize,
            span: n("span", 32),
            workload: v["workload"].as_str().unwrap_or("uniform").to_owned(),
        };
        assert!(c.keys > 0 && c.keys < u64::MAX / 2 && c.value >= 9 && c.value <= 1024 * 1024);
        assert!(
            c.batch > 0
                && c.batch <= 1024
                && c.queries > 0
                && c.seed > 0
                && (1..=16).contains(&c.threads)
        );
        assert!(c.span > 0 && c.span <= c.keys);
        assert!(c.memtable > 0);
        assert!((1..=16).contains(&c.generations));
        c
    }
}
fn key(i: u64) -> [u8; 8] {
    (i * 2).to_be_bytes()
}
fn value(i: u64, generation: u8, len: usize) -> Vec<u8> {
    let mut v = vec![42 ^ generation; len];
    v[..8].copy_from_slice(&key(i));
    v[8] = generation;
    v
}
fn expected(i: u64, c: &Config) -> Option<u8> {
    if c.churn && i.is_multiple_of(4) {
        None
    } else {
        Some(u8::from(c.churn && i.is_multiple_of(2)))
    }
}
fn random(state: &mut u64) -> u64 {
    *state ^= *state << 13;
    *state ^= *state >> 7;
    *state ^= *state << 17;
    *state
}
fn metrics(path: &Path) -> Result<Value> {
    let status = fs::read_to_string("/proc/self/status")?;
    let kb = |label| {
        status.lines().find_map(|l| {
            l.strip_prefix(label)
                .and_then(|s| s.split_whitespace().next())
                .and_then(|s| s.parse::<u64>().ok())
        })
    };
    fn disk(p: &Path) -> std::io::Result<(u64, u64)> {
        use std::os::unix::fs::MetadataExt;
        let mut size = (0, 0);
        for entry in fs::read_dir(p)? {
            let p = entry?.path();
            let m = match fs::metadata(&p) {
                Ok(m) => m,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                Err(e) => return Err(e),
            };
            let n = if m.is_dir() {
                disk(&p)?
            } else {
                (m.len(), m.blocks() * 512)
            };
            size.0 += n.0;
            size.1 += n.1;
        }
        Ok(size)
    }
    let (logical, allocated) = disk(path)?;
    let io = fs::read_to_string("/proc/self/io")?;
    let number = |label| {
        io.lines().find_map(|l| {
            l.strip_prefix(label)
                .and_then(|s| s.trim().parse::<u64>().ok())
        })
    };
    Ok(
        json!({"rss_kib":kb("VmRSS:"),"peak_rss_kib":kb("VmHWM:"),"directory_bytes":logical,"allocated_bytes":allocated,"read_bytes":number("read_bytes:"),"write_bytes":number("write_bytes:"),"rchar":number("rchar:"),"wchar":number("wchar:"),"write_syscalls":number("syscw:"),"voluntary_switches":kb("voluntary_ctxt_switches:"),"involuntary_switches":kb("nonvoluntary_ctxt_switches:")}),
    )
}
fn distribution(samples: &mut [u64]) -> Value {
    samples.sort_unstable();
    let p = |n| {
        samples
            .get(samples.len().saturating_sub(1) * n / 100)
            .map(|v| *v as f64 / 1000.0)
    };
    json!({"samples":samples.len(),"p50_us":p(50),"p95_us":if samples.len()>=20 {p(95)} else {None},"p99_us":if samples.len()>=100 {p(99)} else {None},"max_us":samples.last().map(|v| *v as f64 / 1000.0)})
}
fn report(stage: &str, config: &Value, fields: Value) {
    println!(
        "WORKLOAD_RESULT {}",
        json!({"backend":if cfg!(feature="rocks"){"rocksdb"}else{"engine"},"stage":stage,"config":config,"metrics":fields})
    );
}
fn prepare(path: &Path, c: &Config) -> Result<()> {
    assert!(!path.exists(), "fixture directory must be new");
    let db = Backend::open(path, c)?;
    for generation in 0..if c.churn { 3 } else { 1 } {
        let mut entries = Vec::new();
        for i in 0..c.keys {
            if generation == 1 && !i.is_multiple_of(2) || generation == 2 && !i.is_multiple_of(4) {
                continue;
            }
            entries.push((
                key(i).to_vec(),
                if generation == 2 {
                    None
                } else {
                    Some(value(i, generation, c.value))
                },
            ));
            if entries.len() == 256 {
                db.batch(&entries)?;
                entries.clear();
            }
        }
        if !entries.is_empty() {
            db.batch(&entries)?;
        }
        db.flush()?;
    }
    println!(
        "WORKLOAD_PREPARED {}",
        json!({"stats":db.stats()?,"metrics":metrics(path)?})
    );
    Ok(())
}
fn point_reads(db: &Backend, c: &Config, worker: usize) -> Result<Vec<u64>> {
    let mut rng = c.seed.wrapping_add(worker as u64 * 104729);
    let stride = c.queries.div_ceil(4000).max(1);
    let mut samples = Vec::with_capacity(c.queries.div_ceil(stride));
    let mut expected_value = vec![42; c.value];
    for n in 0..c.queries {
        let choice = random(&mut rng);
        let i = if c.workload == "hot" && choice % 100 < 80 {
            random(&mut rng) % (c.keys / 100).max(1)
        } else {
            random(&mut rng) % c.keys
        };
        let missing = c.workload == "miss";
        let k = (i * 2 + u64::from(missing)).to_be_bytes();
        let start = n.is_multiple_of(stride).then(Instant::now);
        let got = db.get(&k)?;
        if let Some(start) = start {
            samples.push(start.elapsed().as_nanos() as u64);
        }
        let generation = if missing { None } else { expected(i, c) };
        if let Some(g) = generation {
            expected_value.fill(42 ^ g);
            expected_value[..8].copy_from_slice(&k);
            expected_value[8] = g;
            assert_eq!(got.as_deref(), Some(expected_value.as_slice()));
        } else {
            assert!(got.is_none());
        }
        drop(black_box(got));
    }
    Ok(samples)
}

fn prepare_merge(path: &Path, c: &Config) -> Result<()> {
    assert!(!path.exists() && c.fixed);
    let db = Backend::open(path, c)?;
    for generation in 0..c.generations {
        let mut entries = Vec::new();
        for i in 0..c.keys {
            if !c.churn && i % c.generations as u64 != generation as u64 {
                continue;
            }
            entries.push((
                key(i).to_vec(),
                if c.churn && generation + 1 == c.generations && i.is_multiple_of(4) {
                    None
                } else {
                    Some(value(
                        i,
                        if c.churn { generation as u8 } else { 0 },
                        c.value,
                    ))
                },
            ));
            if entries.len() == 256 {
                db.batch(&entries)?;
                entries.clear();
            }
        }
        if !entries.is_empty() {
            db.batch(&entries)?;
        }
        db.flush()?;
    }
    println!(
        "WORKLOAD_PREPARED {}",
        json!({"stats":db.stats()?,"metrics":metrics(path)?})
    );
    verify_merge(&db, c)
}

fn verify_merge(db: &Backend, c: &Config) -> Result<()> {
    for i in 0..c.keys {
        let got = db.get(&key(i))?;
        if c.churn && i.is_multiple_of(4) {
            assert!(got.is_none(), "deleted key {i}");
        } else {
            let expected = value(
                i,
                if c.churn {
                    (c.generations - 1) as u8
                } else {
                    0
                },
                c.value,
            );
            assert_eq!(got.as_deref(), Some(expected.as_slice()), "key {i}");
        }
    }
    Ok(())
}

fn compact_merge(path: &Path, c: &Config, raw: &Value) -> Result<()> {
    assert!(c.fixed);
    let db = Backend::open(path, c)?;
    let before = metrics(path)?;
    let stats_before = db.stats()?;
    let start = Instant::now();
    db.compact()?;
    let seconds = start.elapsed().as_secs_f64();
    report(
        "compact-merge",
        raw,
        json!({"seconds":seconds,"before":before,"after":metrics(path)?,"stats_before":stats_before,"stats_after":db.stats()?}),
    );
    verify_merge(&db, c)?;
    drop(db);
    let db = Backend::open(path, c)?;
    verify_merge(&db, c)
}
fn reads(path: &Path, c: &Config, raw: &Value) -> Result<()> {
    let before_open = metrics(path)?;
    let start = Instant::now();
    let db = Arc::new(Backend::open(path, c)?);
    let open_us = start.elapsed().as_secs_f64() * 1e6;
    report(
        "open",
        raw,
        json!({"us":open_us,"before":before_open,"after":metrics(path)?,"stats":db.stats()?}),
    );
    for stage in ["first", "repeat"] {
        let before = metrics(path)?;
        let stats_before = db.stats()?;
        let start = Instant::now();
        let mut first_rows = Vec::new();
        let mut row_count = 0;
        let mut samples = if c.workload == "scan" {
            let mut samples = Vec::with_capacity(c.queries);
            let mut rng = c.seed;
            for _ in 0..c.queries {
                let lo = random(&mut rng) % (c.keys - c.span + 1);
                let hi = lo + c.span;
                let start = Instant::now();
                let mut iter = db.scan(&key(lo), &key(hi))?;
                let first = iter.next().transpose()?;
                first_rows.push(start.elapsed().as_nanos() as u64);
                let mut iter = first.into_iter().map(Ok).chain(iter);
                for i in lo..hi {
                    if let Some(g) = expected(i, c) {
                        let (k, v) = iter.next().expect("missing scan row")?;
                        assert_eq!(k, key(i));
                        assert_eq!(v.as_ref(), value(i, g, c.value));
                        row_count += 1;
                    }
                }
                assert!(iter.next().is_none(), "unexpected extra scan row");
                samples.push(start.elapsed().as_nanos() as u64);
            }
            samples
        } else if c.threads == 1 {
            point_reads(&db, c, 0)?
        } else {
            let barrier = Arc::new(Barrier::new(c.threads));
            let mut workers = Vec::new();
            for worker in 0..c.threads {
                let db = db.clone();
                let c = c.clone();
                let barrier = barrier.clone();
                workers.push(thread::spawn(move || {
                    barrier.wait();
                    point_reads(&db, &c, worker)
                }));
            }
            let mut samples = Vec::new();
            for worker in workers {
                samples.extend(worker.join().expect("reader panicked")?);
            }
            samples
        };
        let seconds = start.elapsed().as_secs_f64();
        let operations = c.queries * if c.workload == "scan" { 1 } else { c.threads };
        report(
            stage,
            raw,
            json!({"operations":operations,"rows":row_count,"seconds":seconds,"ops_per_second":operations as f64/seconds,"latency":distribution(&mut samples),"first_row_latency":distribution(&mut first_rows),"before":before,"after":metrics(path)?,"stats_before":stats_before,"stats_after":db.stats()?}),
        );
    }
    Ok(())
}
fn writes(path: &Path, c: &Config, raw: &Value) -> Result<()> {
    assert!(!path.exists());
    let db = Backend::open(path, c)?;
    // Prebuild a bounded input trace equally for both libraries. Timed batch calls
    // include construction of each backend's native batch and its durable commit.
    let entries: Vec<Entry> = (0..c.keys)
        .map(|i| (key(i).to_vec(), Some(value(i, 0, c.value))))
        .collect();
    let before = metrics(path)?;
    let mut samples = Vec::new();
    let start = Instant::now();
    for chunk in entries.chunks(c.batch) {
        let one = Instant::now();
        if c.workload == "put" {
            for (k, v) in chunk {
                db.put(k, v.as_ref().unwrap())?;
            }
        } else {
            db.batch(chunk)?;
        }
        samples.push(one.elapsed().as_nanos() as u64);
    }
    let commit_seconds = start.elapsed().as_secs_f64();
    let after_commits = metrics(path)?;
    let stats_before_flush = db.stats()?;
    let f = Instant::now();
    db.flush()?;
    let flush_seconds = f.elapsed().as_secs_f64();
    report(
        "write",
        raw,
        json!({"operations":c.keys,"commit_seconds":commit_seconds,"ops_per_second":c.keys as f64/commit_seconds,"flush_seconds":flush_seconds,"including_flush_ops_per_second":c.keys as f64/(commit_seconds+flush_seconds),"commit_latency":distribution(&mut samples),"before":before,"after_commits":after_commits,"stats_before_flush":stats_before_flush,"after":metrics(path)?,"stats":db.stats()?}),
    );
    for (k, v) in &entries {
        assert_eq!(db.get(k)?.as_deref(), v.as_deref());
    }
    drop(db);
    let db = Backend::open(path, c)?;
    for (k, v) in &entries {
        assert_eq!(db.get(k)?.as_deref(), v.as_deref());
    }
    Ok(())
}
fn main() -> Result<()> {
    let args: Vec<_> = std::env::args().collect();
    assert_eq!(args.len(), 4, "MODE DIRECTORY CONFIG_JSON");
    let path = Path::new(&args[2]);
    let raw: Value = serde_json::from_str(&args[3])?;
    let c = Config::parse(&raw);
    match args[1].as_str() {
        "prepare-merge" => prepare_merge(path, &c)?,
        "compact-merge" => compact_merge(path, &c, &raw)?,
        "prepare" => prepare(path, &c)?,
        "read" => reads(path, &c, &raw)?,
        "write" => writes(path, &c, &raw)?,
        "compact" => {
            let db = Backend::open(path, &c)?;
            let before = metrics(path)?;
            let stats = db.stats()?;
            let start = Instant::now();
            db.compact()?;
            let seconds = start.elapsed().as_secs_f64();
            report(
                "compact",
                &raw,
                json!({"seconds":seconds,"before":before,"after":metrics(path)?,"stats_before":stats,"stats_after":db.stats()?}),
            );
            point_reads(&db, &c, 0)?;
        }
        _ => return Err("unknown mode".into()),
    }
    println!("WORKLOAD_VERIFIED");
    Ok(())
}
