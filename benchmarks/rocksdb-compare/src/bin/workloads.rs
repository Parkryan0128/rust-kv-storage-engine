#[path = "../workloads_backend.rs"]
mod backend;
use backend::Backend;
use serde_json::{json, Value};
use std::{
    fs,
    hint::black_box,
    path::Path,
    sync::{Arc, Barrier, OnceLock},
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
    merge_key_bytes: usize,
    merge_deletes: bool,
    queries: usize,
    scan_repeat_passes: usize,
    seed: u64,
    fixed: bool,
    churn: bool,
    batch: usize,
    threads: usize,
    span: u64,
    workload: String,
    throughput: bool,
    wal_pattern: String,
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
            merge_key_bytes: n("merge_key_bytes", 8) as usize,
            merge_deletes: v["merge_deletes"].as_bool().unwrap_or(true),
            queries: n("queries", 20000) as usize,
            scan_repeat_passes: n("scan_repeat_passes", 1) as usize,
            seed: n("seed", 7919),
            fixed: v["fixed"].as_bool().unwrap_or(true),
            churn: v["churn"].as_bool().unwrap_or(false),
            batch: n("batch", 32) as usize,
            threads: n("threads", 1) as usize,
            span: n("span", 32),
            throughput: v["throughput"].as_bool().unwrap_or(false),
            workload: v["workload"].as_str().unwrap_or("uniform").to_owned(),
            wal_pattern: v["wal_pattern"].as_str().unwrap_or("sorted").to_owned(),
        };
        assert!(c.keys > 0 && c.keys < u64::MAX / 2 && c.value >= 9 && c.value <= 4 * 1024 * 1024);
        assert!(
            c.batch > 0
                && c.batch <= 1024
                && c.queries > 0
                && c.seed > 0
                && (1..=16).contains(&c.threads)
        );
        assert!(c.span > 0 && c.span <= c.keys);
        assert!(c.memtable > 0 && (1..=256).contains(&c.scan_repeat_passes));
        assert!(matches!(
            c.wal_pattern.as_str(),
            "sorted" | "random" | "overwrite"
        ));
        assert!((1..=16).contains(&c.generations));
        assert!((8..=1024).contains(&c.merge_key_bytes));
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
        json!({"rss_kib":kb("VmRSS:"),"peak_rss_kib":kb("VmHWM:"),"directory_bytes":logical,"allocated_bytes":allocated,"read_bytes":number("read_bytes:"),"write_bytes":number("write_bytes:"),"rchar":number("rchar:"),"wchar":number("wchar:"),"write_syscalls":number("syscw:"),"read_syscalls":number("syscr:"),"voluntary_switches":kb("voluntary_ctxt_switches:"),"involuntary_switches":kb("nonvoluntary_ctxt_switches:")}),
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
#[derive(Clone, Copy, PartialEq, Eq)]
enum ReadPass {
    Throughput,
    Latency,
    Verify,
}

fn point_reads(db: &Backend, c: &Config, worker: usize, pass: ReadPass) -> Result<Vec<u64>> {
    let mut rng = c.seed.wrapping_add(worker as u64 * 104729);
    // At most 4,000 API timers across ALL workers, independent of thread count.
    let quota = 4000 / c.threads + usize::from(worker < 4000 % c.threads);
    let stride = c.queries.div_ceil(quota).max(1);
    let mut samples = if pass == ReadPass::Latency {
        Vec::with_capacity(c.queries.div_ceil(stride))
    } else {
        Vec::new()
    };
    let mut expected_value = if pass == ReadPass::Verify {
        vec![42; c.value]
    } else {
        Vec::new()
    };
    for n in 0..c.queries {
        let choice = random(&mut rng);
        let i = if c.workload == "hot" && choice % 100 < 80 {
            random(&mut rng) % (c.keys / 100).max(1)
        } else {
            random(&mut rng) % c.keys
        };
        let missing = c.workload == "miss";
        let k = (i * 2 + u64::from(missing)).to_be_bytes();
        let start = (pass == ReadPass::Latency && n.is_multiple_of(stride)).then(Instant::now);
        let got = db.get(&k)?;
        if let Some(start) = start {
            samples.push(start.elapsed().as_nanos() as u64);
        }
        if pass == ReadPass::Verify {
            let generation = if missing { None } else { expected(i, c) };
            if let Some(g) = generation {
                expected_value.fill(42 ^ g);
                expected_value[..8].copy_from_slice(&k);
                expected_value[8] = g;
                assert_eq!(got.as_deref(), Some(expected_value.as_slice()));
            } else {
                assert!(got.is_none());
            }
        }
        drop(black_box(got));
    }
    Ok(samples)
}

// Ready workers precede the start timestamp. Elapsed time ends at the last
// worker completion; thread creation, joins and per-query timers are excluded.
fn throughput_reads(db: &Arc<Backend>, c: &Config) -> Result<f64> {
    let ready = Arc::new(Barrier::new(c.threads + 1));
    let start = Arc::new(Barrier::new(c.threads + 1));
    let clock = Arc::new(OnceLock::new());
    let mut workers = Vec::new();
    for worker in 0..c.threads {
        let (db, c, ready, start, clock) = (
            db.clone(),
            c.clone(),
            ready.clone(),
            start.clone(),
            clock.clone(),
        );
        workers.push(thread::spawn(move || -> Result<_> {
            ready.wait();
            start.wait();
            let result = point_reads(&db, &c, worker, ReadPass::Throughput);
            let end = Instant::now();
            assert!(result?.is_empty());
            Ok(end.duration_since(*clock.get().unwrap()).as_secs_f64())
        }));
    }
    ready.wait();
    clock.set(Instant::now()).unwrap();
    start.wait();
    let mut seconds = 0.0f64;
    for worker in workers {
        seconds = seconds.max(worker.join().expect("reader panicked")?);
    }
    Ok(seconds)
}

fn merge_key(i: u64, c: &Config) -> Vec<u8> {
    let mut out = vec![b'x'; c.merge_key_bytes];
    out[c.merge_key_bytes - 8..].copy_from_slice(&key(i));
    out
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
                merge_key(i, c),
                if c.churn
                    && c.merge_deletes
                    && generation + 1 == c.generations
                    && i.is_multiple_of(4)
                {
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
        let got = db.get(&merge_key(i, c))?;
        if c.churn && c.merge_deletes && i.is_multiple_of(4) {
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
struct ScanMeasurements {
    rows: usize,
    latency: Vec<u64>,
    first_rows: Vec<u64>,
}
fn scan_reads(db: &Backend, c: &Config, pass: ReadPass) -> Result<ScanMeasurements> {
    let mut out = ScanMeasurements {
        rows: 0,
        latency: Vec::new(),
        first_rows: Vec::new(),
    };
    if pass == ReadPass::Latency {
        out.latency.reserve(c.queries);
        out.first_rows.reserve(c.queries);
    }
    let mut rng = c.seed;
    for _ in 0..c.queries {
        let lo = random(&mut rng) % (c.keys - c.span + 1);
        let hi = lo + c.span;
        let clock = (pass == ReadPass::Latency).then(Instant::now);
        let mut iter = db.scan(&key(lo), &key(hi))?;
        if pass == ReadPass::Verify {
            for i in lo..hi {
                if let Some(g) = expected(i, c) {
                    let (k, v) = iter.next().expect("missing scan row")?;
                    assert_eq!(k, key(i));
                    assert_eq!(v.as_ref(), value(i, g, c.value));
                    out.rows += 1;
                }
            }
            assert!(iter.next().is_none(), "unexpected extra scan row");
        } else {
            let first = iter.next().transpose()?;
            if let Some(clock) = clock {
                out.first_rows.push(clock.elapsed().as_nanos() as u64);
            }
            if let Some(row) = first {
                drop(black_box(row));
                out.rows += 1;
            }
            for row in iter {
                drop(black_box(row?));
                out.rows += 1;
            }
            if let Some(clock) = clock {
                out.latency.push(clock.elapsed().as_nanos() as u64);
            }
        }
    }
    Ok(out)
}
fn sampled_points(db: &Arc<Backend>, c: &Config) -> Result<Vec<u64>> {
    if c.threads == 1 {
        return point_reads(db, c, 0, ReadPass::Latency);
    }
    let barrier = Arc::new(Barrier::new(c.threads));
    let mut workers = Vec::new();
    for worker in 0..c.threads {
        let (db, c, barrier) = (db.clone(), c.clone(), barrier.clone());
        workers.push(thread::spawn(move || {
            barrier.wait();
            point_reads(&db, &c, worker, ReadPass::Latency)
        }));
    }
    let mut samples = Vec::new();
    for worker in workers {
        samples.extend(worker.join().expect("reader panicked")?);
    }
    Ok(samples)
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
        // The first trace remains untouched. Repeat identical seeded traces
        // only after the first latency/verification passes have warmed the DB.
        let repetitions = if stage == "repeat" && c.workload == "scan" {
            c.scan_repeat_passes
        } else {
            1
        };
        let (seconds, row_count) = if c.workload == "scan" {
            let start = Instant::now();
            let mut rows = 0;
            for _ in 0..repetitions {
                rows += scan_reads(&db, c, ReadPass::Throughput)?.rows;
            }
            (start.elapsed().as_secs_f64(), rows)
        } else {
            (throughput_reads(&db, c)?, 0)
        };
        // Capture throughput resources before latency/validation warm the caches.
        let after = metrics(path)?;
        let stats_after = db.stats()?;
        let (mut samples, mut first_rows) = if c.workload == "scan" {
            let measured = scan_reads(&db, c, ReadPass::Latency)?;
            assert_eq!(measured.rows * repetitions, row_count);
            (measured.latency, measured.first_rows)
        } else if c.throughput {
            (Vec::new(), Vec::new())
        } else {
            (sampled_points(&db, c)?, Vec::new())
        };
        // Verify the exact same seeded queries outside every timed pass.
        if c.workload == "scan" {
            assert_eq!(
                scan_reads(&db, c, ReadPass::Verify)?.rows * repetitions,
                row_count
            );
        } else {
            for worker in 0..c.threads {
                point_reads(&db, c, worker, ReadPass::Verify)?;
            }
        }
        let operations = c.queries
            * if c.workload == "scan" {
                repetitions
            } else {
                c.threads
            };
        report(
            stage,
            raw,
            json!({"operations":operations,"rows":row_count,"throughput_trace_repetitions":repetitions,"seconds":seconds,
            "ops_per_second":operations as f64/seconds,"latency":distribution(&mut samples),
            "first_row_latency":distribution(&mut first_rows),"before":before,"after":after,
            "stats_before":stats_before,"stats_after":stats_after,
            "measurement":"v2 separate throughput, warm latency, and full verification passes"}),
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
    #[cfg(feature = "write-profiling")]
    let mut phase_samples = Vec::new();
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
        let elapsed = one.elapsed().as_nanos() as u64;
        samples.push(elapsed);
        #[cfg(feature = "write-profiling")]
        phase_samples.push(
            json!({"total_ns": elapsed, "phase_ns":rust_kv_storage_engine::write_profile::take()}),
        );
    }
    let commit_seconds = start.elapsed().as_secs_f64();
    let after_commits = metrics(path)?;
    #[cfg(feature = "write-profiling")]
    println!(
        "WRITE_TIMINGS {}",
        json!({"phases":rust_kv_storage_engine::write_profile::PHASES,"samples":phase_samples})
    );
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
fn populate_memory(db: &Backend, c: &Config) -> Result<()> {
    let mut entries = Vec::new();
    for i in 0..c.keys {
        entries.push((key(i).to_vec(), Some(value(i, 0, c.value))));
        if entries.len() == c.batch {
            db.batch(&entries)?;
            entries.clear();
        }
    }
    if !entries.is_empty() {
        db.batch(&entries)?;
    }
    Ok(())
}
fn prepare_wal(path: &Path, c: &Config) -> Result<()> {
    use std::io::Write;
    assert!(!path.exists());
    let db = Backend::open(path, c)?;
    let mut order: Vec<_> = (0..c.keys).collect();
    if c.wal_pattern == "random" {
        let mut rng = c.seed;
        for i in (1..order.len()).rev() {
            order.swap(i, (random(&mut rng) % (i as u64 + 1)) as usize);
        }
    }
    let mut entries = Vec::new();
    for i in order {
        let i = if c.wal_pattern == "overwrite" {
            i % (c.keys / 10).max(1)
        } else {
            i
        };
        entries.push((key(i).to_vec(), Some(value(i, 0, c.value))));
        if entries.len() == c.batch {
            db.batch(&entries)?;
            entries.clear();
        }
    }
    if !entries.is_empty() {
        db.batch(&entries)?;
    }
    println!(
        "WORKLOAD_PREPARED {}",
        json!({"stats":db.stats()?,"metrics":metrics(path)?})
    );
    println!("WORKLOAD_VERIFIED");
    std::io::stdout().flush()?;
    // Model process exit after acknowledged sync commits, without shutdown flush.
    std::process::exit(0);
}
fn recover(path: &Path, c: &Config, raw: &Value) -> Result<()> {
    let before = metrics(path)?;
    let start = Instant::now();
    let db = Backend::open(path, c)?;
    let seconds = start.elapsed().as_secs_f64();
    report(
        "recover",
        raw,
        json!({"seconds":seconds,"operations":c.keys,
        "ops_per_second":c.keys as f64/seconds,"before":before,
        "after":metrics(path)?,"stats":db.stats()?}),
    );
    let distinct = if c.wal_pattern == "overwrite" {
        (c.keys / 10).max(1)
    } else {
        c.keys
    };
    for i in 0..distinct {
        assert_eq!(
            db.get(&key(i))?.as_deref(),
            Some(value(i, 0, c.value).as_slice())
        );
    }
    db.put(b"after-recovery", b"appended")?;
    drop(db);
    let db = Backend::open(path, c)?;
    assert_eq!(
        db.get(b"after-recovery")?.as_deref(),
        Some(&b"appended"[..])
    );
    Ok(())
}

fn snapshot_release(path: &Path, c: &Config, raw: &Value) -> Result<()> {
    assert!(!path.exists());
    let db = Backend::open(path, c)?;
    db.put(&key(0), &vec![7; 3 * 1024 * 1024])?;
    let mut old = db.scan(&key(0), &key(1))?;
    db.put(&key(0), b"x")?;
    assert_eq!(old.next().transpose()?.unwrap().1.len(), 3 * 1024 * 1024);
    drop(old);
    let before = metrics(path)?;
    let start = Instant::now();
    for _ in 0..c.queries {
        db.put(&key(0), b"y")?;
    }
    let seconds = start.elapsed().as_secs_f64();
    assert_eq!(db.get(&key(0))?.as_deref(), Some(&b"y"[..]));
    report(
        "snapshot-release",
        raw,
        json!({"seconds":seconds,"operations":c.queries,
        "ops_per_second":c.queries as f64/seconds,"before":before,"after":metrics(path)?,
        "stats":db.stats()?}),
    );
    Ok(())
}

fn memory_scan(path: &Path, c: &Config, raw: &Value) -> Result<()> {
    assert!(!path.exists() && !c.churn && c.threads == 1);
    let db = Backend::open(path, c)?;
    populate_memory(&db, c)?;
    let before = metrics(path)?;
    let stats_before = db.stats()?;
    let mut creation = Vec::with_capacity(c.queries);
    let mut first_rows = Vec::with_capacity(c.queries);
    let mut drops = Vec::with_capacity(c.queries);
    let mut rng = c.seed;
    let vary_start = raw["vary_start"].as_bool().unwrap_or(false);
    let narrow = raw["narrow"].as_bool().unwrap_or(false);
    let start = Instant::now();
    for _ in 0..c.queries {
        let lower = if vary_start {
            random(&mut rng) % (c.keys - c.span + 1)
        } else {
            0
        };
        let upper = if narrow { lower + c.span } else { c.keys };
        let one = Instant::now();
        let mut iter = db.scan(&key(lower), &key(upper))?;
        creation.push(one.elapsed().as_nanos() as u64);
        let first = iter.next().transpose()?.expect("first row");
        first_rows.push(one.elapsed().as_nanos() as u64);
        assert_eq!(first.0, key(lower));
        assert_eq!(first.1.as_ref(), value(lower, 0, c.value));
        for i in lower + 1..lower + c.span {
            let (k, v) = iter.next().transpose()?.expect("memory row");
            assert_eq!(k, key(i));
            assert_eq!(v.as_ref(), value(i, 0, c.value));
        }
        let end = Instant::now();
        drop(iter);
        drops.push(end.elapsed().as_nanos() as u64);
    }
    let seconds = start.elapsed().as_secs_f64();
    report(
        "memory-scan",
        raw,
        json!({"seconds":seconds,"operations":c.queries,
        "ops_per_second":c.queries as f64/seconds,"creation_latency":distribution(&mut creation),
        "first_row_latency":distribution(&mut first_rows),"drop_latency":distribution(&mut drops),
        "before":before,"after":metrics(path)?,"stats_before":stats_before,"stats_after":db.stats()?}),
    );
    Ok(())
}
fn memory_get(path: &Path, c: &Config, raw: &Value) -> Result<()> {
    assert!(!path.exists() && !c.churn && c.threads == 1);
    let db = Backend::open(path, c)?;
    populate_memory(&db, c)?;
    let before = metrics(path)?;
    let mut rng = c.seed;
    let start = Instant::now();
    for _ in 0..c.queries {
        let k = random(&mut rng) % c.keys;
        let actual = db.get(&key(k))?.expect("memory value");
        assert_eq!(actual.as_ref(), value(k, 0, c.value));
        black_box(actual);
    }
    let seconds = start.elapsed().as_secs_f64();
    report(
        "memory-get",
        raw,
        json!({"seconds":seconds,"operations":c.queries,
        "ops_per_second":c.queries as f64/seconds,"before":before,"after":metrics(path)?,"stats":db.stats()?}),
    );
    Ok(())
}
fn mixed_large(path: &Path, c: &Config, raw: &Value) -> Result<()> {
    assert!(!path.exists() && c.value >= 2 * 1024 * 1024);
    let db = Backend::open(path, c)?;
    let large = value(c.keys, 0, c.value);
    db.put(&key(c.keys), &large)?;
    let small = Config {
        value: 128,
        ..c.clone()
    };
    populate_memory(&db, &small)?;
    db.flush()?;
    // Both size classes are warmed before the paired repeat measurement.
    for stage in ["first", "repeat"] {
        let before = metrics(path)?;
        let stats_before = db.stats()?;
        let mut rng = c.seed;
        let start = Instant::now();
        for n in 0..c.queries {
            if n % 16 == 0 {
                assert_eq!(db.get(&key(c.keys))?.as_deref(), Some(large.as_slice()));
            } else {
                let i = random(&mut rng) % c.keys;
                assert_eq!(
                    db.get(&key(i))?.as_deref(),
                    Some(value(i, 0, 128).as_slice())
                );
            }
        }
        let seconds = start.elapsed().as_secs_f64();
        report(
            stage,
            raw,
            json!({"seconds":seconds,"operations":c.queries,
            "ops_per_second":c.queries as f64/seconds,"before":before,"after":metrics(path)?,
            "stats_before":stats_before,"stats_after":db.stats()?}),
        );
    }
    Ok(())
}

fn cache_phase(path: &Path, c: &Config, raw: &Value) -> Result<()> {
    assert!(!path.exists());
    let db = Backend::open(path, c)?;
    let large = value(c.keys, 0, c.value);
    db.put(&key(c.keys), &large)?;
    populate_memory(
        &db,
        &Config {
            value: 128,
            ..c.clone()
        },
    )?;
    db.flush()?;
    for _ in 0..2 {
        for i in 0..c.keys {
            assert_eq!(
                db.get(&key(i))?.as_deref(),
                Some(value(i, 0, 128).as_slice())
            );
        }
    }
    // One large lookup after warming small records. Subsequent phases contain
    // only small reads, exposing reservations stranded by a workload change.
    assert_eq!(db.get(&key(c.keys))?.as_deref(), Some(large.as_slice()));
    for stage in ["first", "repeat"] {
        let before = metrics(path)?;
        let stats_before = db.stats()?;
        let mut rng = c.seed;
        let mut latencies = Vec::new();
        let start = Instant::now();
        for n in 0..c.queries {
            let i = random(&mut rng) % c.keys;
            let one = n
                .is_multiple_of((c.queries / 4000).max(1))
                .then(Instant::now);
            assert_eq!(
                db.get(&key(i))?.as_deref(),
                Some(value(i, 0, 128).as_slice())
            );
            if let Some(one) = one {
                latencies.push(one.elapsed().as_nanos() as u64);
            }
        }
        let seconds = start.elapsed().as_secs_f64();
        report(
            stage,
            raw,
            json!({"seconds":seconds,"operations":c.queries,
            "ops_per_second":c.queries as f64/seconds,"latency":distribution(&mut latencies),
            "before":before,"after":metrics(path)?,"stats_before":stats_before,"stats_after":db.stats()?}),
        );
    }
    Ok(())
}

fn snapshot_write(path: &Path, c: &Config, raw: &Value) -> Result<()> {
    let db = Backend::open(path, c)?;
    populate_memory(&db, c)?;
    let mut generations = vec![0u8; c.keys as usize];
    let mut writes = Vec::new();
    #[cfg(feature = "write-profiling")]
    let mut phase_samples = Vec::new();
    let before = metrics(path)?;
    let start = Instant::now();
    for round in 0..c.queries {
        let expected = generations[..10].to_vec();
        let mut snapshot = db.scan(
            &key(0),
            &key(if raw["narrow"].as_bool().unwrap_or(false) {
                10
            } else {
                c.keys
            }),
        )?;
        let entries: Vec<_> = (0..c.batch)
            .map(|j| {
                let i = ((round * c.batch + j) as u64 * 7919) % c.keys;
                generations[i as usize] = (round % 251 + 1) as u8;
                (
                    key(i).to_vec(),
                    Some(value(i, (round % 251 + 1) as u8, c.value)),
                )
            })
            .collect();
        let one = Instant::now();
        db.batch(&entries)?;
        let elapsed = one.elapsed().as_nanos() as u64;
        writes.push(elapsed);
        #[cfg(feature = "write-profiling")]
        phase_samples.push(json!({"total_ns":elapsed,
            "phase_ns":rust_kv_storage_engine::write_profile::take()}));
        for (i, generation) in expected.into_iter().enumerate() {
            let row = snapshot.next().transpose()?.unwrap();
            assert_eq!(row.0, key(i as u64));
            assert_eq!(row.1.as_ref(), value(i as u64, generation, c.value));
        }
    }
    let seconds = start.elapsed().as_secs_f64();
    #[cfg(feature = "write-profiling")]
    println!(
        "WRITE_TIMINGS {}",
        json!({
        "phases":rust_kv_storage_engine::write_profile::PHASES,"samples":phase_samples})
    );
    report(
        "snapshot-write",
        raw,
        json!({"seconds":seconds,"operations":c.queries,
        "ops_per_second":c.queries as f64/seconds,"batch_seconds":writes.iter().sum::<u64>() as f64/1e9,"batch_latency":distribution(&mut writes),
        "before":before,"after":metrics(path)?,"stats_after":db.stats()?}),
    );
    Ok(())
}

fn shaped_keys(path: &Path, c: &Config, raw: &Value) -> Result<()> {
    let db = Backend::open(path, c)?;
    let common = raw["common_prefix"].as_bool().unwrap_or(false);
    // Keep key generation outside the timed lookup loop on every backend.
    let keys: Vec<Vec<u8>> = (0..c.keys)
        .map(|i| {
            let mut k = if common { vec![b'x'; 128] } else { Vec::new() };
            k.extend_from_slice(&i.to_be_bytes());
            k.resize(k.len() + (i % 31) as usize, b'z');
            k
        })
        .collect();
    for begin in (0..c.keys as usize).step_by(c.batch) {
        let entries: Vec<_> = (begin..(begin + c.batch).min(keys.len()))
            .map(|i| (keys[i].clone(), Some(value(i as u64, 0, c.value))))
            .collect();
        db.batch(&entries)?;
    }
    if raw["disk"].as_bool().unwrap_or(false) {
        db.flush()?;
    }
    let before = metrics(path)?;
    let stats_before = db.stats()?;
    let mut rng = c.seed;
    let mut latencies = Vec::new();
    let mut expected = value(0, 0, c.value);
    let stride = c.queries.div_ceil(4000);
    let start = Instant::now();
    for n in 0..c.queries {
        let i = (random(&mut rng) % c.keys) as usize;
        let one = n.is_multiple_of(stride).then(Instant::now);
        let got = db.get(&keys[i])?;
        if let Some(one) = one {
            latencies.push(one.elapsed().as_nanos() as u64);
        }
        expected[..8].copy_from_slice(&key(i as u64));
        assert_eq!(got.as_deref(), Some(expected.as_slice()));
        drop(black_box(got));
    }
    let seconds = start.elapsed().as_secs_f64();
    report(
        "shaped-keys",
        raw,
        json!({"seconds":seconds,"operations":c.queries,"ops_per_second":c.queries as f64/seconds,"latency":distribution(&mut latencies),"before":before,"after":metrics(path)?,"stats_before":stats_before,"stats_after":db.stats()?}),
    );
    Ok(())
}

fn disjoint_range(path: &Path, c: &Config, raw: &Value) -> Result<()> {
    let db = Backend::open(path, c)?;
    let per = c.keys / c.generations as u64;
    for g in 0..c.generations as u64 {
        for begin in (g * per..(g + 1) * per).step_by(c.batch) {
            let entries: Vec<_> = (begin..(begin + c.batch as u64).min((g + 1) * per))
                .map(|i| (key(i).to_vec(), Some(value(i, 0, c.value))))
                .collect();
            db.batch(&entries)?;
        }
        db.flush()?;
    }
    let db = if raw["reopen"].as_bool().unwrap_or(false) {
        drop(db);
        Backend::open(path, c)?
    } else {
        db
    };
    // Separate the first post-open seek from an immediate identical repeat.
    let probe = per * (c.generations as u64 - 1) + per / 2;
    let mut initial_scans = Vec::new();
    for _ in 0..2 {
        let one = Instant::now();
        let rows = db
            .scan(&key(probe), &key(probe + c.span))?
            .collect::<Result<Vec<_>>>()?;
        initial_scans.push(one.elapsed().as_nanos() as f64 / 1000.0);
        assert_eq!(rows.len(), c.span as usize);
        for (i, (k, v)) in rows.into_iter().enumerate() {
            assert_eq!(k, key(probe + i as u64));
            assert_eq!(v.as_ref(), value(probe + i as u64, 0, c.value));
        }
    }
    let before = metrics(path)?;
    let stats_before = db.stats()?;
    let mut rng = c.seed;
    let mut latencies = Vec::new();
    let start = Instant::now();
    for _ in 0..c.queries {
        let lo = random(&mut rng) % (per * c.generations as u64 - c.span);
        let one = Instant::now();
        let rows = db
            .scan(&key(lo), &key(lo + c.span))?
            .collect::<Result<Vec<_>>>()?;
        latencies.push(one.elapsed().as_nanos() as u64);
        assert_eq!(rows.len(), c.span as usize);
        for (j, (k, v)) in rows.into_iter().enumerate() {
            assert_eq!(k, key(lo + j as u64));
            assert_eq!(v.as_ref(), value(lo + j as u64, 0, c.value));
        }
    }
    let seconds = start.elapsed().as_secs_f64();
    report(
        "disjoint-range",
        raw,
        json!({"first_scan_us":initial_scans[0],"repeat_scan_us":initial_scans[1],"seconds":seconds,"operations":c.queries,"ops_per_second":c.queries as f64/seconds,"latency":distribution(&mut latencies),"before":before,"after":metrics(path)?,"stats_before":stats_before,"stats_after":db.stats()?}),
    );
    Ok(())
}

fn mixed_steady(path: &Path, c: &Config, raw: &Value) -> Result<()> {
    let db = Backend::open(path, c)?;
    populate_memory(&db, c)?;
    let mut generations = vec![Some(0u8); c.keys as usize];
    let mut writes = Vec::new();
    let mut reads = Vec::new();
    let mut windows = Vec::new();
    let before = metrics(path)?;
    let stats_before = db.stats()?;
    let mut rng = c.seed;
    let start = Instant::now();
    for round in 0..c.queries {
        let expected = generations[..10].to_vec();
        let snapshot = db.scan(&key(0), &key(10))?;
        let entries: Vec<_> = (0..c.batch)
            .map(|j| {
                let i = ((round * c.batch + j) as u64 * 7919) % c.keys;
                let generation = (!j.is_multiple_of(7)).then_some((round % 251 + 1) as u8);
                generations[i as usize] = generation;
                (key(i).to_vec(), generation.map(|g| value(i, g, c.value)))
            })
            .collect();
        let one = Instant::now();
        db.batch(&entries)?;
        writes.push(one.elapsed().as_nanos() as u64);
        for _ in 0..c.batch {
            let i = random(&mut rng) % c.keys;
            let one = Instant::now();
            let got = db.get(&key(i))?;
            reads.push(one.elapsed().as_nanos() as u64);
            assert_eq!(
                got.as_deref(),
                generations[i as usize]
                    .map(|g| value(i, g, c.value))
                    .as_deref()
            );
        }
        let rows = snapshot.collect::<Result<Vec<_>>>()?;
        let expected: Vec<_> = expected
            .into_iter()
            .enumerate()
            .filter_map(|(i, g)| g.map(|g| (key(i as u64).to_vec(), value(i as u64, g, c.value))))
            .collect();
        assert_eq!(rows.len(), expected.len());
        for ((k, v), (ek, ev)) in rows.into_iter().zip(expected) {
            assert_eq!((k, v.as_ref()), (ek, ev.as_slice()));
        }
        if (round + 1).is_multiple_of(500) {
            let mut samples = writes[round + 1 - 500..].to_vec();
            windows.push(json!({"round":round+1,"elapsed":start.elapsed().as_secs_f64(),"batch_latency":distribution(&mut samples)}));
        }
    }
    let seconds = start.elapsed().as_secs_f64();
    report(
        "mixed-steady",
        raw,
        json!({"seconds":seconds,"operations":c.queries * c.batch * 2,"ops_per_second":(c.queries*c.batch*2) as f64/seconds,"batch_latency":distribution(&mut writes),"read_latency":distribution(&mut reads),"windows":windows,"before":before,"after":metrics(path)?,"stats_before":stats_before,"stats_after":db.stats()?}),
    );
    drop(db);
    let db = Backend::open(path, c)?;
    for i in 0..c.keys {
        assert_eq!(
            db.get(&key(i))?.as_deref(),
            generations[i as usize]
                .map(|g| value(i, g, c.value))
                .as_deref()
        );
    }
    Ok(())
}

fn tombstone_scan(path: &Path, c: &Config, raw: &Value) -> Result<()> {
    let db = Backend::open(path, c)?;
    let keys: Vec<_> = (0..c.keys)
        .map(|i| {
            let mut k = vec![b'x'; 128];
            k.extend_from_slice(&i.to_be_bytes());
            k
        })
        .collect();
    for chunk in keys.chunks(c.batch) {
        let entries: Vec<_> = chunk
            .iter()
            .map(|k| (k.clone(), Some(vec![9; c.value])))
            .collect();
        db.batch(&entries)?;
    }
    let deleted: Vec<_> = keys
        .iter()
        .enumerate()
        .filter(|(i, _)| i % 17 != 0)
        .map(|(_, k)| (k.clone(), None))
        .collect();
    for chunk in deleted.chunks(c.batch) {
        db.batch(chunk)?;
    }
    if raw["disk"].as_bool().unwrap_or(false) {
        db.flush()?;
    }
    let mut hi = vec![b'x'; 128];
    hi.extend_from_slice(&c.keys.to_be_bytes());
    let verify = || -> Result<()> {
        let mut scan = db.scan(&keys[0], &hi)?;
        for i in (0..keys.len()).step_by(17) {
            let row = scan.next().transpose()?.unwrap();
            assert_eq!(row.0, keys[i]);
            assert_eq!(row.1.as_ref(), vec![9; c.value]);
        }
        assert!(scan.next().is_none());
        Ok(())
    };
    verify()?;
    let before = metrics(path)?;
    let start = Instant::now();
    for _ in 0..c.queries {
        let mut count = 0;
        for row in db.scan(&keys[0], &hi)? {
            black_box(row?);
            count += 1;
        }
        assert_eq!(count, c.keys.div_ceil(17));
    }
    let seconds = start.elapsed().as_secs_f64();
    report(
        "tombstone-scan",
        raw,
        json!({"seconds":seconds,"operations":c.queries,
        "ops_per_second":c.queries as f64/seconds,"before":before,"after":metrics(path)?,"stats_after":db.stats()?}),
    );
    verify()?;
    Ok(())
}

fn reopen_loop(path: &Path, c: &Config, raw: &Value) -> Result<()> {
    let db = Backend::open(path, c)?;
    populate_memory(&db, c)?;
    db.flush()?;
    drop(db);
    let mut samples = Vec::with_capacity(c.queries);
    for _ in 0..c.queries {
        let start = Instant::now();
        let db = Backend::open(path, c)?;
        samples.push(start.elapsed().as_nanos() as u64);
        assert_eq!(
            db.get(&key(0))?.as_deref(),
            Some(value(0, 0, c.value).as_slice())
        );
        drop(db);
    }
    let seconds = samples.iter().sum::<u64>() as f64 / 1e9;
    report(
        "reopen-loop",
        raw,
        json!({"seconds":seconds,"operations":c.queries,
        "ops_per_second":c.queries as f64/seconds,"open_latency":distribution(&mut samples),
        "after":metrics(path)?,"measurement":"sum of opens; close and verification excluded; warm OS cache"}),
    );
    Ok(())
}

// Keep the active keys resident so every timed put exercises replacement.
// The same normal harness is compiled against baseline/current/RocksDB.
fn overwrite_keys(path: &Path, c: &Config, raw: &Value) -> Result<()> {
    let key_bytes = raw["key_bytes"].as_u64().unwrap_or(8) as usize;
    assert!((8..=65536).contains(&key_bytes));
    assert!((c.keys as usize + c.queries) * (key_bytes + c.value + 64) < c.memtable);
    let db = Backend::open(path, c)?;
    let keys: Vec<_> = (0..c.keys)
        .map(|i| {
            let mut key = vec![b'x'; key_bytes];
            key[key_bytes - 8..].copy_from_slice(&i.to_be_bytes());
            key
        })
        .collect();
    let initial = vec![0; c.value];
    let replacement = vec![1; c.value];
    for key in &keys {
        db.put(key, &initial)?;
    }
    let mut upper = vec![b'x'; key_bytes];
    upper[key_bytes - 8..].copy_from_slice(&c.keys.to_be_bytes());
    let mut snapshot = if raw["snapshot"].as_bool().unwrap_or(false) {
        Some(db.scan(&keys[0], &upper)?)
    } else {
        None
    };
    let before = metrics(path)?;
    let mut rng = c.seed;
    let mut samples = Vec::with_capacity(c.queries);
    let mut seen = vec![false; keys.len()];
    // Precompute the identical trace so only the durable API call is timed.
    let trace: Vec<_> = (0..c.queries)
        .map(|_| (random(&mut rng) % c.keys) as usize)
        .collect();
    let start = Instant::now();
    for &i in &trace {
        let one = Instant::now();
        db.put(&keys[i], &replacement)?;
        samples.push(one.elapsed().as_nanos() as u64);
    }
    let seconds = start.elapsed().as_secs_f64();
    let after = metrics(path)?;
    let stats = db.stats()?;
    for &i in &trace {
        seen[i] = true;
    }
    for (i, key) in keys.iter().enumerate() {
        assert_eq!(
            db.get(key)?.as_deref(),
            Some(if seen[i] {
                replacement.as_slice()
            } else {
                initial.as_slice()
            })
        );
    }
    if let Some(mut snapshot) = snapshot.take() {
        for key in &keys {
            let row = snapshot.next().transpose()?.unwrap();
            assert_eq!(&row.0, key);
            assert_eq!(row.1.as_ref(), initial);
        }
        assert!(snapshot.next().is_none());
    }
    drop(snapshot);
    drop(db);
    let db = Backend::open(path, c)?;
    for (i, key) in keys.iter().enumerate() {
        assert_eq!(
            db.get(key)?.as_deref(),
            Some(if seen[i] {
                replacement.as_slice()
            } else {
                initial.as_slice()
            })
        );
    }
    report(
        "overwrite-keys",
        raw,
        json!({
            "operations":c.queries,"seconds":seconds,"ops_per_second":c.queries as f64/seconds,
            "commit_latency":distribution(&mut samples),"before":before,"after":after,"stats":stats,
            "measurement":"normal release; sync WAL; resident active keys; snapshot optional; validation/reopen outside timer"
        }),
    );
    Ok(())
}

fn main() -> Result<()> {
    let args: Vec<_> = std::env::args().collect();
    assert_eq!(args.len(), 4, "MODE DIRECTORY CONFIG_JSON");
    let path = Path::new(&args[2]);
    let raw: Value = serde_json::from_str(&args[3])?;
    let c = Config::parse(&raw);
    match args[1].as_str() {
        "overwrite-keys" => overwrite_keys(path, &c, &raw)?,
        "tombstone-scan" => tombstone_scan(path, &c, &raw)?,
        "reopen-loop" => reopen_loop(path, &c, &raw)?,
        "prepare-merge" => prepare_merge(path, &c)?,
        "compact-merge" => compact_merge(path, &c, &raw)?,
        "prepare" => prepare(path, &c)?,
        "prepare-wal" => prepare_wal(path, &c)?,
        "recover" => recover(path, &c, &raw)?,
        "snapshot-release" => snapshot_release(path, &c, &raw)?,
        "memory-scan" => memory_scan(path, &c, &raw)?,
        "memory-get" => memory_get(path, &c, &raw)?,
        "mixed-large" => mixed_large(path, &c, &raw)?,
        "cache-phase" => cache_phase(path, &c, &raw)?,
        "snapshot-write" => snapshot_write(path, &c, &raw)?,
        "shaped-keys" => shaped_keys(path, &c, &raw)?,
        "disjoint-range" => disjoint_range(path, &c, &raw)?,
        "mixed-steady" => mixed_steady(path, &c, &raw)?,
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
            point_reads(&db, &c, 0, ReadPass::Verify)?;
        }
        _ => return Err("unknown mode".into()),
    }
    println!("WORKLOAD_VERIFIED");
    Ok(())
}
