use super::*;

pub(super) fn prepare_bounds(path: &Path, c: &Config) -> Result<()> {
    assert!(!path.exists());
    let db = Backend::open(path, c)?;
    assert_eq!(c.keys % c.generations as u64, 0);
    let per = c.keys / c.generations as u64;
    for group in 0..c.generations as u64 {
        let lo = group * per * 2;
        for begin in (lo..lo + per).step_by(c.batch) {
            let entries: Vec<_> = (begin..(begin + c.batch as u64).min(lo + per))
                .map(|i| (key(i).to_vec(), Some(value(i, 0, c.value))))
                .collect();
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

pub(super) fn bounds_read(path: &Path, c: &Config, raw: &Value) -> Result<()> {
    let per = c.keys / c.generations as u64;
    let (lo, expected_count) = match raw["range"].as_str().unwrap() {
        "gap" => ((2 * c.generations as u64 - 3) * per, 0),
        "above" => (2 * c.keys, 0),
        "hit" => (
            2 * (c.generations as u64 - 1) * per + per / 2,
            c.span as usize,
        ),
        _ => return Err("invalid bounds probe".into()),
    };
    let low = key(lo);
    let high = key(lo + c.span);
    let before = metrics(path)?;
    let timer = Instant::now();
    let db = Backend::open(path, c)?;
    let open_seconds = timer.elapsed().as_secs_f64();
    report(
        "bounds-open",
        raw,
        json!({"seconds":open_seconds,"before":before,"after":metrics(path)?,"stats_after":db.stats()?}),
    );
    let stats_before = db.stats()?;
    let timer = Instant::now();
    let rows = db.scan(&low, &high)?.collect::<Result<Vec<_>>>()?;
    let seconds = timer.elapsed().as_secs_f64();
    report(
        "bounds-first",
        raw,
        json!({"seconds":seconds,"operations":1,"rows":rows.len(),"stats_before":stats_before,"stats_after":db.stats()?}),
    );
    assert_eq!(rows.len(), expected_count);
    for (n, (k, v)) in rows.iter().enumerate() {
        assert_eq!(*k, key(lo + n as u64));
        assert_eq!(v.as_ref(), value(lo + n as u64, 0, c.value));
    }
    drop(rows);
    let stats_before = db.stats()?;
    let mut found = 0;
    let timer = Instant::now();
    for _ in 0..c.queries {
        let rows = db.scan(&low, &high)?.collect::<Result<Vec<_>>>()?;
        found += rows.len();
        black_box(rows);
    }
    let seconds = timer.elapsed().as_secs_f64();
    report(
        "bounds-repeat",
        raw,
        json!({"seconds":seconds,"operations":c.queries,"stats_before":stats_before,"stats_after":db.stats()?}),
    );
    assert_eq!(found, expected_count * c.queries);
    // Validate every stored key/value after timing, including across reopen.
    drop(db);
    let db = Backend::open(path, c)?;
    let mut rows = db.scan(&key(0), &key(2 * c.keys))?;
    for group in 0..c.generations as u64 {
        for i in group * per * 2..group * per * 2 + per {
            let (k, v) = rows.next().ok_or("missing bounds fixture row")??;
            assert_eq!(k, key(i));
            assert_eq!(v.as_ref(), value(i, 0, c.value));
        }
    }
    assert!(rows.next().is_none());
    Ok(())
}
