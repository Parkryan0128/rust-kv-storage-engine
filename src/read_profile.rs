//! Diagnostic microbenchmark of an uncached block path, not an end-to-end profile.
use super::*;
use std::{env, hint::black_box, time::Instant};

#[test]
#[ignore = "profiles an explicit benchmark fixture at KV_READ_FIXTURE_DIR"]
fn profile_random_read_stages() -> Result<()> {
    let dir = std::path::PathBuf::from(env::var("KV_READ_FIXTURE_DIR").expect("fixture directory"));
    let parse = |name| env::var(name).expect(name).parse::<u64>().expect(name);
    let keys = parse("KV_READ_FIXTURE_KEYS");
    let value_bytes = parse("KV_READ_FIXTURE_VALUE_BYTES") as usize;
    let table_count = parse("KV_READ_FIXTURE_TABLES");
    let block_bytes = parse("KV_READ_FIXTURE_BLOCK_BYTES");
    assert!(keys > 0 && value_bytes >= 9 && (1..=64).contains(&table_count));
    let tables = (2..table_count + 2)
        .map(|id| Table::open(&dir.join(format!("sst/{id:020}.sst")), id))
        .collect::<Result<Vec<_>>>()?;
    let max_frame_bytes = tables
        .iter()
        .flat_map(|t| &t.index)
        .map(|i| i.len as u64)
        .max()
        .expect("nonempty fixture");
    let record_bytes = value_bytes as u64 + 25;
    assert!(max_frame_bytes <= block_bytes + HEADER as u64);
    if keys / table_count * record_bytes >= block_bytes {
        // Accept both encoded-size packing and the older baseline writer's
        // memory-size estimate. This distinguishes power-of-two block sizes.
        assert!(max_frame_bytes > block_bytes / 2);
    }
    let mut buffers = (Vec::new(), Vec::new());
    let mut rng = 0xace123u64;
    let mut read_ns = 0u128;
    let mut decode_ns = 0u128;
    let mut lookup_ns = 0u128;
    let mut crc_ns = 0u128;
    let mut bytes_read = 0u64;
    let samples = 10_000u64;
    let mut expected = vec![42; value_bytes];
    expected[8] = 0;
    for _ in 0..samples {
        rng ^= rng << 13;
        rng ^= rng >> 7;
        rng ^= rng << 17;
        let key = (rng % keys).to_be_bytes();
        // These benchmark fixtures have disjoint, ascending key ranges.
        let t =
            &tables[tables.partition_point(|t| t.index[0].first.as_slice() <= key.as_slice()) - 1];
        let n = t
            .index
            .partition_point(|i| i.first.as_slice() <= key.as_slice())
            - 1;
        let i = &t.index[n];
        let (mut bytes, offsets) = buffers;
        bytes.resize(i.len as usize, 0);
        let start = Instant::now();
        t.file.read_exact_at(&mut bytes, i.offset)?;
        read_ns += start.elapsed().as_nanos();
        bytes_read += i.len as u64;
        let start = Instant::now();
        let block = t.decode(n, bytes, offsets)?;
        decode_ns += start.elapsed().as_nanos();
        let start = Instant::now();
        let record = block.get(&key).expect("fixture key");
        lookup_ns += start.elapsed().as_nanos();
        expected[..8].copy_from_slice(&key);
        assert_eq!(record.value.as_deref(), Some(expected.as_slice()));
        buffers = block.into_buffers();
        // Separate warmed-buffer CRC measurement. It overlaps decode's work;
        // never add it to the three preceding stage times.
        let start = Instant::now();
        black_box(frame_payload(&buffers.0)?);
        crc_ns += start.elapsed().as_nanos();
    }
    let mean = |ns: u128| ns as f64 / samples as f64;
    println!(
        "READ_PROFILE {}",
        serde_json::json!({
            "keys":keys,"value_bytes":value_bytes,"tables":table_count,
            "block_bytes":block_bytes,"samples":samples,
            "max_frame_bytes":max_frame_bytes,
            "block_count":tables.iter().map(|t| t.index.len()).sum::<usize>(),
            "source_root":env!("CARGO_MANIFEST_DIR"),
            "decoder_source_crc32":crc32fast::hash(include_str!("codec.rs").as_bytes()),
            "mean_read_ns":mean(read_ns),"mean_decode_crc_records_ns":mean(decode_ns),
            "mean_lookup_copy_ns":mean(lookup_ns),"mean_crc_only_ns":mean(crc_ns),
            "mean_frame_bytes":bytes_read as f64/samples as f64,
            "notes":"OS-warm uncached-block microbenchmark with reused buffers and per-table validation certificates; first touches fully validate, repeat touches reuse validation after checking CRC. Excludes routing/cache/locks. CRC-only overlaps decode. Timer overhead is included."
        })
    );
    Ok(())
}

#[test]
#[ignore = "bounded production cache-hit profile; run explicitly in release mode"]
fn current_cache_hit_profile() -> Result<()> {
    use bytes::Bytes;
    let dir = tempfile::tempdir()?;
    let keys = 20_000u64;
    let table = Table::write(
        &dir.path().join("cache.sst"),
        1,
        (0..keys).map(|k| {
            Ok((
                k.to_be_bytes(),
                Record {
                    seq: k + 1,
                    value: Some(Bytes::from(vec![42; 128])),
                },
            ))
        }),
        keys,
        16384,
        10,
    )?;
    let cache = BlockCache::with_block_size(8 * 1024 * 1024, 16384);
    let counters = Counters::default();
    for k in 0..keys {
        table.get(&k.to_be_bytes(), &cache, &counters)?;
    }
    let trials: Vec<u64> = std::env::var("KV_PROFILE_TRIAL")
        .ok()
        .map(|n| vec![n.parse().unwrap()])
        .unwrap_or_else(|| (1..=6).collect());
    for trial in trials {
        let before_reads = counters.reads.load(Ordering::Relaxed);
        let before_hits = cache.stats().1;
        let mut rng = 7919u64;
        let operations = 200_000;
        let start = Instant::now();
        for _ in 0..operations {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            black_box(
                table
                    .get(&(rng % keys).to_be_bytes(), &cache, &counters)?
                    .unwrap()
                    .value
                    .unwrap(),
            );
        }
        let seconds = start.elapsed().as_secs_f64();
        assert_eq!(counters.reads.load(Ordering::Relaxed), before_reads);
        assert_eq!(cache.stats().1 - before_hits, operations);
        println!(
            "CACHE_HIT_PROFILE {}",
            serde_json::json!({"trial":trial,
            "operations":operations,"seconds":seconds,"ops_per_second":operations as f64/seconds,
            "block_reads":0,"cache_hits":operations,"cache_bytes":cache.stats().0,
            "notes":"Production Table::get: Bloom/index routing, shard/map/LRU/hit counter and owned value copy. Excludes Engine state lock; no per-query timer."})
        );
    }
    Ok(())
}
