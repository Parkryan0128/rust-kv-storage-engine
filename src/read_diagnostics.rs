//! Opt-in read-path experiments. Fixture creation is outside measured regions.
use super::*;
use crate::{manifest::Manifest, Engine, Options};
use bytes::Bytes;
use serde_json::json;
use std::{hint::black_box, time::Instant};

const KEYS: u64 = 1_000_000;
const VALUE_BYTES: usize = 128;

fn value(key: u64) -> Bytes {
    let mut b = vec![42; VALUE_BYTES];
    b[..8].copy_from_slice(&key.to_be_bytes());
    b[8] = 0;
    b.into()
}

fn check_value(key: &[u8], actual: &Bytes) {
    assert_eq!(actual.len(), VALUE_BYTES);
    assert_eq!(&actual[..8], key);
    assert_eq!(actual[8], 0);
    assert!(actual[9..].iter().all(|b| *b == 42));
}

fn queries(count: usize) -> Vec<[u8; 8]> {
    let mut rng = 0xace123u64;
    (0..count)
        .map(|_| {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            (rng % KEYS).to_be_bytes()
        })
        .collect()
}

fn fixture(tables: u64) -> Result<(tempfile::TempDir, Vec<Table>)> {
    let dir = tempfile::tempdir()?;
    drop(Engine::open(dir.path())?);
    let mut opened = Vec::new();
    for part in 0..tables {
        let start = KEYS * part / tables;
        let end = KEYS * (part + 1) / tables;
        let id = part + 2;
        let path = dir.path().join(format!("sst/{id:020}.sst"));
        opened.push(Table::write(
            &path,
            id,
            (start..end).map(|key| {
                Ok((
                    key.to_be_bytes().to_vec(),
                    Record {
                        seq: key + 1,
                        value: Some(value(key)),
                    },
                ))
            }),
            end - start,
            16 * 1024,
            10,
        )?);
    }
    Manifest {
        wal_floor: 0,
        max_seq: KEYS,
        tables: opened.iter().map(|t| t.id).collect(),
    }
    .save(dir.path())?;
    Ok((dir, opened))
}

// Equivalent record/order/boundary validation to Table::block, materializing
// only the selected value. This is a diagnostic control, not a new read API.
fn borrowed_lookup(table: &Table, n: usize, payload: &[u8], key: &[u8]) -> Result<Option<Bytes>> {
    let payload = if table.compact_offsets {
        crate::block::compact_records(payload)?
    } else if table.indexed {
        indexed_records(payload)?
    } else {
        payload
    };
    let mut cursor = Cursor { b: payload };
    let mut previous: Option<&[u8]> = None;
    let mut found = None;
    while !cursor.b.is_empty() {
        let seq = cursor.u64()?;
        let tag = cursor.take(1)?[0];
        let kl = cursor.u32()? as usize;
        let vl = cursor.u32()? as usize;
        if seq == 0
            || tag > 1
            || (tag == 0 && vl != 0)
            || kl.saturating_add(vl).saturating_add(17) > MAX_RECORD
        {
            return Err(corrupt("invalid record header"));
        }
        let k = cursor.take(kl)?;
        let v = cursor.take(vl)?;
        if previous.is_some_and(|p| p >= k)
            || seq > table.max_seq
            || table
                .index
                .get(n + 1)
                .is_some_and(|next| k >= next.first.as_slice())
        {
            return Err(corrupt("SST record order/sequence"));
        }
        if previous.is_none() && k != table.index[n].first.as_slice() {
            return Err(corrupt("SST first key mismatch"));
        }
        if k == key && tag == 1 {
            found = Some(v);
        }
        previous = Some(k);
    }
    if previous.is_none() {
        return Err(corrupt("SST first key mismatch"));
    }
    Ok(found.map(Bytes::copy_from_slice))
}

fn decode_block(table: &Table, n: usize, payload: &[u8]) -> Result<Block> {
    let payload = if table.compact_offsets {
        crate::block::compact_records(payload)?
    } else if table.indexed {
        indexed_records(payload)?
    } else {
        payload
    };
    let mut cursor = Cursor { b: payload };
    let mut block: Block = vec![];
    while !cursor.b.is_empty() {
        let item = decode_record(&mut cursor)?;
        if block.last().is_some_and(|p| p.0 >= item.0)
            || item.1.seq > table.max_seq
            || table
                .index
                .get(n + 1)
                .is_some_and(|next| item.0 >= next.first)
        {
            return Err(corrupt("SST record order/sequence"));
        }
        block.push(item);
    }
    if block.first().is_none_or(|x| x.0 != table.index[n].first) {
        return Err(corrupt("SST first key mismatch"));
    }
    Ok(block)
}

fn block_queries(table: &Table, keys: &[[u8; 8]], trial: usize) -> Result<()> {
    let positions: Vec<_> = keys
        .iter()
        .map(|key| {
            table
                .index
                .partition_point(|i| i.first.as_slice() <= key.as_slice())
                - 1
        })
        .collect();
    let mut totals = [0u128; 5];
    let mut decoded_records = 0;
    let mut bytes_read = 0;
    for (key, &n) in keys.iter().zip(&positions) {
        let index = &table.index[n];
        let clock = Instant::now();
        let raw = at(&table.file, index.offset, index.len as usize)?;
        totals[0] += clock.elapsed().as_nanos();
        let clock = Instant::now();
        let payload = read_frame(&mut raw.as_slice(), false)?.unwrap();
        totals[1] += clock.elapsed().as_nanos();
        let clock = Instant::now();
        let block = decode_block(table, n, &payload)?;
        totals[2] += clock.elapsed().as_nanos();
        let clock = Instant::now();
        let at = block
            .binary_search_by(|(k, _)| k.as_slice().cmp(key))
            .unwrap();
        let actual = block[at].1.value.clone().unwrap();
        totals[3] += clock.elapsed().as_nanos();
        check_value(key, &actual);
        decoded_records += block.len();
        bytes_read += raw.len();
        let clock = Instant::now();
        drop((actual, block, payload, raw));
        totals[4] += clock.elapsed().as_nanos();
    }
    println!(
        "READ_DIAGNOSTIC {}",
        json!({
            "experiment":"block_phase_costs","trial":trial,"operations":keys.len(),
            "mean_ns":totals.map(|n| n as f64/keys.len() as f64),
            "phases":["pread_and_raw_allocation","frame_copy_and_crc","decode_and_validate_all_records","binary_search_and_value_clone","drop_buffers_and_records"],
            "mean_records_decoded":decoded_records as f64/keys.len() as f64,
            "mean_bytes_read":bytes_read as f64/keys.len() as f64
        })
    );

    // Alternate order to avoid always favoring the second implementation.
    let modes = if trial % 2 == 0 {
        [true, false]
    } else {
        [false, true]
    };
    for borrowed in modes {
        let start = Instant::now();
        for (key, &n) in keys.iter().zip(&positions) {
            let actual = if borrowed {
                let index = &table.index[n];
                let raw = at(&table.file, index.offset, index.len as usize)?;
                let mut frame = raw.as_slice();
                let payload = read_frame(&mut frame, false)?.unwrap();
                assert!(frame.is_empty());
                borrowed_lookup(table, n, &payload, key)?.unwrap()
            } else {
                let block = table.block(n)?;
                let at = block
                    .binary_search_by(|(k, _)| k.as_slice().cmp(key))
                    .unwrap();
                block[at].1.value.clone().unwrap()
            };
            check_value(key, black_box(&actual));
        }
        println!(
            "READ_DIAGNOSTIC {}",
            json!({
                "experiment":"forced_block_miss","trial":trial,
                "mode":if borrowed {"borrowed_record_control"} else {"production_materialized_block"},
                "operations":keys.len(),"seconds":start.elapsed().as_secs_f64()
            })
        );
    }
    Ok(())
}

fn engine_queries(dir: &Path, tables: &[Table], keys: &[[u8; 8]], trial: usize) -> Result<()> {
    let capacities = if trial % 2 == 0 {
        [256, 8, 0]
    } else {
        [0, 8, 256]
    };
    for mib in capacities {
        let db = Engine::open_with_options(
            dir,
            Options {
                block_cache_capacity: mib * 1024 * 1024,
                // Hold the fixture's table layout constant during read-only work.
                compaction_file_threshold: 64,
                ..Options::default()
            },
        )?;
        if mib == 256 {
            for table in tables {
                for entry in &table.index {
                    assert!(db.get(&entry.first)?.is_some());
                }
            }
        } else {
            for key in keys {
                check_value(key, &db.get(key)?.unwrap());
            }
        }
        let before = db.stats();
        let start = Instant::now();
        for key in keys {
            check_value(key, black_box(&db.get(key)?.unwrap()));
        }
        let elapsed = start.elapsed();
        let after = db.stats();
        assert_eq!(after.sst_files, tables.len());
        println!(
            "READ_DIAGNOSTIC {}",
            json!({
                "experiment":"public_get_cache_sweep","trial":trial,"tables":tables.len(),
                "cache_mib":mib,"operations":keys.len(),"seconds":elapsed.as_secs_f64(),
                "block_reads":after.block_reads-before.block_reads,
                "cache_hits":after.cache_hits-before.cache_hits,
                "bloom_negatives":after.bloom_negatives-before.bloom_negatives,
                "cache_bytes":after.cache_bytes
            })
        );
    }
    Ok(())
}

#[test]
#[ignore = "read-only diagnostic benchmark; run explicitly in release mode"]
fn read_cost_breakdown() -> Result<()> {
    let keys = queries(100_000);
    for count in [1, 5] {
        let (dir, tables) = fixture(count)?;
        println!(
            "READ_DIAGNOSTIC {}",
            json!({"experiment":"fixture","keys":KEYS,"value_bytes":VALUE_BYTES,
                "tables":count,"blocks":tables.iter().map(|t|t.index.len()).sum::<usize>()})
        );
        for trial in 1..=3 {
            engine_queries(dir.path(), &tables, &keys, trial)?;
            if count == 1 {
                block_queries(&tables[0], &keys[..20_000], trial)?;
            }
        }
    }
    println!("READ_DIAGNOSTIC_COMPLETE");
    Ok(())
}
