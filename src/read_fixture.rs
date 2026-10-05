//! Fast, explicit test-only fixture construction for the random-read benchmark.
use crate::{manifest::Manifest, memtable::Record, sstable::Table, Engine, Result};
use std::{env, path::PathBuf};

#[test]
#[ignore = "creates a benchmark fixture at KV_READ_FIXTURE_DIR"]
fn write_read_benchmark_fixture() -> Result<()> {
    let dir = PathBuf::from(env::var("KV_READ_FIXTURE_DIR").expect("fixture directory"));
    let parse = |name| env::var(name).expect(name).parse::<u64>().expect(name);
    let keys = parse("KV_READ_FIXTURE_KEYS");
    let value_bytes = parse("KV_READ_FIXTURE_VALUE_BYTES") as usize;
    let tables = parse("KV_READ_FIXTURE_TABLES");
    let block_bytes = env::var("KV_READ_FIXTURE_BLOCK_BYTES")
        .map(|s| s.parse::<usize>().expect("block size"))
        .unwrap_or(16 * 1024);
    assert!(!dir.exists(), "fixture directory must be new");
    assert!(keys > 0 && (1..=64).contains(&tables) && tables <= keys);
    assert!((9..=16 * 1024 * 1024).contains(&value_bytes));
    assert!((64..=crate::codec::MAX_RECORD).contains(&block_bytes));
    println!(
        "READ_FIXTURE_CONFIG {}",
        serde_json::json!({
            "keys":keys,"value_bytes":value_bytes,"tables":tables,
            "block_bytes":block_bytes,"source_root":env!("CARGO_MANIFEST_DIR")
        })
    );
    drop(Engine::open(&dir)?);
    let mut ids = Vec::new();
    for part in 0..tables {
        let start = keys * part / tables;
        let end = keys * (part + 1) / tables;
        let id = part + 2;
        let records = (start..end).map(|key| {
            let mut value = vec![42; value_bytes];
            value[..8].copy_from_slice(&key.to_be_bytes());
            value[8] = 0;
            Ok((
                key.to_be_bytes().to_vec(),
                Record {
                    seq: key + 1,
                    value: Some(value.into()),
                },
            ))
        });
        Table::write(
            &dir.join(format!("sst/{id:020}.sst")),
            id,
            records,
            end - start,
            block_bytes,
            10,
        )?;
        ids.push(id);
        println!("READ_FIXTURE engine table={}/{tables}", part + 1);
    }
    Manifest {
        wal_floor: 0,
        max_seq: keys,
        tables: ids,
    }
    .save(&dir)?;
    Ok(())
}
