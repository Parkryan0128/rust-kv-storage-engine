use rust_kv_storage_engine::{Engine, Options, WriteBatch};
use std::ops::Bound::{Excluded, Unbounded};

#[test]
fn reopened_disjoint_tables_skip_gaps_and_above_maximum_without_data_reads() {
    let dir = tempfile::tempdir().unwrap();
    let options = Options {
        compaction_file_threshold: 1000,
        block_cache_capacity: 0,
        block_size: 4096,
        ..Options::default()
    };
    let db = Engine::open_with_options(dir.path(), options.clone()).unwrap();
    for group in 0..16u64 {
        let mut batch = WriteBatch::new();
        for row in 0..8u64 {
            batch.put((group * 16 + row).to_be_bytes(), row.to_be_bytes());
        }
        db.write_batch(batch).unwrap();
        db.flush().unwrap();
    }
    assert_eq!(db.stats().sst_files, 16);
    drop(db);
    let db = Engine::open_with_options(dir.path(), options).unwrap();
    for (lo, hi) in [(232u64, 240u64), (256, 300)] {
        assert!(db
            .scan(lo.to_be_bytes().as_slice()..hi.to_be_bytes().as_slice())
            .unwrap()
            .next()
            .is_none());
        assert_eq!(db.stats().block_reads, 0);
    }
    let last = 247u64.to_be_bytes();
    assert!(db
        .scan((Excluded(last.as_slice()), Unbounded))
        .unwrap()
        .next()
        .is_none());
    assert_eq!(db.stats().block_reads, 0);
    let row = db
        .scan(last.as_slice()..=last.as_slice())
        .unwrap()
        .next()
        .unwrap()
        .unwrap();
    assert_eq!(row.0, last);
    assert_eq!(row.1.as_ref(), 7u64.to_be_bytes());
    assert_eq!(db.stats().block_reads, 1);
    // A newer in-memory record outside every SST must remain visible.
    db.put(&300u64.to_be_bytes(), b"memory").unwrap();
    let rows = db
        .scan(256u64.to_be_bytes().as_slice()..)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].1, "memory");
    assert_eq!(db.stats().block_reads, 1);
}
