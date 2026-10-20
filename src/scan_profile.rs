use super::*;
use crate::memtable::Record;
use std::{
    collections::BTreeMap,
    hint::black_box,
    time::{Duration, Instant},
};

#[test]
#[ignore = "bounded fan-in policy experiment; run explicitly in release"]
fn merge_policy_profile() {
    for count in [2usize, 4, 8, 9, 16] {
        for prefix in [0usize, 128] {
            for shape in ["disjoint", "overlap", "deletions"] {
                let dir = tempfile::tempdir().unwrap();
                let mut tables = Vec::new();
                let mut model = BTreeMap::new();
                for source in 0..count {
                    let rows: Vec<_> = (0..512u64)
                        .map(|n| {
                            let index = if shape == "disjoint" {
                                n * count as u64 + source as u64
                            } else {
                                n
                            };
                            let mut key = vec![b'x'; prefix];
                            key.extend(index.to_be_bytes());
                            let record = Record {
                                seq: source as u64 * 512 + n + 1,
                                value: (shape != "deletions" || n % 17 == 0)
                                    .then(|| Bytes::from(vec![source as u8; 128])),
                            };
                            model.insert(key.clone(), record.value.clone());
                            (key, record)
                        })
                        .collect();
                    tables.push(Arc::new(
                        Table::write(
                            &dir.path().join(format!("{source}.sst")),
                            source as u64 + 1,
                            rows.into_iter().map(Ok),
                            512,
                            4096,
                            10,
                        )
                        .unwrap(),
                    ));
                }
                let expected: Vec<_> = model
                    .into_iter()
                    .filter_map(|(k, v)| v.map(|v| (k, v)))
                    .collect();
                let engine = Engine::new();
                let range = Arc::new(KeyRange::new(..).unwrap());
                let make_scan = |borrowed| {
                    let mut scan = Scan::new(engine.clone(), vec![], &tables, range.clone());
                    scan.borrowed_merge = borrowed;
                    scan
                };
                for borrowed in [false, true] {
                    assert_eq!(
                        make_scan(borrowed).collect::<Result<Vec<_>>>().unwrap(),
                        expected
                    );
                }
                for trial in 1..=3 {
                    for borrowed in if trial % 2 == 1 {
                        [true, false]
                    } else {
                        [false, true]
                    } {
                        let start = Instant::now();
                        let mut iterations = 0;
                        while start.elapsed() < Duration::from_millis(80) {
                            let mut rows = 0;
                            for row in make_scan(borrowed) {
                                black_box(row.unwrap());
                                rows += 1;
                            }
                            black_box(rows);
                            iterations += 1;
                        }
                        let seconds = start.elapsed().as_secs_f64();
                        println!(
                            "MERGE_PROFILE_RESULT {}",
                            serde_json::json!({
                                "sources":count,"key_bytes":prefix+8,"shape":shape,
                                "mode":if borrowed { "borrowed" } else { "heap" },
                                "trial":trial,"iterations":iterations,"seconds":seconds,
                                "rows_per_scan":expected.len(),"scans_per_second":iterations as f64/seconds,
                                "measurement":"release libtest; warm OS cache; policy diagnostic, not RocksDB comparison"
                            })
                        );
                    }
                }
            }
        }
    }
}
