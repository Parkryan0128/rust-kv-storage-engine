use super::*;
use std::{
    hint::black_box,
    time::{Duration, Instant},
};

#[test]
#[ignore = "bounded selection-only diagnostic; run explicitly in release"]
fn picker_profile() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("fixture.sst");
    drop(
        Table::write(
            &path,
            1,
            std::iter::once(Ok((
                b"key".to_vec(),
                Record {
                    seq: 1,
                    value: None,
                },
            ))),
            1,
            64,
            10,
        )
        .unwrap(),
    );
    for count in [32usize, 256, 768] {
        let tables: Vec<_> = (1..=count)
            .rev()
            .map(|id| Arc::new(Table::open(&path, id as u64).unwrap()))
            .collect();
        for (case, threshold, style) in [
            ("below-threshold", count + 1, CompactionStyle::SizeTiered),
            ("full", 2, CompactionStyle::Full),
            ("tiered-4", 4, CompactionStyle::SizeTiered),
        ] {
            let expected: Vec<_> = tables
                .iter()
                .filter(|t| match case {
                    "below-threshold" => false,
                    "full" => true,
                    _ => t.id <= 4,
                })
                .map(|t| t.id)
                .collect();
            assert_eq!(
                pick(&tables, threshold, style)
                    .iter()
                    .map(|t| t.id)
                    .collect::<Vec<_>>(),
                expected
            );
            let start = Instant::now();
            let mut operations = 0u64;
            while start.elapsed() < Duration::from_millis(150) {
                for _ in 0..64 {
                    black_box(pick(
                        black_box(&tables),
                        black_box(threshold),
                        black_box(style),
                    ));
                    operations += 1;
                }
            }
            let seconds = start.elapsed().as_secs_f64();
            println!(
                "PICKER_PROFILE {}",
                serde_json::json!({
                    "case":case,"tables":count,"operations":operations,"seconds":seconds,
                    "ops_per_second":operations as f64/seconds,
                    "measurement":"selection only; release libtest; excludes fixture/open; no RocksDB equivalent"
                })
            );
        }
    }
}
