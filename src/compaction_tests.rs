use super::*;
use bytes::Bytes;

#[test]
fn tournament_and_linear_publish_identical_bytes_across_merge_shapes() {
    let dir = tempfile::tempdir().unwrap();
    for long_keys in [false, true] {
        for shape in ["distinct", "overlap", "mixed"] {
            let key = |n: u64| {
                let mut key = if long_keys { vec![b'x'; 128] } else { vec![] };
                key.extend_from_slice(&n.to_be_bytes());
                key
            };
            let mut tables = Vec::new();
            let mut versions = Vec::new();
            for source in 0..MAX_INPUTS {
                let rows: Vec<_> = (0..48u64)
                    .filter(|row| {
                        source != 3 && (shape != "mixed" || (row + source as u64) % 3 != 0)
                    })
                    .map(|row| {
                        let n = if shape == "distinct" {
                            row * 16 + source as u64
                        } else {
                            row
                        };
                        let seq = if row == 0 {
                            1
                        } else {
                            1 + source as u64 * 100 + row
                        };
                        (
                            key(n),
                            Record {
                                seq,
                                value: ((row + source as u64) % 5 != 0).then(|| {
                                    Bytes::from(vec![
                                        source as u8;
                                        if row % 17 == 0 { 4096 } else { 32 }
                                    ])
                                }),
                            },
                        )
                    })
                    .collect();
                tables.push(Arc::new(
                    Table::write(
                        &dir.path().join(format!("input-{source}.sst")),
                        source as u64,
                        rows.iter().cloned().map(Ok),
                        rows.len() as u64,
                        256,
                        10,
                    )
                    .unwrap(),
                ));
                versions.push(rows);
            }
            for count in [2, 4, 8, 9, 16] {
                let inputs = &tables[..count];
                let mut model = BTreeMap::<Vec<u8>, Record>::new();
                for (key, record) in versions[..count].iter().flatten() {
                    if model.get(key).is_none_or(|old| record.seq > old.seq) {
                        model.insert(key.clone(), record.clone());
                    }
                }
                for drop_deletions in [false, true] {
                    let expected: Vec<_> = model
                        .iter()
                        .filter(|(_, r)| !drop_deletions || r.value.is_some())
                        .map(|(k, r)| (k.clone(), r.clone()))
                        .collect();
                    let mut output_bytes = Vec::new();
                    for tournament in [false, true] {
                        let actual = Merge::with_selector(inputs, drop_deletions, tournament)
                            .unwrap()
                            .collect::<Result<Vec<_>>>()
                            .unwrap();
                        assert_eq!(actual, expected, "{shape}/{long_keys}/{count}/{tournament}");
                        crate::codec::take_materialized_values();
                        let output = dir.path().join("output.sst");
                        Table::write_with(&output, 1000, 10000, 256, 10, |writer| {
                            Merge::with_selector(inputs, drop_deletions, tournament)?
                                .write_into(writer)
                        })
                        .unwrap();
                        assert_eq!(crate::codec::take_materialized_values(), 0);
                        output_bytes.push(std::fs::read(&output).unwrap());
                        std::fs::remove_file(output).unwrap();
                    }
                    assert_eq!(
                        output_bytes[0], output_bytes[1],
                        "{shape}/{long_keys}/{count}"
                    );
                }
            }
        }
    }
}

#[test]
fn tournament_visits_corrupt_losing_sources_before_publication() {
    let dir = tempfile::tempdir().unwrap();
    let mut tables = Vec::new();
    for i in 0..16u64 {
        let path = dir.path().join(format!("{i}.sst"));
        let rows = (b'a'..=b'c').map(|k| {
            Ok((
                vec![k],
                Record {
                    seq: 1 + i * 100 + u64::from(k),
                    value: Some(Bytes::from(vec![k; 256])),
                },
            ))
        });
        tables.push(Arc::new(Table::write(&path, i, rows, 3, 64, 10).unwrap()));
    }
    let old_path = dir.path().join("0.sst");
    let mut bytes = std::fs::read(&old_path).unwrap();
    let first = u32::from_le_bytes(bytes[8..12].try_into().unwrap()) as usize;
    bytes[8 + crate::codec::HEADER + first + crate::codec::HEADER + 18] ^= 1;
    std::fs::write(old_path, bytes).unwrap();
    for count in [9, 16] {
        let inputs = &tables[..count];
        let mut merge = Merge::new(inputs, true).unwrap();
        assert_eq!(merge.next().unwrap().unwrap().0, b"a");
        assert!(matches!(
            merge.next(),
            Some(Err(crate::EngineError::Corruption(_)))
        ));
        assert!(merge.next().is_none());
        let output = dir.path().join("output.sst");
        assert!(matches!(
            Table::write_with(&output, 1000, 48, 64, 10, |writer| {
                Merge::new(inputs, true)?.write_into(writer)
            }),
            Err(crate::EngineError::Corruption(_))
        ));
        assert!(!output.exists());
    }
}
