use super::*;
use crate::{manifest::Manifest, Engine, Options};
use bytes::Bytes;

// Rebuild checksums deliberately so semantic validation is exercised too.
fn rewrite_summary(path: &Path, magic: &[u8; 8], edit: impl FnOnce(&mut Vec<u8>)) {
    let original = fs::read(path).unwrap();
    let footer = original.len() - FOOTER;
    let offset = u64::from_le_bytes(original[footer + 8..footer + 16].try_into().unwrap()) as usize;
    let mut input = &original[offset..footer];
    let mut summary = read_frame(&mut input, false).unwrap().unwrap();
    edit(&mut summary);
    let mut output = original[..offset].to_vec();
    output[..8].copy_from_slice(magic);
    write_frame(&mut output, &summary).unwrap();
    output.extend_from_slice(input);
    let size = output.len() - offset;
    let mut footer = magic.to_vec();
    put_u64(&mut footer, offset as u64);
    put_u64(&mut footer, size as u64);
    let crc = crc32fast::hash(&footer);
    put_u32(&mut footer, crc);
    output.extend(footer);
    fs::write(path, output).unwrap();
}

fn maximum_offset(summary: &[u8]) -> usize {
    let mut c = Cursor { b: summary };
    c.u64().unwrap();
    c.u64().unwrap();
    Bloom::decode(&mut c).unwrap();
    c.u64().unwrap();
    summary.len() - c.b.len()
}

fn table(path: &Path, keys: &[&[u8]], block: usize) -> Table {
    Table::write(
        path,
        2,
        keys.iter().enumerate().map(|(n, key)| {
            Ok((
                *key,
                Record {
                    seq: n as u64 + 1,
                    value: Some(Bytes::from_static(b"v")),
                },
            ))
        }),
        keys.len() as u64,
        block,
        10,
    )
    .unwrap()
}

#[test]
fn maximum_round_trips_empty_binary_and_large_keys_without_data_reads() {
    let dir = tempfile::tempdir().unwrap();
    let large = vec![255; 2 * 1024 * 1024];
    for keys in [
        vec![],
        vec![b"".as_slice()],
        vec![b"", b"\0", b"\xff"],
        vec![large.as_slice()],
    ] {
        let path = dir.path().join("bounds.sst");
        let published = table(&path, &keys, 64);
        drop(published);
        let reopened = Table::open(&path, 2).unwrap();
        assert_eq!(
            reopened.last_key.get().map(Vec::as_slice),
            keys.last().copied()
        );
        let cache = BlockCache::new(0);
        let counters = Counters::default();
        let mut beyond = keys.last().copied().unwrap_or_default().to_vec();
        beyond.push(0);
        let mut hash = None;
        assert!(reopened
            .get_hashed(&beyond, &mut hash, &cache, &counters)
            .unwrap()
            .is_none());
        assert!(hash.is_none());
        assert_eq!(counters.reads.load(Ordering::Relaxed), 0);
        for key in keys {
            assert_eq!(
                reopened
                    .get(key, &cache, &counters)
                    .unwrap()
                    .unwrap()
                    .value
                    .unwrap(),
                b"v".as_slice()
            );
        }
        drop(reopened);
        fs::remove_file(path).unwrap();
    }
}

#[test]
fn maximum_checksum_length_order_and_tail_disagreement_are_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("corrupt.sst");
    drop(table(&path, &[b"a", b"b", b"z"], 4096));
    let original = fs::read(&path).unwrap();
    for mode in 0..5 {
        fs::write(&path, &original).unwrap();
        rewrite_summary(&path, MAGIC, |summary| {
            let at = maximum_offset(summary);
            match mode {
                0 => summary[at..at + 4].copy_from_slice(&u32::MAX.to_le_bytes()),
                1 => summary[at..at + 4].copy_from_slice(&2u32.to_le_bytes()),
                2 => summary[at + 4] = 0,
                3 => summary[at + 4] = b'y',
                _ => summary.push(0),
            }
        });
        if mode == 3 {
            let table = Table::open(&path, 2).unwrap();
            assert!(matches!(
                table.block(0),
                Err(crate::EngineError::Corruption(_))
            ));
        } else {
            assert!(
                matches!(
                    Table::open(&path, 2),
                    Err(crate::EngineError::Corruption(_))
                ),
                "mode={mode}"
            );
        }
    }
    let mut damaged = original;
    let footer = damaged.len() - FOOTER;
    let offset = u64::from_le_bytes(damaged[footer + 8..footer + 16].try_into().unwrap()) as usize;
    damaged[offset + HEADER + 3] ^= 1;
    fs::write(&path, damaged).unwrap();
    assert!(matches!(
        Table::open(&path, 2),
        Err(crate::EngineError::Corruption(_))
    ));
}

#[test]
fn empty_and_single_record_metadata_reject_impossible_maxima() {
    let dir = tempfile::tempdir().unwrap();
    for keys in [vec![], vec![b"a".as_slice()]] {
        let path = dir.path().join(format!("{}.sst", keys.len()));
        drop(table(&path, &keys, 64));
        rewrite_summary(&path, MAGIC, |summary| {
            let at = maximum_offset(summary);
            summary.truncate(at);
            put_u32(summary, 1);
            summary.push(b'z');
        });
        assert!(matches!(
            Table::open(&path, 2),
            Err(crate::EngineError::Corruption(_))
        ));
    }
}

#[test]
fn version_three_learns_bounds_lazily_and_merges_with_version_four() {
    let dir = tempfile::tempdir().unwrap();
    drop(Engine::open(dir.path()).unwrap());
    let path = dir.path().join("sst/00000000000000000002.sst");
    drop(table(&path, &[b"old-a", b"old-z"], 4096));
    rewrite_summary(&path, INDEXED_MAGIC, |summary| {
        summary.truncate(maximum_offset(summary))
    });
    let old = Table::open(&path, 2).unwrap();
    assert!(old.indexed);
    assert!(old.last_key.get().is_none());
    old.block(0).unwrap();
    assert_eq!(old.last_key.get().unwrap(), b"old-z");
    drop(old);
    Manifest {
        wal_floor: 0,
        max_seq: 2,
        tables: vec![2],
    }
    .save(dir.path())
    .unwrap();
    let options = Options {
        compaction_file_threshold: 1000,
        ..Options::default()
    };
    let db = Engine::open_with_options(dir.path(), options.clone()).unwrap();
    assert_eq!(db.scan(b"z".as_slice()..).unwrap().count(), 0);
    assert_eq!(db.stats().block_reads, 1);
    db.put(b"new", b"value").unwrap();
    db.flush().unwrap();
    db.delete(b"old-a").unwrap();
    db.compact().unwrap();
    drop(db);
    let db = Engine::open_with_options(dir.path(), options).unwrap();
    assert_eq!(db.scan(b"z".as_slice()..).unwrap().count(), 0);
    assert_eq!(db.stats().block_reads, 0);
    assert_eq!(db.get(b"new").unwrap().unwrap(), "value");
    assert!(db.get(b"old-a").unwrap().is_none());
    assert_eq!(db.get(b"old-z").unwrap().unwrap(), "v");
    for entry in fs::read_dir(dir.path().join("sst")).unwrap() {
        assert_eq!(&fs::read(entry.unwrap().path()).unwrap()[..8], MAGIC);
    }
}

#[test]
fn version_three_scan_bounds_work_before_maximum_is_known() {
    use crate::scan::KeyRange;
    let dir = tempfile::tempdir().unwrap();
    let keys = [b"a".as_slice(), b"b", b"c", b"d", b"e"];
    for block in [1, 64, 4096] {
        let path = dir.path().join(format!("{block}.sst"));
        drop(table(&path, &keys, block));
        rewrite_summary(&path, INDEXED_MAGIC, |summary| {
            summary.truncate(maximum_offset(summary));
        });
        for end in [b"".as_slice(), b"a", b"bb", b"d", b"e", b"z"] {
            for included in [false, true] {
                let table = Arc::new(Table::open(&path, 2).unwrap());
                assert!(table.last_key.get().is_none());
                let range = if included {
                    KeyRange::new(..=end).unwrap()
                } else {
                    KeyRange::new(..end).unwrap()
                };
                let mut scan = table.scan(Arc::new(range));
                let counters = Counters::default();
                let mut rows = Vec::new();
                while let Some(key) = scan.advance(&counters, |r| r.key.to_vec()).unwrap() {
                    rows.push(key);
                }
                let expected: Vec<_> = keys
                    .iter()
                    .copied()
                    .filter(|key| if included { *key <= end } else { *key < end })
                    .map(<[u8]>::to_vec)
                    .collect();
                assert_eq!(
                    rows, expected,
                    "block={block}, end={end:?}, included={included}"
                );
                assert!(scan.advance(&counters, |_| ()).unwrap().is_none());
            }
        }
    }
}
