use crate::{manifest::Manifest, memtable::Record, sstable::Table, Engine, Options};
use bytes::Bytes;

type Entry = (&'static [u8], u64, Option<&'static [u8]>);

fn fixture() -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    drop(Engine::open(dir.path()).unwrap());
    // Neither manifest order nor a file's maximum sequence determines which
    // version of an individual key is newest.
    let tables: &[(u64, &[Entry])] = &[
        (
            7,
            &[
                (b"a", 10, Some(b"old")),
                (b"b", 5, Some(b"live")),
                (b"m", 8, Some(b"old")),
            ],
        ),
        (
            4,
            &[
                (b"a", 30, Some(b"newest")),
                (b"b", 25, None),
                (b"m", 50, Some(b"older")),
            ],
        ),
        (
            9,
            &[
                (b"a", 20, Some(b"middle")),
                (b"m", 60, Some(b"keep")),
                (b"z", 100, Some(b"filler")),
            ],
        ),
    ];
    for &(id, entries) in tables {
        Table::write(
            &dir.path().join(format!("sst/{id:020}.sst")),
            id,
            entries.iter().map(|&(key, seq, value)| {
                Ok((
                    key.to_vec(),
                    Record {
                        seq,
                        value: value.map(Bytes::from_static),
                    },
                ))
            }),
            entries.len() as u64,
            4096,
            10,
        )
        .unwrap();
    }
    Manifest {
        wal_floor: 0,
        max_seq: 100,
        tables: vec![7, 4, 9],
    }
    .save(dir.path())
    .unwrap();
    dir
}

fn open(path: &std::path::Path) -> Engine {
    Engine::open_with_options(
        path,
        Options {
            block_cache_capacity: 0,
            compaction_file_threshold: 64,
            ..Options::default()
        },
    )
    .unwrap()
}

#[test]
fn reads_prune_obsolete_files_without_confusing_file_and_record_sequences() {
    let dir = fixture();
    let e = open(dir.path());
    assert_eq!(e.get(b"\0").unwrap(), None);
    assert_eq!(e.stats().block_reads, 0);
    assert_eq!(e.stats().bloom_negatives, 0);
    assert_eq!(e.get(b"m").unwrap().as_deref(), Some(&b"keep"[..]));
    assert_eq!(e.stats().block_reads, 1);
    let before = e.stats().block_reads;
    assert_eq!(e.get(b"a").unwrap().as_deref(), Some(&b"newest"[..]));
    assert_eq!(e.stats().block_reads - before, 2);
    assert_eq!(e.get(b"b").unwrap(), None);
    assert_eq!(e.get(b"n").unwrap(), None);
}

#[test]
fn read_order_stays_correct_after_flush_compaction_and_reopen() {
    let dir = fixture();
    let e = open(dir.path());
    e.put(b"a", b"replacement").unwrap();
    e.flush().unwrap();
    assert_eq!(e.get(b"a").unwrap().as_deref(), Some(&b"replacement"[..]));
    e.delete(b"a").unwrap();
    e.flush().unwrap();
    let before = e.stats().block_reads;
    assert_eq!(e.get(b"a").unwrap(), None);
    assert_eq!(e.stats().block_reads - before, 1);
    e.put(b"b", b"resurrected").unwrap();
    e.flush().unwrap();
    drop(e);
    for compact in [false, true] {
        let e = open(dir.path());
        if compact {
            e.compact().unwrap();
        }
        assert_eq!(e.get(b"a").unwrap(), None);
        assert_eq!(e.get(b"b").unwrap().as_deref(), Some(&b"resurrected"[..]));
        assert_eq!(e.get(b"m").unwrap().as_deref(), Some(&b"keep"[..]));
        assert_eq!(e.get(b"z").unwrap().as_deref(), Some(&b"filler"[..]));
    }
    let e = open(dir.path());
    assert_eq!(e.get(b"a").unwrap(), None);
    assert_eq!(e.get(b"b").unwrap().as_deref(), Some(&b"resurrected"[..]));
}
