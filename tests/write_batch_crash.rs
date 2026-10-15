#![cfg(feature = "fault-injection")]
mod common;
use rust_kv_storage_engine::{Engine, WriteBatch};
use std::{process::Command, time::Duration};

#[test]
fn crash_and_io_error_recover_the_entire_batch_or_none() {
    for mode in ["batch", "batch-error"] {
        for point in [
            "wal_before_append",
            "wal_after_reserve",
            "wal_before_sync",
            "wal_after_sync",
            "batch_after_first_apply",
        ] {
            let dir = tempfile::tempdir().unwrap();
            let db = Engine::open(dir.path()).unwrap();
            db.put(b"base", b"safe").unwrap();
            db.put(b"batch-old", b"old").unwrap();
            drop(db);
            let status = common::run_with_deadline(
                Command::new(env!("CARGO_BIN_EXE_crash_worker"))
                    .arg(dir.path())
                    .arg(mode)
                    .arg(point),
                Duration::from_secs(10),
            )
            .expect("batch child timed out");
            assert_eq!(
                status.code(),
                Some(if mode == "batch" { 86 } else { 0 }),
                "{mode}/{point}"
            );
            let db = Engine::open(dir.path()).unwrap();
            let committed = db.get(b"batch-a").unwrap().is_some();
            assert_eq!(db.get(b"base").unwrap().unwrap(), "safe");
            assert_eq!(
                db.get(b"batch-a").unwrap().as_deref(),
                committed.then_some(&b"new"[..])
            );
            assert_eq!(
                db.get(b"batch-b").unwrap().as_deref(),
                committed.then_some(&b"new"[..])
            );
            assert_eq!(
                db.get(b"batch-old").unwrap().as_deref(),
                (!committed).then_some(&b"old"[..])
            );
            assert_eq!(db.stats().sequence, if committed { 5 } else { 2 });
            if matches!(point, "wal_before_append" | "wal_after_reserve") {
                assert!(!committed);
            } else if point != "wal_before_sync" {
                assert!(committed, "{mode}/{point}");
            }
            db.put(b"after", b"recovery").unwrap();
            drop(db);
            let db = Engine::open(dir.path()).unwrap();
            assert_eq!(db.get(b"after").unwrap().unwrap(), "recovery");
        }
    }
}

#[test]
fn acknowledged_batches_survive_flush_and_compaction_publication_failures() {
    for compact in [false, true] {
        let mut points = vec![
            "sst_before_sync",
            "sst_after_sync",
            "sst_after_rename",
            "manifest_before_sync",
            "manifest_after_sync",
            "manifest_after_rename",
            "manifest_after_dir_sync",
        ];
        if compact {
            points.extend([
                "compaction_before_old_delete",
                "compaction_after_old_delete",
                "compaction_after_dir_sync",
            ]);
        } else {
            points.extend(["wal_after_rotation", "flush_before_wal_delete"]);
        }
        for point in points {
            for io_error in [false, true] {
                // The generic error worker uses a put for WAL failpoints;
                // rotation is covered here by a crash during compact's flush.
                if io_error && point == "wal_after_rotation" {
                    continue;
                }
                let dir = tempfile::tempdir().unwrap();
                let db = Engine::open(dir.path()).unwrap();
                db.put(b"batch-old", b"old").unwrap();
                db.put(b"base", b"safe").unwrap();
                db.flush().unwrap();
                // Both values exceed a block target: publication must preserve
                // the batch even when its records occupy separate SST blocks.
                let value = vec![7; 20 * 1024];
                let mut batch = WriteBatch::new();
                batch
                    .put("batch-a", &value)
                    .delete("batch-old")
                    .put("batch-b", &value);
                db.write_batch(batch).unwrap();
                if compact {
                    db.flush().unwrap();
                }
                drop(db);
                let mode = if !io_error {
                    "compact"
                } else if compact {
                    "compact-error"
                } else {
                    "error"
                };
                let status = common::run_with_deadline(
                    Command::new(env!("CARGO_BIN_EXE_crash_worker"))
                        .arg(dir.path())
                        .arg(mode)
                        .arg(point),
                    Duration::from_secs(10),
                )
                .expect("maintenance child timed out");
                assert_eq!(
                    status.code(),
                    Some(if io_error { 0 } else { 86 }),
                    "{compact}/{mode}/{point}"
                );
                let db = Engine::open(dir.path()).unwrap();
                assert_eq!(db.get(b"base").unwrap().unwrap(), "safe");
                assert_eq!(
                    db.get(b"batch-a").unwrap().as_deref(),
                    Some(value.as_slice())
                );
                assert_eq!(
                    db.get(b"batch-b").unwrap().as_deref(),
                    Some(value.as_slice())
                );
                assert!(db.get(b"batch-old").unwrap().is_none());
                assert!(db.get(b"must-reject").unwrap().is_none());
                assert_eq!(db.stats().sequence, 5);
                db.compact().unwrap();
                drop(db);
                let db = Engine::open(dir.path()).unwrap();
                assert_eq!(db.stats().sequence, 5);
                assert_eq!(
                    db.get(b"batch-a").unwrap().as_deref(),
                    Some(value.as_slice())
                );
                assert_eq!(
                    db.get(b"batch-b").unwrap().as_deref(),
                    Some(value.as_slice())
                );
                assert!(db.get(b"batch-old").unwrap().is_none());
            }
        }
    }
}
