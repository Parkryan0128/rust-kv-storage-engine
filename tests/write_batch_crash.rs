#![cfg(feature = "fault-injection")]
mod common;
use rust_kv_storage_engine::Engine;
use std::{process::Command, time::Duration};

#[test]
fn crash_and_io_error_recover_the_entire_batch_or_none() {
    for mode in ["batch", "batch-error"] {
        for point in [
            "wal_before_append",
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
            if point == "wal_before_append" {
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
