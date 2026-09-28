#![cfg(feature = "fault-injection")]
mod common;
use common::*;
use rust_kv_storage_engine::Engine;
use std::{
    io::{BufRead, BufReader},
    process::{Command, Stdio},
};
fn worker() -> Command {
    Command::new(env!("CARGO_BIN_EXE_crash_worker"))
}
fn seed(path: &std::path::Path) {
    let e = Engine::open(path).unwrap();
    e.put(b"base", b"durable").unwrap();
    e.put(b"deleted", b"old").unwrap();
    e.flush().unwrap();
    e.delete(b"deleted").unwrap();
}
fn verify_base(e: &Engine) {
    assert_eq!(e.get(b"base").unwrap().unwrap(), "durable");
    assert_eq!(e.get(b"deleted").unwrap(), None);
}
#[test]
fn every_flush_publication_crash_boundary_preserves_acknowledged_writes() {
    let points = [
        "wal_before_append",
        "wal_before_sync",
        "wal_after_sync",
        "wal_after_rotation",
        "sst_before_sync",
        "sst_after_sync",
        "sst_after_rename",
        "manifest_before_sync",
        "manifest_after_sync",
        "manifest_after_rename",
        "manifest_after_dir_sync",
        "flush_before_wal_delete",
    ];
    for point in points {
        let d = tempfile::tempdir().unwrap();
        seed(d.path());
        let out = worker()
            .arg(d.path())
            .arg("flush")
            .arg(point)
            .output()
            .unwrap();
        assert_eq!(
            out.status.code(),
            Some(86),
            "{point}: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        let text = String::from_utf8(out.stdout).unwrap();
        let e = Engine::open_with_options(d.path(), options()).unwrap();
        verify_base(&e);
        for line in text.lines() {
            let i: u64 = line.strip_prefix("ACK ").unwrap().parse().unwrap();
            assert_eq!(
                e.get(&i.to_be_bytes()).unwrap().as_deref(),
                Some(&i.to_le_bytes()[..]),
                "point {point}, key {i}"
            );
        }
        e.put(b"after-crash", point.as_bytes()).unwrap();
        e.compact().unwrap();
        drop(e);
        let e = Engine::open(d.path()).unwrap();
        verify_base(&e);
        assert_eq!(
            e.get(b"after-crash").unwrap().as_deref(),
            Some(point.as_bytes())
        );
    }
}
#[test]
fn every_compaction_publication_crash_boundary_preserves_values_and_deletes() {
    for point in [
        "sst_before_sync",
        "sst_after_sync",
        "sst_after_rename",
        "manifest_before_sync",
        "manifest_after_sync",
        "manifest_after_rename",
        "manifest_after_dir_sync",
        "compaction_before_old_delete",
    ] {
        let d = tempfile::tempdir().unwrap();
        seed(d.path());
        {
            let e = Engine::open(d.path()).unwrap();
            e.flush().unwrap();
            e.put(b"base", b"newest").unwrap();
            e.flush().unwrap();
        }
        let out = worker()
            .arg(d.path())
            .arg("compact")
            .arg(point)
            .output()
            .unwrap();
        assert_eq!(
            out.status.code(),
            Some(86),
            "{point}: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        let e = Engine::open(d.path()).unwrap();
        assert_eq!(e.get(b"base").unwrap().unwrap(), "newest");
        assert_eq!(e.get(b"deleted").unwrap(), None);
        e.compact().unwrap();
        assert_eq!(e.stats().sst_records, 1);
    }
}
#[test]
fn actual_sigkill_during_writes_flushes_and_compactions_recovers_all_acks() {
    for trial in 0..8 {
        let d = tempfile::tempdir().unwrap();
        seed(d.path());
        let mut child = worker()
            .arg(d.path())
            .arg("stream")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let mut r = BufReader::new(child.stdout.take().unwrap());
        let count = 75 + trial * 11;
        for i in 0..count {
            let mut line = String::new();
            assert!(r.read_line(&mut line).unwrap() > 0);
            assert_eq!(line.trim(), format!("ACK {i}"));
        }
        child.kill().unwrap();
        let status = child.wait().unwrap();
        assert!(!status.success());
        let e = Engine::open_with_options(d.path(), options()).unwrap();
        verify_base(&e);
        for i in 0..count as u64 {
            assert_eq!(
                e.get(&i.to_be_bytes()).unwrap().as_deref(),
                Some(&i.to_le_bytes()[..])
            );
        }
        e.compact().unwrap();
    }
}
#[test]
fn io_failure_poisons_engine_until_reopen() {
    for point in [
        "wal_before_append",
        "wal_before_sync",
        "wal_after_sync",
        "sst_before_sync",
        "manifest_before_sync",
        "manifest_after_rename",
        "flush_before_wal_delete",
    ] {
        let d = tempfile::tempdir().unwrap();
        seed(d.path());
        let out = worker()
            .arg(d.path())
            .arg("error")
            .arg(point)
            .output()
            .unwrap();
        assert!(
            out.status.success(),
            "{point}: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        let e = Engine::open(d.path()).unwrap();
        verify_base(&e);
        assert_eq!(e.get(b"must-reject").unwrap(), None);
        e.put(b"healthy", b"yes").unwrap();
    }
}
#[test]
fn second_process_cannot_open_a_locked_database() {
    let d = tempfile::tempdir().unwrap();
    let _e = Engine::open(d.path()).unwrap();
    assert!(worker()
        .arg(d.path())
        .arg("lock")
        .status()
        .unwrap()
        .success());
}

#[test]
fn partial_compaction_crashes_and_io_errors_preserve_unselected_files() {
    use rust_kv_storage_engine::Options;
    for mode in ["tiered", "tiered-error"] {
        for point in [
            "sst_before_sync",
            "sst_after_sync",
            "sst_after_rename",
            "manifest_before_sync",
            "manifest_after_sync",
            "manifest_after_rename",
            "manifest_after_dir_sync",
            "compaction_before_old_delete",
            "compaction_after_old_delete",
            "compaction_after_dir_sync",
        ] {
            let d = tempfile::tempdir().unwrap();
            let o = Options {
                memtable_size_limit: 8 * 1024 * 1024,
                compaction_file_threshold: 10000,
                ..Options::default()
            };
            let e = Engine::open_with_options(d.path(), o.clone()).unwrap();
            for k in 0..512u64 {
                e.put(&k.to_be_bytes(), &[1; 128]).unwrap();
            }
            e.flush().unwrap();
            let cold = files(&d.path().join("sst"), "sst")[0].clone();
            let original = std::fs::read(&cold).unwrap();
            for generation in 2..5u8 {
                e.delete(&0u64.to_be_bytes()).unwrap();
                for k in 1..16u64 {
                    e.put(&k.to_be_bytes(), &[generation; 128]).unwrap();
                }
                e.flush().unwrap();
            }
            drop(e);
            let out = worker()
                .arg(d.path())
                .arg(mode)
                .arg(point)
                .output()
                .unwrap();
            assert_eq!(
                out.status.code(),
                Some(if mode == "tiered" { 86 } else { 0 }),
                "{mode}/{point}: {}",
                String::from_utf8_lossy(&out.stderr)
            );
            assert_eq!(std::fs::read(&cold).unwrap(), original, "{point}");
            let e = Engine::open_with_options(d.path(), o).unwrap();
            assert_eq!(e.get(b"must-reject").unwrap(), None);
            assert_eq!(e.get(&0u64.to_be_bytes()).unwrap(), None, "{point}");
            for k in 1..512u64 {
                let v = if k < 16 { 4 } else { 1 };
                assert_eq!(
                    e.get(&k.to_be_bytes()).unwrap().as_deref(),
                    Some(&[v; 128][..]),
                    "{point}/{k}"
                );
            }
            e.compact().unwrap();
            assert_eq!(e.stats().sst_records, 511);
        }
    }
}
