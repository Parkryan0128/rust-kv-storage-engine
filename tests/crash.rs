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
