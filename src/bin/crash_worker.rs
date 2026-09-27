use rust_kv_storage_engine::{Engine, EngineError, Options};
use std::io::{self, Write};
fn main() {
    let a: Vec<_> = std::env::args().collect();
    let dir = &a[1];
    let mode = &a[2];
    let opts = Options {
        memtable_size_limit: 1024 * 1024,
        compaction_file_threshold: 10000,
        ..Options::default()
    };
    if mode == "lock" {
        assert!(matches!(Engine::open(dir), Err(EngineError::Locked)));
        return;
    }
    let e = Engine::open_with_options(dir, opts).unwrap();
    if a.len() > 3 {
        std::env::set_var("KV_FAILPOINT", &a[3]);
    }
    if mode == "error" {
        std::env::set_var("KV_FAIL_ACTION", "error");
        let result = if a[3].starts_with("wal_") {
            e.put(b"uncertain", b"v")
        } else {
            e.flush()
        };
        assert!(result.is_err(), "injection did not fail");
        assert!(matches!(
            e.put(b"must-reject", b"v"),
            Err(EngineError::Background(_))
        ));
        assert!(matches!(e.get(b"base"), Err(EngineError::Background(_))));
        return;
    }
    if mode == "compact" {
        e.compact().unwrap();
        return;
    }
    let n = if mode == "stream" { 1_000_000 } else { 50 };
    for i in 0..n as u64 {
        e.put(&i.to_be_bytes(), &i.to_le_bytes()).unwrap();
        println!("ACK {i}");
        io::stdout().flush().unwrap();
        if mode == "stream" && i % 17 == 16 {
            e.flush().unwrap();
            if i % 51 == 50 {
                e.compact().unwrap();
            }
        }
    }
    e.flush().unwrap();
}
