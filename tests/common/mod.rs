#![allow(dead_code)]
use rust_kv_storage_engine::{Engine, Options};
use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
};
pub fn options() -> Options {
    Options {
        memtable_size_limit: 2048,
        max_immutable_memtables: 2,
        block_size: 256,
        block_cache_capacity: 8192,
        compaction_file_threshold: 3,
        ..Options::default()
    }
}
pub fn files(path: &Path, ext: &str) -> Vec<PathBuf> {
    let mut v: Vec<_> = fs::read_dir(path)
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|e| e == ext))
        .collect();
    v.sort();
    v
}
pub fn verify(e: &Engine, model: &BTreeMap<Vec<u8>, Vec<u8>>, keys: usize) {
    for k in 0..keys {
        let key = (k as u64).to_be_bytes();
        assert_eq!(
            e.get(&key).unwrap().as_deref(),
            model.get(key.as_slice()).map(Vec::as_slice),
            "key {k}"
        );
    }
}
pub struct Rng(pub u64);
pub fn run_with_deadline(
    command: &mut std::process::Command,
    deadline: std::time::Duration,
) -> Option<std::process::ExitStatus> {
    struct Child(std::process::Child);
    impl Drop for Child {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    let mut child = Child(command.spawn().unwrap());
    let start = std::time::Instant::now();
    loop {
        if let Some(status) = child.0.try_wait().unwrap() {
            return Some(status);
        }
        if start.elapsed() >= deadline {
            return None;
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
}
impl Rng {
    pub fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }
}
