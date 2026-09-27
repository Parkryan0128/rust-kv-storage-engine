use rust_kv_storage_engine::{Engine, Options};
use std::{
    collections::HashSet,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Barrier, Mutex,
    },
    thread,
};
#[derive(Clone, Debug)]
enum Op {
    Put(u64),
    Delete,
    Get(Option<u64>),
}
#[derive(Clone, Debug)]
struct Event {
    start: u64,
    end: u64,
    op: Op,
}
fn possible(
    history: &[Event],
    pred: &[u64],
    done: u64,
    value: Option<u64>,
    failed: &mut HashSet<(u64, Option<u64>)>,
) -> bool {
    if done == (1 << history.len()) - 1 {
        return true;
    }
    if !failed.insert((done, value)) {
        return false;
    }
    for (i, event) in history.iter().enumerate() {
        let bit = 1 << i;
        if done & bit != 0 || pred[i] & done != pred[i] {
            continue;
        }
        let next = match event.op {
            Op::Put(v) => Some(v),
            Op::Delete => None,
            Op::Get(v) if v == value => value,
            Op::Get(_) => continue,
        };
        if possible(history, pred, done | bit, next, failed) {
            return true;
        }
    }
    false
}
#[test]
fn concurrent_histories_admit_a_sequential_order_respecting_real_time() {
    for round in 0..60u64 {
        let d = tempfile::tempdir().unwrap();
        let e = Engine::open_with_options(
            d.path(),
            Options {
                memtable_size_limit: 64,
                block_size: 64,
                compaction_file_threshold: 2,
                ..Options::default()
            },
        )
        .unwrap();
        let clock = Arc::new(AtomicU64::new(0));
        let history = Arc::new(Mutex::new(vec![]));
        let barrier = Arc::new(Barrier::new(3));
        let mut workers = vec![];
        for t in 0..3u64 {
            let e = e.clone();
            let clock = clock.clone();
            let history = history.clone();
            let barrier = barrier.clone();
            workers.push(thread::spawn(move || {
                barrier.wait();
                for step in 0..4u64 {
                    let start = clock.fetch_add(1, Ordering::SeqCst);
                    let op = match (round + t + step) % 3 {
                        0 => {
                            let v = t * 10 + step;
                            e.put(b"key", &v.to_le_bytes()).unwrap();
                            Op::Put(v)
                        }
                        1 => {
                            e.delete(b"key").unwrap();
                            Op::Delete
                        }
                        _ => Op::Get(
                            e.get(b"key")
                                .unwrap()
                                .map(|b| u64::from_le_bytes(b[..].try_into().unwrap())),
                        ),
                    };
                    let end = clock.fetch_add(1, Ordering::SeqCst);
                    history.lock().unwrap().push(Event { start, end, op });
                    thread::yield_now();
                }
            }));
        }
        for worker in workers {
            worker.join().unwrap();
        }
        let h = history.lock().unwrap();
        let pred: Vec<u64> = h
            .iter()
            .map(|e| {
                h.iter()
                    .enumerate()
                    .filter(|(_, p)| p.end < e.start)
                    .fold(0, |bits, (j, _)| bits | (1 << j))
            })
            .collect();
        assert!(
            possible(&h, &pred, 0, None, &mut HashSet::new()),
            "not linearizable, round {round}: {h:?}"
        );
        e.compact().unwrap();
    }
}
