use crate::memtable::Record;
use std::{
    collections::{BTreeMap, HashMap},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};
pub(crate) type Block = Vec<(Vec<u8>, Record)>;
type Key = (u64, usize);
struct Entry {
    block: Arc<Block>,
    size: usize,
    stamp: u64,
}
#[derive(Default)]
pub(crate) struct Counters {
    pub reads: AtomicU64,
    pub flush_bytes: AtomicU64,
    pub compaction_input_bytes: AtomicU64,
    pub compaction_output_bytes: AtomicU64,
    pub compactions: AtomicU64,
    pub hits: AtomicU64,
    pub bloom_negatives: AtomicU64,
}
pub(crate) struct Cache {
    capacity: usize,
    bytes: usize,
    clock: u64,
    entries: HashMap<Key, Entry>,
    order: BTreeMap<u64, Key>,
}
impl Cache {
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            bytes: 0,
            clock: 0,
            entries: HashMap::new(),
            order: BTreeMap::new(),
        }
    }
    fn tick(&mut self) -> u64 {
        if self.clock == u64::MAX {
            self.entries.clear();
            self.order.clear();
            self.bytes = 0;
            self.clock = 0;
        }
        self.clock += 1;
        self.clock
    }
    pub fn get(&mut self, key: Key, counters: &Counters) -> Option<Arc<Block>> {
        let stamp = self.tick();
        let e = self.entries.get_mut(&key)?;
        self.order.remove(&e.stamp);
        e.stamp = stamp;
        self.order.insert(stamp, key);
        counters.hits.fetch_add(1, Ordering::Relaxed);
        Some(e.block.clone())
    }
    pub fn insert(&mut self, key: Key, block: Arc<Block>) {
        let size = block.iter().map(|(k, r)| r.size(k)).sum::<usize>() + 64;
        if size > self.capacity {
            return;
        }
        if let Some(old) = self.entries.remove(&key) {
            self.bytes -= old.size;
            self.order.remove(&old.stamp);
        }
        while self.bytes + size > self.capacity {
            let (stamp, old) = self.order.pop_first().unwrap();
            let e = self.entries.remove(&old).unwrap();
            debug_assert_eq!(stamp, e.stamp);
            self.bytes -= e.size;
        }
        let stamp = self.tick();
        self.bytes += size;
        self.order.insert(stamp, key);
        self.entries.insert(key, Entry { block, size, stamp });
    }
    pub fn bytes(&self) -> usize {
        self.bytes
    }
}
