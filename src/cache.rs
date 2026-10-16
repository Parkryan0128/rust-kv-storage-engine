use crate::block::ReadBlock;
use parking_lot::{Mutex, MutexGuard};
use std::{
    collections::{HashMap, HashSet},
    hash::{BuildHasherDefault, Hash, Hasher},
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
};
type Key = (u64, usize);
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct CacheKey {
    identity: Key,
    hash: u64,
}
impl From<Key> for CacheKey {
    fn from(identity: Key) -> Self {
        let mut h = identity
            .0
            .wrapping_add((identity.1 as u64).wrapping_mul(0x9e3779b97f4a7c15));
        h = (h ^ (h >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
        h = (h ^ (h >> 27)).wrapping_mul(0x94d049bb133111eb);
        Self {
            identity,
            hash: h ^ (h >> 31),
        }
    }
}
impl Hash for CacheKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_u64(self.hash);
    }
}
// Only internally generated table/block identities use this map. Mix once for
// both shard routing and lookup; equality still checks the complete identity.
#[derive(Default)]
struct KeyHasher(u64);
impl Hasher for KeyHasher {
    fn finish(&self) -> u64 {
        self.0
    }
    fn write(&mut self, _: &[u8]) {
        unreachable!("CacheKey hashes exactly one u64")
    }
    fn write_u64(&mut self, value: u64) {
        self.0 = value;
    }
}
const NONE: usize = usize::MAX;
// Includes the retained hash in both the map key and the LRU entry.
const ENTRY_OVERHEAD: usize = 112;
const MAX_SHARDS: usize = 8;
const MIN_SHARD_BYTES: usize = 256 * 1024;

#[repr(align(64))]
struct Shard(Mutex<Cache>);

// Small caches stay whole. Larger caches divide the same total entry budget
// across independent LRUs; table AND block identity distribute a single SST.
pub(crate) struct BlockCache {
    shards: Vec<Shard>,
    capacity: usize,
    overflow: Mutex<Cache>,
    regular_capacity: usize,
    frame_limits: [usize; 2],
}
impl BlockCache {
    #[cfg(test)]
    pub fn new(capacity: usize) -> Self {
        Self::with_block_size(capacity, 0)
    }
    pub fn with_block_size(capacity: usize, block_size: usize) -> Self {
        let minimum = MIN_SHARD_BYTES.max(block_size.saturating_mul(2).saturating_add(
            crate::codec::HEADER + std::mem::size_of::<ReadBlock>() + ENTRY_OVERHEAD,
        ));
        let mut count = 1;
        while count < MAX_SHARDS && capacity / (count * 2) >= minimum {
            count *= 2;
        }
        let regular_capacity = capacity / count;
        let mut frame_limits = [usize::MAX; 2];
        if count > 1 {
            for (indexed, limit) in frame_limits.iter_mut().enumerate() {
                let (mut lo, mut hi) = (0, regular_capacity);
                while lo < hi {
                    let mid = lo + (hi - lo).div_ceil(2);
                    if frame_charge(mid, indexed != 0) <= regular_capacity {
                        lo = mid;
                    } else {
                        hi = mid - 1;
                    }
                }
                *limit = lo;
            }
        }
        Self {
            shards: (0..count)
                .map(|n| {
                    Shard(Mutex::new(Cache::new(
                        capacity / count + usize::from(n < capacity % count),
                    )))
                })
                .collect(),
            capacity,
            overflow: Mutex::new(Cache::new(0)),
            regular_capacity,
            frame_limits,
        }
    }
    fn shard_index(&self, key: impl Into<CacheKey>) -> usize {
        key.into().hash as usize & (self.shards.len() - 1)
    }
    pub fn lock(&self, key: impl Into<CacheKey>) -> MutexGuard<'_, Cache> {
        self.shards[self.shard_index(key)].0.lock()
    }
    // Hit lookup never reserves space or reads the table retirement flag.
    // All cross-pool mutations obey overflow -> shards lock order.
    #[inline]
    pub fn is_regular(&self, frame_len: usize, indexed: bool) -> bool {
        frame_len <= self.frame_limits[usize::from(indexed)]
    }
    pub fn lock_for_frame(
        &self,
        key: CacheKey,
        frame_len: usize,
        indexed: bool,
    ) -> MutexGuard<'_, Cache> {
        if self.is_regular(frame_len, indexed) {
            self.lock(key)
        } else {
            self.overflow.lock()
        }
    }
    pub fn insert(
        &self,
        key: CacheKey,
        frame_len: usize,
        indexed: bool,
        block: Arc<ReadBlock>,
        cacheable: &AtomicBool,
    ) {
        if !self.is_regular(frame_len, indexed) {
            self.insert_oversized(key, block, cacheable);
            return;
        }
        let mut shard = self.lock(key);
        if !cacheable.load(Ordering::Acquire) {
            return;
        }
        // Pressure is counted only on misses/admissions, not the hit fast path.
        if shard.capacity < self.regular_capacity {
            shard.pressure += 1;
            if shard.pressure >= 32 {
                shard.pressure = 0;
                drop(shard);
                self.reclaim_idle_overflow(key);
                shard = self.lock(key);
                if !cacheable.load(Ordering::Acquire) {
                    return;
                }
            }
        }
        shard.insert(key, block);
    }
    #[cold]
    fn insert_oversized(&self, key: CacheKey, block: Arc<ReadBlock>, cacheable: &AtomicBool) {
        let mut overflow = self.overflow.lock();
        if !cacheable.load(Ordering::Acquire) || overflow.entries.contains_key(&key) {
            return;
        }
        let charge = block.allocated_bytes().saturating_add(ENTRY_OVERHEAD);
        // Leave at least a quarter of the total budget to ordinary blocks.
        let maximum = self.capacity - self.capacity / 4;
        if charge > maximum {
            return;
        }
        let needed = overflow.bytes().saturating_add(charge).min(maximum);
        if needed > overflow.capacity {
            self.resize_shards(self.capacity - needed);
            overflow.resize(needed);
        }
        overflow.insert(key, block);
        self.release_unused(&mut overflow);
    }
    #[cold]
    fn reclaim_idle_overflow(&self, key: CacheKey) {
        let mut overflow = self.overflow.lock();
        let activity = overflow.hits.wrapping_add(overflow.admissions);
        let mut shard = self.lock(key);
        let idle = shard.observed_overflow == activity;
        shard.observed_overflow = activity;
        drop(shard);
        if idle && overflow.tail != NONE {
            let tail = overflow.tail;
            overflow.remove(tail);
        }
        self.release_unused(&mut overflow);
    }
    fn release_unused(&self, overflow: &mut Cache) {
        let used = overflow.bytes();
        if used < overflow.capacity {
            overflow.resize(used);
            self.resize_shards(self.capacity - used);
        }
    }
    fn resize_shards(&self, budget: usize) {
        let count = self.shards.len();
        for (n, shard) in self.shards.iter().enumerate() {
            shard
                .0
                .lock()
                .resize(budget / count + usize::from(n < budget % count));
        }
    }
    pub fn is_disabled(&self) -> bool {
        self.capacity == 0
    }
    pub fn stats(&self) -> (usize, u64) {
        let overflow = self.overflow.lock();
        self.shards
            .iter()
            .fold((overflow.bytes(), overflow.hits), |(bytes, hits), shard| {
                let cache = shard.0.lock();
                (bytes + cache.bytes(), hits.wrapping_add(cache.hits))
            })
    }
    #[cfg(test)]
    pub fn bytes(&self) -> usize {
        self.stats().0
    }
    // Mark tables retired before calling this. An admission already in progress
    // finishes before its shard is purged; later admissions see the retirement.
    pub fn remove_tables(&self, retired: &HashSet<u64>) {
        let mut overflow = self.overflow.lock();
        overflow.remove_tables(retired);
        self.release_unused(&mut overflow);
        for shard in &self.shards {
            shard.0.lock().remove_tables(retired);
        }
    }
}
fn frame_charge(frame_len: usize, indexed: bool) -> usize {
    frame_len
        .saturating_add(std::mem::size_of::<ReadBlock>())
        .saturating_add(ENTRY_OVERHEAD)
        .saturating_add(if indexed {
            0
        } else {
            (frame_len.saturating_sub(crate::codec::HEADER) / 17)
                .max(1)
                .saturating_mul(std::mem::size_of::<u32>())
        })
}

struct Entry {
    key: CacheKey,
    block: Arc<ReadBlock>,
    size: usize,
    prev: usize,
    next: usize,
}
#[derive(Default)]
pub(crate) struct Counters {
    pub reads: AtomicU64,
    pub flush_bytes: AtomicU64,
    pub compaction_input_bytes: AtomicU64,
    pub compaction_output_bytes: AtomicU64,
    pub compactions: AtomicU64,
    pub max_compaction_inputs: AtomicU64,
    pub bloom_negatives: AtomicU64,
}
pub(crate) struct Cache {
    capacity: usize,
    bytes: usize,
    entries: HashMap<CacheKey, usize, BuildHasherDefault<KeyHasher>>,
    hits: u64,
    admissions: u64,
    pressure: u32,
    observed_overflow: u64,
    slots: Vec<Option<Entry>>,
    free: Vec<usize>,
    head: usize,
    tail: usize,
}
impl Cache {
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            bytes: 0,
            entries: HashMap::default(),
            hits: 0,
            admissions: 0,
            pressure: 0,
            observed_overflow: 0,
            slots: Vec::new(),
            free: Vec::new(),
            head: NONE,
            tail: NONE,
        }
    }
    fn resize(&mut self, capacity: usize) {
        while self.bytes > capacity {
            self.remove(self.tail);
        }
        self.capacity = capacity;
    }
    fn unlink(&mut self, index: usize) {
        let e = self.slots[index].as_ref().unwrap();
        let (prev, next) = (e.prev, e.next);
        if prev == NONE {
            self.head = next;
        } else {
            self.slots[prev].as_mut().unwrap().next = next;
        }
        if next == NONE {
            self.tail = prev;
        } else {
            self.slots[next].as_mut().unwrap().prev = prev;
        }
    }
    fn link_front(&mut self, index: usize) {
        let e = self.slots[index].as_mut().unwrap();
        e.prev = NONE;
        e.next = self.head;
        if self.head == NONE {
            self.tail = index;
        } else {
            self.slots[self.head].as_mut().unwrap().prev = index;
        }
        self.head = index;
    }
    fn remove(&mut self, index: usize) -> Entry {
        self.unlink(index);
        let e = self.slots[index].take().unwrap();
        self.entries.remove(&e.key);
        self.bytes -= e.size;
        self.free.push(index);
        e
    }
    pub fn get(&mut self, key: impl Into<CacheKey>) -> Option<Arc<ReadBlock>> {
        let key = key.into();
        let index = *self.entries.get(&key)?;
        if index != self.head {
            self.unlink(index);
            self.link_front(index);
        }
        // Already protected by this shard's mutex; no shared atomic cache line.
        self.hits = self.hits.wrapping_add(1);
        Some(self.slots[index].as_ref().unwrap().block.clone())
    }
    pub fn insert(&mut self, key: impl Into<CacheKey>, block: Arc<ReadBlock>) {
        let key = key.into();
        let size = block.allocated_bytes() + ENTRY_OVERHEAD;
        if size > self.capacity {
            return;
        }
        if let Some(&index) = self.entries.get(&key) {
            self.remove(index);
        }
        while self.bytes > self.capacity - size {
            self.remove(self.tail);
        }
        let index = self.free.pop().unwrap_or_else(|| {
            self.slots.push(None);
            self.slots.len() - 1
        });
        self.slots[index] = Some(Entry {
            key,
            block,
            size,
            prev: NONE,
            next: NONE,
        });
        self.link_front(index);
        self.entries.insert(key, index);
        self.bytes += size;
        self.admissions = self.admissions.wrapping_add(1);
    }
    // Recycle a victim only when even the minimum incoming charge needs space.
    // Arc::try_unwrap protects concurrent readers. Frames that cannot fit do
    // not evict useful blocks. No spare buffer is retained outside the budget.
    pub fn take_reusable(&mut self, frame_len: usize, indexed: bool) -> Option<ReadBlock> {
        // Before decoding legacy records, use an upper bound for their index.
        // An optimistic one-offset charge could evict a victim even though the
        // incoming dense block can never fit. Indexed frames carry no extra index.
        let index_bytes = if indexed {
            0
        } else {
            (frame_len.saturating_sub(crate::codec::HEADER) / 17).max(1)
                * std::mem::size_of::<u32>()
        };
        let frame_charge = frame_len
            .saturating_add(std::mem::size_of::<ReadBlock>())
            .saturating_add(ENTRY_OVERHEAD);
        let maximum = frame_charge.saturating_add(index_bytes);
        let minimum = frame_charge.saturating_add(if indexed { 0 } else { 4 });
        // The upper bound proves admission; the lower bound proves eviction is
        // necessary. Do not evict just because the pessimistic charge needs space.
        if self.tail == NONE || maximum > self.capacity || self.bytes <= self.capacity - minimum {
            return None;
        }
        let old = self.remove(self.tail).block;
        let block = Arc::try_unwrap(old).ok()?;
        (block.frame_capacity() <= frame_len.saturating_mul(2)).then_some(block)
    }
    pub fn bytes(&self) -> usize {
        self.bytes
    }
    pub fn block_budget(&self) -> usize {
        self.capacity.saturating_sub(ENTRY_OVERHEAD)
    }
    pub fn remove_tables(&mut self, retired: &HashSet<u64>) {
        let mut index = self.head;
        while index != NONE {
            let e = self.slots[index].as_ref().unwrap();
            let (next, table) = (e.next, e.key.identity.0);
            if retired.contains(&table) {
                self.remove(index);
            }
            index = next;
        }
        if self.entries.is_empty() {
            self.entries.shrink_to_fit();
            self.slots = Vec::new();
            self.free = Vec::new();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memtable::Record;
    use bytes::Bytes;

    #[test]
    fn four_megabyte_admission_preserves_small_cache_and_idle_budget_returns() {
        let capacity = 8 * 1024 * 1024;
        let cache = BlockCache::with_block_size(capacity, 16384);
        let live = AtomicBool::new(true);
        let small = block(32 * 1024);
        let mut counts = [0; MAX_SHARDS];
        let mut keys = Vec::new();
        for n in 0..10_000 {
            let key = CacheKey::from((1, n));
            let shard = cache.shard_index(key);
            if counts[shard] < 24 {
                counts[shard] += 1;
                keys.push(key);
                cache.insert(key, small.frame_capacity(), false, small.clone(), &live);
            }
            if keys.len() == 24 * MAX_SHARDS {
                break;
            }
        }
        assert_eq!(keys.len(), 192);
        let large = block(4 * 1024 * 1024);
        let large_key = CacheKey::from((2, 0));
        // Lookup (including a later I/O/decode failure) cannot reserve anything.
        assert!(cache
            .lock_for_frame(large_key, large.frame_capacity(), false)
            .get(large_key)
            .is_none());
        assert_eq!(cache.overflow.lock().capacity, 0);
        cache.insert(
            large_key,
            large.frame_capacity(),
            false,
            large.clone(),
            &live,
        );
        assert_eq!(
            cache.overflow.lock().capacity,
            large.allocated_bytes() + ENTRY_OVERHEAD
        );
        assert!(cache.shards.iter().all(|s| s.0.lock().capacity > 0));
        assert!(cache.bytes() <= capacity);
        for _ in 0..8 {
            for &key in &keys {
                let miss = cache.lock(key).get(key).is_none();
                if miss {
                    cache.insert(key, small.frame_capacity(), false, small.clone(), &live);
                }
            }
        }
        assert_eq!(cache.overflow.lock().capacity, 0);
        assert_eq!(
            cache
                .shards
                .iter()
                .map(|s| s.0.lock().capacity)
                .sum::<usize>(),
            capacity
        );
        assert!(keys.iter().all(|&key| cache.lock(key).get(key).is_some()));
    }

    #[test]
    fn frequently_used_oversized_entries_survive_small_miss_pressure() {
        let cache = BlockCache::with_block_size(8 * 1024 * 1024, 16384);
        let live = AtomicBool::new(true);
        let large = block(2 * 1024 * 1024);
        let key = CacheKey::from((1, 0));
        cache.insert(key, large.frame_capacity(), false, large.clone(), &live);
        let small = block(16384);
        for n in 0..2048 {
            if n % 16 == 0 {
                assert!(cache
                    .lock_for_frame(key, large.frame_capacity(), false)
                    .get(key)
                    .is_some());
            }
            cache.insert(
                CacheKey::from((2, n)),
                small.frame_capacity(),
                false,
                small.clone(),
                &live,
            );
        }
        assert!(cache
            .lock_for_frame(key, large.frame_capacity(), false)
            .get(key)
            .is_some());
        let before = cache.bytes();
        cache.insert(
            CacheKey::from((3, 0)),
            large.frame_capacity(),
            false,
            large,
            &AtomicBool::new(false),
        );
        assert_eq!(cache.bytes(), before);
        assert!(cache.bytes() <= 8 * 1024 * 1024);
    }

    #[test]
    fn precomputed_frame_limits_match_charge_classification() {
        for capacity in [0, 256, 512 * 1024, 8 * 1024 * 1024] {
            let cache = BlockCache::with_block_size(capacity, 16384);
            for indexed in [false, true] {
                let limit = cache.frame_limits[usize::from(indexed)];
                for length in [
                    0,
                    12,
                    16384,
                    limit.saturating_sub(1),
                    limit,
                    limit.saturating_add(1),
                ] {
                    assert_eq!(
                        cache.is_regular(length, indexed),
                        cache.shards.len() == 1
                            || frame_charge(length, indexed) <= cache.regular_capacity
                    );
                }
            }
        }
    }

    #[test]
    fn prehashed_collisions_keep_full_identity_and_retirement() {
        let a = CacheKey {
            identity: (1, 0),
            hash: 7,
        };
        let b = CacheKey {
            identity: (2, 0),
            hash: 7,
        };
        let mut cache = Cache::new(4096);
        let first = block(15);
        let second = block(31);
        cache.insert(a, first.clone());
        cache.insert(b, second.clone());
        assert!(Arc::ptr_eq(&cache.get(a).unwrap(), &first));
        assert!(Arc::ptr_eq(&cache.get(b).unwrap(), &second));
        cache.remove_tables(&HashSet::from([1]));
        assert!(cache.get(a).is_none());
        assert!(Arc::ptr_eq(&cache.get(b).unwrap(), &second));
        assert_eq!(cache.hits, 3);
        for identity in [(0, 0), (u64::MAX, usize::MAX), (1, 0), (1, 1)] {
            let key = CacheKey::from(identity);
            let mut hasher = KeyHasher::default();
            key.hash(&mut hasher);
            assert_eq!(hasher.finish(), key.hash);
            assert_eq!(key, CacheKey::from(identity));
        }
    }

    #[test]
    fn concurrent_shard_hits_are_cumulative_across_retirement() {
        let cache = Arc::new(BlockCache::new(8 * MIN_SHARD_BYTES));
        let mut keys = Vec::new();
        for shard in 0..8 {
            let key = (0..1000)
                .map(|n| CacheKey::from((1, n)))
                .find(|key| cache.shard_index(*key) == shard)
                .unwrap();
            cache.lock(key).insert(key, block(15));
            keys.push(key);
        }
        std::thread::scope(|scope| {
            for key in keys {
                let cache = &cache;
                scope.spawn(move || {
                    for _ in 0..1000 {
                        assert!(cache.lock(key).get(key).is_some());
                    }
                });
            }
        });
        assert_eq!(cache.stats().1, 8000);
        cache.remove_tables(&HashSet::from([1]));
        assert_eq!(cache.stats(), (0, 8000));
        cache.lock((2, 0)).insert((2, 0), block(15));
        assert!(cache.lock((2, 0)).get((2, 0)).is_some());
        assert_eq!(cache.stats().1, 8001);
    }

    #[test]
    fn shards_share_one_budget_and_do_not_serialize_unrelated_blocks() {
        use std::{sync::mpsc::sync_channel, thread, time::Duration};
        let capacity = 8 * MIN_SHARD_BYTES + 3;
        let cache = Arc::new(BlockCache::new(capacity));
        assert_eq!(cache.shards.len(), 8);
        assert_eq!(
            cache
                .shards
                .iter()
                .map(|s| s.0.lock().capacity)
                .sum::<usize>(),
            capacity
        );
        assert_eq!(BlockCache::new(1024).shards.len(), 1);
        assert_eq!(
            BlockCache::with_block_size(8 * 1024 * 1024, 1024 * 1024)
                .shards
                .len(),
            2
        );
        assert!(BlockCache::new(0).is_disabled());
        let first = (1, 0);
        let second = (1..100)
            .map(|n| (1, n))
            .find(|&key| cache.shard_index(key) != cache.shard_index(first))
            .unwrap();
        let held = cache.lock(first);
        let worker_cache = cache.clone();
        let (tx, rx) = sync_channel(1);
        let worker = thread::spawn(move || {
            worker_cache.lock(second).insert(second, block(15));
            tx.send(()).unwrap();
        });
        let completed = rx.recv_timeout(Duration::from_secs(5));
        drop(held);
        worker.join().unwrap();
        completed.unwrap();
        let used: HashSet<_> = (0..256).map(|n| cache.shard_index((1, n))).collect();
        assert_eq!(used.len(), 8, "blocks of one table must reach every shard");
        let value = block(64 * 1024);
        for n in 0..256 {
            cache.lock((1, n)).insert((1, n), value.clone());
        }
        assert!(cache.bytes() <= capacity);
        assert!(cache.bytes() > 0);
        cache.remove_tables(&HashSet::from([1]));
        assert_eq!(cache.bytes(), 0);
    }

    #[test]
    fn oversized_pool_borrows_one_budget_and_releases_retired_capacity() {
        let capacity = 8 * 1024 * 1024;
        let cache = BlockCache::with_block_size(capacity, 16384);
        let large = block(2 * 1024 * 1024);
        let key = CacheKey::from((9, 0));
        let frame_len = large.frame_capacity();
        cache.insert(key, frame_len, false, large.clone(), &AtomicBool::new(true));
        let pinned = cache
            .lock_for_frame(key, frame_len, false)
            .get(key)
            .unwrap();
        assert!(Arc::ptr_eq(&large, &pinned));
        for n in 0..512 {
            cache.lock((1, n)).insert((1, n), block(16384));
        }
        assert!(cache.bytes() <= capacity);
        assert!(cache
            .lock_for_frame(key, frame_len, false)
            .get(key)
            .is_some());
        let used = cache.bytes();
        // An impossible frame never consumes a reservation or evicts hot blocks.
        assert!(cache
            .lock_for_frame(CacheKey::from((10, 0)), capacity * 2, true)
            .take_reusable(capacity * 2, true)
            .is_none());
        assert_eq!(cache.bytes(), used);
        cache.remove_tables(&HashSet::from([9]));
        assert_eq!(cache.overflow.lock().capacity, 0);
        assert_eq!(
            cache
                .shards
                .iter()
                .map(|s| s.0.lock().capacity)
                .sum::<usize>(),
            capacity
        );
        assert_eq!(
            pinned.get(b"k").unwrap().value.unwrap().len(),
            2 * 1024 * 1024
        );
        assert!(cache.bytes() <= capacity);
    }

    #[test]
    fn concurrent_oversized_and_skewed_small_admissions_stay_bounded() {
        let cache = BlockCache::with_block_size(8 * 1024 * 1024, 16384);
        std::thread::scope(|scope| {
            for thread in 0..4u64 {
                let cache = &cache;
                scope.spawn(move || {
                    let block = block(if thread % 2 == 0 {
                        2 * 1024 * 1024
                    } else {
                        4096
                    });
                    for n in 0..80 {
                        let key = CacheKey::from((thread, n));
                        cache.insert(
                            key,
                            block.frame_capacity(),
                            false,
                            block.clone(),
                            &AtomicBool::new(true),
                        );
                        assert!(cache.bytes() <= 8 * 1024 * 1024);
                    }
                });
            }
        });
        cache.remove_tables(&HashSet::from([0, 1, 2, 3]));
        assert_eq!(cache.bytes(), 0);
    }

    fn block(value_len: usize) -> Arc<ReadBlock> {
        let mut payload = Vec::new();
        crate::codec::encode_record(
            b"k",
            &Record {
                seq: 1,
                value: Some(Bytes::from(vec![42; value_len])),
            },
            &mut payload,
        );
        let mut frame = Vec::new();
        crate::codec::write_frame(&mut frame, &payload).unwrap();
        Arc::new(ReadBlock::decode(frame, b"k", None, 1).unwrap())
    }

    #[test]
    fn hits_promote_blocks_and_eviction_preserves_outstanding_readers() {
        let first = block(15);
        let size = first.allocated_bytes() + ENTRY_OVERHEAD;
        let mut cache = Cache::new(size * 2);
        cache.insert((1, 0), first.clone());
        cache.insert((1, 1), block(15));
        let reader = cache.get((1, 0)).unwrap();
        assert!(Arc::ptr_eq(&reader, &first));
        cache.insert((2, 0), block(15));
        assert!(cache.get((1, 1)).is_none());
        assert!(cache.get((1, 0)).is_some());
        assert!(cache.get((2, 0)).is_some());
        assert_eq!(cache.hits, 3);
        assert_eq!(cache.bytes(), size * 2);
        cache.insert((3, 0), block(15));
        assert!(cache.get((1, 0)).is_none());
        assert_eq!(
            reader.get(b"k").unwrap().value.as_deref(),
            Some(&[42; 15][..])
        );
    }

    #[test]
    fn replacements_oversized_blocks_and_disabled_cache_respect_capacity() {
        let size = block(15).allocated_bytes() + ENTRY_OVERHEAD;
        let mut cache = Cache::new(size * 2);
        cache.insert((1, 0), block(15));
        cache.insert((2, 0), block(15));
        let smaller = block(0);
        cache.insert((1, 0), smaller.clone());
        let smaller_size = smaller.allocated_bytes() + ENTRY_OVERHEAD;
        assert_eq!(cache.bytes(), size + smaller_size);
        assert!(Arc::ptr_eq(&cache.get((1, 0)).unwrap(), &smaller));
        cache.insert((3, 0), block(256));
        assert_eq!(cache.bytes(), size + smaller_size);
        assert!(cache.get((3, 0)).is_none());
        let larger = block(100);
        let larger_size = larger.allocated_bytes() + ENTRY_OVERHEAD;
        assert!(larger_size <= size * 2 && larger_size + smaller_size > size * 2);
        cache.insert((2, 0), larger);
        assert_eq!(cache.bytes(), larger_size);
        assert!(cache.get((1, 0)).is_none());
        assert!(cache.get((2, 0)).is_some());
        let mut disabled = Cache::new(0);
        disabled.insert((1, 0), block(0));
        assert_eq!(disabled.bytes(), 0);
        assert!(disabled.get((1, 0)).is_none());
    }

    #[test]
    fn dense_legacy_blocks_that_cannot_fit_do_not_evict_for_recycling() {
        let mut cache = Cache::new(1200);
        cache.insert((1, 0), block(920));
        let before = cache.bytes();
        assert!(before > 0);
        // 55 empty values with one-byte keys: frame 1002, required offsets 220.
        assert!(cache.take_reusable(1002, false).is_none());
        assert_eq!(cache.bytes(), before);
        assert!(cache.get((1, 0)).is_some());
        // V3 carries its directory within the frame, so this bound is exact.
        assert!(cache.take_reusable(1226, true).is_none());
        assert_eq!(cache.bytes(), before);
    }

    #[test]
    fn uncertain_legacy_index_charge_does_not_force_early_eviction() {
        let existing = block(15);
        let minimum = 200 + std::mem::size_of::<ReadBlock>() + ENTRY_OVERHEAD + 4;
        let mut cache = Cache::new(existing.allocated_bytes() + ENTRY_OVERHEAD + minimum);
        cache.insert((1, 0), existing);
        let before = cache.bytes();
        // A single-record 200-byte frame fits in the remaining space, even
        // though the conservative dense-record upper bound would not.
        assert!(cache.take_reusable(200, false).is_none());
        assert_eq!(cache.bytes(), before);
        assert!(cache.get((1, 0)).is_some());
        // One byte less really does require eviction. Derive the boundary from
        // charged sizes so added bookkeeping cannot silently change the case.
        cache.capacity -= 1;
        assert!(cache.take_reusable(200, false).is_some());
    }

    #[test]
    fn recycling_reuses_storage_but_never_overwrites_an_outstanding_reader() {
        let first = block(15);
        let size = first.allocated_bytes() + ENTRY_OVERHEAD;
        let mut cache = Cache::new(size);
        cache.insert((1, 0), first.clone());
        assert!(cache.take_reusable(45, false).is_none());
        assert_eq!(cache.bytes(), 0);
        assert_eq!(
            first.get(b"k").unwrap().value.as_deref(),
            Some(&[42; 15][..])
        );
        let addresses = first.buffer_addresses();
        cache.insert((2, 0), first);
        assert!(cache.take_reusable(size * 2, false).is_none());
        assert_eq!(cache.bytes(), size);
        let recycled = cache.take_reusable(45, false).unwrap();
        assert_eq!(cache.bytes(), 0);
        // Reclaiming the block transferred its buffers rather than cloning them.
        let (frame, offsets) = recycled.into_buffers();
        assert_eq!((frame.as_ptr(), offsets.as_ptr()), addresses);
        assert_eq!(frame.len(), 45);
        assert!(!offsets.is_empty());
        let mut next = Vec::new();
        crate::codec::encode_record(
            b"z",
            &Record {
                seq: 2,
                value: None,
            },
            &mut next,
        );
        let mut new_frame = frame;
        new_frame.clear();
        crate::codec::write_frame(&mut new_frame, &next).unwrap();
        let recycled = ReadBlock::decode_reusing(new_frame, offsets, b"z", None, 2).unwrap();
        assert_eq!(recycled.get(b"z").unwrap().seq, 2);
        assert!(recycled.get(b"k").is_none());
    }

    #[test]
    fn indexed_lru_matches_a_reference_under_mixed_sizes_and_replacements() {
        let mut cache = Cache::new(2048);
        let mut reference: Vec<(Key, Arc<ReadBlock>, usize)> = Vec::new();
        let mut rng = 0x12345678u64;
        for step in 0..10_000 {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            let key = (rng % 3, (rng % 31) as usize);
            if step % 3 == 0 {
                let position = reference.iter().position(|(k, _, _)| *k == key);
                let expected = position.map(|i| {
                    let e = reference.remove(i);
                    let block = e.1.clone();
                    reference.insert(0, e);
                    block
                });
                let actual = cache.get(key);
                assert_eq!(actual.is_some(), expected.is_some());
                if let (Some(actual), Some(expected)) = (actual, expected) {
                    assert!(Arc::ptr_eq(&actual, &expected));
                }
            } else {
                let block = block((rng % 4096) as usize);
                let size = block.allocated_bytes() + ENTRY_OVERHEAD;
                cache.insert(key, block.clone());
                if size <= 2048 {
                    reference.retain(|(k, _, _)| *k != key);
                    while reference.iter().map(|e| e.2).sum::<usize>() + size > 2048 {
                        reference.pop().unwrap();
                    }
                    reference.insert(0, (key, block, size));
                }
            }
            assert_eq!(cache.bytes(), reference.iter().map(|e| e.2).sum::<usize>());
            assert_eq!(cache.entries.len(), reference.len());
            let mut index = cache.head;
            let mut previous = NONE;
            for (key, _, _) in &reference {
                let e = cache.slots[index].as_ref().unwrap();
                assert_eq!(e.key.identity, *key);
                assert_eq!(e.prev, previous);
                previous = index;
                index = e.next;
            }
            assert_eq!(index, NONE);
            assert_eq!(cache.tail, previous);
        }
    }

    #[test]
    fn retiring_tables_preserves_live_lru_entries_and_outstanding_values() {
        let mut cache = Cache::new(4096);
        let old = block(15);
        cache.insert((1, 0), old.clone());
        cache.insert((2, 0), block(15));
        cache.insert((1, 1), block(15));
        cache.insert((3, 0), block(15));
        cache.remove_tables(&HashSet::from([1, 3]));
        assert!(cache.get((1, 0)).is_none());
        assert!(cache.get((1, 1)).is_none());
        assert!(cache.get((3, 0)).is_none());
        assert!(cache.get((2, 0)).is_some());
        assert_eq!(cache.head, cache.tail);
        assert_eq!(old.get(b"k").unwrap().value.unwrap().len(), 15);
        cache.remove_tables(&HashSet::from([2]));
        assert_eq!(cache.bytes(), 0);
        assert_eq!((cache.head, cache.tail), (NONE, NONE));
        assert!(cache.slots.is_empty());
        cache.insert((4, 0), block(0));
        assert!(cache.get((4, 0)).is_some());
    }
}
