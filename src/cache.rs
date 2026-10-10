use crate::{block::ReadBlock, memtable::Record};
use std::{
    collections::{HashMap, HashSet},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};
pub(crate) type Block = Vec<(Vec<u8>, Record)>;
type Key = (u64, usize);
const NONE: usize = usize::MAX;
const ENTRY_OVERHEAD: usize = 96;
struct Entry {
    key: Key,
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
    pub hits: AtomicU64,
    pub bloom_negatives: AtomicU64,
}
pub(crate) struct Cache {
    capacity: usize,
    bytes: usize,
    entries: HashMap<Key, usize>,
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
            entries: HashMap::new(),
            slots: Vec::new(),
            free: Vec::new(),
            head: NONE,
            tail: NONE,
        }
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
    pub fn get(&mut self, key: Key, counters: &Counters) -> Option<Arc<ReadBlock>> {
        let index = *self.entries.get(&key)?;
        if index != self.head {
            self.unlink(index);
            self.link_front(index);
        }
        counters.hits.fetch_add(1, Ordering::Relaxed);
        Some(self.slots[index].as_ref().unwrap().block.clone())
    }
    pub fn insert(&mut self, key: Key, block: Arc<ReadBlock>) {
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
            let (next, table) = (e.next, e.key.0);
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
    use bytes::Bytes;

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
        let counters = Counters::default();
        cache.insert((1, 0), first.clone());
        cache.insert((1, 1), block(15));
        let reader = cache.get((1, 0), &counters).unwrap();
        assert!(Arc::ptr_eq(&reader, &first));
        cache.insert((2, 0), block(15));
        assert!(cache.get((1, 1), &counters).is_none());
        assert!(cache.get((1, 0), &counters).is_some());
        assert!(cache.get((2, 0), &counters).is_some());
        assert_eq!(counters.hits.load(Ordering::Relaxed), 3);
        assert_eq!(cache.bytes(), size * 2);
        cache.insert((3, 0), block(15));
        assert!(cache.get((1, 0), &counters).is_none());
        assert_eq!(
            reader.get(b"k").unwrap().value.as_deref(),
            Some(&[42; 15][..])
        );
    }

    #[test]
    fn replacements_oversized_blocks_and_disabled_cache_respect_capacity() {
        let size = block(15).allocated_bytes() + ENTRY_OVERHEAD;
        let mut cache = Cache::new(size * 2);
        let counters = Counters::default();
        cache.insert((1, 0), block(15));
        cache.insert((2, 0), block(15));
        let smaller = block(0);
        cache.insert((1, 0), smaller.clone());
        let smaller_size = smaller.allocated_bytes() + ENTRY_OVERHEAD;
        assert_eq!(cache.bytes(), size + smaller_size);
        assert!(Arc::ptr_eq(
            &cache.get((1, 0), &counters).unwrap(),
            &smaller
        ));
        cache.insert((3, 0), block(256));
        assert_eq!(cache.bytes(), size + smaller_size);
        assert!(cache.get((3, 0), &counters).is_none());
        let larger = block(100);
        let larger_size = larger.allocated_bytes() + ENTRY_OVERHEAD;
        assert!(larger_size <= size * 2 && larger_size + smaller_size > size * 2);
        cache.insert((2, 0), larger);
        assert_eq!(cache.bytes(), larger_size);
        assert!(cache.get((1, 0), &counters).is_none());
        assert!(cache.get((2, 0), &counters).is_some());
        let mut disabled = Cache::new(0);
        disabled.insert((1, 0), block(0));
        assert_eq!(disabled.bytes(), 0);
        assert!(disabled.get((1, 0), &counters).is_none());
    }

    #[test]
    fn dense_legacy_blocks_that_cannot_fit_do_not_evict_for_recycling() {
        let mut cache = Cache::new(1200);
        let counters = Counters::default();
        cache.insert((1, 0), block(920));
        let before = cache.bytes();
        assert!(before > 0);
        // 55 empty values with one-byte keys: frame 1002, required offsets 220.
        assert!(cache.take_reusable(1002, false).is_none());
        assert_eq!(cache.bytes(), before);
        assert!(cache.get((1, 0), &counters).is_some());
        // V3 carries its directory within the frame, so this bound is exact.
        assert!(cache.take_reusable(1226, true).is_none());
        assert_eq!(cache.bytes(), before);
    }

    #[test]
    fn uncertain_legacy_index_charge_does_not_force_early_eviction() {
        let mut cache = Cache::new(600);
        cache.insert((1, 0), block(15));
        let before = cache.bytes();
        // A single-record 200-byte frame fits in the remaining space, even
        // though the conservative dense-record upper bound would not.
        assert!(cache.take_reusable(200, false).is_none());
        assert_eq!(cache.bytes(), before);
        assert!(cache.get((1, 0), &Counters::default()).is_some());
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
        let counters = Counters::default();
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
                let actual = cache.get(key, &counters);
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
                assert_eq!(e.key, *key);
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
        let counters = Counters::default();
        let old = block(15);
        cache.insert((1, 0), old.clone());
        cache.insert((2, 0), block(15));
        cache.insert((1, 1), block(15));
        cache.insert((3, 0), block(15));
        cache.remove_tables(&HashSet::from([1, 3]));
        assert!(cache.get((1, 0), &counters).is_none());
        assert!(cache.get((1, 1), &counters).is_none());
        assert!(cache.get((3, 0), &counters).is_none());
        assert!(cache.get((2, 0), &counters).is_some());
        assert_eq!(cache.head, cache.tail);
        assert_eq!(old.get(b"k").unwrap().value.unwrap().len(), 15);
        cache.remove_tables(&HashSet::from([2]));
        assert_eq!(cache.bytes(), 0);
        assert_eq!((cache.head, cache.tail), (NONE, NONE));
        assert!(cache.slots.is_empty());
        cache.insert((4, 0), block(0));
        assert!(cache.get((4, 0), &counters).is_some());
    }
}
