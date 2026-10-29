use crate::{
    memtable::MemScan,
    sstable::{Table, TableScan},
    Engine, EngineError, Result,
};
use bytes::Bytes;
use std::{
    cmp::Reverse,
    collections::BinaryHeap,
    iter::FusedIterator,
    ops::{Bound, RangeBounds},
    sync::Arc,
};

pub(crate) struct KeyRange {
    pub start: Bound<Vec<u8>>,
    pub end: Bound<Vec<u8>>,
}

impl KeyRange {
    pub fn new<'a>(range: impl RangeBounds<&'a [u8]>) -> Result<Self> {
        let own = |bound: Bound<&&[u8]>| bound.map(|key| key.to_vec());
        let out = Self {
            start: own(range.start_bound()),
            end: own(range.end_bound()),
        };
        if let (Bound::Included(a) | Bound::Excluded(a), Bound::Included(b) | Bound::Excluded(b)) =
            (&out.start, &out.end)
        {
            if a > b {
                return Err(EngineError::InvalidConfig("range start exceeds end".into()));
            }
        }
        Ok(out)
    }

    pub fn is_empty(&self) -> bool {
        match (&self.start, &self.end) {
            (Bound::Included(a), Bound::Excluded(b))
            | (Bound::Excluded(a), Bound::Included(b) | Bound::Excluded(b)) => a == b,
            (_, Bound::Excluded(b)) if b.is_empty() => true,
            _ => false,
        }
    }

    pub fn allows_end(&self, key: &[u8]) -> bool {
        match &self.end {
            Bound::Included(end) => key <= end.as_slice(),
            Bound::Excluded(end) => key < end.as_slice(),
            Bound::Unbounded => true,
        }
    }
}

// Memory keys share their immutable allocation; SST keys retain their owned buffer.
// Both compare by bytes so source type never affects merge ordering.
enum CandidateKey {
    Shared(Bytes),
    Owned(Vec<u8>),
}
impl CandidateKey {
    fn bytes(&self) -> &[u8] {
        match self {
            Self::Shared(k) => k,
            Self::Owned(k) => k,
        }
    }
    fn into_vec(self) -> Vec<u8> {
        match self {
            Self::Shared(k) => k.to_vec(),
            Self::Owned(k) => k,
        }
    }
}
impl PartialEq for CandidateKey {
    fn eq(&self, other: &Self) -> bool {
        self.bytes() == other.bytes()
    }
}
impl Eq for CandidateKey {}
impl PartialOrd for CandidateKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for CandidateKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.bytes().cmp(other.bytes())
    }
}

const BORROWED_MERGE_SOURCES: usize = 8;

// Keep default release-libtest timing free of key-count instrumentation.
#[cfg(all(test, feature = "fault-injection"))]
thread_local! {
    static COPIED_KEYS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}
#[inline]
pub(crate) fn copy_scan_key(key: &[u8]) -> Vec<u8> {
    #[cfg(all(test, feature = "fault-injection"))]
    COPIED_KEYS.with(|count| count.set(count.get() + 1));
    key.to_vec()
}

enum Source {
    Memory { iter: MemScan, started: bool },
    Table(TableScan),
    Exhausted,
}

// A range often touches exactly one memory page stream or SST. Keep that
// source inline instead of allocating a merge directory for every short scan.
enum Sources {
    Empty,
    One(Source),
    Many(Vec<Source>),
}
impl Sources {
    fn collect(sources: impl IntoIterator<Item = Source>) -> Self {
        let mut sources = sources.into_iter().fuse();
        match (sources.next(), sources.next()) {
            (None, None) => Self::Empty,
            (Some(one), None) => Self::One(one),
            (Some(first), Some(second)) => {
                let mut many = vec![first, second];
                many.extend(sources);
                Self::Many(many)
            }
            _ => unreachable!("source iterator is fused"),
        }
    }
    fn as_mut_slice(&mut self) -> &mut [Source] {
        match self {
            Self::Empty => &mut [],
            Self::One(one) => std::slice::from_mut(one),
            Self::Many(many) => many,
        }
    }
    #[cfg(test)]
    fn capacity(&self) -> usize {
        match self {
            Self::Empty => 0,
            Self::One(_) => 1,
            Self::Many(many) => many.capacity(),
        }
    }
}
impl std::ops::Deref for Sources {
    type Target = [Source];
    fn deref(&self) -> &[Source] {
        match self {
            Self::Empty => &[],
            Self::One(one) => std::slice::from_ref(one),
            Self::Many(many) => many,
        }
    }
}
impl std::ops::DerefMut for Sources {
    fn deref_mut(&mut self) -> &mut [Source] {
        self.as_mut_slice()
    }
}

impl Source {
    fn head(&self) -> Option<(&[u8], u64)> {
        match self {
            Self::Memory { iter, .. } => iter
                .current()
                .map(|(key, record)| (key.as_ref(), record.seq)),
            Self::Table(iter) => iter.current().map(|record| (record.key, record.seq)),
            Self::Exhausted => None,
        }
    }
    fn owned_live_head(&self) -> Option<(Vec<u8>, Bytes)> {
        match self {
            Self::Memory { iter, .. } => {
                let (key, record) = iter.current()?;
                record
                    .value
                    .as_ref()
                    .map(|value| (key.to_vec(), value.clone()))
            }
            Self::Table(iter) => {
                let record = iter.current()?;
                record
                    .to_owned()
                    .value
                    .map(|value| (record.key.to_vec(), value))
            }
            Self::Exhausted => None,
        }
    }
}

/// An owned, forward iterator over one point-in-time key range.
///
/// Items are ordered by raw key bytes. Each live key appears once with its newest
/// value; deleted keys are omitted. Disk I/O is lazy and errors appear as items.
/// After the first error or exhaustion, all subsequent calls return `None`.
///
/// The iterator retains the database lock, overlapping immutable memory page groups and
/// selected SST handles. Consumed sources release their handles early. Compaction may unlink those SSTs,
/// but their disk space cannot be reclaimed while a scan still holds them.
/// SST buffering is one encoded block per source. Small disk/mixed merges borrow
/// their heads; larger merges additionally retain one candidate key per source.
/// Scans bypass the point-read cache. Collecting the iterator allocates the full
/// result in the caller. Later writes and batches do not change this snapshot.
#[must_use = "scans read SST data only when iterated"]
pub struct Scan {
    engine: Option<Engine>,
    sources: Sources,
    heap: BinaryHeap<Reverse<(CandidateKey, usize, u64)>>,
    pending: Vec<usize>,
    initialized: bool,
    borrowed_merge: bool,
    live_sources: usize,
}

impl Scan {
    pub(crate) fn new(
        engine: Engine,
        memory: Vec<MemScan>,
        tables: &[Arc<Table>],
        range: Arc<KeyRange>,
    ) -> Self {
        let sources = Sources::collect(
            memory
                .into_iter()
                .map(|iter| Source::Memory {
                    iter,
                    started: false,
                })
                .chain(
                    tables
                        .iter()
                        .filter(|table| table.overlaps(&range))
                        .map(|table| Source::Table(table.scan(range.clone()))),
                ),
        );
        let borrowed_merge = sources.len() <= BORROWED_MERGE_SOURCES
            && sources
                .iter()
                .any(|source| matches!(source, Source::Table(_)));
        Self {
            engine: Some(engine),
            borrowed_merge,
            live_sources: sources.len(),
            sources,
            heap: BinaryHeap::new(),
            pending: Vec::new(),
            initialized: false,
        }
    }

    pub(crate) fn empty() -> Self {
        Self {
            engine: None,
            borrowed_merge: false,
            sources: Sources::Empty,
            heap: BinaryHeap::new(),
            pending: Vec::new(),
            initialized: true,
            live_sources: 0,
        }
    }

    fn advance(&mut self, index: usize) -> Result<()> {
        match &mut self.sources[index] {
            Source::Memory { iter, started } => {
                if *started {
                    iter.advance();
                }
                *started = true;
                if let Some((key, record)) = iter.current() {
                    self.heap.push(Reverse((
                        CandidateKey::Shared(key.clone()),
                        index,
                        record.seq,
                    )));
                } else {
                    self.sources[index] = Source::Exhausted;
                    self.live_sources -= 1;
                }
            }
            Source::Table(iter) => {
                if let Some((key, seq)) = iter
                    .advance(self.engine.as_ref().unwrap().scan_counters(), |record| {
                        (record.key.to_vec(), record.seq)
                    })?
                {
                    self.heap
                        .push(Reverse((CandidateKey::Owned(key), index, seq)));
                } else {
                    self.sources[index] = Source::Exhausted;
                    self.live_sources -= 1;
                }
            }
            Source::Exhausted => {}
        }
        Ok(())
    }

    fn read_next(&mut self) -> Result<Option<(Vec<u8>, Bytes)>> {
        // With one source there are no competing versions. Consume its borrowed
        // record directly, without heap bookkeeping or decoding the winner again.
        if let [source] = self.sources.as_mut_slice() {
            loop {
                let item = match source {
                    Source::Memory { iter, .. } => iter.next_live().map(Some),
                    Source::Table(iter) => {
                        iter.advance(self.engine.as_ref().unwrap().scan_counters(), |r| {
                            r.to_owned()
                                .value
                                .map(|value| (copy_scan_key(r.key), value))
                        })?
                    }
                    Source::Exhausted => None,
                };
                match item {
                    Some(Some(row)) => return Ok(Some(row)),
                    Some(None) => continue,
                    None => return Ok(None),
                }
            }
        }
        if self.borrowed_merge {
            return self.read_borrowed();
        }
        if !self.initialized {
            for index in 0..self.sources.len() {
                self.advance(index)?;
            }
            self.initialized = true;
        }
        loop {
            // Delay advancing consumed sources until the next requested item.
            // Taking one result must not read the following block just to prefetch.
            while let Some(index) = self.pending.pop() {
                self.advance(index)?;
            }
            let Some(Reverse((key, index, mut seq))) = self.heap.pop() else {
                return Ok(None);
            };
            let mut winner = index;
            self.pending.push(index);
            while self
                .heap
                .peek()
                .is_some_and(|Reverse((other, _, _))| other == &key)
            {
                let Reverse((_, index, candidate_seq)) = self.heap.pop().unwrap();
                if candidate_seq > seq {
                    winner = index;
                    seq = candidate_seq;
                }
                self.pending.push(index);
            }
            // Each source has strictly increasing keys, so all versions of this
            // key are already in the heap; no source needs advancing to find more.
            let value = match &mut self.sources[winner] {
                Source::Memory { iter, .. } => iter.current().unwrap().1.value.clone(),
                Source::Table(iter) => iter.current().unwrap().to_owned().value,
                Source::Exhausted => unreachable!("exhausted sources have no heap entry"),
            };
            if let Some(value) = value {
                if self.live_sources == 1 && self.sources.len() > 1 {
                    self.keep_only(winner);
                }
                return Ok(Some((key.into_vec(), value)));
            }
        }
    }

    // No key allocation for losing versions or deletions. The linear walk is
    // bounded; large fan-in and memory-only merges keep the existing heap.
    fn advance_borrowed(&mut self, index: usize) -> Result<()> {
        let live = match &mut self.sources[index] {
            Source::Memory { iter, started } => {
                if *started {
                    iter.advance();
                }
                *started = true;
                iter.current().is_some()
            }
            Source::Table(iter) => iter
                .advance(self.engine.as_ref().unwrap().scan_counters(), |_| ())?
                .is_some(),
            Source::Exhausted => return Ok(()),
        };
        if !live {
            self.sources[index] = Source::Exhausted;
            self.live_sources -= 1;
        }
        Ok(())
    }

    fn read_borrowed(&mut self) -> Result<Option<(Vec<u8>, Bytes)>> {
        if !self.initialized {
            for index in 0..self.sources.len() {
                self.advance_borrowed(index)?;
            }
            self.initialized = true;
        }
        loop {
            while let Some(index) = self.pending.pop() {
                self.advance_borrowed(index)?;
            }
            let mut best: Option<(&[u8], usize, u64)> = None;
            for (index, source) in self.sources.iter().enumerate() {
                let Some((key, seq)) = source.head() else {
                    continue;
                };
                match best {
                    None => {
                        self.pending.push(index);
                        best = Some((key, index, seq));
                    }
                    Some((old, _, _)) if key < old => {
                        self.pending.clear();
                        self.pending.push(index);
                        best = Some((key, index, seq));
                    }
                    Some((old, _, old_seq)) if key == old => {
                        self.pending.push(index);
                        if seq > old_seq {
                            best = Some((key, index, seq));
                        }
                    }
                    _ => {}
                }
            }
            let Some((_, winner, _)) = best else {
                return Ok(None);
            };
            if let Some(item) = self.sources[winner].owned_live_head() {
                if self.live_sources == 1 {
                    self.keep_only(winner);
                }
                return Ok(Some(item));
            }
        }
    }

    fn keep_only(&mut self, winner: usize) {
        // The returned memory head is consumed, while TableScan advances on
        // its next call. Preserve that distinction without disk prefetch.
        debug_assert!(self.heap.is_empty());
        let mut source = std::mem::replace(&mut self.sources[winner], Source::Exhausted);
        if let Source::Memory { iter, started } = &mut source {
            iter.advance();
            *started = false;
        }
        self.sources = Sources::One(source);
        self.pending.clear();
    }

    fn finish(&mut self) {
        self.sources = Sources::Empty;
        self.heap = BinaryHeap::new();
        self.pending = Vec::new();
        self.engine = None;
    }
}

impl Iterator for Scan {
    type Item = Result<(Vec<u8>, Bytes)>;

    fn next(&mut self) -> Option<Self::Item> {
        self.engine.as_ref()?;
        match self.read_next() {
            Ok(Some(item)) => Some(Ok(item)),
            Ok(None) => {
                self.finish();
                None
            }
            Err(error) => {
                self.finish();
                Some(Err(error))
            }
        }
    }
}
impl FusedIterator for Scan {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Options;

    #[test]
    fn single_source_copies_only_live_keys_across_pages_and_bounds() {
        use crate::memtable::{MemIndex, Record};
        let dir = tempfile::tempdir().unwrap();
        let mut memory = MemIndex::default();
        let rows: Vec<_> = (0..1200u64)
            .map(|n| {
                let mut key = vec![7; 128];
                key.extend(n.to_be_bytes());
                let record = Record {
                    seq: n + 1,
                    value: (n % 17 == 0).then(|| Bytes::from_static(b"live")),
                };
                memory.insert(key.clone(), record.clone());
                (key, record)
            })
            .collect();
        let table = Arc::new(
            Table::write(
                &dir.path().join("one.sst"),
                1,
                rows.clone().into_iter().map(Ok),
                rows.len() as u64,
                512,
                10,
            )
            .unwrap(),
        );
        for disk in [false, true] {
            for (start, end) in [(0, 1200), (1, 17), (250, 800)] {
                let lo = &rows[start].0;
                let mut upper = vec![7; 128];
                upper.extend((end as u64).to_be_bytes());
                let range = Arc::new(KeyRange::new(lo.as_slice()..upper.as_slice()).unwrap());
                let mut scan = if disk {
                    Scan::new(Engine::new(), vec![], std::slice::from_ref(&table), range)
                } else {
                    Scan::new(
                        Engine::new(),
                        vec![memory.snapshot(range.clone()).unwrap()],
                        &[],
                        range,
                    )
                };
                #[cfg(feature = "fault-injection")]
                COPIED_KEYS.with(|n| n.set(0));
                let actual = scan.by_ref().collect::<Result<Vec<_>>>().unwrap();
                let expected: Vec<_> = rows[start..end]
                    .iter()
                    .filter_map(|(key, r)| r.value.clone().map(|v| (key.clone(), v)))
                    .collect();
                assert_eq!(actual, expected);
                #[cfg(feature = "fault-injection")]
                assert_eq!(COPIED_KEYS.with(|n| n.get()), expected.len());
                assert!(scan.next().is_none());
            }
        }
    }

    #[test]
    fn multi_source_late_errors_are_lazy_fused_and_release_the_database() {
        use std::fs;
        for count in [2, 8, 9] {
            for force_heap in [false, true] {
                for truncate in [false, true] {
                    let dir = tempfile::tempdir().unwrap();
                    let db = Engine::open_with_options(
                        dir.path(),
                        Options {
                            block_size: 64,
                            compaction_file_threshold: 1000,
                            ..Options::default()
                        },
                    )
                    .unwrap();
                    for generation in 0..count {
                        let mut batch = crate::WriteBatch::new();
                        for key in [b"a", b"b", b"c"] {
                            batch.put(key, [generation as u8; 64]);
                        }
                        db.write_batch(batch).unwrap();
                        db.flush().unwrap();
                    }
                    let path = fs::read_dir(dir.path().join("sst"))
                        .unwrap()
                        .next()
                        .unwrap()
                        .unwrap()
                        .path();
                    let original = fs::read(&path).unwrap();
                    let second =
                        8 + 12 + u32::from_le_bytes(original[8..12].try_into().unwrap()) as usize;
                    if truncate {
                        fs::OpenOptions::new()
                            .write(true)
                            .open(&path)
                            .unwrap()
                            .set_len(second as u64)
                            .unwrap();
                    } else {
                        let mut damaged = original.clone();
                        damaged[second + 12] ^= 1;
                        fs::write(&path, damaged).unwrap();
                    }
                    let make_scan = || {
                        let mut scan = db.scan(..).unwrap();
                        assert_eq!(scan.sources.len(), count);
                        if force_heap {
                            scan.borrowed_merge = false;
                        }
                        scan
                    };
                    let before = db.stats().block_reads;
                    let first = make_scan().take(1).collect::<Result<Vec<_>>>().unwrap();
                    assert_eq!(first[0].0, b"a");
                    assert_eq!(first[0].1.as_ref(), &[(count - 1) as u8; 64]);
                    assert_eq!(db.stats().block_reads - before, count as u64);
                    let mut scan = make_scan();
                    assert_eq!(scan.next().unwrap().unwrap().0, b"a");
                    drop(db);
                    assert!(matches!(Engine::open(dir.path()), Err(EngineError::Locked)));
                    let error = scan.next().unwrap().unwrap_err();
                    if truncate {
                        assert!(matches!(error, EngineError::Io(_)));
                    } else {
                        assert!(matches!(error, EngineError::Corruption(_)));
                    }
                    assert!(scan.next().is_none());
                    assert!(scan.next().is_none());
                    // Restore the fixture so reopening checks pin/lock release,
                    // independently of the intentionally damaged table.
                    fs::write(&path, original).unwrap();
                    assert!(Engine::open(dir.path()).is_ok());
                }
            }
        }
    }

    #[test]
    fn borrowed_and_heap_merges_match_with_binary_keys_deletes_and_memory_tail() {
        use crate::memtable::{MemIndex, Record};
        use std::collections::BTreeMap;
        for count in [1, 2, 3, 7, 8, 15] {
            let dir = tempfile::tempdir().unwrap();
            let mut tables = Vec::new();
            let mut model = BTreeMap::new();
            for generation in 0..count {
                let rows: Vec<_> = (0..40u64)
                    .map(|n| {
                        let mut key = vec![0; 80];
                        key.extend(n.to_be_bytes());
                        let record = Record {
                            seq: generation as u64 * 100 + n + 1,
                            value: (n % 5 != generation as u64 % 5)
                                .then(|| Bytes::from(vec![generation as u8; 128])),
                        };
                        model.insert(key.clone(), record.value.clone());
                        (key, record)
                    })
                    .collect();
                tables.push(Arc::new(
                    Table::write(
                        &dir.path().join(format!("{generation}.sst")),
                        generation as u64 + 1,
                        rows.into_iter().map(Ok),
                        40,
                        128,
                        10,
                    )
                    .unwrap(),
                ));
            }
            let mut memory = MemIndex::default();
            memory.insert(
                vec![255; 90],
                Record {
                    seq: 100_000,
                    value: Some(Bytes::from_static(b"tail")),
                },
            );
            model.insert(vec![255; 90], Some(Bytes::from_static(b"tail")));
            let range = Arc::new(KeyRange::new(..).unwrap());
            for force_heap in [false, true] {
                let mut scan = Scan::new(
                    Engine::new(),
                    vec![memory.snapshot(range.clone()).unwrap()],
                    &tables,
                    range.clone(),
                );
                let borrowed = count < BORROWED_MERGE_SOURCES && !force_heap;
                if force_heap {
                    scan.borrowed_merge = false;
                }
                assert_eq!(scan.borrowed_merge, borrowed);
                let mut expected = model
                    .iter()
                    .filter_map(|(key, value)| value.as_ref().map(|v| (key.clone(), v.clone())));
                while let Some(row) = scan.next() {
                    assert_eq!(row.unwrap(), expected.next().unwrap());
                    if borrowed {
                        assert!(
                            scan.heap.is_empty(),
                            "borrowed heads must not allocate heap keys"
                        );
                    }
                }
                assert!(expected.next().is_none());
                assert!(scan.next().is_none());
            }
        }
    }

    #[test]
    fn borrowed_memory_heads_merge_versions_and_switch_without_skipping() {
        use crate::memtable::{MemIndex, Record};
        let mut older = MemIndex::default();
        let mut newer = MemIndex::default();
        for k in 0..600u64 {
            older.insert(
                k.to_be_bytes().to_vec(),
                Record {
                    seq: k + 1,
                    value: Some(Bytes::from(vec![1; 128])),
                },
            );
            if k < 300 {
                newer.insert(
                    k.to_be_bytes().to_vec(),
                    Record {
                        seq: k + 1000,
                        value: (k % 3 != 0).then(|| Bytes::from(vec![2; 128])),
                    },
                );
            }
        }
        let range = Arc::new(KeyRange::new(..).unwrap());
        let mut scan = Scan::new(
            Engine::new(),
            vec![
                older.snapshot(range.clone()).unwrap(),
                newer.snapshot(range.clone()).unwrap(),
            ],
            &[],
            range,
        );
        for i in 0..2 {
            scan.advance(i).unwrap();
        }
        scan.initialized = true;
        assert!(scan
            .heap
            .iter()
            .all(|Reverse((key, _, _))| matches!(key, CandidateKey::Shared(_))));
        let rows = scan.collect::<Result<Vec<_>>>().unwrap();
        let expected: Vec<_> = (0..600u64).filter(|k| *k >= 300 || k % 3 != 0).collect();
        assert_eq!(rows.len(), expected.len());
        for ((key, value), k) in rows.into_iter().zip(expected) {
            assert_eq!(key, k.to_be_bytes());
            assert_eq!(value.as_ref(), &[if k < 300 { 2 } else { 1 }; 128]);
        }
        assert!(
            CandidateKey::Shared(Bytes::from_static(b"a")) < CandidateKey::Owned(b"b".to_vec())
        );
        assert!(
            CandidateKey::Owned(b"a".to_vec()) == CandidateKey::Shared(Bytes::from_static(b"a"))
        );
    }

    #[test]
    fn merge_switches_to_single_source_without_skipping_rows_or_tombstones() {
        let dir = tempfile::tempdir().unwrap();
        let db = Engine::open_with_options(
            dir.path(),
            Options {
                compaction_file_threshold: 1000,
                ..Options::default()
            },
        )
        .unwrap();
        for k in [b"a", b"b", b"c", b"d"] {
            db.put(k, k).unwrap();
        }
        db.delete(b"c").unwrap();
        db.flush().unwrap();
        db.put(b"a", b"new").unwrap();
        let mut scan = db.scan(..).unwrap();
        assert_eq!(scan.sources.len(), 2);
        assert_eq!(scan.next().unwrap().unwrap().1.as_ref(), b"new");
        assert_eq!(scan.next().unwrap().unwrap().0, b"b");
        assert_eq!(scan.sources.len(), 1);
        assert_eq!(scan.next().unwrap().unwrap().0, b"d");
        assert!(scan.next().is_none());
        assert!(scan.next().is_none());
    }

    #[test]
    fn disjoint_tables_are_pruned_before_and_after_reopen_without_eager_io() {
        let dir = tempfile::tempdir().unwrap();
        let opts = Options {
            compaction_file_threshold: 1000,
            block_size: 4096,
            ..Options::default()
        };
        let db = Engine::open_with_options(dir.path(), opts.clone()).unwrap();
        for g in 0..12u64 {
            for i in 0..20u64 {
                db.put(&(g * 100 + i).to_be_bytes(), b"value").unwrap();
            }
            db.flush().unwrap();
        }
        let lo = 1005u64.to_be_bytes();
        let hi = 1008u64.to_be_bytes();
        let scan = db.scan(lo.as_slice()..hi.as_slice()).unwrap();
        assert_eq!(scan.sources.len(), 1);
        assert!(scan.sources.capacity() <= 4);
        let empty = db.scan(2000u64.to_be_bytes().as_slice()..).unwrap();
        assert_eq!(empty.sources.capacity(), 0);
        drop(empty);
        let before = db.stats().block_reads;
        assert_eq!(scan.count(), 3);
        assert_eq!(db.stats().block_reads - before, 1);
        drop(db);
        let db = Engine::open_with_options(dir.path(), opts).unwrap();
        assert_eq!(db.stats().block_reads, 0);
        let first = db.scan(lo.as_slice()..hi.as_slice()).unwrap();
        assert_eq!(first.sources.len(), 1);
        drop(first);
        assert_eq!(
            db.scan(lo.as_slice()..hi.as_slice())
                .unwrap()
                .collect::<Result<Vec<_>>>()
                .unwrap()
                .len(),
            3
        );
        assert_eq!(db.stats().block_reads, 1);
        let scan = db.scan(lo.as_slice()..hi.as_slice()).unwrap();
        assert_eq!(scan.sources.len(), 1);
        assert_eq!(scan.collect::<Result<Vec<_>>>().unwrap().len(), 3);
    }
}

#[cfg(test)]
#[path = "scan_profile.rs"]
mod scan_profile;
