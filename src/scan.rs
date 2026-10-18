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

enum Source {
    Memory { iter: MemScan, started: bool },
    Table(TableScan),
    Exhausted,
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
/// SST buffering is one encoded block plus one candidate key per source;
/// scans bypass the point-read cache. Collecting the iterator allocates the full
/// result in the caller. Later writes and batches do not change this snapshot.
#[must_use = "scans read SST data only when iterated"]
pub struct Scan {
    engine: Option<Engine>,
    sources: Vec<Source>,
    heap: BinaryHeap<Reverse<(CandidateKey, usize, u64)>>,
    pending: Vec<usize>,
    initialized: bool,
    live_sources: usize,
}

impl Scan {
    pub(crate) fn new(
        engine: Engine,
        memory: Vec<MemScan>,
        tables: &[Arc<Table>],
        range: Arc<KeyRange>,
    ) -> Self {
        let mut sources = Vec::with_capacity(memory.len());
        sources.extend(memory.into_iter().map(|iter| Source::Memory {
            iter,
            started: false,
        }));
        sources.extend(
            tables
                .iter()
                .filter(|table| table.overlaps(&range))
                .map(|table| Source::Table(table.scan(range.clone()))),
        );
        Self {
            engine: Some(engine),
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
            sources: Vec::new(),
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
                    Source::Memory { iter, .. } => iter.next().map(|(key, r)| (key, r.value)),
                    Source::Table(iter) => iter
                        .advance(self.engine.as_ref().unwrap().scan_counters(), |r| {
                            (r.key.to_vec(), r.to_owned().value)
                        })?,
                    Source::Exhausted => None,
                };
                match item {
                    Some((key, Some(value))) => return Ok(Some((key, value))),
                    Some((_, None)) => continue,
                    None => return Ok(None),
                }
            }
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
                    // The head being returned is already consumed. The next
                    // call can advance this sole remaining source directly.
                    debug_assert!(self.heap.is_empty());
                    let mut source =
                        std::mem::replace(&mut self.sources[winner], Source::Exhausted);
                    if let Source::Memory { iter, started } = &mut source {
                        iter.advance();
                        *started = false;
                    }
                    self.sources = vec![source];
                    self.pending.clear();
                }
                return Ok(Some((key.into_vec(), value)));
            }
        }
    }

    fn finish(&mut self) {
        self.sources = Vec::new();
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
    fn disjoint_tables_are_pruned_and_reopened_bounds_are_learned_without_eager_io() {
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
        assert_eq!(
            db.scan(lo.as_slice()..hi.as_slice())
                .unwrap()
                .collect::<Result<Vec<_>>>()
                .unwrap()
                .len(),
            3
        );
        let scan = db.scan(lo.as_slice()..hi.as_slice()).unwrap();
        assert_eq!(scan.sources.len(), 1);
        assert_eq!(scan.collect::<Result<Vec<_>>>().unwrap().len(), 3);
    }
}
