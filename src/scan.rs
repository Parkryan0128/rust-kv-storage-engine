use crate::{
    memtable::{MemScan, Record},
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

enum Source {
    Memory { iter: MemScan, head: Option<Record> },
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
    heap: BinaryHeap<Reverse<(Vec<u8>, usize, u64)>>,
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
        let mut sources = Vec::with_capacity(tables.len() + memory.len());
        sources.extend(
            memory
                .into_iter()
                .map(|iter| Source::Memory { iter, head: None }),
        );
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
            Source::Memory { iter, head } => {
                if let Some((key, record)) = iter.next() {
                    self.heap.push(Reverse((key, index, record.seq)));
                    *head = Some(record);
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
                    self.heap.push(Reverse((key, index, seq)));
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
                Source::Memory { head, .. } => head.take().unwrap().value,
                Source::Table(iter) => iter.current()?.unwrap().to_owned().value,
                Source::Exhausted => unreachable!("exhausted sources have no heap entry"),
            };
            for &index in &self.pending {
                if let Source::Memory { head, .. } = &mut self.sources[index] {
                    *head = None;
                }
            }
            if let Some(value) = value {
                if self.live_sources == 1 && self.sources.len() > 1 {
                    // The head being returned is already consumed. The next
                    // call can advance this sole remaining source directly.
                    debug_assert!(self.heap.is_empty());
                    let source = std::mem::replace(&mut self.sources[winner], Source::Exhausted);
                    self.sources = vec![source];
                    self.pending.clear();
                }
                return Ok(Some((key, value)));
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
