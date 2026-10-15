use crate::{
    memtable::Record,
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

    pub fn bounds(&self) -> (Bound<&[u8]>, Bound<&[u8]>) {
        (
            self.start.as_ref().map(Vec::as_slice),
            self.end.as_ref().map(Vec::as_slice),
        )
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
    Memory {
        iter: std::collections::btree_map::IntoIter<Vec<u8>, Record>,
        head: Option<Record>,
    },
    Table(TableScan),
}

/// An owned, forward iterator over one point-in-time key range.
///
/// Items are ordered by raw key bytes. Each live key appears once with its newest
/// value; deleted keys are omitted. Disk I/O is lazy and errors appear as items.
/// After the first error or exhaustion, all subsequent calls return `None`.
///
/// The iterator retains the database lock, selected in-memory keys/values and
/// old SST handles until dropped or exhausted. Compaction may unlink those SSTs,
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
}

impl Scan {
    pub(crate) fn new(
        engine: Engine,
        memory: std::collections::BTreeMap<Vec<u8>, Record>,
        tables: &[Arc<Table>],
        range: KeyRange,
    ) -> Self {
        let range = Arc::new(range);
        let mut sources = Vec::with_capacity(tables.len() + 1);
        if !memory.is_empty() {
            sources.push(Source::Memory {
                iter: memory.into_iter(),
                head: None,
            });
        }
        sources.extend(
            tables
                .iter()
                .map(|table| Source::Table(table.scan(range.clone()))),
        );
        Self {
            engine: Some(engine),
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
        }
    }

    fn advance(&mut self, index: usize) -> Result<()> {
        match &mut self.sources[index] {
            Source::Memory { iter, head } => {
                if let Some((key, record)) = iter.next() {
                    self.heap.push(Reverse((key, index, record.seq)));
                    *head = Some(record);
                } else {
                    *head = None;
                }
            }
            Source::Table(iter) => {
                if let Some((key, seq)) =
                    iter.advance(self.engine.as_ref().unwrap().scan_counters())?
                {
                    self.heap.push(Reverse((key, index, seq)));
                }
            }
        }
        Ok(())
    }

    fn read_next(&mut self) -> Result<Option<(Vec<u8>, Bytes)>> {
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
            };
            for &index in &self.pending {
                if let Source::Memory { head, .. } = &mut self.sources[index] {
                    *head = None;
                }
            }
            if let Some(value) = value {
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
