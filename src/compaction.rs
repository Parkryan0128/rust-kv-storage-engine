use crate::{
    memtable::Record,
    sstable::{Table, TableIter},
    Result,
};
use std::{
    cmp::Reverse,
    collections::{BTreeMap, BinaryHeap},
    sync::Arc,
};
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum CompactionStyle {
    #[default]
    SizeTiered,
    Full,
}

pub(crate) fn pick(
    tables: &[Arc<Table>],
    threshold: usize,
    style: CompactionStyle,
) -> Vec<Arc<Table>> {
    let files: Vec<_> = tables.iter().map(|t| (t.id, t.file_bytes)).collect();
    let ids = pick_ids(&files, threshold, style);
    tables
        .iter()
        .filter(|t| ids.contains(&t.id))
        .cloned()
        .collect()
}

fn pick_ids(files: &[(u64, u64)], threshold: usize, style: CompactionStyle) -> Vec<u64> {
    if files.len() < threshold {
        return vec![];
    }
    if style == CompactionStyle::Full {
        return files.iter().map(|(id, _)| *id).collect();
    }
    let mut buckets: BTreeMap<u32, Vec<u64>> = BTreeMap::new();
    for &(id, bytes) in files {
        buckets.entry(bytes.max(1).ilog2()).or_default().push(id);
    }
    for mut ids in buckets.into_values() {
        if ids.len() >= threshold {
            ids.sort_unstable();
            ids.truncate(threshold);
            return ids;
        }
    }
    vec![]
}

pub(crate) struct Merge {
    iters: Vec<TableIter>,
    heap: BinaryHeap<Reverse<(Vec<u8>, usize)>>,
    records: Vec<Option<Record>>,
    error: Option<crate::EngineError>,
    drop_tombstones: bool,
}
impl Merge {
    pub fn new(tables: &[Arc<Table>], drop_tombstones: bool) -> Result<Self> {
        let mut m = Self {
            iters: tables.iter().map(|t| t.iter()).collect(),
            heap: BinaryHeap::new(),
            records: vec![None; tables.len()],
            error: None,
            drop_tombstones,
        };
        for i in 0..m.iters.len() {
            m.advance(i)?;
        }
        Ok(m)
    }
    fn advance(&mut self, i: usize) -> Result<()> {
        if let Some(item) = self.iters[i].next() {
            let (key, r) = item?;
            self.heap.push(Reverse((key, i)));
            self.records[i] = Some(r);
        }
        Ok(())
    }
}
impl Iterator for Merge {
    type Item = Result<(Vec<u8>, Record)>;
    fn next(&mut self) -> Option<Self::Item> {
        if let Some(e) = self.error.take() {
            self.heap.clear();
            return Some(Err(e));
        }
        loop {
            let Reverse((key, i)) = self.heap.pop()?;
            let mut newest = self.records[i].take().unwrap();
            if let Err(e) = self.advance(i) {
                self.error = Some(e);
                return self.next();
            }
            while self.heap.peek().is_some_and(|Reverse((k, _))| k == &key) {
                let Reverse((_, j)) = self.heap.pop().unwrap();
                let r = self.records[j].take().unwrap();
                if r.seq > newest.seq {
                    newest = r;
                }
                if let Err(e) = self.advance(j) {
                    self.error = Some(e);
                    return self.next();
                }
            }
            // Partial merges must keep deletions that hide records in other SSTs.
            if newest.value.is_some() || !self.drop_tombstones {
                return Some(Ok((key, newest)));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn picker_uses_smallest_ready_bucket_and_oldest_ids() {
        let files = [
            (9, 1_000_000),
            (8, 120),
            (2, 100),
            (6, 90),
            (4, 110),
            (10, 400),
            (11, 450),
            (12, 500),
        ];
        assert_eq!(pick_ids(&files, 3, CompactionStyle::SizeTiered), [2, 4, 6]);
        assert!(pick_ids(&files, 9, CompactionStyle::SizeTiered).is_empty());
        assert_eq!(
            pick_ids(&files, 3, CompactionStyle::Full).len(),
            files.len()
        );
    }

    #[test]
    fn size_classes_handle_boundaries_and_large_files() {
        assert_eq!(
            pick_ids(
                &[(1, 127), (2, 128), (3, 255)],
                2,
                CompactionStyle::SizeTiered
            ),
            [2, 3]
        );
        assert_eq!(
            pick_ids(&[(1, 1), (2, 2), (3, 4)], 2, CompactionStyle::SizeTiered),
            Vec::<u64>::new()
        );
        assert_eq!(
            pick_ids(
                &[(1, u64::MAX), (2, u64::MAX - 1)],
                2,
                CompactionStyle::SizeTiered
            ),
            [1, 2]
        );
    }
}
