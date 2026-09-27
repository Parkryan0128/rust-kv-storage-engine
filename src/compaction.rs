use crate::{
    memtable::Record,
    sstable::{Table, TableIter},
    Result,
};
use std::{cmp::Reverse, collections::BinaryHeap, sync::Arc};
pub(crate) struct Merge {
    iters: Vec<TableIter>,
    heap: BinaryHeap<Reverse<(Vec<u8>, usize)>>,
    records: Vec<Option<Record>>,
    error: Option<crate::EngineError>,
}
impl Merge {
    pub fn new(tables: &[Arc<Table>]) -> Result<Self> {
        let mut m = Self {
            iters: tables.iter().map(|t| t.iter()).collect(),
            heap: BinaryHeap::new(),
            records: vec![None; tables.len()],
            error: None,
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
            // Safe to drop tombstones: every disk table is included in this merge.
            if newest.value.is_some() {
                return Some(Ok((key, newest)));
            }
        }
    }
}
