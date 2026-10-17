use super::*;
use crate::scan::KeyRange;
use std::ops::Bound;

pub(crate) struct TableScan {
    table: Arc<Table>,
    range: Arc<KeyRange>,
    next_block: usize,
    block: Option<ReadBlock>,
    record: usize,
    seek: bool,
    done: bool,
}

impl Table {
    pub(crate) fn overlaps(&self, range: &KeyRange) -> bool {
        self.index
            .first()
            .is_some_and(|first| range.allows_end(&first.first))
            && self.last_key.get().is_none_or(|last| match &range.start {
                Bound::Included(start) => last >= start,
                Bound::Excluded(start) => last > start,
                Bound::Unbounded => true,
            })
    }
    pub fn scan(self: &Arc<Self>, range: Arc<KeyRange>) -> TableScan {
        let next_block = match &range.start {
            Bound::Included(start) | Bound::Excluded(start) => self
                .index
                .partition_point(|entry| entry.first <= *start)
                .saturating_sub(1),
            Bound::Unbounded => 0,
        };
        TableScan {
            table: self.clone(),
            range,
            next_block,
            block: None,
            record: 0,
            seek: true,
            done: false,
        }
    }
}

impl TableScan {
    pub fn current(&self) -> Result<Option<RecordRef<'_>>> {
        self.block
            .as_ref()
            .map(|b| b.record_at(self.record))
            .transpose()
    }

    pub fn advance<T>(
        &mut self,
        counters: &Counters,
        consume: impl FnOnce(RecordRef<'_>) -> T,
    ) -> Result<Option<T>> {
        if self.block.is_some() {
            self.record += 1;
        }
        while !self.done {
            if let Some(block) = &self.block {
                if self.record < block.record_count() {
                    let record = block.record_at(self.record)?;
                    if !self.range.allows_end(record.key) {
                        self.done = true;
                        self.block = None;
                        return Ok(None);
                    }
                    return Ok(Some(consume(record)));
                }
            }
            let Some(entry) = self.table.index.get(self.next_block) else {
                break;
            };
            if !self.range.allows_end(&entry.first) {
                break;
            }
            counters.reads.fetch_add(1, Ordering::Relaxed);
            #[cfg(test)]
            crate::test_hooks::pause("scan_before_block_read");
            let block = self
                .table
                .read_stream_block(self.next_block, self.block.take())?;
            self.record = 0;
            if self.seek {
                if let Bound::Included(start) | Bound::Excluded(start) = &self.range.start {
                    let excluded = matches!(&self.range.start, Bound::Excluded(_));
                    let (mut lo, mut hi) = (0, block.record_count());
                    while lo < hi {
                        let mid = lo + (hi - lo) / 2;
                        let key = block.key_at(mid)?;
                        if key < start.as_slice() || (excluded && key == start.as_slice()) {
                            lo = mid + 1;
                        } else {
                            hi = mid;
                        }
                    }
                    self.record = lo;
                }
                self.seek = false;
            }
            self.next_block += 1;
            self.block = Some(block);
        }
        self.done = true;
        self.block = None;
        Ok(None)
    }
}
