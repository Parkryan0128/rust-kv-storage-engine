use super::*;
use crate::scan::KeyRange;
use std::ops::Bound;

pub(crate) struct TableScan {
    table: Arc<Table>,
    range: Arc<KeyRange>,
    next_block: usize,
    block: Option<BlockCursor>,
    seek: bool,
    block_in_range: bool,
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
            seek: true,
            block_in_range: false,
            done: false,
        }
    }
}

impl TableScan {
    pub fn current(&self) -> Option<RecordRef<'_>> {
        self.block.as_ref().and_then(BlockCursor::current)
    }

    pub fn advance<T>(
        &mut self,
        counters: &Counters,
        consume: impl FnOnce(RecordRef<'_>) -> T,
    ) -> Result<Option<T>> {
        if let Some(block) = &mut self.block {
            block.advance()?;
        }
        while !self.done {
            if let Some(block) = &self.block {
                if let Some(record) = block.current() {
                    if !self.block_in_range && !self.range.allows_end(record.key) {
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
            let block = self.table.read_stream_block(
                self.next_block,
                self.block.take().map(BlockCursor::into_block),
            )?;
            // Decoding validates every key against the next block's first key.
            // If that boundary is allowed, this whole block is in range. The
            // final block has a validated maximum after read_stream_block.
            // A one-record block is already covered by the checked first key.
            // Partial boundary blocks still check each returned record.
            self.block_in_range = block.record_count() == 1
                || self
                    .table
                    .index
                    .get(self.next_block + 1)
                    .map(|next| next.first.as_slice())
                    .or_else(|| self.table.last_key.get().map(Vec::as_slice))
                    .is_some_and(|upper| self.range.allows_end(upper));
            let mut record = 0;
            if self.seek {
                if let Bound::Included(start) | Bound::Excluded(start) = &self.range.start {
                    let excluded = matches!(&self.range.start, Bound::Excluded(_));
                    let (mut lo, mut hi) = (0, block.record_count());
                    while lo < hi {
                        let mid = lo + (hi - lo) / 2;
                        let order = block.compare_key_at(mid, start)?;
                        if order.is_lt() || (excluded && order.is_eq()) {
                            lo = mid + 1;
                        } else {
                            hi = mid;
                        }
                    }
                    record = lo;
                }
                self.seek = false;
            }
            self.next_block += 1;
            self.block = Some(BlockCursor::new(block, record)?);
        }
        self.done = true;
        self.block = None;
        Ok(None)
    }
}
