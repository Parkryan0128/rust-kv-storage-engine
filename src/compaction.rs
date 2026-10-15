#[cfg(test)]
use crate::memtable::Record;
use crate::{
    sstable::{Table, TableCursor, TableWriter},
    Result,
};
use std::{collections::BTreeMap, sync::Arc};
pub(crate) const MAX_INPUTS: usize = 16;
pub(crate) const INPUT_BUFFER_BUDGET: usize = 512 * 1024;

// Budget encoded input buffers, including reuse slack and legacy offset arrays.
// Two inputs are always allowed so oversized records cannot prevent progress.
pub(crate) fn input_count(costs: impl Iterator<Item = usize>) -> usize {
    let mut bytes = 0usize;
    let mut count = 0;
    for cost in costs.take(MAX_INPUTS) {
        let next = bytes.saturating_add(cost);
        if count >= 2 && next > INPUT_BUFFER_BUDGET {
            break;
        }
        bytes = next;
        count += 1;
    }
    count
}

#[cfg(test)]
#[test]
fn fan_in_respects_buffer_budget_hard_cap_and_oversized_progress() {
    assert_eq!(input_count(std::iter::empty()), 0);
    assert_eq!(input_count([usize::MAX].into_iter()), 1);
    assert_eq!(input_count([usize::MAX; 3].into_iter()), 2);
    assert_eq!(input_count([INPUT_BUFFER_BUDGET / 4; 8].into_iter()), 4);
    assert_eq!(input_count([1; MAX_INPUTS + 1].into_iter()), MAX_INPUTS);
    assert_eq!(input_count([32 * 1024 + 64; 7].into_iter()), 7);
}
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
    cursors: Vec<TableCursor>,
    pending: Vec<usize>,
    #[cfg(test)]
    finished: bool,
    drop_tombstones: bool,
}
impl Merge {
    pub fn new(tables: &[Arc<Table>], drop_tombstones: bool) -> Result<Self> {
        Ok(Self {
            cursors: tables
                .iter()
                .cloned()
                .map(TableCursor::new)
                .collect::<Result<_>>()?,
            pending: Vec::with_capacity(tables.len()),
            #[cfg(test)]
            finished: false,
            drop_tombstones,
        })
    }

    fn consume_next<T>(
        &mut self,
        consume: impl FnOnce(crate::codec::RecordRef<'_>) -> Result<T>,
    ) -> Result<Option<T>> {
        loop {
            while let Some(i) = self.pending.pop() {
                self.cursors[i].advance()?;
            }
            // Fan-in is bounded by both input memory and a small hard cap.
            // Comparing borrowed heads
            // avoids heap keys and materializing values for superseded versions.
            let mut newest: Option<crate::codec::RecordRef<'_>> = None;
            for (i, cursor) in self.cursors.iter().enumerate() {
                if let Some(record) = cursor.current()? {
                    if newest.as_ref().is_none_or(|old| record.key < old.key) {
                        self.pending.clear();
                        self.pending.push(i);
                        newest = Some(record);
                    } else if let Some(old) = &mut newest {
                        if record.key == old.key {
                            self.pending.push(i);
                            if record.seq > old.seq {
                                *old = record;
                            }
                        }
                    }
                }
            }
            let Some(newest) = newest else {
                return Ok(None);
            };
            // Partial merges must keep deletions that hide records in other SSTs.
            // Advance these cursors only on the next call: the winner still
            // borrows its block until it has been serialized.
            if newest.value.is_some() || !self.drop_tombstones {
                return consume(newest).map(Some);
            }
        }
    }
    pub fn write_into(mut self, writer: &mut TableWriter) -> Result<()> {
        while self
            .consume_next(|record| writer.push(record.key, record.seq, record.value))?
            .is_some()
        {}
        Ok(())
    }
}

#[cfg(test)]
impl Iterator for Merge {
    type Item = Result<(Vec<u8>, Record)>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }
        match self.consume_next(|record| Ok((record.key.to_vec(), record.to_owned()))) {
            Ok(Some(record)) => Some(Ok(record)),
            result => {
                self.finished = true;
                self.cursors.clear();
                self.pending.clear();
                result.err().map(Err)
            }
        }
    }
}
#[cfg(test)]
impl std::iter::FusedIterator for Merge {}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[test]
    fn borrowed_merge_selects_newest_across_blocks_and_preserves_partial_deletions() {
        let dir = tempfile::tempdir().unwrap();
        let versions = [
            vec![
                (b'a', 9, Some(4096)),
                (b'b', 1, Some(4096)),
                (b'c', 8, Some(128)),
            ],
            vec![
                (b'a', 5, Some(128)),
                (b'b', 12, None),
                (b'd', 7, Some(128)),
                (b'y', 100, Some(128)),
            ],
            vec![
                (b'a', 13, None),
                (b'b', 15, Some(0)),
                (b'c', 2, Some(8192)),
                (b'e', 14, None),
            ],
            vec![
                (b'a', 11, Some(128)),
                (b'c', 16, Some(128)),
                (b'd', 17, None),
                (b'e', 4, Some(128)),
            ],
        ];
        let mut model = BTreeMap::<Vec<u8>, Record>::new();
        let mut tables = Vec::new();
        for (i, rows) in versions.into_iter().enumerate() {
            let records: Vec<_> = rows
                .into_iter()
                .map(|(key, seq, len)| {
                    let record = Record {
                        seq,
                        value: len.map(|len| Bytes::from(vec![seq as u8; len])),
                    };
                    if model.get([key].as_slice()).is_none_or(|old| old.seq < seq) {
                        model.insert(vec![key], record.clone());
                    }
                    (vec![key], record)
                })
                .collect();
            tables.push(Arc::new(
                Table::write(
                    &dir.path().join(format!("{i}.sst")),
                    i as u64,
                    records.iter().cloned().map(Ok),
                    records.len() as u64,
                    64,
                    10,
                )
                .unwrap(),
            ));
        }
        for order in [[0, 1, 2, 3], [3, 0, 2, 1], [1, 3, 0, 2]] {
            let inputs: Vec<_> = order.map(|i| tables[i].clone()).into();
            for drop_deletions in [false, true] {
                let actual = Merge::new(&inputs, drop_deletions)
                    .unwrap()
                    .collect::<Result<Vec<_>>>()
                    .unwrap();
                let expected: Vec<_> = model
                    .iter()
                    .filter(|(_, r)| !drop_deletions || r.value.is_some())
                    .map(|(k, r)| (k.clone(), r.clone()))
                    .collect();
                assert_eq!(actual, expected);
                let owned = dir.path().join("owned-output.sst");
                let streamed = dir.path().join("streamed-output.sst");
                Table::write(&owned, 10, actual.into_iter().map(Ok), 1000, 64, 10).unwrap();
                crate::codec::take_materialized_values();
                Table::write_with(&streamed, 11, 1000, 64, 10, |writer| {
                    Merge::new(&inputs, drop_deletions)?.write_into(writer)
                })
                .unwrap();
                assert_eq!(crate::codec::take_materialized_values(), 0);
                assert_eq!(
                    std::fs::read(&owned).unwrap(),
                    std::fs::read(&streamed).unwrap()
                );
                std::fs::remove_file(owned).unwrap();
                std::fs::remove_file(streamed).unwrap();
            }
        }
        let mut empty = Merge::new(&[], true).unwrap();
        assert!(empty.next().is_none());
        assert!(empty.next().is_none());
    }

    #[test]
    fn corruption_in_a_superseded_block_still_aborts_merge_and_publication() {
        let dir = tempfile::tempdir().unwrap();
        let old_path = dir.path().join("old.sst");
        let make = |seq| {
            (b'a'..=b'c').map(move |key| {
                Ok((
                    vec![key],
                    Record {
                        seq: seq + u64::from(key),
                        value: Some(Bytes::from(vec![key; 256])),
                    },
                ))
            })
        };
        let old = Arc::new(Table::write(&old_path, 1, make(1), 3, 64, 10).unwrap());
        let new =
            Arc::new(Table::write(&dir.path().join("new.sst"), 2, make(1000), 3, 64, 10).unwrap());
        let mut bytes = std::fs::read(&old_path).unwrap();
        let first_len = u32::from_le_bytes(bytes[8..12].try_into().unwrap()) as usize;
        let second = 8 + crate::codec::HEADER + first_len;
        bytes[second + crate::codec::HEADER + 18] ^= 1;
        std::fs::write(&old_path, bytes).unwrap();
        let tables = [old, new];
        let mut merge = Merge::new(&tables, true).unwrap();
        assert_eq!(merge.next().unwrap().unwrap().0, b"a");
        assert!(matches!(
            merge.next(),
            Some(Err(crate::EngineError::Corruption(_)))
        ));
        assert!(merge.next().is_none());
        let output = dir.path().join("merged.sst");
        assert!(matches!(
            Table::write_with(&output, 3, 6, 64, 10, |writer| {
                Merge::new(&tables, true)?.write_into(writer)
            }),
            Err(crate::EngineError::Corruption(_))
        ));
        assert!(!output.exists());
    }

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
