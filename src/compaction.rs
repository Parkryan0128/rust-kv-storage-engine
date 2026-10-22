#[cfg(test)]
use crate::memtable::Record;
use crate::{
    sstable::{Table, TableCursor, TableWriter},
    Result,
};
use std::{collections::BTreeMap, sync::Arc};
#[path = "compaction_selector.rs"]
mod selector;
use selector::Tournament;
pub(crate) const MAX_INPUTS: usize = 16;
pub(crate) const INPUT_BUFFER_BUDGET: usize = 512 * 1024;
// Actual publisher measurements favor the tree at the largest fan-in.
const TOURNAMENT_INPUTS: usize = 16;

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
    if tables.len() < threshold {
        return Vec::new();
    }
    if style == CompactionStyle::Full {
        return tables.to_vec();
    }
    // Positions preserve the table snapshot's sequence order without searching
    // every table for every selected ID.
    tiered_positions(tables.iter().map(|t| (t.id, t.file_bytes)), threshold)
        .into_iter()
        .map(|index| tables[index].clone())
        .collect()
}

fn tiered_positions(files: impl Iterator<Item = (u64, u64)>, threshold: usize) -> Vec<usize> {
    let mut buckets: BTreeMap<u32, Vec<(u64, usize)>> = BTreeMap::new();
    for (position, (id, bytes)) in files.enumerate() {
        buckets
            .entry(bytes.max(1).ilog2())
            .or_default()
            .push((id, position));
    }
    for mut candidates in buckets.into_values() {
        if candidates.len() >= threshold {
            candidates.sort_unstable_by_key(|&(id, _)| id);
            candidates.truncate(threshold);
            let mut positions: Vec<_> = candidates
                .into_iter()
                .map(|(_, position)| position)
                .collect();
            positions.sort_unstable();
            return positions;
        }
    }
    Vec::new()
}

pub(crate) struct Merge {
    cursors: Vec<TableCursor>,
    pending: Vec<usize>,
    tournament: Option<Box<Tournament>>,
    #[cfg(test)]
    finished: bool,
    drop_tombstones: bool,
}
impl Merge {
    pub fn new(tables: &[Arc<Table>], drop_tombstones: bool) -> Result<Self> {
        Self::with_selector(tables, drop_tombstones, tables.len() >= TOURNAMENT_INPUTS)
    }

    fn with_selector(
        tables: &[Arc<Table>],
        drop_tombstones: bool,
        tournament: bool,
    ) -> Result<Self> {
        let cursors = tables
            .iter()
            .cloned()
            .map(TableCursor::new)
            .collect::<Result<Vec<_>>>()?;
        let tournament = tournament.then(|| Box::new(Tournament::new(&cursors)));
        Ok(Self {
            cursors,
            tournament,
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
            let newest = if let Some(tree) = &mut self.tournament {
                // A dense duplicate group already requires advancing most
                // sources. Linear selection avoids repairing most tree nodes
                // as well; rebuild only when sparse groups resume.
                let dense = self.pending.len() >= self.cursors.len().div_ceil(2);
                let rebuild = tree.is_stale();
                let mut dirty = 0;
                while let Some(i) = self.pending.pop() {
                    self.cursors[i].advance()?;
                    if !dense && !rebuild {
                        dirty |= tree.update_leaf(i, &self.cursors);
                    }
                }
                if dense {
                    tree.invalidate();
                    Self::select_linear(&self.cursors, &mut self.pending)
                } else {
                    if rebuild {
                        tree.rebuild(&self.cursors);
                    } else {
                        tree.repair(dirty, &self.cursors);
                    }
                    tree.selected(&mut self.pending)
                        .and_then(|i| self.cursors[i].current())
                }
            } else {
                while let Some(i) = self.pending.pop() {
                    self.cursors[i].advance()?;
                }
                Self::select_linear(&self.cursors, &mut self.pending)
            };
            let Some(newest) = newest else {
                return Ok(None);
            };
            // Partial merges keep deletions hiding records in unselected SSTs.
            // Advance only on the next call: the winner still borrows its block.
            if newest.value.is_some() || !self.drop_tombstones {
                return consume(newest).map(Some);
            }
        }
    }

    #[inline]
    fn select_linear<'a>(
        cursors: &'a [TableCursor],
        pending: &mut Vec<usize>,
    ) -> Option<crate::codec::RecordRef<'a>> {
        let mut newest: Option<crate::codec::RecordRef<'_>> = None;
        for (i, cursor) in cursors.iter().enumerate() {
            if let Some(record) = cursor.current() {
                let order = newest
                    .as_ref()
                    .map_or(std::cmp::Ordering::Less, |old| record.key.cmp(old.key));
                match order {
                    std::cmp::Ordering::Less => {
                        pending.clear();
                        pending.push(i);
                        newest = Some(record);
                    }
                    std::cmp::Ordering::Equal => {
                        pending.push(i);
                        if let Some(old) = &mut newest {
                            if record.seq > old.seq {
                                *old = record;
                            }
                        }
                    }
                    std::cmp::Ordering::Greater => {}
                }
            }
        }
        newest
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

    fn selected_ids(files: &[(u64, u64)], threshold: usize) -> Vec<u64> {
        tiered_positions(files.iter().copied(), threshold)
            .into_iter()
            .map(|i| files[i].0)
            .collect()
    }

    #[test]
    fn picker_preserves_snapshot_order_and_handles_full_and_below_threshold() {
        let dir = tempfile::tempdir().unwrap();
        let tables: Vec<_> = [9, 2, 6, 4, 8]
            .into_iter()
            .map(|id| {
                Arc::new(
                    Table::write(
                        &dir.path().join(format!("{id}.sst")),
                        id,
                        std::iter::once(Ok((
                            vec![id as u8],
                            Record {
                                seq: id,
                                value: None,
                            },
                        ))),
                        1,
                        64,
                        10,
                    )
                    .unwrap(),
                )
            })
            .collect();
        for style in [CompactionStyle::Full, CompactionStyle::SizeTiered] {
            assert!(pick(&[], 2, style).is_empty());
            assert!(pick(&tables, 6, style).is_empty());
        }
        let full = pick(&tables, 2, CompactionStyle::Full);
        assert!(full.iter().zip(&tables).all(|(a, b)| Arc::ptr_eq(a, b)));
        assert_eq!(full.len(), tables.len());
        assert_eq!(
            pick(&tables, 3, CompactionStyle::SizeTiered)
                .iter()
                .map(|t| t.id)
                .collect::<Vec<_>>(),
            [2, 6, 4]
        );
        let files: Vec<_> = (1..=1024).rev().map(|id| (id, 100)).collect();
        assert_eq!(
            selected_ids(&files, 1000),
            (1..=1000).rev().collect::<Vec<_>>()
        );
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
        assert_eq!(selected_ids(&files, 3), [2, 6, 4]);
        assert!(selected_ids(&files, 9).is_empty());
    }

    #[test]
    fn size_classes_handle_boundaries_and_large_files() {
        assert_eq!(selected_ids(&[(1, 127), (2, 128), (3, 255)], 2), [2, 3]);
        assert_eq!(
            selected_ids(&[(1, 1), (2, 2), (3, 4)], 2),
            Vec::<u64>::new()
        );
        assert_eq!(selected_ids(&[(1, u64::MAX), (2, u64::MAX - 1)], 2), [1, 2]);
    }
}

#[cfg(test)]
#[path = "picker_profile.rs"]
mod picker_profile;

#[cfg(test)]
#[path = "compaction_tests.rs"]
mod compaction_tests;
