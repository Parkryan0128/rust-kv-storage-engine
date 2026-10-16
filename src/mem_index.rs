use super::Record;
use crate::scan::KeyRange;
use bytes::Bytes;
use std::{collections::VecDeque, ops::Bound, sync::Arc};

const PAGE_RECORDS: usize = 256;
const PAGE_KEY_BYTES: usize = 64 * 1024;
// Snapshot acquisition/drop touches groups, not every page in the range.
// Mutations copy at most this many page handles before copying a shared page.
const GROUP_PAGES: usize = 64;

// Monotone prefix only: equal prefixes always fall back to complete key bytes.
fn prefix(key: &[u8]) -> u64 {
    if let Some(bytes) = key.get(..8) {
        return u64::from_be_bytes(bytes.try_into().unwrap());
    }
    let mut bytes = [0; 8];
    bytes[..key.len()].copy_from_slice(key);
    u64::from_be_bytes(bytes)
}

fn preceding(prefixes: &[u64], needle: u64, before: impl Fn(usize) -> bool) -> usize {
    let lo = prefixes.partition_point(|p| *p < needle);
    if prefixes.get(lo) != Some(&needle) {
        return lo.saturating_sub(1);
    }
    let hi = lo + prefixes[lo..].partition_point(|p| *p == needle);
    let mut left = lo;
    let mut right = hi;
    while left < right {
        let middle = left + (right - left) / 2;
        if before(middle) {
            left = middle + 1;
        } else {
            right = middle;
        }
    }
    left.saturating_sub(1)
}

#[derive(Clone, Default)]
struct Page {
    rows: Vec<(Bytes, Record)>,
    prefixes: Vec<u64>,
    key_bytes: usize,
}
impl Page {
    fn overfull(&self) -> bool {
        self.rows.len() > 1 && (self.rows.len() > PAGE_RECORDS || self.key_bytes > PAGE_KEY_BYTES)
    }
    fn position(&self, key: &[u8], prefix: u64) -> std::result::Result<usize, usize> {
        let lo = self.prefixes.partition_point(|p| *p < prefix);
        if self.prefixes.get(lo) != Some(&prefix) {
            return Err(lo);
        }
        let hi = if self.prefixes.get(lo + 1) == Some(&prefix) {
            lo + self.prefixes[lo..].partition_point(|p| *p == prefix)
        } else {
            lo + 1
        };
        self.rows[lo..hi]
            .binary_search_by(|(k, _)| k.as_ref().cmp(key))
            .map(|i| lo + i)
            .map_err(|i| lo + i)
    }
}

#[derive(Clone, Default)]
struct PageGroup {
    pages: Vec<Arc<Page>>,
    prefixes: Vec<u64>,
}
impl PageGroup {
    fn first_key(&self) -> &[u8] {
        &self.pages[0].rows[0].0
    }
    fn page_for(&self, key: &[u8], prefix: u64) -> usize {
        preceding(&self.prefixes, prefix, |i| {
            self.pages[i].rows[0].0.as_ref() <= key
        })
    }
    fn insert(
        &mut self,
        key: Vec<u8>,
        record: Record,
        prefix: u64,
        append: bool,
    ) -> Option<Record> {
        if self.pages.is_empty() {
            self.pages.push(Arc::new(Page::default()));
            self.prefixes.push(prefix);
        }
        let index = if append {
            self.pages.len() - 1
        } else {
            self.page_for(&key, prefix)
        };
        let page = Arc::make_mut(&mut self.pages[index]);
        let position = if append {
            Err(page.rows.len())
        } else {
            page.position(&key, prefix)
        };
        match position {
            Ok(i) => Some(std::mem::replace(&mut page.rows[i].1, record)),
            Err(i) => {
                page.key_bytes += key.len();
                let key = Bytes::from(key);
                if append {
                    page.rows.push((key, record));
                    page.prefixes.push(prefix);
                } else {
                    page.rows.insert(i, (key, record));
                    page.prefixes.insert(i, prefix);
                }
                self.prefixes[index] = page.prefixes[0];
                let (mut cursor, mut end) = (index, index + 1);
                while cursor < end {
                    if self.pages[cursor].overfull() {
                        let page = Arc::make_mut(&mut self.pages[cursor]);
                        let rows = page.rows.split_off(page.rows.len() / 2);
                        let prefixes = page.prefixes.split_off(page.rows.len());
                        let key_bytes = rows.iter().map(|(k, _)| k.len()).sum();
                        page.key_bytes -= key_bytes;
                        page.rows.shrink_to_fit();
                        page.prefixes.shrink_to_fit();
                        self.prefixes.insert(cursor + 1, prefixes[0]);
                        self.pages.insert(
                            cursor + 1,
                            Arc::new(Page {
                                rows,
                                prefixes,
                                key_bytes,
                            }),
                        );
                        end += 1;
                    } else {
                        cursor += 1;
                    }
                }
                None
            }
        }
    }
}

#[derive(Default)]
pub(crate) struct MemIndex {
    groups: Vec<Arc<PageGroup>>,
    prefixes: Vec<u64>,
    len: usize,
}
impl MemIndex {
    fn group_for(&self, key: &[u8], prefix: u64) -> usize {
        preceding(&self.prefixes, prefix, |i| {
            self.groups[i].first_key() <= key
        })
    }
    pub fn len(&self) -> usize {
        self.len
    }
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
    #[inline]
    pub fn get(&self, key: &[u8]) -> Option<&Record> {
        if self.len == 0 {
            return None;
        }
        let prefix = prefix(key);
        let group = &self.groups[self.group_for(key, prefix)];
        let p = &group.pages[group.page_for(key, prefix)];
        p.position(key, prefix).ok().map(|i| &p.rows[i].1)
    }
    pub fn insert(&mut self, key: Vec<u8>, record: Record) -> Option<Record> {
        let prefix = prefix(&key);
        let append = self.groups.last().is_none_or(|g| {
            let p = g.pages.last().unwrap();
            let last = *p.prefixes.last().unwrap();
            last < prefix || (last == prefix && p.rows.last().unwrap().0.as_ref() < key.as_slice())
        });
        if self.groups.is_empty() {
            self.groups.push(Arc::new(PageGroup::default()));
            self.prefixes.push(prefix);
        }
        let index = if append {
            self.groups.len() - 1
        } else {
            self.group_for(&key, prefix)
        };
        let group = Arc::make_mut(&mut self.groups[index]);
        let previous = group.insert(key, record, prefix, append);
        if previous.is_none() {
            self.len += 1;
            self.prefixes[index] = group.prefixes[0];
            if group.pages.len() > GROUP_PAGES {
                let middle = group.pages.len() / 2;
                let pages = group.pages.split_off(middle);
                let prefixes = group.prefixes.split_off(middle);
                group.pages.shrink_to_fit();
                group.prefixes.shrink_to_fit();
                self.prefixes.insert(index + 1, prefixes[0]);
                self.groups
                    .insert(index + 1, Arc::new(PageGroup { pages, prefixes }));
            }
        }
        previous
    }
    pub fn remove(&mut self, key: &[u8]) -> Option<Record> {
        if self.len == 0 {
            return None;
        }
        let prefix = prefix(key);
        let g = self.group_for(key, prefix);
        let p = self.groups[g].page_for(key, prefix);
        // Missing deletes must not copy a snapshot's directory or page.
        let row = self.groups[g].pages[p].position(key, prefix).ok()?;
        let group = Arc::make_mut(&mut self.groups[g]);
        let page = Arc::make_mut(&mut group.pages[p]);
        let (key, record) = page.rows.remove(row);
        page.prefixes.remove(row);
        page.key_bytes -= key.len();
        self.len -= 1;
        if page.rows.is_empty() {
            group.pages.remove(p);
            group.prefixes.remove(p);
        } else {
            group.prefixes[p] = page.prefixes[0];
        }
        if group.pages.is_empty() {
            self.groups.remove(g);
            self.prefixes.remove(g);
        } else {
            self.prefixes[g] = group.prefixes[0];
        }
        Some(record)
    }
    pub fn iter(&self) -> impl Iterator<Item = (&[u8], &Record)> {
        self.groups
            .iter()
            .flat_map(|g| g.pages.iter())
            .flat_map(|p| p.rows.iter().map(|(k, r)| (k.as_ref(), r)))
    }
    pub fn snapshot(&self, range: Arc<KeyRange>) -> Option<MemScan> {
        if self.len == 0 || range.is_empty() {
            return None;
        }
        let (mut start, mut page, mut row) = match &range.start {
            Bound::Included(k) | Bound::Excluded(k) => {
                let prefix = prefix(k);
                let g = self.group_for(k, prefix);
                let p = self.groups[g].page_for(k, prefix);
                let rows = &self.groups[g].pages[p].rows;
                let r = match &range.start {
                    Bound::Included(_) => {
                        rows.partition_point(|(key, _)| key.as_ref() < k.as_slice())
                    }
                    _ => rows.partition_point(|(key, _)| key.as_ref() <= k.as_slice()),
                };
                (g, p, r)
            }
            Bound::Unbounded => (0, 0, 0),
        };
        if row == self.groups[start].pages[page].rows.len() {
            row = 0;
            page += 1;
            if page == self.groups[start].pages.len() {
                start += 1;
                page = 0;
            }
        }
        let first_key = &self.groups.get(start)?.pages[page].rows[row].0;
        if !range.allows_end(first_key) {
            return None;
        }
        // Copy one handle per group; per-page Arc counters stay untouched.
        let groups = self.groups[start..]
            .iter()
            .take_while(|g| range.allows_end(g.first_key()))
            .cloned()
            .collect();
        Some(MemScan {
            groups,
            page,
            row,
            range,
        })
    }
}

pub(crate) struct MemScan {
    groups: VecDeque<Arc<PageGroup>>,
    page: usize,
    row: usize,
    range: Arc<KeyRange>,
}
impl Iterator for MemScan {
    type Item = (Vec<u8>, Record);
    fn next(&mut self) -> Option<Self::Item> {
        let group = self.groups.front()?;
        let page = &group.pages[self.page];
        let (key, record) = &page.rows[self.row];
        if !self.range.allows_end(key) {
            self.groups.clear();
            return None;
        }
        let out = (key.to_vec(), record.clone());
        self.row += 1;
        if self.row == page.rows.len() {
            self.page += 1;
            self.row = 0;
            if self.page == group.pages.len() {
                self.groups.pop_front();
                self.page = 0;
            }
        }
        Some(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn record(n: u64) -> Record {
        Record {
            seq: n,
            value: None,
        }
    }
    fn all() -> Arc<KeyRange> {
        Arc::new(KeyRange::new(..).unwrap())
    }

    #[test]
    fn mutations_and_retained_snapshots_match_ordered_model() {
        let mut map = MemIndex::default();
        let mut model = std::collections::BTreeMap::new();
        let mut saved = Vec::new();
        let mut rng = 17u64;
        for n in 0..12_000 {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            let key = (rng % 2000).to_be_bytes().to_vec();
            if n % 4 == 0 {
                assert_eq!(map.remove(&key), model.remove(&key));
            } else {
                assert_eq!(
                    map.insert(key.clone(), record(n)),
                    model.insert(key, record(n))
                );
            }
            if n % 1000 == 999 {
                assert_eq!(
                    map.iter()
                        .map(|(k, r)| (k.to_vec(), r.clone()))
                        .collect::<Vec<_>>(),
                    model
                        .iter()
                        .map(|(k, r)| (k.clone(), r.clone()))
                        .collect::<Vec<_>>()
                );
                saved.push((map.snapshot(all()).unwrap(), model.clone()));
            }
        }
        for (snapshot, expected) in saved {
            assert_eq!(
                snapshot.collect::<Vec<_>>(),
                expected.into_iter().collect::<Vec<_>>()
            );
        }
        for (key, expected) in model {
            assert_eq!(map.remove(&key), Some(expected));
        }
        assert!(map.is_empty());
        assert!(map.groups.is_empty());
    }

    #[test]
    fn snapshots_share_bounded_groups_without_cloning_each_page() {
        let mut map = MemIndex::default();
        for n in 0..100_000u64 {
            map.insert(n.to_be_bytes().to_vec(), record(n));
        }
        let page_count: usize = map.groups.iter().map(|g| g.pages.len()).sum();
        let mut scan = map.snapshot(all()).unwrap();
        assert!(scan.groups.len() * (GROUP_PAGES / 2) <= page_count);
        assert!(map.groups.iter().all(|g| Arc::strong_count(g) == 2));
        assert!(map
            .groups
            .iter()
            .flat_map(|g| &g.pages)
            .all(|p| Arc::strong_count(p) == 1));
        for n in 0..10u64 {
            assert_eq!(scan.next(), Some((n.to_be_bytes().to_vec(), record(n))));
        }
        drop(scan);
        assert!(map.groups.iter().all(|g| Arc::strong_count(g) == 1));
        // A narrow range retains only its bounded group, not the whole directory.
        let range = Arc::new(
            KeyRange::new(600u64.to_be_bytes().as_slice()..700u64.to_be_bytes().as_slice())
                .unwrap(),
        );
        let mut scan = map.snapshot(range).unwrap();
        assert_eq!(scan.groups.len(), 1);
        let weak = Arc::downgrade(scan.groups.front().unwrap());
        let old_group = scan.groups.front().unwrap().clone();
        let g = map.group_for(&600u64.to_be_bytes(), 600);
        assert!(Arc::ptr_eq(&map.groups[g], &old_group));
        map.insert(600u64.to_be_bytes().to_vec(), record(9999));
        assert!(!Arc::ptr_eq(&map.groups[g], &old_group));
        drop(old_group);
        drop(map);
        for n in 600..700u64 {
            assert_eq!(scan.next(), Some((n.to_be_bytes().to_vec(), record(n))));
        }
        assert!(scan.next().is_none());
        assert!(weak.upgrade().is_none());
    }

    #[test]
    fn group_boundaries_splits_removals_and_old_snapshots() {
        let mut map = MemIndex::default();
        for n in 0..24_000u64 {
            map.insert(n.to_be_bytes().to_vec(), record(n));
        }
        let old = map.snapshot(all()).unwrap();
        let boundaries: Vec<u64> = map
            .groups
            .iter()
            .skip(1)
            .map(|g| u64::from_be_bytes(g.first_key().try_into().unwrap()))
            .collect();
        assert!(boundaries.len() >= 2);
        for b in boundaries {
            for excluded in [false, true] {
                let begin = b.to_be_bytes();
                let end = (b + 3).to_be_bytes();
                let start = if excluded {
                    Bound::Excluded(begin.as_slice())
                } else {
                    Bound::Included(begin.as_slice())
                };
                let range =
                    Arc::new(KeyRange::new((start, Bound::Excluded(end.as_slice()))).unwrap());
                let rows = map.snapshot(range).unwrap().collect::<Vec<_>>();
                let expected = (b + u64::from(excluded)..b + 3)
                    .map(|n| (n.to_be_bytes().to_vec(), record(n)))
                    .collect::<Vec<_>>();
                assert_eq!(rows, expected);
            }
        }
        // Delete whole leading groups, change surviving pages, then split again.
        for n in 0..12_000u64 {
            assert_eq!(map.remove(&n.to_be_bytes()), Some(record(n)));
        }
        for n in (12_000..36_000u64).rev() {
            map.insert(n.to_be_bytes().to_vec(), record(n + 1));
        }
        assert_eq!(
            old.collect::<Vec<_>>(),
            (0..24_000u64)
                .map(|n| (n.to_be_bytes().to_vec(), record(n)))
                .collect::<Vec<_>>()
        );
        assert_eq!(
            map.snapshot(all()).unwrap().collect::<Vec<_>>(),
            (12_000..36_000u64)
                .map(|n| (n.to_be_bytes().to_vec(), record(n + 1)))
                .collect::<Vec<_>>()
        );
        let mut scan = map.snapshot(all()).unwrap();
        let first = Arc::downgrade(scan.groups.front().unwrap());
        let count: usize = scan.groups[0].pages.iter().map(|p| p.rows.len()).sum();
        drop(map);
        for _ in 0..count {
            assert!(scan.next().is_some());
        }
        assert!(
            first.upgrade().is_none(),
            "consumed groups must be released"
        );
        assert!(scan.next().is_some());
    }

    #[test]
    fn large_keys_bound_copy_on_write_pages_and_are_not_prefetched_by_scan() {
        let mut map = MemIndex::default();
        for n in 0..70u8 {
            map.insert(vec![n; 32 * 1024], record(n as u64));
        }
        map.insert(vec![100; 1024 * 1024], record(100));
        assert!(map
            .groups
            .iter()
            .flat_map(|g| &g.pages)
            .all(|p| !p.overfull()));
        let mut scan = map.snapshot(all()).unwrap();
        assert!(Arc::ptr_eq(&map.groups[0], scan.groups.front().unwrap()));
        let key_bytes = map.groups[0].pages[0].rows[0].0.as_ptr();
        map.insert(vec![0; 32 * 1024], record(999));
        assert_eq!(map.groups[0].pages[0].rows[0].0.as_ptr(), key_bytes);
        assert_eq!(scan.next().unwrap().1.seq, 0);
        assert_eq!(scan.count(), 70);
    }

    #[test]
    fn equal_padded_prefixes_keep_full_byte_order_across_pages_and_removals() {
        let mut keys = vec![
            vec![],
            vec![0],
            vec![0, 0],
            b"a".to_vec(),
            b"a\0".to_vec(),
            b"a\0\0".to_vec(),
            b"abcdefgh".to_vec(),
        ];
        for n in 0..20_000u32 {
            let mut key = b"abcdefgh".to_vec();
            key.extend_from_slice(&n.to_be_bytes());
            keys.push(key);
        }
        let mut map = MemIndex::default();
        let mut model = std::collections::BTreeMap::new();
        for (n, key) in keys.iter().rev().enumerate() {
            map.insert(key.clone(), record(n as u64));
            model.insert(key.clone(), record(n as u64));
        }
        for (key, value) in &model {
            assert_eq!(map.get(key), Some(value));
        }
        let snapshot = map.snapshot(all()).unwrap();
        for key in keys.iter().step_by(3) {
            map.remove(key);
        }
        assert_eq!(
            snapshot.collect::<Vec<_>>(),
            model.into_iter().collect::<Vec<_>>()
        );
        let expected: Vec<_> = map
            .iter()
            .filter(|(key, _)| *key > b"abcdefgh".as_slice())
            .map(|(k, r)| (k.to_vec(), r.clone()))
            .collect();
        let range = Arc::new(
            KeyRange::new((Bound::Excluded(b"abcdefgh".as_slice()), Bound::Unbounded)).unwrap(),
        );
        assert_eq!(map.snapshot(range).unwrap().collect::<Vec<_>>(), expected);
        assert_eq!(map.prefixes.len(), map.groups.len());
        for (p, group) in map.prefixes.iter().zip(&map.groups) {
            assert_eq!(*p, prefix(group.first_key()));
            assert!(group.pages.len() <= GROUP_PAGES);
            assert_eq!(group.prefixes.len(), group.pages.len());
            for (p, page) in group.prefixes.iter().zip(&group.pages) {
                assert_eq!(*p, prefix(&page.rows[0].0));
                assert_eq!(page.prefixes.len(), page.rows.len());
            }
        }
    }
}
