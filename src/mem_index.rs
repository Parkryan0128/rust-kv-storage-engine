use super::Record;
use crate::scan::KeyRange;
use bytes::Bytes;
use std::{collections::VecDeque, ops::Bound, sync::Arc};

const PAGE_RECORDS: usize = 256;
const PAGE_KEY_BYTES: usize = 64 * 1024;

// Monotone prefix only: equal prefixes always fall back to complete key bytes.
// Separate dense prefix arrays avoid chasing a heap key at every search probe.
fn prefix(key: &[u8]) -> u64 {
    if let Some(bytes) = key.get(..8) {
        return u64::from_be_bytes(bytes.try_into().unwrap());
    }
    let mut bytes = [0; 8];
    bytes[..key.len()].copy_from_slice(key);
    u64::from_be_bytes(bytes)
}

// A snapshot owns only overlapping pages. Writes copy a shared page, never an
// entire memtable. Keys and values share immutable Bytes storage; page splits
// move handles. Only consumed scan keys are materialized as owned Vec values.
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

#[derive(Default)]
pub(crate) struct MemIndex {
    pages: Vec<Arc<Page>>,
    prefixes: Vec<u64>,
    len: usize,
}
impl MemIndex {
    fn page_for(&self, key: &[u8]) -> usize {
        self.page_for_prefix(key, prefix(key))
    }
    fn page_for_prefix(&self, key: &[u8], prefix: u64) -> usize {
        let lo = self.prefixes.partition_point(|p| *p < prefix);
        if self.prefixes.get(lo) != Some(&prefix) {
            return lo.saturating_sub(1);
        }
        let hi = lo + self.prefixes[lo..].partition_point(|p| *p == prefix);
        (lo + self.pages[lo..hi].partition_point(|p| p.rows[0].0.as_ref() <= key)).saturating_sub(1)
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
        let p = &self.pages[self.page_for_prefix(key, prefix)];
        p.position(key, prefix).ok().map(|i| &p.rows[i].1)
    }
    pub fn insert(&mut self, key: Vec<u8>, record: Record) -> Option<Record> {
        let prefix = prefix(&key);
        let append = self.pages.last().is_none_or(|p| {
            let last = *p.prefixes.last().unwrap();
            last < prefix || (last == prefix && p.rows.last().unwrap().0.as_ref() < key.as_slice())
        });
        if self.pages.is_empty() {
            self.pages.push(Arc::new(Page::default()));
            self.prefixes.push(prefix);
        }
        let index = if append {
            self.pages.len() - 1
        } else {
            self.page_for_prefix(&key, prefix)
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
                self.len += 1;
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
    pub fn remove(&mut self, key: &[u8]) -> Option<Record> {
        if self.len == 0 {
            return None;
        }
        let prefix = prefix(key);
        let index = self.page_for_prefix(key, prefix);
        let row = self.pages[index].position(key, prefix).ok()?;
        let page = Arc::make_mut(&mut self.pages[index]);
        let (key, record) = page.rows.remove(row);
        page.prefixes.remove(row);
        page.key_bytes -= key.len();
        self.len -= 1;
        if page.rows.is_empty() {
            self.pages.remove(index);
            self.prefixes.remove(index);
        } else {
            self.prefixes[index] = page.prefixes[0];
        }
        Some(record)
    }
    pub fn iter(&self) -> impl Iterator<Item = (&[u8], &Record)> {
        self.pages
            .iter()
            .flat_map(|p| p.rows.iter().map(|(k, r)| (k.as_ref(), r)))
    }
    pub fn snapshot(&self, range: Arc<KeyRange>) -> Option<MemScan> {
        if self.len == 0 {
            return None;
        }
        let start = match &range.start {
            Bound::Included(k) | Bound::Excluded(k) => self.page_for(k),
            Bound::Unbounded => 0,
        };
        let first = self.pages.get(start)?;
        let row = match &range.start {
            Bound::Included(k) => first
                .rows
                .partition_point(|(key, _)| key.as_ref() < k.as_slice()),
            Bound::Excluded(k) => first
                .rows
                .partition_point(|(key, _)| key.as_ref() <= k.as_slice()),
            Bound::Unbounded => 0,
        };
        let (start, row) = if row == first.rows.len() {
            (start + 1, 0)
        } else {
            (start, row)
        };
        let first_key = &self.pages.get(start)?.rows[row].0;
        if !range.allows_end(first_key) {
            return None;
        }
        let pages = self.pages[start..]
            .iter()
            .take_while(|p| range.allows_end(&p.rows[0].0))
            .cloned()
            .collect();
        Some(MemScan { pages, row, range })
    }
}

pub(crate) struct MemScan {
    pages: VecDeque<Arc<Page>>,
    row: usize,
    range: Arc<KeyRange>,
}
impl Iterator for MemScan {
    type Item = (Vec<u8>, Record);
    fn next(&mut self) -> Option<Self::Item> {
        let page = self.pages.front()?;
        let (key, record) = &page.rows[self.row];
        if !self.range.allows_end(key) {
            self.pages.clear();
            return None;
        }
        let out = (key.to_vec(), record.clone());
        self.row += 1;
        if self.row == page.rows.len() {
            self.pages.pop_front();
            self.row = 0;
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
        assert!(map.pages.is_empty());
    }

    #[test]
    fn snapshots_share_only_overlapping_pages_and_release_consumed_pages() {
        let mut map = MemIndex::default();
        for n in 0..2000u64 {
            map.insert(n.to_be_bytes().to_vec(), record(n));
        }
        let range = Arc::new(
            KeyRange::new(600u64.to_be_bytes().as_slice()..700u64.to_be_bytes().as_slice())
                .unwrap(),
        );
        let mut scan = map.snapshot(range).unwrap();
        assert!(scan.pages.len() < map.pages.len());
        let weak = Arc::downgrade(scan.pages.front().unwrap());
        let snapshot_first = scan.pages.front().unwrap().clone();
        let index = map.page_for(&600u64.to_be_bytes());
        assert!(Arc::ptr_eq(&map.pages[index], &snapshot_first));
        map.insert(600u64.to_be_bytes().to_vec(), record(9999));
        assert!(!Arc::ptr_eq(&map.pages[index], &snapshot_first));
        drop(snapshot_first);
        drop(map);
        for n in 600..700u64 {
            assert_eq!(scan.next(), Some((n.to_be_bytes().to_vec(), record(n))));
        }
        assert!(scan.next().is_none());
        assert!(weak.upgrade().is_none());
    }

    #[test]
    fn large_keys_bound_copy_on_write_pages_and_are_not_prefetched_by_scan() {
        let mut map = MemIndex::default();
        for n in 0..70u8 {
            map.insert(vec![n; 32 * 1024], record(n as u64));
        }
        map.insert(vec![100; 1024 * 1024], record(100));
        assert!(map.pages.iter().all(|p| !p.overfull()));
        let mut scan = map.snapshot(all()).unwrap();
        assert!(Arc::ptr_eq(&map.pages[0], scan.pages.front().unwrap()));
        let key_bytes = map.pages[0].rows[0].0.as_ptr();
        map.insert(vec![0; 32 * 1024], record(999));
        assert_eq!(map.pages[0].rows[0].0.as_ptr(), key_bytes);
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
        for n in 0..700u32 {
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
        assert_eq!(map.prefixes.len(), map.pages.len());
        for (p, page) in map.prefixes.iter().zip(&map.pages) {
            assert_eq!(*p, prefix(&page.rows[0].0));
            assert_eq!(page.prefixes.len(), page.rows.len());
        }
    }
}
