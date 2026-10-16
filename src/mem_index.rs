use super::Record;
use crate::scan::KeyRange;
use std::{collections::VecDeque, ops::Bound, sync::Arc};

const PAGE_RECORDS: usize = 256;
const PAGE_KEY_BYTES: usize = 64 * 1024;

// A snapshot owns only overlapping pages. Writes copy a shared page, never an
// entire memtable. Values remain immutable Bytes handles; page splits move keys.
#[derive(Clone, Default)]
struct Page {
    rows: Vec<(Vec<u8>, Record)>,
    key_bytes: usize,
}
impl Page {
    fn overfull(&self) -> bool {
        self.rows.len() > 1 && (self.rows.len() > PAGE_RECORDS || self.key_bytes > PAGE_KEY_BYTES)
    }
    fn position(&self, key: &[u8]) -> std::result::Result<usize, usize> {
        self.rows.binary_search_by(|(k, _)| k.as_slice().cmp(key))
    }
}

#[derive(Default)]
pub(crate) struct MemIndex {
    pages: Vec<Arc<Page>>,
    len: usize,
}
impl MemIndex {
    fn page_for(&self, key: &[u8]) -> usize {
        self.pages
            .partition_point(|p| p.rows[0].0.as_slice() <= key)
            .saturating_sub(1)
    }
    pub fn len(&self) -> usize {
        self.len
    }
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
    pub fn get(&self, key: &[u8]) -> Option<&Record> {
        let p = self.pages.get(self.page_for(key))?;
        p.position(key).ok().map(|i| &p.rows[i].1)
    }
    pub fn insert(&mut self, key: Vec<u8>, record: Record) -> Option<Record> {
        if self.pages.is_empty() {
            self.pages.push(Arc::new(Page::default()));
        }
        let index = if self.len == 0 {
            0
        } else {
            self.page_for(&key)
        };
        let page = Arc::make_mut(&mut self.pages[index]);
        match page.position(&key) {
            Ok(i) => Some(std::mem::replace(&mut page.rows[i].1, record)),
            Err(i) => {
                page.key_bytes += key.len();
                page.rows.insert(i, (key, record));
                self.len += 1;
                let (mut cursor, mut end) = (index, index + 1);
                while cursor < end {
                    if self.pages[cursor].overfull() {
                        let page = Arc::make_mut(&mut self.pages[cursor]);
                        let rows = page.rows.split_off(page.rows.len() / 2);
                        let key_bytes = rows.iter().map(|(k, _)| k.len()).sum();
                        page.key_bytes -= key_bytes;
                        page.rows.shrink_to_fit();
                        self.pages
                            .insert(cursor + 1, Arc::new(Page { rows, key_bytes }));
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
        let index = self.page_for(key);
        let row = self.pages.get(index)?.position(key).ok()?;
        let page = Arc::make_mut(&mut self.pages[index]);
        let (key, record) = page.rows.remove(row);
        page.key_bytes -= key.len();
        self.len -= 1;
        if page.rows.is_empty() {
            self.pages.remove(index);
        }
        Some(record)
    }
    pub fn iter(&self) -> impl Iterator<Item = (&Vec<u8>, &Record)> {
        self.pages
            .iter()
            .flat_map(|p| p.rows.iter().map(|(k, r)| (k, r)))
    }
    pub fn snapshot(&self, range: Arc<KeyRange>) -> Option<MemScan> {
        let start = match &range.start {
            Bound::Included(k) | Bound::Excluded(k) => self.page_for(k),
            Bound::Unbounded => 0,
        };
        let first = self.pages.get(start)?;
        let row = match &range.start {
            Bound::Included(k) => first.rows.partition_point(|(key, _)| key < k),
            Bound::Excluded(k) => first.rows.partition_point(|(key, _)| key <= k),
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
        let out = (key.clone(), record.clone());
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
                    map.iter().collect::<Vec<_>>(),
                    model.iter().collect::<Vec<_>>()
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
        map.insert(vec![0; 32 * 1024], record(999));
        assert_eq!(scan.next().unwrap().1.seq, 0);
        assert_eq!(scan.count(), 70);
    }
}
