use super::{Record, RECORD_CHARGE};
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
    if prefixes.len() <= 1 {
        return 0;
    }
    let right = prefixes.partition_point(|p| *p <= needle);
    let last = right.saturating_sub(1);
    if right == 0 || prefixes[last] != needle || before(last) {
        return last;
    }
    // With a shared prefix the lower bound is already known. Keep complete
    // key comparisons for ordering; the prefix is only a routing hint.
    let mut left = if prefixes[0] == needle {
        0
    } else {
        prefixes[..right].partition_point(|p| *p < needle)
    };
    let mut right = right;
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

// Replacing values never changes key order or routing. Share those arrays
// independently so snapshot delta folds copy values only, not every key handle.
#[derive(Clone, Default)]
struct PageKeys {
    rows: Vec<Bytes>,
    prefixes: Vec<u64>,
    key_bytes: usize,
}
impl PageKeys {
    fn position(&self, key: &[u8], prefix: u64) -> std::result::Result<usize, usize> {
        let lo = self.prefixes.partition_point(|p| *p < prefix);
        if self.prefixes.get(lo) != Some(&prefix) {
            return Err(lo);
        }
        let hi = if self.prefixes.get(lo + 1) == Some(&prefix) {
            // Common-prefix keys often fill the rest of the page. The last
            // prefix gives the upper bound without another binary search.
            if self.prefixes.last() == Some(&prefix) {
                self.prefixes.len()
            } else {
                lo + self.prefixes[lo..].partition_point(|p| *p == prefix)
            }
        } else {
            lo + 1
        };
        self.rows[lo..hi]
            .binary_search_by(|k| k.as_ref().cmp(key))
            .map(|i| lo + i)
            .map_err(|i| lo + i)
    }
}

#[derive(Clone, Default)]
struct PageData {
    keys: Arc<PageKeys>,
    records: Vec<Record>,
    value_bytes: usize,
}
impl std::ops::Deref for PageData {
    type Target = PageKeys;
    fn deref(&self) -> &PageKeys {
        &self.keys
    }
}
impl PageData {
    fn replace(&mut self, row: usize, record: Record) -> usize {
        let previous_bytes = self.records[row].value.as_ref().map_or(0, Bytes::len);
        self.value_bytes -= previous_bytes;
        self.value_bytes += record.value.as_ref().map_or(0, Bytes::len);
        self.records[row] = record;
        previous_bytes
    }
    fn insert_at(&mut self, row: usize, key: Vec<u8>, record: Record, prefix: u64) {
        let keys = Arc::make_mut(&mut self.keys);
        keys.key_bytes += key.len();
        keys.rows.insert(row, Bytes::from(key));
        keys.prefixes.insert(row, prefix);
        self.value_bytes += record.value.as_ref().map_or(0, Bytes::len);
        self.records.insert(row, record);
    }
    // Called only after a structural insertion has been selected. Detach the
    // key arrays once for the rest of this page-local run, rather than doing an
    // atomic ownership check for every inserted row. Value-only runs continue
    // to share keys with retained snapshots.
    fn apply_inserting_run(
        &mut self,
        entries: &mut std::iter::Peekable<impl Iterator<Item = (Vec<u8>, Record)>>,
        upper: Option<&[u8]>,
        remove_deletes: bool,
        bytes: &mut usize,
        len: &mut usize,
    ) {
        let keys = Arc::make_mut(&mut self.keys);
        while let Some((key, record)) = entries.peek() {
            if (remove_deletes && record.value.is_none())
                || key.as_slice() < keys.rows[0].as_ref()
                || upper.is_some_and(|end| key.as_slice() >= end)
            {
                break;
            }
            let prefix = prefix(key);
            let position = if key.as_slice() > keys.rows.last().unwrap().as_ref() {
                Err(keys.rows.len())
            } else {
                keys.position(key, prefix)
            };
            if position.is_err()
                && (keys.rows.len() >= PAGE_RECORDS || keys.key_bytes + key.len() > PAGE_KEY_BYTES)
            {
                break;
            }
            let (key, record) = entries.next().unwrap();
            let value_bytes = record.value.as_ref().map_or(0, Bytes::len);
            *bytes += record.memory_size(&key);
            self.value_bytes += value_bytes;
            match position {
                Ok(row) => {
                    let old = std::mem::replace(&mut self.records[row], record);
                    self.value_bytes -= old.value.as_ref().map_or(0, Bytes::len);
                    *bytes -= old.memory_size(&key);
                }
                Err(row) => {
                    keys.key_bytes += key.len();
                    keys.rows.insert(row, Bytes::from(key));
                    keys.prefixes.insert(row, prefix);
                    self.records.insert(row, record);
                    *len += 1;
                }
            }
        }
    }
    fn fold(&mut self, updates: &mut Vec<(usize, Record)>) {
        for (row, record) in updates.drain(..) {
            self.replace(row, record);
        }
    }
}

// A snapshot update copies only a bounded delta and one base handle. Keys and
// unchanged values stay in the immutable base until a structural edit or fold.
const PAGE_UPDATES: usize = 16;
#[derive(Clone, Default)]
struct Page {
    base: Arc<PageData>,
    updates: Vec<(usize, Record)>,
}
impl std::ops::Deref for Page {
    type Target = PageData;
    fn deref(&self) -> &PageData {
        &self.base
    }
}
impl Page {
    #[inline]
    fn record(&self, row: usize) -> &Record {
        if !self.updates.is_empty() {
            if let Ok(i) = self.updates.binary_search_by_key(&row, |(i, _)| *i) {
                return &self.updates[i].1;
            }
        }
        &self.records[row]
    }
    fn materialize(&mut self) -> &mut PageData {
        if !self.updates.is_empty() && Arc::get_mut(&mut self.base).is_none() {
            // Copy unchanged handles once and move delta records directly into place.
            // Sorted deltas merge linearly; no per-row binary search or obsolete value clone.
            let mut updates = std::mem::take(&mut self.updates).into_iter().peekable();
            let mut value_bytes = self.value_bytes;
            let records = self
                .records
                .iter()
                .enumerate()
                .map(|(i, original)| {
                    if updates.peek().is_some_and(|(row, _)| *row == i) {
                        let (_, record) = updates.next().unwrap();
                        value_bytes -= original.value.as_ref().map_or(0, Bytes::len);
                        value_bytes += record.value.as_ref().map_or(0, Bytes::len);
                        record
                    } else {
                        original.clone()
                    }
                })
                .collect();
            self.base = Arc::new(PageData {
                keys: self.keys.clone(),
                records,
                value_bytes,
            });
        }
        let base = Arc::make_mut(&mut self.base);
        base.fold(&mut self.updates);
        base
    }
    fn replace(&mut self, row: usize, record: Record) -> usize {
        // A dropped snapshot must not leave superseded payloads in our base.
        if let Some(base) = Arc::get_mut(&mut self.base) {
            base.fold(&mut self.updates);
            return base.replace(row, record);
        }
        match self.updates.binary_search_by_key(&row, |(i, _)| *i) {
            Ok(i) => {
                let previous_bytes = self.updates[i].1.value.as_ref().map_or(0, Bytes::len);
                self.updates[i].1 = record;
                previous_bytes
            }
            Err(i) if self.updates.len() < (self.rows.len() / 8).clamp(PAGE_UPDATES, 32) => {
                let old = self.records[row].value.as_ref().map_or(0, Bytes::len);
                self.updates.insert(i, (row, record));
                old
            }
            Err(_) => self.materialize().replace(row, record),
        }
    }
    fn insert_at(&mut self, row: usize, key: Vec<u8>, record: Record, prefix: u64) {
        self.materialize().insert_at(row, key, record, prefix);
    }
    fn allocated_bytes(&self) -> usize {
        std::mem::size_of::<Self>() + std::mem::size_of::<PageData>()
            + std::mem::size_of::<PageKeys>()
            + 6 * std::mem::size_of::<usize>() // Page, base and keys Arc headers
            + self.rows.capacity() * std::mem::size_of::<Bytes>()
            + self.records.capacity() * std::mem::size_of::<Record>()
            + self.prefixes.capacity() * std::mem::size_of::<u64>()
            + self.key_bytes + self.value_bytes
            + self.updates.capacity() * std::mem::size_of::<(usize, Record)>()
            + self.updates.iter().map(|(_, r)| r.value.as_ref().map_or(0, Bytes::len)).sum::<usize>()
    }
    fn overfull(&self) -> bool {
        self.rows.len() > 1 && (self.rows.len() > PAGE_RECORDS || self.key_bytes > PAGE_KEY_BYTES)
    }
}

#[derive(Clone, Default)]
struct PageGroup {
    pages: Vec<Arc<Page>>,
    prefixes: Vec<u64>,
}
impl PageGroup {
    fn first_key(&self) -> &[u8] {
        &self.pages[0].rows[0]
    }
    fn page_for(&self, key: &[u8], prefix: u64) -> usize {
        preceding(&self.prefixes, prefix, |i| {
            self.pages[i].rows[0].as_ref() <= key
        })
    }
    #[inline]
    fn insert<K: AsRef<[u8]> + Into<Vec<u8>>>(
        &mut self,
        key: K,
        record: Record,
        prefix: u64,
        append: bool,
    ) -> (Option<usize>, Option<usize>) {
        if self.pages.is_empty() {
            self.pages.push(Arc::new(Page::default()));
            self.prefixes.push(prefix);
        }
        // Sorted ingestion seals full pages instead of copying half their rows.
        if append
            && self.pages.last().is_some_and(|p| {
                !p.rows.is_empty()
                    && (p.rows.len() >= PAGE_RECORDS
                        || p.key_bytes + key.as_ref().len() > PAGE_KEY_BYTES)
            })
        {
            self.pages.push(Arc::new(Page::default()));
            self.prefixes.push(prefix);
        }
        let index = if append {
            self.pages.len() - 1
        } else {
            self.page_for(key.as_ref(), prefix)
        };
        let page = Arc::make_mut(&mut self.pages[index]);
        let position = if append {
            Err(page.rows.len())
        } else {
            page.position(key.as_ref(), prefix)
        };
        match position {
            Ok(i) => (Some(page.replace(i, record)), Some(index)),
            Err(i) => {
                page.insert_at(i, key.into(), record, prefix);
                self.prefixes[index] = page.prefixes[0];
                let (mut cursor, mut end) = (index, index + 1);
                while cursor < end {
                    if self.pages[cursor].overfull() {
                        let page = Arc::make_mut(&mut self.pages[cursor]).materialize();
                        let middle = page.rows.len() / 2;
                        let keys = Arc::make_mut(&mut page.keys);
                        let rows = keys.rows.split_off(middle);
                        let prefixes = keys.prefixes.split_off(middle);
                        let records = page.records.split_off(middle);
                        let key_bytes = rows.iter().map(Bytes::len).sum();
                        let value_bytes = records
                            .iter()
                            .map(|r| r.value.as_ref().map_or(0, Bytes::len))
                            .sum();
                        keys.key_bytes -= key_bytes;
                        page.value_bytes -= value_bytes;
                        keys.rows.shrink_to_fit();
                        keys.prefixes.shrink_to_fit();
                        page.records.shrink_to_fit();
                        self.prefixes.insert(cursor + 1, prefixes[0]);
                        self.pages.insert(
                            cursor + 1,
                            Arc::new(Page {
                                base: Arc::new(PageData {
                                    keys: Arc::new(PageKeys {
                                        rows,
                                        prefixes,
                                        key_bytes,
                                    }),
                                    records,
                                    value_bytes,
                                }),
                                updates: Vec::new(),
                            }),
                        );
                        end += 1;
                    } else {
                        cursor += 1;
                    }
                }
                (None, (end == index + 1).then_some(index))
            }
        }
    }
}

#[derive(Default)]
pub(crate) struct MemIndex {
    groups: Vec<Arc<PageGroup>>,
    prefixes: Vec<u64>,
    len: usize,
    last_insert: Option<(usize, usize)>,
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
    pub(crate) fn page_count(&self) -> usize {
        self.groups.iter().map(|group| group.pages.len()).sum()
    }
    // Inclusive page span for a key interval, including insertion positions.
    // Used only as a recovery locality estimate; it neither retains snapshots
    // nor changes routing or the order in which records become visible.
    pub(crate) fn page_span(&self, lower: &[u8], upper: &[u8]) -> usize {
        if self.groups.is_empty() {
            return 0;
        }
        debug_assert!(lower <= upper);
        let first = self.group_for(lower, prefix(lower));
        let last = self.group_for(upper, prefix(upper));
        let begin = self.groups[first].page_for(lower, prefix(lower));
        let end = self.groups[last].page_for(upper, prefix(upper));
        if first == last {
            end - begin + 1
        } else {
            self.groups[first].pages.len() - begin
                + self.groups[first + 1..last]
                    .iter()
                    .map(|group| group.pages.len())
                    .sum::<usize>()
                + end
                + 1
        }
    }
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
    // Includes reachable base/delta payload and container capacities. Allocator
    // headers, spare capacity inside Bytes and snapshots no longer in this
    // index are excluded; this is an allocation estimate, not process RSS.
    pub fn allocated_bytes(&self) -> usize {
        self.groups.capacity() * std::mem::size_of::<Arc<PageGroup>>()
            + self.prefixes.capacity() * std::mem::size_of::<u64>()
            + self
                .groups
                .iter()
                .map(|g| {
                    std::mem::size_of::<PageGroup>()
                        + 2 * std::mem::size_of::<usize>()
                        + g.pages.capacity() * std::mem::size_of::<Arc<Page>>()
                        + g.prefixes.capacity() * std::mem::size_of::<u64>()
                        + g.pages.iter().map(|p| p.allocated_bytes()).sum::<usize>()
                })
                .sum::<usize>()
    }
    pub fn apply(
        &mut self,
        entries: impl IntoIterator<Item = (Vec<u8>, Record)>,
        remove_deletes: bool,
        bytes: &mut usize,
    ) {
        let mut entries = entries.into_iter().peekable();
        while let Some((key, record)) = entries.next() {
            if remove_deletes && record.value.is_none() {
                if let Some(old) = self.remove(&key) {
                    *bytes -= old.memory_size(&key);
                }
                continue;
            }
            let key_len = key.len();
            *bytes += record.memory_size(&key);
            if let Some(old) = self.upsert(key, record) {
                *bytes -= RECORD_CHARGE + key_len + old;
            }
            let Some((g, p)) = self.last_insert else {
                continue;
            };
            let upper = self.groups[g]
                .pages
                .get(p + 1)
                .map(|p| &p.rows[0])
                .or_else(|| self.groups.get(g + 1).map(|g| &g.pages[0].rows[0]));
            let first = &self.groups[g].pages[p].rows[0];
            let fits = |key: &[u8], record: &Record| {
                !(remove_deletes && record.value.is_none())
                    && key >= first.as_ref()
                    && upper.is_none_or(|end| key < end.as_ref())
            };
            if !entries.peek().is_some_and(|(k, r)| fits(k, r)) {
                continue;
            }
            let upper = upper.cloned();
            let group = Arc::make_mut(&mut self.groups[g]);
            let page = Arc::make_mut(&mut group.pages[p]);
            // Both new rows and replacements amortize ownership checks across
            // a page-local run. Shared bases retain the bounded-delta path below.
            if let Some(base) = Arc::get_mut(&mut page.base) {
                base.fold(&mut page.updates);
                while let Some((key, record)) = entries.peek() {
                    if (remove_deletes && record.value.is_none())
                        || key.as_slice() < base.rows[0].as_ref()
                        || upper
                            .as_ref()
                            .is_some_and(|end| key.as_slice() >= end.as_ref())
                    {
                        break;
                    }
                    let prefix = prefix(key);
                    let position = if key.as_slice() > base.rows.last().unwrap().as_ref() {
                        Err(base.rows.len())
                    } else {
                        base.position(key, prefix)
                    };
                    if position.is_err()
                        && (base.rows.len() >= PAGE_RECORDS
                            || base.key_bytes + key.len() > PAGE_KEY_BYTES)
                    {
                        break;
                    }
                    if let Ok(row) = position {
                        let (key, record) = entries.next().unwrap();
                        *bytes += record.memory_size(&key);
                        *bytes -= RECORD_CHARGE + key.len() + base.replace(row, record);
                    } else {
                        base.apply_inserting_run(
                            &mut entries,
                            upper.as_deref(),
                            remove_deletes,
                            bytes,
                            &mut self.len,
                        );
                        break;
                    }
                }
                continue;
            }
            // A contiguous run stays within this page. Splits, lower-bound
            // changes and physical deletes go back through the general path.
            while let Some((key, record)) = entries.peek() {
                if (remove_deletes && record.value.is_none())
                    || key.as_slice() < page.rows[0].as_ref()
                    || upper
                        .as_ref()
                        .is_some_and(|end| key.as_slice() >= end.as_ref())
                {
                    break;
                }
                let prefix = prefix(key);
                let position = page.position(key, prefix);
                if position.is_err()
                    && (page.rows.len() == PAGE_RECORDS
                        || page.key_bytes + key.len() > PAGE_KEY_BYTES)
                {
                    break;
                }
                let (key, record) = entries.next().unwrap();
                *bytes += record.memory_size(&key);
                match position {
                    Ok(row) => {
                        let old = page.replace(row, record);
                        *bytes -= RECORD_CHARGE + key.len() + old;
                    }
                    Err(row) => {
                        page.insert_at(row, key, record, prefix);
                        self.len += 1;
                    }
                }
            }
        }
    }
    #[inline]
    pub fn get(&self, key: &[u8]) -> Option<&Record> {
        if self.len == 0 {
            return None;
        }
        let prefix = prefix(key);
        let group = &self.groups[self.group_for(key, prefix)];
        let p = &group.pages[group.page_for(key, prefix)];
        p.position(key, prefix).ok().map(|i| p.record(i))
    }
    #[inline]
    // Production writes need only the old payload charge, not an owned old
    // value. Shared snapshot pages can report it without incrementing Bytes'
    // reference count just to decrement it immediately after accounting.
    pub fn upsert<K: AsRef<[u8]> + Into<Vec<u8>>>(
        &mut self,
        key: K,
        record: Record,
    ) -> Option<usize> {
        let prefix = prefix(key.as_ref());
        let append = self.groups.last().is_none_or(|g| {
            let p = g.pages.last().unwrap();
            let last = *p.prefixes.last().unwrap();
            last < prefix || (last == prefix && p.rows.last().unwrap().as_ref() < key.as_ref())
        });
        if self.groups.is_empty() {
            self.groups.push(Arc::new(PageGroup::default()));
            self.prefixes.push(prefix);
        }
        let index = if append {
            self.groups.len() - 1
        } else {
            self.group_for(key.as_ref(), prefix)
        };
        let group = Arc::make_mut(&mut self.groups[index]);
        let (previous, page) = group.insert(key, record, prefix, append);
        self.last_insert = page.map(|p| (index, p));
        if previous.is_none() {
            self.len += 1;
            self.prefixes[index] = group.prefixes[0];
            if group.pages.len() > GROUP_PAGES {
                self.last_insert = None;
                let middle = if append {
                    GROUP_PAGES
                } else {
                    group.pages.len() / 2
                };
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
    #[cfg(test)]
    pub fn insert<K: AsRef<[u8]> + Into<Vec<u8>>>(
        &mut self,
        key: K,
        record: Record,
    ) -> Option<Record> {
        let old = self.get(key.as_ref()).cloned();
        let charge = self.upsert(key, record);
        assert_eq!(
            charge,
            old.as_ref().map(|r| r.value.as_ref().map_or(0, Bytes::len))
        );
        old
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
        let page = Arc::make_mut(&mut group.pages[p]).materialize();
        let keys = Arc::make_mut(&mut page.keys);
        let key = keys.rows.remove(row);
        keys.prefixes.remove(row);
        keys.key_bytes -= key.len();
        let record = page.records.remove(row);
        page.value_bytes -= record.value.as_ref().map_or(0, Bytes::len);
        if keys.rows.capacity() > keys.rows.len().max(4) * 4 {
            keys.rows.shrink_to_fit();
            keys.prefixes.shrink_to_fit();
            page.records.shrink_to_fit();
        }
        self.len -= 1;
        if page.rows.is_empty() {
            group.pages.remove(p);
            group.prefixes.remove(p);
        } else {
            group.prefixes[p] = page.prefixes[0];
            if page.rows.len() < PAGE_RECORDS / 4 && group.pages.len() > 1 {
                let can_merge = |left: usize| {
                    let right = left + 1;
                    right < group.pages.len()
                        && group.pages[left].rows.len() + group.pages[right].rows.len()
                            <= PAGE_RECORDS / 2
                        && group.pages[left].key_bytes + group.pages[right].key_bytes
                            <= PAGE_KEY_BYTES
                };
                let left = p
                    .checked_sub(1)
                    .filter(|&left| can_merge(left))
                    .or_else(|| can_merge(p).then_some(p));
                if let Some(left) = left {
                    let right = left + 1;
                    let mut removed = Arc::unwrap_or_clone(group.pages.remove(right));
                    group.prefixes.remove(right);
                    let removed = removed.materialize();
                    let target = Arc::make_mut(&mut group.pages[left]).materialize();
                    let removed_keys = Arc::make_mut(&mut removed.keys);
                    let target_keys = Arc::make_mut(&mut target.keys);
                    target_keys.rows.append(&mut removed_keys.rows);
                    target_keys.prefixes.append(&mut removed_keys.prefixes);
                    target_keys.key_bytes += removed_keys.key_bytes;
                    target.records.append(&mut removed.records);
                    target.value_bytes += removed.value_bytes;
                    target_keys.rows.shrink_to_fit();
                    target_keys.prefixes.shrink_to_fit();
                    target.records.shrink_to_fit();
                }
            }
        }
        if group.pages.capacity() > group.pages.len().max(4) * 4 {
            group.pages.shrink_to_fit();
            group.prefixes.shrink_to_fit();
        }
        if group.pages.is_empty() {
            self.groups.remove(g);
            self.prefixes.remove(g);
        } else {
            self.prefixes[g] = group.prefixes[0];
            if group.pages.len() < GROUP_PAGES / 4 && self.groups.len() > 1 {
                let can_merge = |left: usize| {
                    let right = left + 1;
                    right < self.groups.len()
                        && self.groups[left].pages.len() + self.groups[right].pages.len()
                            <= GROUP_PAGES / 2
                };
                let left = g
                    .checked_sub(1)
                    .filter(|&left| can_merge(left))
                    .or_else(|| can_merge(g).then_some(g));
                if let Some(left) = left {
                    let right = left + 1;
                    let mut removed = Arc::unwrap_or_clone(self.groups.remove(right));
                    self.prefixes.remove(right);
                    let target = Arc::make_mut(&mut self.groups[left]);
                    target.pages.append(&mut removed.pages);
                    target.prefixes.append(&mut removed.prefixes);
                    target.pages.shrink_to_fit();
                    target.prefixes.shrink_to_fit();
                }
            }
        }
        if self.groups.capacity() > self.groups.len().max(1) * 4 || self.len == 0 {
            self.groups.shrink_to_fit();
            self.prefixes.shrink_to_fit();
        }
        self.last_insert = None;
        Some(record)
    }
    pub fn iter(&self) -> impl Iterator<Item = (&[u8], &Record)> {
        self.groups
            .iter()
            .flat_map(|g| g.pages.iter())
            .flat_map(|p| {
                p.rows
                    .iter()
                    .enumerate()
                    .map(|(i, k)| (k.as_ref(), p.record(i)))
            })
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
                    Bound::Included(_) => rows.partition_point(|key| key.as_ref() < k.as_slice()),
                    _ => rows.partition_point(|key| key.as_ref() <= k.as_slice()),
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
        let first_key = &self.groups.get(start)?.pages[page].rows[row];
        if !range.allows_end(first_key) {
            return None;
        }
        // Copy one handle per group and retain only the current page directly.
        let current = Some(self.groups[start].pages[page].clone());
        let page_end = current.as_ref().unwrap().records.len();
        let page_in_range = range.allows_end(current.as_ref().unwrap().rows.last().unwrap());
        if !page_in_range {
            // A range wholly inside one page needs no synthetic group or
            // directory allocation; the current page is the complete snapshot.
            return Some(MemScan {
                groups: VecDeque::new(),
                current,
                page: 0,
                row,
                page_end,
                page_in_range,
                range,
            });
        }
        let mut groups: VecDeque<_> = self.groups[start..]
            .iter()
            .take_while(|g| range.allows_end(g.first_key()))
            .cloned()
            .collect();
        // Trim small boundary slices instead of retaining unrelated pages in
        // their groups. Broad ranges still clone only one handle per group.
        let last = groups.len() - 1;
        for i in [0, last] {
            let group = &groups[i];
            let begin = if i == 0 { page } else { 0 };
            let end = if i != last || range.allows_end(&group.pages.last().unwrap().rows[0]) {
                group.pages.len()
            } else {
                group
                    .pages
                    .partition_point(|p| range.allows_end(&p.rows[0]))
            };
            if end - begin <= 4 && (begin != 0 || end != group.pages.len()) {
                groups[i] = Arc::new(PageGroup {
                    pages: group.pages[begin..end].to_vec(),
                    // Scan traversal needs page handles only, never routing.
                    prefixes: Vec::new(),
                });
                if i == 0 {
                    page = 0;
                }
            }
            if last == 0 {
                break;
            }
        }
        Some(MemScan {
            groups,
            current,
            page,
            row,
            page_end,
            page_in_range,
            range,
        })
    }
}

pub(crate) struct MemScan {
    groups: VecDeque<Arc<PageGroup>>,
    current: Option<Arc<Page>>,
    page: usize,
    row: usize,
    // Cache the physical end and whether this whole page is inside the range.
    // Partial boundary pages retain the cheap per-row check: pre-seeking their
    // upper bound costs more than consuming only a few rows.
    page_end: usize,
    page_in_range: bool,
    range: Arc<KeyRange>,
}
impl MemScan {
    fn finish_page(&mut self) {
        if !self.page_in_range || self.groups.is_empty() {
            self.current = None;
            self.groups.clear();
            return;
        }
        self.page += 1;
        self.row = 0;
        if self.page == self.groups.front().unwrap().pages.len() {
            self.groups.pop_front();
            self.page = 0;
        }
        self.current = self.groups.front().map(|g| g.pages[self.page].clone());
        if let Some(page) = &self.current {
            self.page_end = page.records.len();
            self.page_in_range = self.range.allows_end(page.rows.last().unwrap());
        }
    }
    // Full pages need no per-row bound comparison; the last partial page does.
    pub(crate) fn current(&self) -> Option<(&Bytes, &Record)> {
        let page = self.current.as_ref()?;
        let key = &page.rows[self.row];
        (self.page_in_range || self.range.allows_end(key)).then(|| (key, page.record(self.row)))
    }
    pub(crate) fn advance(&mut self) {
        if self.current.is_none() {
            return;
        }
        self.row += 1;
        if self.row == self.page_end {
            self.finish_page();
        }
    }
    // This loop is reached through the shared memory/SST scan dispatch. Keep
    // it separate so its page and tombstone handling does not expand that path.
    #[inline(never)]
    pub(crate) fn next_live(&mut self) -> Option<(Vec<u8>, Bytes)> {
        loop {
            let page = self.current.as_ref()?;
            if !self.page_in_range && !self.range.allows_end(&page.rows[self.row]) {
                self.current = None;
                self.groups.clear();
                return None;
            }
            let out = page.record(self.row).value.as_ref().map(|value| {
                (
                    crate::scan::copy_scan_key(&page.rows[self.row]),
                    value.clone(),
                )
            });
            self.row += 1;
            if self.row == self.page_end {
                self.finish_page();
            }
            if out.is_some() {
                return out;
            }
        }
    }
}
impl Iterator for MemScan {
    type Item = (Vec<u8>, Record);
    fn next(&mut self) -> Option<Self::Item> {
        let page = self.current.as_ref()?;
        let key = &page.rows[self.row];
        if !self.page_in_range && !self.range.allows_end(key) {
            self.current = None;
            self.groups.clear();
            return None;
        }
        let out = (key.to_vec(), page.record(self.row).clone());
        self.row += 1;
        if self.row == self.page_end {
            self.finish_page();
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
    fn replay_page_spans_match_complete_byte_order_across_groups() {
        assert_eq!(MemIndex::default().page_span(b"", b"z"), 0);
        for common_prefix in [false, true] {
            let key = |n: u64| {
                let mut bytes = if common_prefix {
                    vec![42; 128]
                } else {
                    Vec::new()
                };
                bytes.extend_from_slice(&n.to_be_bytes());
                bytes
            };
            let mut map = MemIndex::default();
            for n in 0..17_000u64 {
                map.insert(
                    key(n * 2 + 2),
                    Record {
                        seq: n + 1,
                        value: None,
                    },
                );
            }
            assert!(map.groups.len() > 1);
            let starts: Vec<_> = map
                .groups
                .iter()
                .flat_map(|g| &g.pages)
                .map(|page| page.rows[0].clone())
                .collect();
            let mut probes = vec![Vec::new(), vec![255; 200]];
            for n in [0, 1, 255, 256, 257, 16383, 16384, 16385, 16999, 17000] {
                probes.push(key(n * 2 + 1));
                probes.push(key(n * 2 + 2));
            }
            probes.sort();
            probes.dedup();
            for (i, lower) in probes.iter().enumerate() {
                for upper in &probes[i..] {
                    let begin = starts
                        .partition_point(|k| k.as_ref() <= lower.as_slice())
                        .saturating_sub(1);
                    let end = starts
                        .partition_point(|k| k.as_ref() <= upper.as_slice())
                        .saturating_sub(1);
                    assert_eq!(map.page_span(lower, upper), end - begin + 1);
                }
            }
        }
    }

    #[test]
    fn live_cursor_bounds_cross_pages_and_stop_amid_tombstones() {
        let mut map = MemIndex::default();
        let rows: Vec<_> = (0..2048u64)
            .map(|n| {
                let key = n.to_be_bytes().to_vec();
                let record = Record {
                    seq: n + 1,
                    value: (n % 17 == 0 || n % 257 == 0)
                        .then(|| Bytes::copy_from_slice(&n.to_be_bytes())),
                };
                map.insert(key.clone(), record.clone());
                (key, record)
            })
            .collect();
        for (lo, hi) in [
            (0, 0),
            (0, 1),
            (0, 255),
            (0, 256),
            (255, 257),
            (257, 768),
            (1024, 1600),
            (2047, 2048),
        ] {
            for include_start in [false, true] {
                for include_end in [false, true] {
                    let lo = (lo as u64).to_be_bytes();
                    let hi = (hi as u64).to_be_bytes();
                    let start = if include_start {
                        Bound::Included(lo.as_slice())
                    } else {
                        Bound::Excluded(lo.as_slice())
                    };
                    let end = if include_end {
                        Bound::Included(hi.as_slice())
                    } else {
                        Bound::Excluded(hi.as_slice())
                    };
                    let range = Arc::new(KeyRange::new((start, end)).unwrap());
                    let expected: Vec<_> = rows
                        .iter()
                        .filter(|(key, _)| {
                            (key.as_slice() > lo.as_slice()
                                || (include_start && key.as_slice() == lo))
                                && (key.as_slice() < hi.as_slice()
                                    || (include_end && key.as_slice() == hi))
                        })
                        .cloned()
                        .collect();
                    let all: Vec<_> = map.snapshot(range.clone()).into_iter().flatten().collect();
                    assert_eq!(all, expected);
                    let mut scan = map.snapshot(range);
                    let actual: Vec<_> =
                        std::iter::from_fn(|| scan.as_mut()?.next_live()).collect();
                    let expected: Vec<_> = expected
                        .into_iter()
                        .filter_map(|(key, record)| record.value.map(|value| (key, value)))
                        .collect();
                    assert_eq!(actual, expected);
                    if let Some(scan) = scan {
                        assert!(scan.current.is_none());
                        assert!(scan.groups.is_empty());
                    }
                }
            }
        }
    }

    #[test]
    fn borrowed_upserts_copy_only_new_keys_and_preserve_shared_versions() {
        struct Input<'a> {
            key: &'a [u8],
            copies: &'a std::cell::Cell<usize>,
        }
        impl AsRef<[u8]> for Input<'_> {
            fn as_ref(&self) -> &[u8] {
                self.key
            }
        }
        impl From<Input<'_>> for Vec<u8> {
            fn from(input: Input<'_>) -> Self {
                input.copies.set(input.copies.get() + 1);
                input.key.to_vec()
            }
        }
        let copies = std::cell::Cell::new(0);
        let keys = [
            vec![],
            vec![0],
            vec![0, 255, 0],
            vec![7; 65536],
            vec![8; 1024 * 1024],
        ];
        let mut map = MemIndex::default();
        let input = |key| Input {
            key,
            copies: &copies,
        };
        for (i, key) in keys.iter().enumerate() {
            assert!(map
                .insert(
                    input(key),
                    Record {
                        seq: i as u64 + 1,
                        value: Some(Bytes::from_static(b"old")),
                    }
                )
                .is_none());
        }
        assert_eq!(copies.get(), keys.len());
        let before = map.snapshot(all()).unwrap();
        copies.set(0);
        for pass in 0..40u64 {
            for (i, key) in keys.iter().enumerate() {
                let value = (pass % 3 != 0).then(|| Bytes::from(vec![pass as u8; pass as usize]));
                let record = Record {
                    seq: 100 + pass * keys.len() as u64 + i as u64,
                    value,
                };
                assert!(map.insert(input(key), record.clone()).is_some());
                assert_eq!(map.get(key), Some(&record));
            }
        }
        assert_eq!(
            copies.get(),
            0,
            "replacement must not materialize an owned key"
        );
        assert_eq!(map.len(), keys.len());
        for ((key, old), expected) in before.zip(&keys) {
            assert_eq!(&key, expected);
            assert_eq!(old.value.as_deref(), Some(b"old".as_slice()));
        }
        let latest = map.snapshot(all()).unwrap().collect::<Vec<_>>();
        map.insert(
            input(b"new"),
            Record {
                seq: 9999,
                value: None,
            },
        );
        assert_eq!(copies.get(), 1);
        assert_eq!(latest.len(), keys.len());
    }

    #[test]
    fn value_folds_share_keys_but_structural_edits_preserve_old_snapshots() {
        let mut map = MemIndex::default();
        for n in 0..256u64 {
            map.insert(
                n.to_be_bytes().to_vec(),
                Record {
                    seq: n + 1,
                    value: Some(Bytes::from(vec![1; 1024])),
                },
            );
        }
        let old = map.snapshot(all()).unwrap();
        let keys = map.groups[0].pages[0].keys.clone();
        for n in 0..64u64 {
            map.insert(
                n.to_be_bytes().to_vec(),
                Record {
                    seq: 1000 + n,
                    value: (n % 3 != 0).then(|| Bytes::from(vec![2; 17])),
                },
            );
        }
        assert!(Arc::ptr_eq(&keys, &map.groups[0].pages[0].keys));
        assert!(old
            .enumerate()
            .all(|(n, (_, r))| r.seq == n as u64 + 1
                && r.value.as_ref().unwrap().as_ref() == [1; 1024]));
        let middle = map.snapshot(all()).unwrap().collect::<Vec<_>>();
        let retained = map.snapshot(all()).unwrap();
        // Insertion splits a full page; physical removal and later merges must
        // update both arrays without changing a retained generation's keys.
        let mut inserted = 12u64.to_be_bytes().to_vec();
        inserted.push(1);
        map.insert(inserted.clone(), record(5000));
        assert!(!Arc::ptr_eq(&keys, &map.groups[0].pages[0].keys));
        for n in 0..220u64 {
            map.remove(&n.to_be_bytes());
        }
        assert_eq!(retained.collect::<Vec<_>>(), middle);
        assert_eq!(map.get(&inserted), Some(&record(5000)));
        for n in 220..256u64 {
            assert_eq!(map.get(&n.to_be_bytes()).unwrap().seq, n + 1);
        }
        drop(keys);
        for n in 220..256u64 {
            map.insert(n.to_be_bytes().to_vec(), record(6000 + n));
        }
        assert!(map.allocated_bytes() < 16 * 1024);
    }

    #[test]
    fn repeated_snapshot_folds_preserve_each_generation_and_release_payloads() {
        let mut map = MemIndex::default();
        let mut model = std::collections::BTreeMap::new();
        for n in 0..256u64 {
            let r = Record {
                seq: n + 1,
                value: Some(Bytes::from(vec![0; 128])),
            };
            map.insert(n.to_be_bytes().to_vec(), r.clone());
            model.insert(n.to_be_bytes().to_vec(), r);
        }
        let mut saved = Vec::new();
        for generation in 1..=8u64 {
            saved.push((map.snapshot(all()).unwrap(), model.clone()));
            for n in 0..64u64 {
                let key = ((n * 17 + generation * 7) % 256).to_be_bytes().to_vec();
                let r = Record {
                    seq: 1000 + generation * 64 + n,
                    value: (n % 7 != 0).then(|| Bytes::from(vec![generation as u8; 1024])),
                };
                assert_eq!(map.insert(key.clone(), r.clone()), model.insert(key, r));
            }
        }
        for (scan, expected) in saved {
            assert_eq!(
                scan.collect::<Vec<_>>(),
                expected.into_iter().collect::<Vec<_>>()
            );
        }
        for n in 0..256u64 {
            map.insert(
                n.to_be_bytes().to_vec(),
                Record {
                    seq: 10_000 + n,
                    value: Some(Bytes::new()),
                },
            );
        }
        assert_eq!(map.groups[0].pages[0].value_bytes, 0);
        assert!(map.groups[0].pages[0].updates.is_empty());
    }

    #[test]
    fn full_pages_keep_bounded_deltas_and_fold_only_visible_versions() {
        let mut map = MemIndex::default();
        for n in 0..256u64 {
            map.insert(n.to_be_bytes().to_vec(), record(n + 1));
        }
        let old = map.snapshot(all()).unwrap();
        let base = map.groups[0].pages[0].base.clone();
        for n in 0..32u64 {
            map.insert(
                n.to_be_bytes().to_vec(),
                Record {
                    seq: 1000 + n,
                    value: Some(Bytes::from(vec![3; 17])),
                },
            );
            assert!(Arc::ptr_eq(&base, &map.groups[0].pages[0].base));
        }
        let middle = map.snapshot(all()).unwrap();
        map.insert(32u64.to_be_bytes().to_vec(), record(2000));
        assert!(!Arc::ptr_eq(&base, &map.groups[0].pages[0].base));
        assert!(map.groups[0].pages[0].updates.is_empty());
        assert_eq!(map.groups[0].pages[0].value_bytes, 32 * 17);
        assert!(old.enumerate().all(|(n, (_, r))| r.seq == n as u64 + 1));
        assert!(middle.enumerate().all(|(n, (_, r))| r.seq
            == if n < 32 {
                1000 + n as u64
            } else {
                n as u64 + 1
            }));
        map.insert(0u64.to_be_bytes().to_vec(), record(3000));
        assert_eq!(map.groups[0].pages[0].value_bytes, 31 * 17);
    }

    #[test]
    fn dropping_snapshots_allows_overwrites_to_release_old_payloads() {
        let mut map = MemIndex::default();
        map.insert(
            b"k".to_vec(),
            Record {
                seq: 1,
                value: Some(Bytes::from(vec![7; 3 * 1024 * 1024])),
            },
        );
        let mut old = map.snapshot(all()).unwrap();
        map.insert(
            b"k".to_vec(),
            Record {
                seq: 2,
                value: Some(Bytes::from_static(b"x")),
            },
        );
        assert_eq!(map.get(b"k").unwrap().value.as_deref(), Some(&b"x"[..]));
        assert_eq!(old.next().unwrap().1.value.unwrap().len(), 3 * 1024 * 1024);
        drop(old);
        for seq in 3..8 {
            map.insert(
                b"k".to_vec(),
                Record {
                    seq,
                    value: Some(Bytes::from_static(b"y")),
                },
            );
        }
        assert!(map.groups[0].pages[0].updates.is_empty());
        assert_eq!(map.groups[0].pages[0].value_bytes, 1);
        assert!(map.allocated_bytes() < 4096);
    }

    #[test]
    fn sorted_append_fills_pages_and_survives_later_random_edits() {
        let mut map = MemIndex::default();
        let mut bytes = 0;
        map.apply(
            (0..20_000u64).map(|n| (n.to_be_bytes().to_vec(), record(n))),
            false,
            &mut bytes,
        );
        assert_eq!(
            map.groups.iter().map(|g| g.pages.len()).sum::<usize>(),
            20_000usize.div_ceil(PAGE_RECORDS)
        );
        assert_eq!(map.groups[0].pages.len(), GROUP_PAGES);
        assert!(map.groups[0]
            .pages
            .iter()
            .all(|p| p.rows.len() == PAGE_RECORDS));
        let old = map.snapshot(all()).unwrap();
        // Replace boundary rows, then insert between them in a full first page.
        for n in [0u64, 255, 256, 257, 16383, 16384, 19999] {
            map.insert(n.to_be_bytes().to_vec(), record(n + 50_000));
            let mut key = n.to_be_bytes().to_vec();
            key.push(1);
            map.insert(key, record(80_000 + n));
        }
        assert_eq!(
            old.map(|(_, r)| r.seq).collect::<Vec<_>>(),
            (0..20_000).collect::<Vec<_>>()
        );
        let rows = map
            .iter()
            .map(|(k, r)| (k.to_vec(), r.clone()))
            .collect::<Vec<_>>();
        assert!(rows.windows(2).all(|w| w[0].0 < w[1].0));
        assert_eq!(rows.len(), 20_007);
        for (key, r) in rows {
            assert_eq!(map.get(&key), Some(&r));
        }
    }

    #[test]
    fn inserting_runs_detach_shared_keys_and_preserve_batch_order() {
        let mut map = MemIndex::default();
        let mut model = std::collections::BTreeMap::new();
        let mut bytes = 0;
        let key = |n: u64| n.to_be_bytes().to_vec();
        for n in (0..200).step_by(2) {
            let r = Record {
                seq: n + 1,
                value: Some(Bytes::from(vec![1; 32])),
            };
            map.apply([(key(n), r.clone())], false, &mut bytes);
            model.insert(key(n), r);
        }
        let before = map.snapshot(all()).unwrap();
        let expected_before = model.clone();
        // Fold values while retaining the old page: the new base is exclusive,
        // but its key array is still shared with the snapshot.
        let shared_keys = map.groups[0].pages[0].keys.clone();
        for n in (0..80).step_by(2) {
            let r = Record {
                seq: 1000 + n,
                value: Some(Bytes::from_static(b"changed")),
            };
            map.apply([(key(n), r.clone())], false, &mut bytes);
            model.insert(key(n), r);
        }
        assert!(Arc::ptr_eq(&shared_keys, &map.groups[0].pages[0].keys));
        for remove_deletes in [false, true] {
            // Mix inserts, duplicates, tombstones and physical removals, then
            // cross the page and group limits in a single lazy input stream.
            let entries: Vec<_> = (0..20_000u64)
                .flat_map(|n| {
                    [
                        (
                            key(n),
                            Record {
                                seq: 10_000 + n * 2,
                                value: Some(Bytes::from(vec![3; (n % 31) as usize])),
                            },
                        ),
                        (
                            key(n),
                            Record {
                                seq: 10_001 + n * 2,
                                value: (n % 7 != 0).then(|| Bytes::from_static(b"last")),
                            },
                        ),
                    ]
                })
                .collect();
            for (k, r) in &entries {
                if remove_deletes && r.value.is_none() {
                    model.remove(k);
                } else {
                    model.insert(k.clone(), r.clone());
                }
            }
            map.apply(entries, remove_deletes, &mut bytes);
            assert_eq!(
                map.iter()
                    .map(|(k, r)| (k.to_vec(), r.clone()))
                    .collect::<Vec<_>>(),
                model.clone().into_iter().collect::<Vec<_>>()
            );
            assert_eq!(
                bytes,
                model.iter().map(|(k, r)| r.memory_size(k)).sum::<usize>()
            );
        }
        assert!(!Arc::ptr_eq(&shared_keys, &map.groups[0].pages[0].keys));
        assert_eq!(
            before.collect::<Vec<_>>(),
            expected_before.into_iter().collect::<Vec<_>>()
        );
    }

    #[test]
    fn exclusive_batch_folds_deltas_and_preserves_duplicate_order_and_charge() {
        let mut map = MemIndex::default();
        let mut bytes = 0;
        let key = b"k".to_vec();
        map.apply(
            [(
                key.clone(),
                Record {
                    seq: 1,
                    value: Some(Bytes::from(vec![0; 4096])),
                },
            )],
            false,
            &mut bytes,
        );
        let old = map.snapshot(all()).unwrap();
        map.apply([(key.clone(), record(2))], false, &mut bytes);
        drop(old);
        map.apply(
            (3..40).map(|seq| {
                (
                    key.clone(),
                    Record {
                        seq,
                        value: Some(Bytes::from(vec![seq as u8; seq as usize])),
                    },
                )
            }),
            false,
            &mut bytes,
        );
        assert_eq!(map.get(&key).unwrap().seq, 39);
        assert_eq!(bytes, RECORD_CHARGE + 1 + 39);
        assert_eq!(map.groups[0].pages[0].value_bytes, 39);
        assert!(map.groups[0].pages[0].updates.is_empty());
    }

    #[test]
    fn single_page_range_needs_no_directory_and_preserves_inclusive_end() {
        let mut map = MemIndex::default();
        for n in 0..1000u64 {
            map.insert(n.to_be_bytes().to_vec(), record(n));
        }
        let mut scan = map
            .snapshot(Arc::new(
                KeyRange::new(11u64.to_be_bytes().as_slice()..=19u64.to_be_bytes().as_slice())
                    .unwrap(),
            ))
            .unwrap();
        assert!(scan.groups.is_empty());
        assert!(map.groups.iter().all(|g| Arc::strong_count(g) == 1));
        for n in 11..=19u64 {
            map.insert(n.to_be_bytes().to_vec(), record(9999));
        }
        for n in 11..=19u64 {
            assert_eq!(scan.next().unwrap().1, record(n));
        }
        assert!(scan.next().is_none());
        assert!(scan.current.is_none());
        assert!(scan.next().is_none());
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
            .skip(1)
            .all(|p| Arc::strong_count(p) == 1));
        assert_eq!(Arc::strong_count(&map.groups[0].pages[0]), 2);
        for n in 0..10u64 {
            assert_eq!(scan.next(), Some((n.to_be_bytes().to_vec(), record(n))));
        }
        drop(scan);
        assert!(map.groups.iter().all(|g| Arc::strong_count(g) == 1));
        // A narrow range retains only the intersecting pages.
        let range = Arc::new(
            KeyRange::new(600u64.to_be_bytes().as_slice()..800u64.to_be_bytes().as_slice())
                .unwrap(),
        );
        let mut scan = map.snapshot(range).unwrap();
        assert_eq!(scan.groups.len(), 1);
        let weak = Arc::downgrade(scan.groups.front().unwrap());
        let old_group = scan.groups.front().unwrap().clone();
        let g = map.group_for(&600u64.to_be_bytes(), 600);
        assert!(!Arc::ptr_eq(&map.groups[g], &old_group));
        assert_eq!(old_group.pages.len(), 2);
        assert_eq!(Arc::strong_count(&map.groups[g]), 1);
        map.insert(600u64.to_be_bytes().to_vec(), record(9999));
        assert!(!Arc::ptr_eq(&map.groups[g], &old_group));
        drop(old_group);
        drop(map);
        for n in 600..800u64 {
            assert_eq!(scan.next(), Some((n.to_be_bytes().to_vec(), record(n))));
        }
        assert!(scan.next().is_none());
        assert!(weak.upgrade().is_none());
    }

    #[test]
    fn deltas_fold_without_changing_retained_versions_or_payload_accounting() {
        let mut map = MemIndex::default();
        for n in 0..100u64 {
            map.insert(
                n.to_be_bytes().to_vec(),
                Record {
                    seq: n + 1,
                    value: Some(Bytes::from(vec![7; 64])),
                },
            );
        }
        let old = map.snapshot(all()).unwrap();
        let base = map.groups[0].pages[0].base.clone();
        for n in 0..PAGE_UPDATES as u64 {
            map.insert(
                n.to_be_bytes().to_vec(),
                Record {
                    seq: n + 101,
                    value: Some(Bytes::from(vec![9; 3])),
                },
            );
            assert!(Arc::ptr_eq(&base, &map.groups[0].pages[0].base));
        }
        let middle = map.snapshot(all()).unwrap().collect::<Vec<_>>();
        map.insert((PAGE_UPDATES as u64).to_be_bytes().to_vec(), record(1000));
        assert!(!Arc::ptr_eq(&base, &map.groups[0].pages[0].base));
        assert!(map.groups[0].pages[0].updates.is_empty());
        assert_eq!(
            map.groups[0].pages[0].value_bytes,
            (100 - PAGE_UPDATES - 1) * 64 + PAGE_UPDATES * 3
        );
        assert!(old
            .into_iter()
            .all(|(_, r)| r.value.unwrap().as_ref() == [7; 64]));
        for (i, (_, r)) in middle.into_iter().enumerate() {
            assert_eq!(
                r.value.unwrap().len(),
                if i < PAGE_UPDATES { 3 } else { 64 }
            );
        }
    }

    #[test]
    fn page_local_batches_match_ordered_model_across_splits_duplicates_and_deletes() {
        for remove_deletes in [false, true] {
            let mut map = MemIndex::default();
            let mut model = std::collections::BTreeMap::new();
            let mut bytes = 0;
            let mut saved = Vec::new();
            for batch in 0..80u64 {
                let entries: Vec<_> = (0..320u64)
                    .map(|i| {
                        // Equal prefixes, ascending runs, repeated keys and wraps.
                        let mut key = b"shared-prefix/".to_vec();
                        key.extend_from_slice(&((batch * 211 + i / 2) % 6000).to_be_bytes());
                        let r = Record {
                            seq: batch * 320 + i + 1,
                            value: (i % 7 != 0)
                                .then(|| Bytes::from(vec![batch as u8; (i % 19) as usize])),
                        };
                        (key, r)
                    })
                    .collect();
                for (k, r) in &entries {
                    if remove_deletes && r.value.is_none() {
                        model.remove(k);
                    } else {
                        model.insert(k.clone(), r.clone());
                    }
                }
                map.apply(entries, remove_deletes, &mut bytes);
                assert_eq!(map.len(), model.len());
                assert_eq!(
                    bytes,
                    model.iter().map(|(k, r)| r.memory_size(k)).sum::<usize>()
                );
                assert_eq!(
                    map.iter()
                        .map(|(k, r)| (k.to_vec(), r.clone()))
                        .collect::<Vec<_>>(),
                    model
                        .iter()
                        .map(|(k, r)| (k.clone(), r.clone()))
                        .collect::<Vec<_>>()
                );
                if batch % 10 == 0 {
                    saved.push((map.snapshot(all()).unwrap(), model.clone()));
                }
            }
            for (snapshot, model) in saved {
                assert_eq!(
                    snapshot.collect::<Vec<_>>(),
                    model.into_iter().collect::<Vec<_>>()
                );
            }
        }
    }

    #[test]
    fn sparse_deletes_release_capacity_and_narrow_snapshots_do_not_pin_unrelated_pages() {
        let mut map = MemIndex::default();
        for n in 0..20_000u64 {
            map.insert(n.to_be_bytes().to_vec(), record(n));
        }
        let before = map.allocated_bytes();
        let scan = map
            .snapshot(Arc::new(
                KeyRange::new(10u64.to_be_bytes().as_slice()..20u64.to_be_bytes().as_slice())
                    .unwrap(),
            ))
            .unwrap();
        let unrelated = Arc::downgrade(map.groups[0].pages.last().unwrap());
        for n in 0..20_000u64 {
            if n % 100 != 0 {
                map.remove(&n.to_be_bytes());
            }
        }
        assert!(unrelated.upgrade().is_none());
        assert!(map.allocated_bytes() < before / 8);
        assert!(map.groups.iter().map(|g| g.pages.len()).sum::<usize>() < 20);
        assert_eq!(
            scan.map(|(_, r)| r.seq).collect::<Vec<_>>(),
            (10..20).collect::<Vec<_>>()
        );
        for n in (0..20_000u64).step_by(100) {
            map.remove(&n.to_be_bytes());
        }
        assert_eq!(map.allocated_bytes(), 0);
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
        assert!(!boundaries.is_empty());
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
        for n in 0..20_000u64 {
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
        let key_bytes = map.groups[0].pages[0].rows[0].as_ptr();
        map.insert(vec![0; 32 * 1024], record(999));
        assert_eq!(map.groups[0].pages[0].rows[0].as_ptr(), key_bytes);
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
        // Missing keys inside an equal-prefix run must preserve full byte
        // order at page/group boundaries, including an exclusive lower bound.
        for n in [0u32, 1, 255, 256, 257, 16_383, 16_384, 19_999, 20_000] {
            let mut probe = b"abcdefgh".to_vec();
            probe.extend_from_slice(&n.to_be_bytes());
            probe.push(0);
            assert_eq!(map.get(&probe), model.get(&probe));
            let upper = b"abcdefgi".as_slice();
            let expected: Vec<_> = model
                .iter()
                .filter(|(key, _)| key.as_slice() > probe.as_slice() && key.as_slice() < upper)
                .take(17)
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect();
            let range = Arc::new(
                KeyRange::new((Bound::Excluded(probe.as_slice()), Bound::Excluded(upper))).unwrap(),
            );
            let actual: Vec<_> = map.snapshot(range).into_iter().flatten().take(17).collect();
            assert_eq!(actual, expected);
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
                assert_eq!(*p, prefix(&page.rows[0]));
                assert_eq!(page.prefixes.len(), page.rows.len());
                assert_eq!(page.records.len(), page.rows.len());
            }
        }
    }
}
