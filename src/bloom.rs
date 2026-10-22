use crate::{codec::*, error::corrupt, Result};

pub(crate) struct Bloom {
    bits: Vec<u8>,
    probes: u32,
}
// Bloom format depends on this hash; bump the version if it changes.
const HASH_SEED: u64 = 0xcbf29ce484222325;

pub(crate) fn hash(key: &[u8]) -> u64 {
    finish_hash(extend_hash(HASH_SEED, key))
}

#[inline]
fn extend_hash(mut h: u64, key: &[u8]) -> u64 {
    for b in key {
        h ^= *b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    h
}

#[inline]
fn finish_hash(mut h: u64) -> u64 {
    h ^= h >> 33;
    h = h.wrapping_mul(0xff51afd7ed558ccd);
    h ^= h >> 33;
    h
}
// Sorted SST keys often share a long prefix. Keep only a bounded prefix and
// its unfinished FNV state; the persisted hash remains exactly the same.
// Short keys bypass the cache, and arbitrary key order is still supported.
pub(crate) struct PrefixHasher {
    prefix: [u8; 128],
    len: u8,
    misses: u8,
    cooldown: u8,
    state: u64,
}

impl Default for PrefixHasher {
    fn default() -> Self {
        Self {
            prefix: [0; 128],
            len: 0,
            misses: 0,
            cooldown: 0,
            state: HASH_SEED,
        }
    }
}

impl PrefixHasher {
    pub fn hash(&mut self, key: &[u8]) -> u64 {
        if key.len() < 64 {
            return hash(key);
        }
        // Unrelated prefixes should not pay for a comparison and a copy on
        // every key. Sample again after a bounded run of ordinary hashes.
        if self.cooldown != 0 {
            self.cooldown -= 1;
            return hash(key);
        }
        // Leave the usual numeric suffix out of the reusable prefix.
        let len = (key.len() - 8).min(self.prefix.len());
        let (prefix, suffix) = key.split_at(len);
        if usize::from(self.len) != len || &self.prefix[..len] != prefix {
            self.state = extend_hash(HASH_SEED, prefix);
            self.prefix[..len].copy_from_slice(prefix);
            self.len = len as u8;
            self.misses += 1;
            if self.misses == 2 {
                self.misses = 0;
                self.cooldown = 32;
            }
        } else {
            self.misses = 0;
        }
        finish_hash(extend_hash(self.state, suffix))
    }
}

fn delta(hash: u64) -> u64 {
    hash.rotate_left(31).wrapping_mul(0xc4ceb9fe1a85ec53) | 1
}
impl Bloom {
    pub fn new(count: u64, bits_per_key: usize) -> Self {
        Self {
            bits: vec![0; Self::byte_len(count, bits_per_key)],
            probes: ((bits_per_key as f64 * 0.69) as u32).clamp(1, 20),
        }
    }
    fn byte_len(count: u64, bits_per_key: usize) -> usize {
        (count.saturating_mul(bits_per_key as u64).div_ceil(8)).clamp(8, 8 * 1024 * 1024) as usize
    }
    pub fn needs_rebuild(&self, count: u64, bits_per_key: usize) -> bool {
        let needed = Self::byte_len(count, bits_per_key);
        (count == 0 && needed < self.bits.len()) || needed < self.bits.len() / 2
    }
    #[cfg(test)]
    pub fn insert(&mut self, key: &[u8]) {
        self.insert_hash(hash(key));
    }
    pub fn insert_hash(&mut self, mut h: u64) {
        let delta = delta(h);
        let n = self.bits.len() as u64 * 8;
        for _ in 0..self.probes {
            let bit = (h % n) as usize;
            self.bits[bit / 8] |= 1 << (bit % 8);
            h = h.wrapping_add(delta);
        }
    }
    #[cfg(test)]
    pub fn contains(&self, key: &[u8]) -> bool {
        self.contains_hash(hash(key))
    }
    pub fn contains_hash(&self, mut h: u64) -> bool {
        let delta = delta(h);
        let n = self.bits.len() as u64 * 8;
        for _ in 0..self.probes {
            let bit = (h % n) as usize;
            if self.bits[bit / 8] & (1 << (bit % 8)) == 0 {
                return false;
            }
            h = h.wrapping_add(delta);
        }
        true
    }
    pub fn encode(&self, out: &mut Vec<u8>) {
        put_u32(out, self.probes);
        put_u32(out, self.bits.len() as u32);
        out.extend(&self.bits);
    }
    pub fn decode(c: &mut Cursor<'_>) -> Result<Self> {
        let probes = c.u32()?;
        let len = c.u32()? as usize;
        if !(1..=20).contains(&probes) || !(8..=8 * 1024 * 1024).contains(&len) {
            return Err(corrupt("invalid Bloom filter"));
        }
        Ok(Self {
            bits: c.take(len)?.to_vec(),
            probes,
        })
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn prefix_hash_preserves_format_across_lengths_prefix_changes_and_binary_keys() {
        // Independent reference for the original persisted hash algorithm.
        fn reference(key: &[u8]) -> u64 {
            let mut h = 0xcbf29ce484222325u64;
            for &byte in key {
                h = (h ^ u64::from(byte)).wrapping_mul(0x100000001b3);
            }
            h ^= h >> 33;
            h = h.wrapping_mul(0xff51afd7ed558ccd);
            h ^ (h >> 33)
        }
        let mut cached = PrefixHasher::default();
        let mut state = 7u64;
        for round in 0..8 {
            for len in [
                0, 1, 8, 31, 32, 33, 63, 64, 65, 127, 128, 135, 136, 137, 1024,
            ] {
                let mut key = vec![round; len];
                for _ in 0..16 {
                    assert_eq!(cached.hash(&key), reference(&key));
                    assert_eq!(hash(&key), reference(&key));
                    // Change either the suffix or any part of the cached prefix.
                    state ^= state << 13;
                    state ^= state >> 7;
                    state ^= state << 17;
                    if !key.is_empty() {
                        key[state as usize % len] ^= (state >> 32) as u8;
                    }
                }
            }
        }
        // Thousands of adjacent keys reuse a full 128-byte prefix.
        for n in 0u64..4096 {
            let mut key = vec![255; 128];
            key.extend(n.to_be_bytes());
            assert_eq!(cached.hash(&key), reference(&key));
        }
        // Prefix reuse resumes after unrelated keys trigger the cooldown.
        assert_eq!(cached.len, 128);
        assert_eq!(cached.cooldown, 0);
        assert_eq!(cached.misses, 0);
    }

    #[test]
    fn retained_hashes_preserve_persisted_bits_and_binary_keys() {
        let keys: &[&[u8]] = &[b"", b"a", b"b", b"a\0", &[0, 255, 128], b"long-key"];
        let mut golden = Bloom::new(keys.len() as u64, 10);
        for key in keys {
            golden.insert_hash(hash(key));
        }
        // Golden bytes from the pre-hash-retention storage-format algorithm.
        assert_eq!(golden.bits, [158, 37, 100, 240, 129, 3, 3, 242]);
        for bits in [1, 10, 30] {
            let mut direct = Bloom::new(1000, bits);
            let mut retained = Bloom::new(1000, bits);
            for n in 0u64..1000 {
                let key = n.to_be_bytes();
                direct.insert(&key);
                retained.insert_hash(hash(&key));
            }
            assert_eq!(direct.bits, retained.bits);
            for n in 0u64..1000 {
                assert!(retained.contains(&n.to_be_bytes()));
            }
        }
    }
    #[test]
    fn rebuilding_respects_half_size_boundary_minimum_and_cap() {
        let bloom = Bloom::new(160, 10); // 200 bytes
        assert!(!bloom.needs_rebuild(80, 10)); // exactly half
        assert!(bloom.needs_rebuild(79, 10)); // rounded up to 99, below half
        assert!(bloom.needs_rebuild(0, 10));
        assert!(!Bloom::new(0, 10).needs_rebuild(0, 10));
        assert_eq!(Bloom::byte_len(0, 10), 8);
        assert_eq!(Bloom::byte_len(u64::MAX, 30), 8 * 1024 * 1024);
    }
    #[test]
    fn no_false_negatives_and_bounded_false_positives() {
        let mut b = Bloom::new(10000, 10);
        for i in 0u64..10000 {
            b.insert(&i.to_le_bytes());
        }
        for i in 0u64..10000 {
            assert!(b.contains(&i.to_le_bytes()));
        }
        let fp = (10000u64..20000)
            .filter(|i| b.contains(&i.to_le_bytes()))
            .count();
        assert!(fp < 300, "false positives: {fp}");
        let mut bytes = vec![];
        b.encode(&mut bytes);
        let round = Bloom::decode(&mut Cursor { b: &bytes }).unwrap();
        for i in 0u64..10000 {
            assert!(round.contains(&i.to_le_bytes()));
        }
    }
}

#[cfg(test)]
#[path = "bloom_profile.rs"]
mod profile;
