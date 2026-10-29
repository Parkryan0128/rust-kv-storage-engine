use crate::{codec::*, error::corrupt, Result};

pub(crate) struct Bloom {
    bits: Box<[u8]>,
    probes: u32,
    // Filters have a fixed byte length; no spare Vec capacity is retained.
    // The format caps the bit count at 2^26, so u32 preserves every value.
    // Reciprocal is runtime-only; persisted positions still use h % bit_count.
    bit_count: u32,
    reciprocal: u64,
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
        let len = Self::byte_len(count, bits_per_key);
        let bit_count = (len * 8) as u32;
        Self {
            bits: vec![0; len].into_boxed_slice(),
            probes: ((bits_per_key as f64 * 0.69) as u32).clamp(1, 20),
            bit_count,
            reciprocal: u64::MAX / u64::from(bit_count) + u64::from(bit_count.is_power_of_two()),
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
        for _ in 0..self.probes {
            let bit = self.bit_position(h);
            // bit_count is a multiple of eight, so reduction preserves the
            // low three hash bits. Keep the mask independent of the quotient.
            self.bits[bit / 8] |= 1 << (h & 7);
            h = h.wrapping_add(delta);
        }
    }
    #[cfg(test)]
    pub fn contains(&self, key: &[u8]) -> bool {
        self.contains_hash(hash(key))
    }
    #[inline]
    pub fn contains_hash(&self, mut h: u64) -> bool {
        let delta = delta(h);
        for _ in 0..self.probes {
            // Keep the read probe compact: reciprocal reduction wins isolated
            // arithmetic tests but regresses cache-resident public Get calls.
            // Insertions retain it; both paths preserve persisted positions.
            let bit = (h % u64::from(self.bit_count)) as usize;
            if self.bits[bit / 8] & (1 << (h & 7)) == 0 {
                return false;
            }
            h = h.wrapping_add(delta);
        }
        true
    }
    #[inline]
    fn bit_position(&self, hash: u64) -> usize {
        // floor(2^64 / n) gives a quotient at most one below floor(hash / n).
        // Thus the subtraction cannot overflow, and one correction is exact.
        // n is in 64..=2^26, so the reciprocal always fits in u64.
        let bit_count = u64::from(self.bit_count);
        let quotient = ((u128::from(hash) * u128::from(self.reciprocal)) >> 64) as u64;
        let remainder = hash - quotient * bit_count;
        (if remainder >= bit_count {
            remainder - bit_count
        } else {
            remainder
        }) as usize
    }
    pub fn encode(&self, out: &mut Vec<u8>) {
        put_u32(out, self.probes);
        put_u32(out, self.bits.len() as u32);
        out.extend_from_slice(&self.bits);
    }
    pub fn decode(c: &mut Cursor<'_>) -> Result<Self> {
        let probes = c.u32()?;
        let len = c.u32()? as usize;
        if !(1..=20).contains(&probes) || !(8..=8 * 1024 * 1024).contains(&len) {
            return Err(corrupt("invalid Bloom filter"));
        }
        let bit_count = (len * 8) as u32;
        Ok(Self {
            bits: c.take(len)?.into(),
            probes,
            bit_count,
            reciprocal: u64::MAX / u64::from(bit_count) + u64::from(bit_count.is_power_of_two()),
        })
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn reciprocal_positions_match_remainder_and_persisted_filter_bytes() {
        let mut state = 0xace123u64;
        for bytes in [
            8usize, 9, 15, 16, 17, 31, 32, 33, 127, 128, 129, 1000, 125000, 8388607, 8388608,
        ] {
            let bit_count = bytes as u64 * 8;
            // Avoid allocating the largest bitsets just to check the arithmetic.
            let bloom = Bloom {
                bits: Box::default(),
                probes: 6,
                bit_count: bit_count as u32,
                reciprocal: u64::MAX / bit_count + u64::from(bit_count.is_power_of_two()),
            };
            for hash in [
                0,
                1,
                bit_count - 1,
                bit_count,
                bit_count + 1,
                u64::MAX / 2,
                u64::MAX - 1,
                u64::MAX,
            ] {
                assert_eq!(bloom.bit_position(hash) as u64, hash % bit_count);
            }
            for _ in 0..10_000 {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                assert_eq!(bloom.bit_position(state) as u64, state % bit_count);
            }
        }
        for count in [0, 1, 7, 17, 63, 64, 65, 1000, 10001] {
            for probes in [1, 6, 20] {
                let mut bloom = Bloom::new(count, 10);
                bloom.probes = probes;
                let mut reference = vec![0u8; bloom.bits.len()];
                for key in 0..512u64 {
                    let hash = hash(&key.to_be_bytes());
                    bloom.insert_hash(hash);
                    let delta = delta(hash);
                    let mut h = hash;
                    for _ in 0..probes {
                        let bit = (h % (reference.len() as u64 * 8)) as usize;
                        reference[bit / 8] |= 1 << (bit % 8);
                        h = h.wrapping_add(delta);
                    }
                }
                assert_eq!(bloom.bits.as_ref(), reference.as_slice());
                let mut encoded = Vec::new();
                bloom.encode(&mut encoded);
                let decoded = Bloom::decode(&mut Cursor { b: &encoded }).unwrap();
                for key in 0..1024u64 {
                    let h = hash(&key.to_be_bytes());
                    // Compare both positive and negative probes with the
                    // original format algorithm, independently of reduction.
                    let step = delta(h);
                    let mut probe = h;
                    let expected = (0..probes).all(|_| {
                        let bit = (probe % (reference.len() as u64 * 8)) as usize;
                        probe = probe.wrapping_add(step);
                        reference[bit / 8] & (1 << (bit % 8)) != 0
                    });
                    assert_eq!(bloom.contains_hash(h), expected);
                    assert_eq!(decoded.contains_hash(h), bloom.contains_hash(h));
                    if key < 512 {
                        assert!(decoded.contains_hash(h));
                    }
                }
            }
        }
    }

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
        assert_eq!(golden.bits.as_ref(), &[158, 37, 100, 240, 129, 3, 3, 242]);
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

#[cfg(test)]
#[path = "bloom_reduction_profile.rs"]
mod reduction_profile;
