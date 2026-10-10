use crate::{codec::*, error::corrupt, Result};

pub(crate) struct Bloom {
    bits: Vec<u8>,
    probes: u32,
}
// Bloom format depends on this hash; bump the version if it changes.
fn hash(key: &[u8]) -> (u64, u64) {
    let mut h = 0xcbf29ce484222325u64;
    for b in key {
        h ^= *b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    h ^= h >> 33;
    h = h.wrapping_mul(0xff51afd7ed558ccd);
    h ^= h >> 33;
    (h, h.rotate_left(31).wrapping_mul(0xc4ceb9fe1a85ec53) | 1)
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
    pub fn insert(&mut self, key: &[u8]) {
        let (mut h, delta) = hash(key);
        let n = self.bits.len() as u64 * 8;
        for _ in 0..self.probes {
            let bit = (h % n) as usize;
            self.bits[bit / 8] |= 1 << (bit % 8);
            h = h.wrapping_add(delta);
        }
    }
    pub fn contains(&self, key: &[u8]) -> bool {
        let (mut h, delta) = hash(key);
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
