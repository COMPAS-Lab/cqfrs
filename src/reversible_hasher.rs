use std::hash::{BuildHasher, Hasher};

use crate::utils::saturating_bitmask;

/// Implements reversable hash function for values less than HASH_BITS (max 64 bits).
#[derive(Clone, Copy, Default)]
pub struct ReversibleHasher<const HASH_BITS: u64> {
    hash: u64,
    // hash_bits: u64,
}

impl<const HASH_BITS: u64> ReversibleHasher<HASH_BITS> {
    const HASH_MASK: u64 = saturating_bitmask(HASH_BITS);
    fn new() -> Self {
        ReversibleHasher { hash: 0 }
    }

    pub fn invert_hash(hash: u64) -> u64 {
        let mut tmp: u64;
        let mut key = hash;

        // Invert key = key + (key << 31)
        tmp = key.wrapping_sub(key << 31);
        key = (key.wrapping_sub(tmp << 31)) & Self::HASH_MASK;

        // Invert key = key ^ (key >> 28)
        tmp = key ^ key >> 28;
        key = key ^ tmp >> 28;

        // Invert key *= 21
        key = (key.wrapping_mul(14933078535860113213)) & Self::HASH_MASK;

        // Invert key = key ^ (key >> 14)
        tmp = key ^ key >> 14;
        tmp = key ^ tmp >> 14;
        tmp = key ^ tmp >> 14;
        key = key ^ tmp >> 14;

        // Invert key *= 265
        key = (key.wrapping_mul(15244667743933553977)) & Self::HASH_MASK;

        // Invert key = key ^ (key >> 24)
        tmp = key ^ key >> 24;
        key = key ^ tmp >> 24;

        // Invert key = (~key) + (key << 21)
        tmp = !key;
        tmp = !(key.wrapping_sub(tmp << 21));
        tmp = !(key.wrapping_sub(tmp << 21));
        key = (!(key.wrapping_sub(tmp << 21))) & Self::HASH_MASK;

        key
    }
}

impl<const HASH_BITS: u64> Hasher for ReversibleHasher<HASH_BITS> {
    fn finish(&self) -> u64 {
        let mut key = self.hash;
        key = ((!key).wrapping_add(key << 21)) & Self::HASH_MASK; // key = (key << 21) - key - 1;
        key = key ^ (key >> 24);
        key = ((key.wrapping_add(key << 3)).wrapping_add(key << 8)) & Self::HASH_MASK; // key * 265
        key = key ^ (key >> 14);
        key = ((key.wrapping_add(key << 2)).wrapping_add(key << 4)) & Self::HASH_MASK; // key * 21
        key = key ^ (key >> 28);
        key = key.wrapping_add(key << 31) & Self::HASH_MASK;
        key
    }

    fn write(&mut self, bytes: &[u8]) {
        for byte in bytes.iter().rev() {
            self.hash <<= 8;
            self.hash |= *byte as u64;
        }
    }
}

#[derive(Clone, Copy, Default)]
pub struct BuildReversibleHasher<const HASH_BITS: u64>;

impl<const HASH_BITS: u64> BuildHasher for BuildReversibleHasher<HASH_BITS> {
    type Hasher = ReversibleHasher<HASH_BITS>;

    fn build_hasher(&self) -> Self::Hasher {
        ReversibleHasher::new()
    }
}
