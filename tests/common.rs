#![allow(dead_code)]

use fastrand::Rng;
use hashbrown::HashMap;

pub(crate) const fn slots_threshold(logn_slots: u64, fill_factor: f64) -> usize {
    ((1u64 << logn_slots) as f64 * fill_factor) as usize
}

pub(crate) fn test_init(num_elements: impl Into<usize>, hash_mask: u64) -> Vec<u64> {
    let num_elements = num_elements.into();
    let mut numbers: Vec<u64> = Vec::with_capacity(num_elements);
    let mut randgen = Rng::new();
    for _ in 0..num_elements {
        let num: u64 = randgen.u64(..hash_mask);
        numbers.push(num & hash_mask);
    }
    numbers
}

pub(crate) fn test_init_map(max_key: usize, max_count: u64) -> HashMap<u64, u64> {
    let mut numbers: HashMap<u64, u64> = HashMap::with_capacity(max_key);
    let mut randgen = Rng::new();
    for k in 0..max_key {
        if randgen.bool() && randgen.bool() {
            continue;
        }
        let value: u64 = randgen.u64(1..max_count);
        numbers.insert(k as u64, value);
    }
    numbers
}

/// merge src into dst, summing existing values
pub(crate) fn map_merge(dst: &mut HashMap<u64, u64>, src: HashMap<u64, u64>) {
    for (k, v) in src.into_iter() {
        dst.entry(k).and_modify(|c| *c += v).or_insert(v);
    }
}
