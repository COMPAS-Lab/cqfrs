#![allow(dead_code)]

use fastrand::Rng;

pub fn slots_threshold(logn_slots: impl Into<u64>, fill_factor: impl Into<f64>) -> usize {
    ((1u64 << logn_slots.into()) as f64 * fill_factor.into()) as usize
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
