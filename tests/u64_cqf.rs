mod common;

use common::{slots_threshold, test_init};
use cqfrs::{BuildReversibleHasher, CountingQuotientFilter, ReversibleHasher, U64Cqf};
use hashbrown::HashMap;

#[test]
fn consuming_iter() {
    const LOGN_SLOTS: u64 = 29;
    let elements = test_init(slots_threshold(LOGN_SLOTS, 0.9), u64::MAX);
    let mut cqf = U64Cqf::new(LOGN_SLOTS, 46, true, BuildReversibleHasher::<46>::default())
        .expect("failed to make cqf");

    let mut temp: HashMap<u64, u64> = HashMap::new();
    for i in 0..elements.len() {
        cqf.insert(elements[i], 1).expect("insert failed!");
        temp.insert(elements[i], temp.get(&elements[i]).unwrap_or(&0) + 1);
    }

    for (&k, &v) in temp.iter() {
        let count = cqf.query(k);
        assert_eq!(count.0, v);
    }

    for (c, h) in cqf.into_iter() {
        let og = ReversibleHasher::<46>::invert_hash(h);
        let count = temp.get(&og).unwrap();
        assert_eq!(count, &c);
    }
}

#[test]
fn ref_iter() {
    const LOGN_SLOTS: u64 = 29;
    let elements = test_init(slots_threshold(LOGN_SLOTS, 0.9), u64::MAX);
    let mut cqf = U64Cqf::new(LOGN_SLOTS, 46, true, BuildReversibleHasher::<46>::default())
        .expect("failed to make cqf");

    let mut temp: HashMap<u64, u64> = HashMap::new();
    for i in 0..elements.len() {
        cqf.insert(elements[i], 1).expect("insert failed!");
        temp.insert(elements[i], temp.get(&elements[i]).unwrap_or(&0) + 1);
    }

    for (&k, &v) in temp.iter() {
        let count = cqf.query(k);
        assert_eq!(count.0, v);
    }

    for (c, h) in cqf.iter() {
        let og = ReversibleHasher::<46>::invert_hash(h);
        let count = temp.get(&og).unwrap();
        assert_eq!(count, &c);
    }
}
