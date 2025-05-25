mod common;

use std::io::Write;

use common::{map_merge, slots_threshold, test_init, test_init_map};
use cqfrs::{
    BuildReversibleHasher, CountingQuotientFilter, CqfMerge, EitherOrBoth, ReversibleHasher,
    U32Cqf, U64Cqf, ZippedCqfIter,
};
use dashmap::DashSet;
use hashbrown::HashMap;
use rayon::iter::{FromParallelIterator, IntoParallelIterator, ParallelIterator};
use rayon::slice::ParallelSliceMut;

#[test]
fn simple_insert() {
    const LOGN_SLOTS: u64 = 22;

    const HASH_BITS: u64 = 46;
    const TEST_MASK: u64 = (1 << HASH_BITS) - 1;

    const NUM_ELEMENTS: usize = ((1 << (LOGN_SLOTS)) as f32 * 0.94) as usize;
    // let num_elemnts: usize = ((1 << (LOGN_SLOTS-3)) as f32 * 0.90) as usize;
    dbg!(NUM_ELEMENTS);

    let numbers: Vec<_> = test_init(NUM_ELEMENTS, TEST_MASK)
        .iter()
        .map(|n| n % 10000)
        .collect();

    let mut cqf1 = U32Cqf::new(
        LOGN_SLOTS,
        HASH_BITS,
        true,
        BuildReversibleHasher::<HASH_BITS>,
    )
    .expect("failed to make cqf");

    // let mut cqf2 = U32Cqf::new(
    //     LOGN_SLOTS,
    //     HASH_BITS,
    //     true,
    //     BuildReversableHasher::<46>::default(),
    // )
    // .expect("failed to make cqf");

    // let mut cqf3 = U32Cqf::new(
    //     LOGN_SLOTS + 1,
    //     HASH_BITS,
    //     true,
    //     BuildReversableHasher::<46>::default(),
    // )
    // .expect("failed to make cqf");

    // let mut cqf = OldCqf::new(LOGN_SLOTS, HASH_BITS, true, BuildReversableHasher::<46>::default())

    let mut temp = HashMap::new();

    for num in numbers.iter() {
        *temp.entry(num & TEST_MASK).or_insert(0) += 1;
    }
    println!(
        "Map built (max count = {}, min count = {})",
        temp.values().max().unwrap(),
        temp.values().min().unwrap()
    );

    eprintln!("Starting insert");
    let now = std::time::Instant::now();
    for i in 0..NUM_ELEMENTS / 2 {
        cqf1.insert(numbers[i] & TEST_MASK, 1)
            .expect("insert failed!");
    }
    for i in NUM_ELEMENTS / 2..NUM_ELEMENTS {
        cqf1.insert(numbers[i] & TEST_MASK, 1)
            .expect("insert failed!");
    }
    let elapsed = now.elapsed();
    eprintln!(
        "Insert took {:?} ({:?} per iter)",
        elapsed,
        elapsed / NUM_ELEMENTS as u32
    );

    let bytes = cqf1.serialize_to_bytes();
    let temp_file = std::fs::File::create("temp.qf").expect("failed to create file");
    let mut writer = std::io::BufWriter::new(temp_file);
    writer.write_all(bytes).expect("failed to write to file");
    writer.flush().expect("failed to flush file");

    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open("temp.qf")
        .expect("failed to open file");

    let cqf1 =
        U32Cqf::open_file(BuildReversibleHasher::<HASH_BITS>, file).expect("failed to make cqf");

    // CqfMerge::merge(cqf1.into_iter(), cqf2.into_iter(), &mut cqf3);

    eprintln!("Starting query");
    let now = std::time::Instant::now();
    for (&k, &v) in temp.iter() {
        let count = cqf1.query(k);
        assert_eq!(count.0, v);
    }
    let elapsed = now.elapsed();
    eprintln!(
        "Query took {:?} ({:?} per iter)",
        elapsed,
        elapsed / temp.len() as u32
    );

    eprintln!("Starting iter");
    let now = std::time::Instant::now();
    let mut items = 0;
    for (c, h) in cqf1.into_iter() {
        items += 1;
        let og = ReversibleHasher::<HASH_BITS>::invert_hash(h);
        let count = temp.get(&og).unwrap();
        assert_eq!(count, &c);
    }
    assert_eq!(items, temp.len());
    let elapsed = now.elapsed();
    eprintln!(
        "Iter took {:?} ({:?} per iter)",
        elapsed,
        elapsed / temp.len() as u32
    );
}

#[test]
fn simple_merge() {
    const LOGN_SLOTS: u64 = 26;
    const HASH_BITS: u64 = 46;

    const NUM_ELEMENTS_1: usize = slots_threshold(LOGN_SLOTS, 0.1);
    const NUM_ELEMENTS_2: usize = slots_threshold(LOGN_SLOTS, 0.01);

    eprintln!("building maps");
    let mut elements_1 = test_init_map(NUM_ELEMENTS_1, 2);
    let elements_2 = test_init_map(NUM_ELEMENTS_2, 2);
    eprintln!("maps built");
    dbg!(elements_1.len(), elements_2.len());

    let mut cqf1 = U32Cqf::new(
        LOGN_SLOTS,
        HASH_BITS,
        true,
        BuildReversibleHasher::<HASH_BITS>,
    )
    .expect("failed to make cqf");

    let mut cqf2 = U32Cqf::new(
        LOGN_SLOTS,
        HASH_BITS,
        true,
        BuildReversibleHasher::<HASH_BITS>,
    )
    .expect("failed to make cqf");

    eprintln!("Starting insert 1");
    let now = std::time::Instant::now();
    for (&k, &v) in elements_1.iter() {
        cqf1.insert(k, v).expect("insert failed!");
    }
    let elapsed = now.elapsed();
    eprintln!(
        "Insert 1 took {:?} ({:?} per iter)",
        elapsed,
        elapsed / NUM_ELEMENTS_1 as u32
    );

    eprintln!("Starting insert 2");
    let now = std::time::Instant::now();

    for (&k, &v) in elements_2.iter() {
        cqf2.insert(k, v).expect("insert failed!");
    }
    let elapsed = now.elapsed();
    eprintln!(
        "Insert 2 took {:?} ({:?} per iter)",
        elapsed,
        elapsed / NUM_ELEMENTS_2 as u32
    );

    // doesn't work with LOGN_SLOTS + 1, but seems like it should
    let mut cqf3 =
        U32Cqf::new(32, 64, true, BuildReversibleHasher::<HASH_BITS>).expect("failed to make cqf");

    eprintln!("Starting merge");
    let now = std::time::Instant::now();
    CqfMerge::merge(cqf1.into_iter(), cqf2.into_iter(), &mut cqf3);
    let elapsed = now.elapsed();
    eprintln!(
        "Merge took {:?} ({:?} per iter)",
        elapsed,
        elapsed / (NUM_ELEMENTS_1 + NUM_ELEMENTS_2) as u32
    );

    map_merge(&mut elements_1, elements_2);

    for (count, hash) in cqf3.iter() {
        let og = ReversibleHasher::<HASH_BITS>::invert_hash(hash);
        let v = elements_1.get(&og).expect(&format!("{} not found", og));
        assert_eq!(count, *v);
    }

    for (&k, &v) in elements_1.iter() {
        let (count, _) = cqf3.query(k);
        assert_eq!(
            count, v,
            "mismatch for key {}; cqf: {} != map: {}",
            k, count, v
        );
    }
}

#[test]
fn simple_zip_iter() {
    const LOGN_SLOTS: u64 = 26;
    const HASH_BITS: u64 = 46;

    const NUM_ELEMENTS_1: usize = slots_threshold(LOGN_SLOTS, 0.2);
    const NUM_ELEMENTS_2: usize = slots_threshold(LOGN_SLOTS, 0.01);

    eprintln!("building maps");
    let elements_1 = test_init_map(NUM_ELEMENTS_1, NUM_ELEMENTS_1 as u64);
    let elements_2 = test_init_map(NUM_ELEMENTS_2, NUM_ELEMENTS_1 as u64);
    eprintln!("maps built");
    dbg!(elements_1.len(), elements_2.len());

    let mut cqf1 = U32Cqf::new_file(
        LOGN_SLOTS,
        HASH_BITS,
        true,
        BuildReversibleHasher::<HASH_BITS>,
        tempfile::tempfile().unwrap(),
    )
    .expect("failed to make cqf");

    let mut cqf2 = U64Cqf::new_file(
        LOGN_SLOTS,
        HASH_BITS,
        true,
        BuildReversibleHasher::<HASH_BITS>,
        tempfile::tempfile().unwrap(),
    )
    .expect("failed to make cqf");

    eprintln!("Starting insert 1");
    let now = std::time::Instant::now();
    for (&k, &v) in elements_1.iter() {
        cqf1.insert(k, v).expect("insert failed!");
    }
    let elapsed = now.elapsed();
    eprintln!(
        "Insert 1 took {:?} ({:?} per iter)",
        elapsed,
        elapsed / NUM_ELEMENTS_1 as u32
    );

    eprintln!("Starting insert 2");
    let now = std::time::Instant::now();
    for (&k, &v) in elements_2.iter() {
        cqf2.insert(k, v).expect("insert failed!");
    }
    let elapsed = now.elapsed();
    eprintln!(
        "Insert 2 took {:?} ({:?} per iter)",
        elapsed,
        elapsed / NUM_ELEMENTS_2 as u32
    );

    // let mut pairs_set = BTreeSet::<(u32, u64)>::default();
    // let mut c1_set = BTreeSet::<u32>::new();
    // let mut c2_set = BTreeSet::<u64>::default();
    // let mut num_pairs = 0;
    let pairs_set = DashSet::<(u32, u64)>::default();
    let c1_set = DashSet::<u32>::default();
    let c2_set = DashSet::<u64>::default();

    eprintln!("Starting zipped iter");
    let now = std::time::Instant::now();
    let pairs = ZippedCqfIter::new(cqf1.iter(), cqf2.iter()).collect::<Vec<_>>();
    let elapsed = now.elapsed();
    eprintln!(
        "Zipped iter collect took {:?} ({:?} per iter)",
        elapsed,
        elapsed / (NUM_ELEMENTS_1 + NUM_ELEMENTS_2) as u32
    );

    let now = std::time::Instant::now();
    pairs.into_par_iter().for_each(|pair| match pair {
        EitherOrBoth::Left((c1, _)) => {
            c1_set.insert(c1 as u32);
        }
        EitherOrBoth::Right((c2, _)) => {
            c2_set.insert(c2);
        }
        EitherOrBoth::Both((c1, _), (c2, _)) => {
            pairs_set.insert((c1 as u32, c2));
        }
    });

    let mut pairs_set: Vec<_> = Vec::from_par_iter(pairs_set);
    pairs_set.par_sort_unstable();
    let mut c1_set: Vec<_> = Vec::from_par_iter(c1_set);
    c1_set.par_sort_unstable();
    let mut c2_set: Vec<_> = Vec::from_par_iter(c2_set);
    c2_set.par_sort_unstable();

    let elapsed = now.elapsed();
    eprintln!(
        "Build pairs took {:?} ({:?} per iter)",
        elapsed,
        elapsed / (NUM_ELEMENTS_1 + NUM_ELEMENTS_2) as u32
    );
    dbg!(pairs_set.len(), c1_set.len(), c2_set.len());
}

#[test]
fn iter_empty() {
    let cqf = U32Cqf::new(20, 46, true, BuildReversibleHasher::<46>).expect("failed to make cqf");

    for (c, h) in cqf.iter() {
        assert_eq!(c, 0);
        assert_eq!(h, 0);
    }

    assert_eq!(cqf.occupied_slots(), 0);

    for (c, h) in cqf.into_iter() {
        assert_eq!(c, 0);
        assert_eq!(h, 0);
    }
}

#[test]
fn consuming_iter() {
    const LOGN_SLOTS: u64 = 32;

    let elements = test_init(slots_threshold(LOGN_SLOTS, 0.9), u64::MAX);
    dbg!(elements.len());
    let mut cqf = U32Cqf::new(LOGN_SLOTS, 64, true, BuildReversibleHasher::<46>::default())
        .expect("failed to make cqf");

    let mut temp: HashMap<u64, u64> = HashMap::new();
    for (i, el) in elements.iter().copied().enumerate() {
        cqf.insert(el, 1)
            .inspect_err(|e| {
                eprintln!("error at {}/{} inserts: {:?}", i, elements.len(), e);
            })
            .expect("insert failed!");
        *temp.entry(el).or_insert(0) += 1;
    }

    for (&k, &v) in temp.iter() {
        let (count, _hash) = cqf.query(k);
        assert_eq!(count, v);
    }

    for (c, h) in cqf.into_iter() {
        let og = ReversibleHasher::<46>::invert_hash(h);
        let count = temp.get(&og).unwrap();
        assert_eq!(count, &c);
    }
}

#[test]
fn ref_iter() {
    const LOGN_SLOTS: u64 = 32;

    let elements = test_init(slots_threshold(LOGN_SLOTS, 0.9), u64::MAX);
    let mut cqf = U32Cqf::new(LOGN_SLOTS, 64, true, BuildReversibleHasher::<46>::default())
        .expect("failed to make cqf");

    let mut temp: HashMap<u64, u64> = HashMap::new();
    for (i, el) in elements.iter().copied().enumerate() {
        cqf.insert(el, 1)
            .inspect_err(|e| {
                eprintln!("error at {}/{} inserts: {:?}", i, elements.len(), e);
            })
            .expect("insert failed!");
        *temp.entry(el).or_insert(0) += 1;
    }

    for (&k, &v) in temp.iter() {
        let (count, _hash) = cqf.query(k);
        assert_eq!(count, v);
    }

    for (c, h) in cqf.iter() {
        let og = ReversibleHasher::<46>::invert_hash(h);
        let count = temp.get(&og).unwrap();
        assert_eq!(count, &c);
    }
}

// fn main() {
//     let n_strings: usize = ((1 << (LOGN_SLOTS-1)) as f32 * 0.9) as usize;
//     // let n_strings: usize = (1 << 20) as usize;
//     let mut numbers: Vec<u64> = Vec::with_capacity(n_strings);

//     let mut rng = rand::thread_rng();
//     for _ in 0..n_strings {
//         numbers.push(rng.gen())
//     }

//     let mut qf = CountingQuotientFilter::new(
//         LOGN_SLOTS,
//         64,
//         true,
//         BuildReversableHasher::<HASH_BITS>::default(),
//     )
//     .unwrap();

//     let mut qf2 = CountingQuotientFilter::new(
//         LOGN_SLOTS,
//         64,
//         true,
//         BuildReversableHasher::<HASH_BITS>::default(),
//     )
//     .unwrap();

//     // println!("Done with first iter");

//     let now = Instant::now();

//     for i in 0..n_strings / 2 {
//         //qf.insert(strings[i].as_bytes(), 3)?;
//         // println!("inserting {}", numbers[i]);
//         qf.insert(numbers[i] % 1000 as u64, 1)
//             .expect("insert failed!");
//     }

//     for i in (n_strings / 2)..n_strings {
//         qf2.insert(numbers[i] % 1000 as u64, 1)
//             .expect("insert failed!");
//     }

//     let mut temp1 = HashMap::new();
//     let mut temp2 = HashMap::new();

//     for i in qf.into_iter() {
//         temp1.insert(i.hash, i.count);
//     }

//     for i in qf2.into_iter() {
//         temp2.insert(i.hash, i.count);
//     }

//     let qf3 = CountingQuotientFilter::merge(&qf, &qf2).unwrap();

//     for i in qf3.into_iter() {
//         let count = temp1.get(&i.hash).unwrap_or(&0) + temp2.get(&i.hash).unwrap_or(&0);
//         assert_eq!(count, i.count);
//     }

//     // let fin = Instant::now();

//     println!("Time to fin: {:?}", now.elapsed());
// }
