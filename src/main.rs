#[allow(unused_imports)]
// use cqfrs::{BuildReversableHasher, CountingQuotientFilter, ReversibleHasher};
use cqfrs::*;
use rand::Rng;
use std::{collections::HashMap, time::Duration};

const LOGN_SLOTS: u64 = 26;

const HASH_BITS: u64 = 46;
const TEST_MASK: u64 = (1 << HASH_BITS) - 1;

fn main() {
    let num_elements: usize = ((1 << (LOGN_SLOTS)) as f32 * 0.94) as usize;
    // let num_elemnts: usize = ((1 << (LOGN_SLOTS-3)) as f32 * 0.90) as usize;
    let numbers = test_init(num_elements, TEST_MASK);

    let mut cqf1 = U32Cqf::new(
        LOGN_SLOTS,
        HASH_BITS,
        true,
        BuildReversableHasher::<46>::default(),
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

    for i in 0..num_elements {
        let count = temp.get(&(numbers[i] & TEST_MASK)).unwrap_or(&0) + 1;
        temp.insert(numbers[i] & TEST_MASK, count);
    }
    println!("Map built");
    // std::thread::sleep(Duration::from_secs(5));

    println!("Starting insert");
    let now = std::time::Instant::now();
    for i in 0..num_elements / 2 {
        cqf1.insert(numbers[i] & TEST_MASK, 1)
            .expect("insert failed!");
    }
    for i in num_elements / 2..num_elements {
        cqf1.insert(numbers[i] & TEST_MASK, 1)
            .expect("insert failed!");
    }
    println!("Insert took {:?}", now.elapsed());

    use std::io::Write;
    let bytes = cqf1.serialize_to_bytes();
    let temp_file = std::fs::File::create("temp.qf").expect("failed to create file");
    let mut writer = std::io::BufWriter::new(temp_file);
    writer.write_all(&bytes).expect("failed to write to file");
    writer.flush().expect("failed to flush file");

    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open("temp.qf")
        .expect("failed to open file");

    let cqf1 = U32Cqf::open_file(BuildReversableHasher::<46>::default(), file)
        .expect("failed to make cqf");

    // CqfMerge::merge(cqf1.into_iter(), cqf2.into_iter(), &mut cqf3);

    for (&k, &v) in temp.iter() {
        let count = cqf1.query(k);
        assert_eq!(count.0, v);
    }

    let now = std::time::Instant::now();
    println!("Starting iter");
    for (c, h) in cqf1.into_iter() {
        let og = ReversibleHasher::<46>::invert_hash(h);
        let count = temp.get(&og).unwrap();
        assert_eq!(count, &c);
    }
    println!("Iter took {:?}", now.elapsed());
}

fn test_init(num_elements: usize, hash_mask: u64) -> Vec<u64> {
    let num_elements = num_elements as usize;
    let mut numbers: Vec<u64> = Vec::with_capacity(num_elements);
    let mut randgen = rand::thread_rng();
    for _ in 0..num_elements {
        let num: u64 = randgen.gen();
        numbers.push((num & hash_mask) % 10000);
    }
    numbers
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_init(lognslots: u64, hash_mask: u64) -> Vec<u64> {
        let num_elemnts: usize = ((1u64 << (lognslots)) as f64 * 0.90) as usize;
        let mut numbers: Vec<u64> = Vec::with_capacity(num_elemnts);
        let mut randgen = rand::thread_rng();
        for _ in 0..num_elemnts {
            let num: u64 = randgen.gen();
            numbers.push(num & hash_mask);
        }
        numbers
    }

    #[test]
    fn test_u64_con_iter() {
        const LOGN_SLOTS: u64 = 29;
        let elements = test_init(LOGN_SLOTS, u64::MAX);
        let mut cqf = U64Cqf::new(LOGN_SLOTS, 64, true, BuildReversableHasher::<46>::default())
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
    fn test_u64_ref_iter() {
        const LOGN_SLOTS: u64 = 29;
        let elements = test_init(LOGN_SLOTS, u64::MAX);
        let mut cqf = U64Cqf::new(LOGN_SLOTS, 64, true, BuildReversableHasher::<46>::default())
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

    #[test]
    fn test_u32_con_iter() {
        const LOGN_SLOTS: u64 = 32;
        let elements = test_init(LOGN_SLOTS, u64::MAX);
        let mut cqf = U32Cqf::new(LOGN_SLOTS, 64, true, BuildReversableHasher::<46>::default())
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
    fn test_u32_ref_iter() {
        const LOGN_SLOTS: u64 = 32;
        let elements = test_init(LOGN_SLOTS, u64::MAX);
        let mut cqf = U32Cqf::new(LOGN_SLOTS, 64, true, BuildReversableHasher::<46>::default())
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

//     // let n_strings: usize = ((1 << 10)) as usize;
//     // let mut numbers: Vec<u64> = Vec::with_capacity(n_strings);

//     // println!("{:?}", numbers.clone());
//     // println!("{:?}", numbers.clone());

//     // let now = Instant::now();
//     // for i in 0..n_strings / 2 {
//     //     //qf.insert(strings[i].as_bytes(), 3)?;
//     //     // println!("inserting {}", numbers[i]);
//     //     qf.insert(numbers[i] as u64, 1).expect("insert failed!");
//     //     // qf2.insert(numbers[i] as u64, 1).expect("insert failed!");
//     // }

//     // for i in (n_strings / 2)..n_strings {
//     //     //qf.insert(strings[i].as_bytes(), 3)?;
//     //     // println!("inserting {}", numbers[i]);
//     //     qf2.insert(numbers[i] as u64, 1).expect("insert failed!");
//     // }
//     // println!("inserted {} elements", n_strings);

//     // let now = Instant::now();
//     // let qf3 = CountingQuotientFilter::merge(&qf, &qf2).unwrap();
//     // println!("new merge took {}", now.elapsed().as_secs());

//     // let mut count = 0;
//     // for item in qf3.into_iter() {
//     //     count += item.count;
//     // }
//     // println!("count is {}", count);
//     // drop(qf3);

//     // let now = Instant::now();
//     // for v in qf.into_iter() {
//     //     qf2.insert_by_hash(v.hash, v.count).expect("insert failed!");
//     // }
//     // println!("old merge took {}", now.elapsed().as_secs());

//     // let mut count = 0;
//     // for item in qf2.into_iter() {
//     //     count += item.count;
//     // }
//     // println!("count is {}", count);

//     // // ////////////////////////////////////////////////
//     // // for (k, v) in uniques.iter() {
//     // //     let res = qf .query(*k);
//     // //     assert_eq!(res.count, *v);
//     // //     count += res.count;
//     // // }

//     // // for item in qf.into_iter() {
//     // //     count += item.count;
//     // //     // let res = qf.query_by_hash(item.hash);
//     // //     // let res2 = qf2.query_by_hash(item.hash);
//     // //     // assert_eq!(res + res2, *uniques.get(&ReversibleHasher::invert_hash(item.hash)).unwrap());
//     // //     // assert_eq!(res, item.count * 2);
//     // //     // assert_eq!(item.count, *uniques.get(&ReversibleHasher::invert_hash(item.hash)).unwrap() * 2);
//     // // }
//     // // println!("qf1");
//     // // qf.print();
//     // // println!("qf2");
//     // // qf2.print();
//     // // println!("qf3");
//     // // qf3.print();

//     // // return;

//     // let mut uniques: HashMap<u64, u64> = HashMap::new();
//     // for i in 0..n_strings {
//     //     uniques.insert(numbers[i], 1);
//     // }
//     // println!("count should be {}", n_strings);

//     // // qf.print();

//     // let elapsed = now.elapsed();
//     // println!("insert took {} seconds!", elapsed.as_secs());
// }

// // fn main() {
// //     //

// //     ///////////////////////////////////////
// //     // let num_threads = 6;
// //     // let numbers = Arc::new(numbers);
// //     // let qf = Arc::new(qf);

// //     // let mut handles = Vec::with_capacity(num_threads);

// //     // let now = Instant::now();
// //     // for i in 0..num_threads {
// //     //     let qf = qf.clone();
// //     //     let numbers = numbers.clone();
// //     //     handles.push(std::thread::spawn(move || {
// //     //         for j in (i*n_strings/num_threads)..((i+1)*n_strings/num_threads) {
// //     //             // println!("inserting {}", numbers[j]);
// //     //             qf.insert(numbers[j] as u64, 1).expect("insert failed!");
// //     //         }
// //     //     }));
// //     // }

// //     // for handle in handles {
// //     //     handle.join().unwrap();
// //     // }
// //     ////////////////////////////////////////////////
// //     // let inserts = Arc::new(AtomicI32::new(0));
// //     // numbers.par_iter().for_each(|&i| {
// //     //     qf.insert(i as u64, 1).expect("insert failed!");
// //     //     // inserts.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
// //     // });

// //     // println!("inserted {} elements", inserts.load(std::sync::atomic::Ordering::SeqCst));
// //     // println!("{n_strings}");
// //     // for i in 0..n_strings {
// //     //     //qf.insert(strings[i].as_bytes(), 3)?;
// //     //     qf.insert(numbers[i] as u64, 1).expect("insert failed!");
// //     // }

// //     // let elapsed = now.elapsed();
// //     // println!("insert took {} seconds!", elapsed.as_millis());

// //     // let now = Instant::now();

// //     // let qf = Arc::new(qf);

// //     // let elapsed = now.elapsed();
// //     // println!("insert took {} seconds!", elapsed.as_secs());

// //     ///////////////////////////////////////////////////////////////////////

// //     let mut qf =
// //         CountingQuotientFilter::new(25, 25, HashMode::Invertible).expect("failed to make cqf");

// //     let qf = CountingQuotientFilter::new_file(25, 25, HashMode::Invertible, "test.qf".into())
// //         .expect("failed to make cqf");
// //     // let qf = CountingQuotientFilter::open_file("test.qf".into()).expect("failed to make cqf");
// //     let n_strings: usize = 10_000_000;

// //     let mut numbers: Vec<u64> = Vec::with_capacity(n_strings);
// //     let mut rng = rand::thread_rng();

// //     for _ in 0..n_strings {
// //         numbers.push(rng.gen())
// //     }

// //     for i in 0..n_strings / 2 {
// //         //qf.insert(strings[i].as_bytes(), 3)?;
// //         match qf.insert(numbers[i], 3) {
// //             Err(_) => eprintln!("Error inserting"),
// //             _ => continue,
// //         };
// //     }
// //     let mut total_in = 0;
// //     for i in 0..n_strings / 2 {
// //         //assert!(qf.query(strings[i].as_bytes()) > 0, "false negative!");
// //         let p = qf.query(numbers[i]);
// //         total_in += p;
// //         assert!(qf.query(numbers[i]) > 0, "false negative!");
// //     }

// //     let mut present: u32 = 0;
// //     for i in n_strings / 2..n_strings {
// //         /*
// //         if qf.query(strings[i].as_bytes()) > 0 {
// //             present += 1;
// //         }
// //         */
// //         if qf.query(numbers[i]) > 0 {
// //             present += 1;
// //         }
// //     }
// //     assert_eq!(present, 0);
// //     println!("{} elements present", total_in);

// //     drop(qf);

// //     // MMAP LOAD TEST

// //     let qf = CountingQuotientFilter::open_file("test.qf".into()).expect("failed to make cqf");
// //     let n_strings: usize = 10_000_000;

// //     // let mut numbers: Vec<u64> = Vec::with_capacity(n_strings);
// //     // let mut rng = rand::thread_rng();

// //     // for _ in 0..n_strings {
// //     //     numbers.push(rng.gen())
// //     // }

// //     let mut total_in = 0;
// //     for i in 0..n_strings / 2 {
// //         //assert!(qf.query(strings[i].as_bytes()) > 0, "false negative!");
// //         let p = qf.query(numbers[i]);
// //         total_in += p;
// //         assert!(qf.query(numbers[i]) > 0, "false negative!");
// //     }

// //     let mut present: u32 = 0;
// //     for i in n_strings / 2..n_strings {
// //         /*
// //         if qf.query(strings[i].as_bytes()) > 0 {
// //             present += 1;
// //         }
// //         */
// //         if qf.query(numbers[i]) > 0 {
// //             present += 1;
// //         }
// //     }
// //     assert_eq!(present, 0);
// //     println!("{} elements present", total_in);

// //     // let qf = CountingQuotientFilter::new_file(25,25,HashMode::Invertible, "test.qf".into()).expect("failed to make cqf");
// // }

// // use cqfrs::{CountingQuotientFilter, HashMode};
// // use rand::Rng;
// // use rayon::prelude::*;
// // use std::fs;
// // use std::sync::{atomic::AtomicI32, Arc};
// // use std::time::Instant;

// // const LOGN_SLOTS: u64 = 23;
// // use xxhash_rust::xxh3::xxh3_64;
// // fn main() {
// //     // {
// //         if fs::metadata("test.qf").is_ok() {
// //             fs::remove_file("test.qf").expect("failed to remove file");
// //         }
// //         {
// //             let qf = CountingQuotientFilter::new_file(LOGN_SLOTS, LOGN_SLOTS, HashMode::Invertible, "test.qf".into()).unwrap();
// //         }
// //         let qf = CountingQuotientFilter::open_file("test.qf".into()).expect("failed to make cqf");
// //     // }
// //     // {
// //     //     let qf = CountingQuotientFilter::open_file("test.qf".into()).expect("failed to make cqf");
// //     // }
// //         // qf.clear();
// //     let n_strings: usize = ((1 << (LOGN_SLOTS)) as f32 * 0.95) as usize;
// //     let mut numbers: Vec<u64> = (0..n_strings as u64).map(|n| xxh3_64(&n.to_le_bytes())).collect();

// //     /* let mut rng = rand::thread_rng();
// //     for _ in 0..n_strings {
// //         numbers.push(rng.gen())
// //     } */
// //     // numbers = (0..n_strings as u64).map(|n| xxh3_64(&n.to_le_bytes())).collect();
// //     numbers.sort();

// //     println!("inserting now...");
// //     // let now = Instant::now();
// //     for i in 0..n_strings {
// //         //qf.insert(strings[i].as_bytes(), 3)?;
// //         // println!("inserting {}", numbers[i]);
// //         // println!("inserting {}", numbers[i]);
// //         qf.insert_by_hash(numbers[i], 1).expect("insert failed!");
// //     }

// //     println!("{}", qf.query_by_hash(numbers[0]));

// // let qf = CountingQuotientFilter::new(LOGN_SLOTS, LOGN_SLOTS, HashMode::Invertible).unwrap();
// // qf.clear();
// // println!("cleared");
// // let n_strings: usize = ((1 << (LOGN_SLOTS)) as f32 * 0.9) as usize;
// // let mut numbers: Vec<u64> = Vec::with_capacity(n_strings);

// // let mut rng = rand::thread_rng();
// // for _ in 0..n_strings {
// //     numbers.push(rng.gen())
// // }

// // numbers.clone().into_par_iter().for_each(|n| {
// //     qf.insert(n, 1).expect("insert failed!");
// // });

// // let now = Instant::now();
// // for i in 0..n_strings {
// //     //qf.insert(strings[i].as_bytes(), 3)?;
// //     // println!("inserting {}", numbers[i]);
// //     qf.insert((numbers[i]) as u64, 1).expect("insert failed!");
// // }
// // let mut item_count = AtomicI32::new(0);

// // qf.set_count(numbers[0], 100).expect("Failed to set count");
// // println!("count is {}", qf.query(numbers[0]));
// // qf.set_count(numbers[0], 300).expect("Failed to set count");
// // println!("count is {}", qf.query(numbers[0]));
// // qf.set_count(numbers[0], 2).expect("Failed to set count");
// // println!("count is {}", qf.query(numbers[0]));

// // Iterator::for_each(qf.into_iter(), |item| {
// //     item_count.fetch_add(item.count as i32, std::sync::atomic::Ordering::SeqCst);
// // });

// // let now = Instant::now();
// // println!("num threads {}", rayon::current_num_threads());
// // rayon::iter::ParallelIterator::for_each(qf.into_par_iter(), |item| {
// //     item_count.fetch_add(item.count as i32, std::sync::atomic::Ordering::SeqCst);
// // });
// // qf.into_par_iter().for_each(|item| {
// //     item_count.fetch_add(item.count as i32, std::sync::atomic::Ordering::SeqCst);
// // });
// // println!("num threads {}", rayon::current_num_threads());
// // println!("Time to iterate: {:?}", now.elapsed());

// // let mut unique_strings = std::collections::HashSet::new();
// // for i in 0..n_strings {
// //     unique_strings.insert(numbers[i]);
// // }

// // println!("Item Count {}, should be {}",item_count.load(std::sync::atomic::Ordering::SeqCst), unique_strings.len());

// // for i in 0..n_strings {
// //     //qf.insert(strings[i].as_bytes(), 3)?;
// //     // println!("inserting {}", numbers[i]);
// //     assert!(qf.query((numbers[i]) as u64) > 0);
// // }

// // qf.print();
// // NOTE offsets may be off by one ??
// ///////////////////////////////////////
// // let num_threads = 6;
// // let numbers = Arc::new(numbers);
// // let qf = Arc::new(qf);

// // let mut handles = Vec::with_capacity(num_threads);

// // let now = Instant::now();
// // for i in 0..num_threads {
// //     let qf = qf.clone();
// //     let numbers = numbers.clone();
// //     handles.push(std::thread::spawn(move || {
// //         for j in (i*n_strings/num_threads)..((i+1)*n_strings/num_threads) {
// //             // println!("inserting {}", numbers[j]);
// //             qf.insert(numbers[j] as u64, 1).expect("insert failed!");
// //         }
// //     }));
// // }

// // for handle in handles {
// //     handle.join().unwrap();
// // }
// ////////////////////////////////////////////////
// // let inserts = Arc::new(AtomicI32::new(0));
// // numbers.par_iter().for_each(|&i| {
// //     qf.insert(i as u64, 1).expect("insert failed!");
// //     // inserts.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
// // });

// // println!("inserted {} elements", inserts.load(std::sync::atomic::Ordering::SeqCst));
// // println!("{n_strings}");
// // for i in 0..n_strings {
// //     //qf.insert(strings[i].as_bytes(), 3)?;
// //     qf.insert(numbers[i] as u64, 1).expect("insert failed!");
// // }

// // let elapsed = now.elapsed();
// // println!("insert took {} seconds!", elapsed.as_millis());

// // let now = Instant::now();

// // let qf = Arc::new(qf);

// // let elapsed = now.elapsed();
// // println!("insert took {} seconds!", elapsed.as_secs());

// ///////////////////////////////////////////////////////////////////////

// // let mut qf =
// //     CountingQuotientFilter::new(25, 25, HashMode::Invertible).expect("failed to make cqf");

// // let qf = CountingQuotientFilter::new_file(25, 25, HashMode::Invertible, "test.qf".into())
// //     .expect("failed to make cqf");
// // // let qf = CountingQuotientFilter::open_file("test.qf".into()).expect("failed to make cqf");
// // let n_strings: usize = 10_000_000;

// // let mut numbers: Vec<u64> = Vec::with_capacity(n_strings);
// // let mut rng = rand::thread_rng();

// // for _ in 0..n_strings {
// //     numbers.push(rng.gen())
// // }

// // for i in 0..n_strings / 2 {
// //     //qf.insert(strings[i].as_bytes(), 3)?;
// //     match qf.insert(numbers[i], 3) {
// //         Err(_) => eprintln!("Error inserting"),
// //         _ => continue,
// //     };
// // }
// // let mut total_in = 0;
// // for i in 0..n_strings {
// //     //assert!(qf.query(strings[i].as_bytes()) > 0, "false negative!");
// //     let p = qf.query(numbers[i]);
// //     total_in += p;
// //     assert!(qf.query(numbers[i]) > 0, "false negative!");
// // }
// // println!("{} elements present", total_in);

// // let mut present: u32 = 0;
// // for i in n_strings / 2..n_strings {
// //     /*
// //     if qf.query(strings[i].as_bytes()) > 0 {
// //         present += 1;
// //     }
// //     */
// //     if qf.query(numbers[i]) > 0 {
// //         present += 1;
// //     }
// // }
// // assert_eq!(present, 0);
// // println!("{} elements present", total_in);

// // drop(qf);

// // // MMAP LOAD TEST

// // let qf = CountingQuotientFilter::open_file("test.qf".into()).expect("failed to make cqf");
// // let n_strings: usize = 10_000_000;

// // // let mut numbers: Vec<u64> = Vec::with_capacity(n_strings);
// // // let mut rng = rand::thread_rng();

// // // for _ in 0..n_strings {
// // //     numbers.push(rng.gen())
// // // }

// // let mut total_in = 0;
// // for i in 0..n_strings / 2 {
// //     //assert!(qf.query(strings[i].as_bytes()) > 0, "false negative!");
// //     let p = qf.query(numbers[i]);
// //     total_in += p;
// //     assert!(qf.query(numbers[i]) > 0, "false negative!");
// // }

// // let mut present: u32 = 0;
// // for i in n_strings / 2..n_strings {
// //     /*
// //     if qf.query(strings[i].as_bytes()) > 0 {
// //         present += 1;
// //     }
// //     */
// //     if qf.query(numbers[i]) > 0 {
// //         present += 1;
// //     }
// // }
// // assert_eq!(present, 0);
// // println!("{} elements present", total_in);

// // let qf = CountingQuotientFilter::new_file(25,25,HashMode::Invertible, "test.qf".into()).expect("failed to make cqf");
// // }
