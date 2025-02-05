#![feature(sync_unsafe_cell, ptr_internals, unchecked_shifts)]
// #![feature(ptr_internals)]
#![feature(core_intrinsics)]
mod blocks;
mod cqf;
mod reversible_hasher;
// mod utils;
const SLOTS_PER_BLOCK: usize = 64;
// mod old_cqf;
// pub use old_cqf::CountingQuotientFilter as OldCqf;

pub use cqf::*;
pub use reversible_hasher::*;

// pub use reversible_hasher::*;
// use std::hash::BuildHasher;
// use std::ops::{Deref, DerefMut};
// use std::path::PathBuf;
// use std::sync::atomic::AtomicU64;
// pub use utils::*;

// // pub use generic_cqf::{*};
// pub use cqf_u64::{*};
// // pub use cqf_u64::CQFIterator;
// // pub use cqf_u64::CountingQuotientFilter;
// // pub use cqf_u64::CqfMergeCallback;
// // pub use cqf_u64::HashCount;
// // pub use cqf_u64::ZippedCqfIterator;

// #[derive(Debug)]
// pub enum CqfError {
//     FileError,
//     MmapError,
//     InvalidArguments,
//     InvalidFile,
//     InvalidSize,
//     Filled,
// }

#[inline]
fn pdep(val: u64, mut mask: u64) -> u64 {
    #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
    {
        if is_x86_feature_detected!("bmi2") {
            return unsafe { _pdep_bmi2(val, mask) };
        }
    }

    // https://www.intel.com/content/www/us/en/docs/intrinsics-guide/index.html#text=_pdep_u64&ig_expand=4908
    let mut res = 0;
    let mut bb: u64 = 1;
    loop {
        if mask == 0 {
            break;
        }
        if (val & bb) != 0 {
            res |= mask & mask.wrapping_neg();
        }
        mask &= mask - 1;
        bb = bb.wrapping_add(bb);
    }
    res
}

#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
#[target_feature(enable = "bmi2")]
unsafe fn _pdep_bmi2(val: u64, mask: u64) -> u64 {
    #[cfg(target_arch = "x86")]
    use std::arch::x86::_pdep_u64;
    #[cfg(target_arch = "x86_64")]
    use std::arch::x86_64::_pdep_u64;

    _pdep_u64(val, mask)
}

mod utils {

    use crate::pdep;

    pub fn bitrank(val: u64, pos: u64) -> u64 {
        if pos == 63 {
            (val & u64::MAX).count_ones() as u64
        } else {
            (val & ((2 << pos) - 1)).count_ones() as u64
        }
        // unsafe{u64::unchecked_sub(2 << pos, 1)};
        // (val & unsafe{u64::unchecked_sub(2 << pos, 1)}).count_ones() as u64
        // (val & (2 << pos) - 1).count_ones() as usize
    }

    pub fn popcntv(val: u64, ignore: u64) -> u64 {
        if ignore % 64 != 0 {
            (val & !(bitmask(ignore % 64))).count_ones() as u64
        } else {
            val.count_ones() as u64
        }
    }

    pub fn bitselect(val: u64, rank: u64) -> u64 {
        pdep(1 << rank, val).trailing_zeros() as u64
    }

    pub fn bitselectv(val: u64, ignore: u64, rank: u64) -> u64 {
        bitselect(val & !(bitmask(ignore % 64)), rank)
    }

    pub fn bitmask(nbits: u64) -> u64 {
        if nbits == 64 {
            u64::MAX
        } else {
            (1 << nbits) - 1
        }
    }
}
