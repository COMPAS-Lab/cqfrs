#![feature(ptr_internals)]
#![feature(core_intrinsics)]
#![warn(clippy::unwrap_used, clippy::unused_result_ok)]

mod blocks;
mod cqf;
mod reversible_hasher;
// mod utils;
const SLOTS_PER_BLOCK: usize = 64;
// mod old_cqf;
// pub use old_cqf::CountingQuotientFilter as OldCqf;

pub use cqf::*;
pub use reversible_hasher::*;

// use std::hash::BuildHasher;
// use std::ops::{Deref, DerefMut};
// use std::path::PathBuf;
// use std::sync::atomic::AtomicU64;

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

#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
fn _pdep_runtime(val: u64, mask: u64) -> u64 {
    if is_x86_feature_detected!("bmi2") {
        unsafe { _pdep_bmi2(val, mask) }
    } else {
        _pdep_const(val, mask)
    }
}

// https://www.intel.com/content/www/us/en/docs/intrinsics-guide/index.html#text=_pdep_u64&ig_expand=4908
#[inline]
const fn _pdep_const(val: u64, mut mask: u64) -> u64 {
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

#[inline]
#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
#[target_feature(enable = "bmi2")]
unsafe fn _pdep_bmi2(val: u64, mask: u64) -> u64 {
    #[cfg(target_arch = "x86")]
    use std::arch::x86::_pdep_u64;
    #[cfg(target_arch = "x86_64")]
    use std::arch::x86_64::_pdep_u64;

    _pdep_u64(val, mask)
}

#[inline]
#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
fn pdep(val: u64, mask: u64) -> u64 {
    _pdep_runtime(val, mask)
}

#[inline]
#[cfg(not(any(target_arch = "x86", target_arch = "x86_64")))]
fn pdep(val: u64, mask: u64) -> u64 {
    _pdep_const(val, mask)
}

mod utils {
    use crate::pdep;

    /// Returns the number of bits set in `val` up to and including the bit at position `pos`. Saturates to `val.count_ones()` if `pos >= 63`.
    pub const fn saturating_bitrank(val: u64, pos: u64) -> u64 {
        val.unbounded_shl(63u32.saturating_sub(pos as u32))
            .count_ones() as u64
    }

    /// Returns the number of bits set in `val` up to and including the bit at position `pos`.
    ///
    /// # Safety
    /// `pos` must be less than 64.
    pub const unsafe fn bitrank(val: u64, pos: u64) -> u64 {
        (val << (63 - pos as u32)).count_ones() as u64
    }

    /// Returns the population count of the bits of `val`, ignoring the lowest `ignore % 64` bits.
    pub const fn wrapping_popcntv(val: u64, ignore: u64) -> u64 {
        val.wrapping_shr(ignore as u32).count_ones() as u64
    }

    /// Returns the index of the first set bit in `val`.
    #[inline]
    pub const fn ffs(val: u64) -> Option<u64> {
        if val == 0 {
            None
        } else {
            Some(val.trailing_zeros() as u64)
        }
    }

    /// Returns the index of the first set bit in `val`, ignoring the lowest `ignore` bits.
    #[inline]
    pub const fn ffsv(val: u64, ignore: u64) -> Option<u64> {
        ffs(val & !saturating_bitmask(ignore))
    }

    /// Returns the index of the `rank`th set bit in `val`.
    ///
    /// If `rank` is 0, consider using `utils::ffs(val).unwrap_or(64)` instead.
    pub fn bitselect(val: u64, rank: u64) -> u64 {
        pdep(1u64.unbounded_shl(rank as u32), val).trailing_zeros() as u64
    }

    pub fn bitselectv(val: u64, ignore: u64, rank: u64) -> u64 {
        bitselect(val & !(saturating_bitmask(ignore % 64)), rank)
    }

    /// Returns a mask with the first `nbits` bits set. Saturates to [`u64::MAX`] if `nbits >= 64`.
    #[inline]
    pub const fn saturating_bitmask(nbits: u64) -> u64 {
        if nbits >= 64 {
            u64::MAX
        } else {
            (1 << nbits) - 1
        }
    }
}
