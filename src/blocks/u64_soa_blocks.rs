use std::ptr::Unique;

use libc::c_void;

use super::{Blocks, Offset};
use crate::SLOTS_PER_BLOCK;
pub type Remainder = u64;

#[repr(C)]
pub struct Block {
    remainders: [Remainder; SLOTS_PER_BLOCK],
}

pub struct U64SoaBlocks {
    ptr_metadata: Unique<BlockMetadata>,
    ptr_blocks: Unique<Block>,
    len: usize,
}

impl U64SoaBlocks {
    pub fn new(ptr: *mut u8, len: usize) -> Self {
        if ptr.is_null() {
            panic!("ptr is null");
        }
        let ptr_metadata = unsafe { Unique::new_unchecked(ptr as *mut BlockMetadata) };
        let ptr_blocks = unsafe { ptr_metadata.as_ptr().offset(len as isize) } as *mut Block;
        let ptr_blocks = unsafe { Unique::new_unchecked(ptr_blocks) };
        Self {
            ptr_metadata,
            ptr_blocks,
            len,
        }
    }
}

impl Blocks for U64SoaBlocks {
    type Remainder = Remainder;

    fn bytes_needed(num_blocks: usize) -> usize {
        let mut size = 0;
        size += num_blocks * std::mem::size_of::<Block>();
        size += num_blocks * std::mem::size_of::<BlockMetadata>();
        size
    }

    fn offset(&self, quotient: u64) -> Offset {
        let (block_index, _) = Self::split_quotient(quotient);
        self.offset_by_block(block_index)
    }

    #[inline(always)]
    fn decode_counter(&self, quotient: &mut u64, remainder: &mut Self::Remainder, count: &mut u64) {
        let (block_index, slot_index) = Self::split_quotient(*quotient);
        *remainder = *self.slot_by_block(block_index, slot_index);
        if self.is_runend(*quotient) || !self.is_count(*quotient + 1) {
            *count = 1;
        } else {
            // Only works for u64
            *count = *self.slot(*quotient + 1);
            *quotient += 1;
            // let mut qptr = *quotient + 1;
            // let mut c: u64 = 0;
            // while self.is_count(qptr) {
            //     c <<= Self::Remainder::BITS;
            //     c |= self.slot(qptr);
            //     qptr += 1;
            // }
            // *quotient = qptr;
            // *count = c;
        }
    }

    #[inline(always)]
    fn offset_mut(&mut self, quotient: u64) -> &mut Offset {
        let (block_index, _) = Self::split_quotient(quotient);
        self.offset_by_block_mut(block_index)
    }

    #[inline(always)]
    fn occupieds(&self, quotient: u64) -> u64 {
        let (block_index, _) = Self::split_quotient(quotient);
        self.occupieds_by_block(block_index)
    }

    #[inline(always)]
    fn runends(&self, quotient: u64) -> u64 {
        let (block_index, _) = Self::split_quotient(quotient);
        self.runends_by_block(block_index)
    }

    #[inline(always)]
    fn counts(&self, quotient: u64) -> u64 {
        let (block_index, _) = Self::split_quotient(quotient);
        self.counts_by_block(block_index)
    }

    #[inline(always)]
    fn slot(&self, quotient: u64) -> &Self::Remainder {
        let (block_index, slot_index) = Self::split_quotient(quotient);
        &self.slot_by_block(block_index, slot_index)
    }

    #[inline(always)]
    fn slot_mut(&mut self, quotient: u64) -> &mut Self::Remainder {
        let (block_index, slot_index) = Self::split_quotient(quotient);
        self.slot_by_block_mut(block_index, slot_index)
    }

    fn is_occupied(&self, quotient: u64) -> bool {
        let (block_index, slot_index) = Self::split_quotient(quotient);
        self.is_occupied_by_block(block_index, slot_index)
    }

    fn is_runend(&self, quotient: u64) -> bool {
        let (block_index, slot_index) = Self::split_quotient(quotient);
        self.is_runend_by_block(block_index, slot_index)
    }

    fn is_count(&self, quotient: u64) -> bool {
        let (block_index, slot_index) = Self::split_quotient(quotient);
        self.is_count_by_block(block_index, slot_index)
    }

    fn set_occupied(&mut self, quotient: u64, bit: bool) {
        let (block_index, slot_index) = Self::split_quotient(quotient);
        self.set_occupied_by_block(block_index, slot_index, bit)
    }

    fn set_runend(&mut self, quotient: u64, bit: bool) {
        let (block_index, slot_index) = Self::split_quotient(quotient);
        self.set_runend_by_block(block_index, slot_index, bit)
    }

    fn set_count(&mut self, quotient: u64, bit: bool) {
        let (block_index, slot_index) = Self::split_quotient(quotient);
        self.set_count_by_block(block_index, slot_index, bit)
    }

    #[inline(always)]
    fn offset_by_block(&self, block: usize) -> Offset {
        self.metadata()[block].offset as Offset
    }

    #[inline(always)]
    fn offset_by_block_mut(&mut self, block: usize) -> &mut Offset {
        &mut self.metadata_mut()[block].offset
    }

    #[inline(always)]
    fn occupieds_by_block(&self, block: usize) -> u64 {
        self.metadata()[block].occupieds
    }

    #[inline(always)]
    fn runends_by_block(&self, block: usize) -> u64 {
        self.metadata()[block].runends
    }

    #[inline(always)]
    fn counts_by_block(&self, block: usize) -> u64 {
        self.metadata()[block].counts
    }

    #[inline(always)]
    fn slot_by_block(&self, block: usize, slot: usize) -> &Self::Remainder {
        &self.block()[block].remainders[slot]
    }

    #[inline(always)]
    fn slot_by_block_mut(&mut self, block: usize, slot: usize) -> &mut Self::Remainder {
        &mut self.block_mut()[block].remainders[slot]
    }

    #[inline(always)]
    fn is_occupied_by_block(&self, block: usize, slot: usize) -> bool {
        self.metadata()[block].occupieds & (1 << slot) != 0
    }

    #[inline(always)]
    fn is_runend_by_block(&self, block: usize, slot: usize) -> bool {
        self.metadata()[block].runends & (1 << slot) != 0
    }

    #[inline(always)]
    fn is_count_by_block(&self, block: usize, slot: usize) -> bool {
        self.metadata()[block].counts & (1 << slot) != 0
    }

    #[inline(always)]
    fn set_occupied_by_block(&mut self, block: usize, slot: usize, bit: bool) {
        if bit {
            self.metadata_mut()[block].occupieds |= 1 << slot;
        } else {
            self.metadata_mut()[block].occupieds &= !(1 << slot);
        }
    }

    #[inline(always)]
    fn set_runend_by_block(&mut self, block: usize, slot: usize, bit: bool) {
        if bit {
            self.metadata_mut()[block].runends |= 1 << slot;
        } else {
            self.metadata_mut()[block].runends &= !(1 << slot);
        }
    }

    #[inline(always)]
    fn set_count_by_block(&mut self, block: usize, slot: usize, bit: bool) {
        if bit {
            self.metadata_mut()[block].counts |= 1 << slot;
        } else {
            self.metadata_mut()[block].counts &= !(1 << slot);
        }
    }

    #[inline(always)]
    fn num_blocks(&self) -> usize {
        self.len
    }

    fn madvise_dont_need(&self, current_quotient: u64) {
        let ptr_start = self.ptr_blocks.as_ptr() as *mut c_void;
        let aligned_ptr_start = unsafe { ptr_start.offset(ptr_start.align_offset(4096) as isize) };
        let ptr_end =
            unsafe { (self.slot(current_quotient) as *const Self::Remainder).offset(-4096) };
        if ptr_end as usize > aligned_ptr_start as usize {
            let len = ptr_end as usize - aligned_ptr_start as usize;
            let madv_result = unsafe { libc::madvise(aligned_ptr_start, len, libc::MADV_DONTNEED) };
            if madv_result != 0 {
                panic!("madvise failed: {}", madv_result);
            }
        }
    }

    fn len(&self) -> usize {
        self.len
    }
}

impl U64SoaBlocks {
    fn metadata(&self) -> &[BlockMetadata] {
        unsafe { std::slice::from_raw_parts(self.ptr_metadata.as_ptr(), self.len) }
    }

    fn metadata_mut(&mut self) -> &mut [BlockMetadata] {
        unsafe { std::slice::from_raw_parts_mut(self.ptr_metadata.as_ptr(), self.len) }
    }

    fn block(&self) -> &[Block] {
        unsafe { std::slice::from_raw_parts(self.ptr_blocks.as_ptr(), self.len) }
    }

    fn block_mut(&mut self) -> &mut [Block] {
        unsafe { std::slice::from_raw_parts_mut(self.ptr_blocks.as_ptr(), self.len) }
    }
}

pub struct BlockMetadata {
    occupieds: u64,
    runends: u64,
    counts: u64,
    offset: u16,
}
