type Offset = u64;

use crate::utils::{bitrank, bitselectv, ffs, saturating_bitmask, wrapping_popcntv};
use crate::SLOTS_PER_BLOCK;

// pub mod u64_soa_blocks;
pub mod u32_blocks;
pub mod u64_blocks;

pub trait Blocks {
    type Remainder;
    #[inline(always)]
    fn split_quotient(quotient: u64) -> (usize, usize) {
        let block_index = (quotient / crate::SLOTS_PER_BLOCK as u64) as usize;
        let slot_index = (quotient % crate::SLOTS_PER_BLOCK as u64) as usize;
        (block_index, slot_index)
    }

    fn run_start(&self, quotient: u64) -> u64 {
        if std::intrinsics::unlikely(quotient == 0) {
            0
        } else {
            self.run_end(quotient - 1) + 1
        }
    }

    // Assumes quotient holds a remainder
    fn decode_counter(&self, quotient: &mut u64) -> (Self::Remainder, u64);

    #[inline]
    fn run_end(&self, quotient: u64) -> u64 {
        let (block_index, slot_index) = Self::split_quotient(quotient);
        #[allow(clippy::unnecessary_cast)] // Offset may not be u64
        let block_offset: u64 = self.offset_by_block(block_index).into();
        let intrablock_rank =
            unsafe { bitrank(self.occupieds_by_block(block_index), slot_index as u64) };

        if intrablock_rank == 0 {
            if block_offset <= slot_index as u64 {
                return quotient;
            } else {
                return 64 * block_index as u64 + block_offset - 1;
            }
        }

        let mut runend_block_index = block_index + block_offset as usize / 64;
        let mut runend_ignore_bits = block_offset % 64;
        let mut runend_rank = intrablock_rank - 1;
        let mut runend_block_offset: u64 = bitselectv(
            self.runends_by_block(runend_block_index),
            runend_ignore_bits,
            runend_rank,
        );

        if runend_block_offset == 64 {
            if block_offset == 0 && intrablock_rank == 0 {
                return quotient;
            } else {
                loop {
                    runend_rank -= wrapping_popcntv(
                        self.runends_by_block(runend_block_index),
                        runend_ignore_bits,
                    );
                    runend_block_index += 1;
                    runend_ignore_bits = 0;
                    runend_block_offset = bitselectv(
                        self.runends_by_block(runend_block_index),
                        runend_ignore_bits,
                        runend_rank,
                    );
                    if runend_block_offset != 64 {
                        break;
                    }
                }
            }
        }

        let runend_index = 64 * runend_block_index + runend_block_offset as usize;
        if (runend_index as u64) < quotient {
            quotient
        } else {
            runend_index as u64
        }
    }

    // fn run_end_by_block(&self, _block: usize, _slot: usize) -> u64 {
    //     panic!("problem");
    //     // let runend = self.runends_by_block(block);
    //     // if slot == 0 {
    //     //     runend
    //     // } else {
    //     //     runend & ((1 << slot) - 1)
    //     // }
    // }

    // fn advise_seq(&self);

    // fn advise_normal(&self);

    fn bytes_needed(num_blocks: usize) -> usize;

    // Default by quotient

    fn offset(&self, quotient: u64) -> Offset;
    fn offset_mut(&mut self, quotient: u64) -> &mut Offset;

    // fn occupieds(&self, quotient: u64) -> u64;
    // fn runends(&self, quotient: u64) -> u64;
    // fn counts(&self, quotient: u64) -> u64;

    fn slot(&self, quotient: u64) -> &Self::Remainder;
    fn slot_mut(&mut self, quotient: u64) -> &mut Self::Remainder;

    fn is_occupied(&self, quotient: u64) -> bool;
    fn is_runend(&self, quotient: u64) -> bool;
    fn is_count(&self, quotient: u64) -> bool;

    fn set_occupied(&mut self, quotient: u64, bit: bool);
    fn set_runend(&mut self, quotient: u64, bit: bool);
    fn set_count(&mut self, quotient: u64, bit: bool);

    // By block

    fn offset_by_block(&self, block: usize) -> Offset;
    fn offset_by_block_mut(&mut self, block: usize) -> &mut Offset;

    fn occupieds_by_block(&self, block: usize) -> u64;
    fn runends_by_block(&self, block: usize) -> u64;
    // fn counts_by_block(&self, block: usize) -> u64;

    // By block and slot
    fn slot_by_block(&self, block: usize, slot: usize) -> &Self::Remainder;
    fn slot_by_block_mut(&mut self, block: usize, slot: usize) -> &mut Self::Remainder;

    fn is_occupied_by_block(&self, block: usize, slot: usize) -> bool;
    fn is_runend_by_block(&self, block: usize, slot: usize) -> bool;
    fn is_count_by_block(&self, block: usize, slot: usize) -> bool;

    fn set_occupied_by_block(&mut self, block: usize, slot: usize, bit: bool);
    fn set_runend_by_block(&mut self, block: usize, slot: usize, bit: bool);
    fn set_count_by_block(&mut self, block: usize, slot: usize, bit: bool);

    fn offset_lower_bound(&self, quotient: u64) -> u64 {
        let (block_index, slot_index) = Self::split_quotient(quotient);
        self.offset_lower_bound_by_block(block_index, slot_index)
    }

    fn offset_lower_bound_by_block(&self, block: usize, slot: usize) -> u64 {
        let occupieds = self.occupieds_by_block(block) & saturating_bitmask((slot + 1) as u64);
        let offset_u64 = self.offset_by_block(block) as u64;
        if offset_u64 <= slot as u64 {
            let runends =
                (self.runends_by_block(block) & saturating_bitmask(slot as u64)) >> offset_u64;
            (occupieds.count_ones() - runends.count_ones()) as u64
        } else {
            (offset_u64 + occupieds.count_ones() as u64) - slot as u64
        }
    }

    fn has_metadata_bits_set(&self, quotient: u64) -> bool {
        self.is_occupied(quotient) || self.is_runend(quotient) || self.is_count(quotient)
    }

    fn find_first_empty_slot(&self, mut from_quotient: u64) -> u64 {
        loop {
            let jump = self.offset_lower_bound(from_quotient);
            if jump == 0 {
                return from_quotient;
            }
            from_quotient += jump;
        }
    }

    fn find_first_occupied_slot(&self) -> u64 {
        let mut block_index = 0;
        if self.is_occupied_by_block(block_index, 0) {
            return 0;
        }

        while block_index < self.num_blocks() - 1 {
            if let Some(slot) = ffs(self.occupieds_by_block(block_index)) {
                return (block_index * SLOTS_PER_BLOCK + slot as usize) as u64;
            }

            block_index += 1;
        }

        0
    }

    // fn madvise_dont_need(&self, current_quotient: u64);

    fn num_blocks(&self) -> usize;

    /// Returns number of blocks in the CQF
    fn len(&self) -> usize;
}
