use std::fs::File;
use std::hash;
use std::hash::{BuildHasher, Hash};
use std::os::fd::AsRawFd;

use super::{
    CountingQuotientFilter, CqfError, CqfIteratorImpl, Metadata, MetadataWrapper, RuntimeData,
    SLOTS_PER_BLOCK,
};
use crate::blocks::u64_blocks::*;
use crate::blocks::Blocks;
use crate::utils::{ffs, ffsv, saturating_bitmask};

enum InsertOperation {
    /// Insert into empty slot
    InsertEmpty,
    /// Append to slot
    Append,
    /// Insert into slot
    Insert,
}

/// Fixed size counter u64 quotient filter
pub struct U64Cqf<H: BuildHasher> {
    metadata: MetadataWrapper,
    blocks: U64Blocks,
    runtime_data: RuntimeData<H>,
}

impl<H: BuildHasher> CountingQuotientFilter for U64Cqf<H> {
    type Hasher = H;
    type Remainder = Remainder;
    fn new(
        quotient_bits: u64,
        hash_bits: u64,
        invertable: bool,
        hasher: H,
    ) -> Result<Self, CqfError> {
        let (metadata, blocks) =
            Self::make_metadata_blocks(quotient_bits, hash_bits, invertable, None, true)?;

        let num_slots = 1u64 << quotient_bits;

        let runtime_data = RuntimeData::new(None, hasher, num_slots);
        Ok(Self {
            metadata,
            blocks,
            runtime_data,
        })
    }

    fn new_file(
        quotient_bits: u64,
        hash_bits: u64,
        invertable: bool,
        hasher: Self::Hasher,
        mut file: File,
    ) -> Result<Self, CqfError> {
        let (metadata, blocks) = Self::make_metadata_blocks(
            quotient_bits,
            hash_bits,
            invertable,
            Some(&mut file),
            true,
        )?;

        let num_slots = 1u64 << quotient_bits;

        let runtime_data = RuntimeData::new(Some(file), hasher, num_slots);
        Ok(Self {
            metadata,
            blocks,
            runtime_data,
        })
    }

    fn open_file(hasher: Self::Hasher, mut file: File) -> Result<Self, CqfError> {
        // Dummy data to reuse function
        use std::io::Read;
        let md: Metadata;
        // read metadata from file
        unsafe {
            let mut metadata_buffer = [0u8; std::mem::size_of::<Metadata>()];
            file.read_exact(&mut metadata_buffer)
                .map_err(|_| CqfError::FileError)?;
            let metadata_ptr = metadata_buffer.as_ptr() as *const Metadata;
            md = *metadata_ptr;
        }
        let (metadata, blocks) = Self::make_metadata_blocks(
            md.quotient_bits,
            md.quotient_bits + md.remainder_bits,
            md.invertable(),
            Some(&mut file),
            false,
        )?;

        let num_slots = 1u64 << md.quotient_bits;

        let runtime_data = RuntimeData::new(Some(file), hasher, num_slots);
        Ok(Self {
            metadata,
            blocks,
            runtime_data,
        })
    }

    fn calc_hash<Item: Hash>(&self, item: Item) -> u64 {
        let mut hasher = self.runtime_data.hasher.build_hasher();
        item.hash(&mut hasher);
        hash::Hasher::finish(&hasher)
    }

    fn quotient_bits(&self) -> u64 {
        self.metadata.quotient_bits
    }

    fn remainder_bits(&self) -> u64 {
        self.metadata.remainder_bits
    }
    // fn insert_by_hash(&mut self, hash: u64, count: u64) -> Result<u64, CqfError> {
    //     if std::intrinsics::unlikely(count == 0) {
    //         return Err(CqfError::InvalidArguments);
    //     }
    //     if self.occupied_slots() >= self.max_occupied_slots() {
    //         return Err(CqfError::Filled);
    //     }
    //     let (quotient, remainder) = self.quotient_remainder_from_hash(hash);

    //     // Trying something different here
    //     let run_start = self.blocks.run_start(quotient);
    //     if !self.blocks.has_metadata_bits_set(quotient) && run_start <= quotient {
    //         self.blocks.set_occupied(quotient, true);
    //         *self.blocks.slot_mut(quotient) = remainder;
    //         self.metadata.num_occupied_slots += 1;
    //         if count == 1 {
    //             self.blocks.set_runend(quotient, true);
    //             return Ok(count);
    //         }
    //         let slots_needed = match count.checked_next_power_of_two() {
    //             Some(n) => (n.trailing_zeros() / Remainder::BITS) as u64 ,
    //             None => (64/Remainder::BITS) as u64,
    //         };
    //         for offset in 1..=slots_needed {
    //             if self.blocks.has_metadata_bits_set(quotient + offset) {
    //                 return self.insert_by_hash(hash, count-1);
    //             }
    //         }
    //         self.blocks.set_runend(quotient + slots_needed, true);
    //         let mut c = count;
    //         // this needs to be changed for u64
    //         for offset in 1..=slots_needed {
    //             let remainder = c & Remainder::MAX;
    //             c >>= Remainder::BITS;
    //             *self.blocks.slot_mut(quotient + offset) = remainder as Remainder;
    //             self.blocks.set_count(quotient + offset, true);
    //         }
    //         self.metadata.num_occupied_slots += slots_needed;
    //         return Ok(count);
    //     }

    //     todo!()
    // }

    /// Function used internally for merging
    fn merge_insert(
        &mut self,
        current_quotient: &mut u64,
        new_quotient: u64,
        next_quotient: u64,
        new_remainder: u64,
        count: u64,
    ) {
        if count == 0 {
            return;
        }
        let remainder = new_remainder as Remainder;
        self.blocks.set_occupied(new_quotient, true);
        if *current_quotient < new_quotient {
            *current_quotient = new_quotient;
        }
        *self.blocks.slot_mut(*current_quotient) = remainder;
        let slots;
        if count != 1 {
            self.blocks.set_count(*current_quotient + 1, true);
            *self.blocks.slot_mut(*current_quotient + 1) = count as Remainder;
            self.metadata.num_occupied_slots += 2;
            *current_quotient += 2;
            slots = 2;
        } else {
            self.metadata.num_occupied_slots += 1;
            *current_quotient += 1;
            slots = 1;
        }
        if next_quotient != new_quotient {
            self.blocks.set_runend(*current_quotient - 1, true);
        }
        let quotient_block_idx = new_quotient / SLOTS_PER_BLOCK as u64;
        let end_of_insert = *current_quotient - 1;
        // The block we're inserting into
        let insert_block_idx = (end_of_insert) / SLOTS_PER_BLOCK as u64;
        for i in (quotient_block_idx + 1)..=insert_block_idx {
            *self.blocks.offset_mut(i * SLOTS_PER_BLOCK as u64) += slots;
            self.metadata.largest_offset = self
                .metadata
                .largest_offset
                .max(self.blocks.offset(i * SLOTS_PER_BLOCK as u64));
        }
    }

    fn insert_by_hash(&mut self, hash: u64, count: u64) -> Result<(), CqfError> {
        // println!("insert_by_hash {hash} {count}");
        if count == 0 {
            return Ok(());
        } // nothing to do
        if self.occupied_slots() >= self.max_occupied_slots()
            || self.metadata.largest_offset >= self.metadata.largest_possible_offset
        {
            println!(
                "largest offset reached, returning full. largest offset {}, max offset {}",
                self.metadata.largest_offset, self.metadata.largest_possible_offset
            );

            return Err(CqfError::Filled);
        }
        let (quotient, remainder) = self.quotient_remainder_from_hash(hash);
        let mut runstart_index = self.blocks.run_start(quotient);
        // let runend_index = self.blocks.run_end(quotient);
        // if runstart_index != runend_index {
        //     println!("Runstart {runstart_index} Runend {runend_index} Quotient {quotient}");
        // }
        if !self.blocks.has_metadata_bits_set(quotient) && runstart_index == quotient {
            // if !self.blocks.has_metadata_bits_set(quotient) && runend_index == quotient {
            // if runstart_index != runend_index {
            //     println!("Runstart {runstart_index} Runend {runend_index} Quotient {quotient}");
            // }
            self.blocks.set_runend(quotient, true);
            *self.blocks.slot_mut(quotient) = remainder;
            self.blocks.set_occupied(quotient, true);
            self.metadata.num_occupied_slots += 1;
            if count > 1 {
                self.insert_by_hash(hash, count - 1)?;
            }
        } else {
            // let mut runstart_index = self.blocks.run_start(quotient);
            if !self.blocks.is_occupied(quotient) {
                self.insert_and_shift(
                    InsertOperation::InsertEmpty,
                    quotient,
                    remainder,
                    count,
                    runstart_index,
                    0,
                );
            } else {
                let mut qptr = runstart_index;
                let (mut current_remainder, mut current_count) =
                    self.blocks.decode_counter(&mut qptr);
                while current_remainder < remainder && !self.blocks.is_runend(qptr) {
                    runstart_index = qptr + 1;
                    qptr = runstart_index;
                    (current_remainder, current_count) = self.blocks.decode_counter(&mut qptr);
                }

                if current_remainder < remainder {
                    self.insert_and_shift(
                        InsertOperation::Append,
                        quotient,
                        remainder,
                        count,
                        qptr + 1,
                        0,
                    );
                } else if current_remainder == remainder {
                    self.insert_and_shift(
                        if self.blocks.is_runend(qptr) {
                            InsertOperation::Append
                        } else {
                            InsertOperation::Insert
                        },
                        quotient,
                        remainder,
                        current_count + count,
                        runstart_index,
                        qptr - runstart_index + 1,
                    );
                } else {
                    self.insert_and_shift(
                        InsertOperation::Insert,
                        quotient,
                        remainder,
                        count,
                        runstart_index,
                        0,
                    );
                }
            }
            self.blocks.set_occupied(quotient, true);
        }
        Ok(())
    }

    fn query_by_hash(&self, hash: u64) -> u64 {
        let (quotient, remainder) = self.quotient_remainder_from_hash(hash);
        if !self.blocks.is_occupied(quotient) {
            return 0;
        }
        let mut runstart_index = self.blocks.run_start(quotient);
        if runstart_index < quotient {
            runstart_index = quotient;
        }
        // let mut current_end: u64;
        loop {
            let mut qptr = runstart_index;
            let (current_remainder, current_count) = self.blocks.decode_counter(&mut qptr);
            // current_end = self.blocks.decode_counter(
            //     runstart_index,
            //     &mut current_remainder,
            //     &mut current_count,
            // );
            if current_remainder == remainder {
                return current_count;
            }
            if self.blocks.is_runend(qptr) {
                break;
            }
            runstart_index = qptr + 1;
        }
        0
    }

    fn set_count_by_hash(&mut self, hash: u64, count: u64) -> Result<(), CqfError> {
        let (quotient, remainder) = self.quotient_remainder_from_hash(hash);
        // let runend_index = self.run_end(quotient);
        let mut runstart_index = self.blocks.run_start(quotient);
        let mut qptr = runstart_index;
        let (mut current_remainder, _) = self.blocks.decode_counter(&mut qptr);

        while current_remainder < remainder && !self.blocks.is_runend(qptr) {
            runstart_index = qptr + 1;
            (current_remainder, _) = self.blocks.decode_counter(&mut qptr);
        }
        // println!("setting");
        if current_remainder == remainder {
            if self.blocks.is_count(runstart_index + 1) {
                *self.blocks.slot_mut(runstart_index + 1) = count;
                return Ok(());
            }
            self.insert_and_shift(
                if self.blocks.is_runend(qptr) {
                    InsertOperation::Append
                } else {
                    InsertOperation::Insert
                },
                quotient,
                remainder,
                count,
                runstart_index,
                qptr - runstart_index + 1,
            );
        } else {
            return Err(CqfError::InvalidArguments); // error since we didn't find the remainder
        };

        Ok(())
    }

    fn occupied_slots(&self) -> u64 {
        self.metadata.num_occupied_slots
    }

    fn size_bytes(&self) -> u64 {
        self.metadata.total_size_bytes
    }

    fn invertable(&self) -> bool {
        self.metadata.invertable()
    }

    fn max_occupied_slots(&self) -> u64 {
        self.runtime_data.max_occupied_slots
    }

    fn quotient_remainder_from_hash(&self, hash: u64) -> (u64, Remainder) {
        let quotient = (hash >> self.metadata.remainder_bits)
            & saturating_bitmask(self.metadata.quotient_bits);
        let remainder = hash & saturating_bitmask(self.metadata.remainder_bits);
        (quotient, remainder as Remainder)
    }

    fn build_hash(&self, quotient: u64, remainder: u64) -> u64 {
        (quotient << self.metadata.remainder_bits) | remainder
    }

    fn is_file(&self) -> bool {
        self.runtime_data.file.is_some()
    }

    // Layout in memory: Metadata bytes followed by U64Blocks bytes
    fn serialize_to_bytes(&self) -> &[u8] {
        let metadata_ptr = self.metadata.0.as_ptr();
        let metadata_bytes = self.metadata.total_size_bytes;
        unsafe { std::slice::from_raw_parts(metadata_ptr.cast(), metadata_bytes as usize) }
    }
}

impl<H: BuildHasher> U64Cqf<H> {
    // Makes metadata wrapper and blocks.
    // if file is Some, mmaps the file
    // else if file is None, mmaps new empty blocks in memory.
    fn make_metadata_blocks(
        quotient_bits: u64,
        hash_bits: u64,
        invertable: bool,
        file: Option<&mut File>,
        new: bool,
    ) -> Result<(MetadataWrapper, U64Blocks), CqfError> {
        if hash_bits < quotient_bits
            || hash_bits > 64
            || hash_bits - quotient_bits > Remainder::BITS as u64
            || hash_bits.checked_sub(quotient_bits).is_none()
        {
            return Err(CqfError::InvalidArguments);
        }
        let mut metadata = Metadata::new(quotient_bits, hash_bits, invertable);
        let blocks_size = U64Blocks::bytes_needed(metadata.num_blocks as usize);
        metadata.add_size(blocks_size as u64);
        println!("metadata.total_size_bytes: {}", metadata.total_size_bytes);
        let mmap_flags;
        let fd: i32;
        let prot_flags = libc::PROT_READ | libc::PROT_WRITE;
        match file {
            Some(f) => {
                fd = f.as_raw_fd();
                mmap_flags = libc::MAP_SHARED;
                if new {
                    f.set_len(metadata.total_size_bytes)
                        .map_err(|_| CqfError::FileError)?;
                }
            }
            None => {
                fd = -1;
                mmap_flags = libc::MAP_PRIVATE | libc::MAP_ANONYMOUS;
            }
        };

        let buffer = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                metadata.total_size_bytes as usize,
                prot_flags,
                mmap_flags,
                fd,
                0,
            )
        };
        if buffer == libc::MAP_FAILED {
            println!(
                "MMAP ERROR {}, {:?}",
                std::io::Error::last_os_error()
                    .raw_os_error()
                    .expect("unreachable"),
                std::io::Error::last_os_error()
            );
            return Err(CqfError::MmapError);
        }
        let mut metadata_wrapper = MetadataWrapper::from(buffer as *mut Metadata);
        if new {
            *metadata_wrapper = metadata;
        }
        let blocks_ptr = unsafe { buffer.add(std::mem::size_of::<Metadata>()) };
        let blocks = U64Blocks::new(blocks_ptr as *mut u8, metadata_wrapper.num_blocks as usize);
        Ok((metadata_wrapper, blocks))
    }

    fn insert_and_shift(
        &mut self,
        operation: InsertOperation,
        quotient: u64,
        remainder: u64,
        count: u64,
        insert_index: u64,
        noverwrites: u64,
    ) {
        let ninserts = if count == 1 { 1 } else { 2 } - noverwrites;
        if ninserts > 0 {
            match ninserts {
                1 => {
                    let empty = self.blocks.find_first_empty_slot(insert_index);
                    self.shift_remainders(insert_index, empty - 1, 1);
                    self.shift_runends(insert_index, empty - 1, 1);
                    self.shift_counts(insert_index, empty - 1, 1);
                    for i in (((quotient / 64) + 1)..).take_while(|i| *i <= empty / 64) {
                        if empty / 64 < i {
                            break;
                        }
                        // println!("setting offset for block");
                        *self.blocks.offset_mut(i * 64) = self.blocks.offset(i * 64) + 1;
                        self.metadata.largest_offset =
                            self.metadata.largest_offset.max(self.blocks.offset(i * 64))
                    }
                }
                2 => {
                    let first = self.blocks.find_first_empty_slot(insert_index);
                    let second = self.blocks.find_first_empty_slot(first + 1);
                    self.shift_remainders(first + 1, second - 1, 1);
                    self.shift_runends(first + 1, second - 1, 1);
                    self.shift_counts(first + 1, second - 1, 1);
                    self.shift_remainders(insert_index, first - 1, 2);
                    self.shift_runends(insert_index, first - 1, 2);
                    self.shift_counts(insert_index, first - 1, 2);

                    let mut npreceding_empties = 0;
                    for i in (((quotient / 64) + 1)..).take_while(|i| *i <= second / 64) {
                        if npreceding_empties == 0 && first / 64 < i {
                            npreceding_empties += 1;
                        }
                        if npreceding_empties == 1 && second / 64 < i {
                            break;
                        }
                        /*
                        let current_offset = *self.blocks.offset_mut(i * 64);
                        *self.blocks.offset_mut(i * 64) = match current_offset.checked_add((ninserts - npreceding_empties)) {
                            None => panic!("offset addition overflow"),
                            Some(v) => v,
                        };*/

                        *self.blocks.offset_mut(i * 64) += ninserts - npreceding_empties;
                        self.metadata.largest_offset = self
                            .metadata
                            .largest_offset
                            .max(self.blocks.offset(i * SLOTS_PER_BLOCK as u64))
                    }
                }
                _ => panic!("unexpected number of inserts!"),
            }

            match operation {
                InsertOperation::InsertEmpty => {
                    if count == 1 {
                        self.blocks.set_runend(insert_index, true);
                    } else {
                        self.blocks.set_runend(insert_index, false);
                        self.blocks.set_runend(insert_index + 1, true);
                    }
                }
                InsertOperation::Append => {
                    if noverwrites == 0 {
                        self.blocks.set_runend(insert_index - 1, false);
                    }
                    if count == 1 {
                        self.blocks.set_runend(insert_index, true);
                    } else {
                        self.blocks.set_runend(insert_index, false);
                        self.blocks.set_runend(insert_index + 1, true);
                    }
                }
                InsertOperation::Insert => {
                    if count == 1 {
                        self.blocks.set_runend(insert_index, false);
                    } else {
                        self.blocks.set_runend(insert_index, false);
                        self.blocks.set_runend(insert_index + 1, false);
                    }
                }
            }
        }
        *self.blocks.slot_mut(insert_index) = remainder;
        if count != 1 {
            // if the count isn't one, put a count in the next slot
            self.blocks.set_count(insert_index + 1, true);
            *self.blocks.slot_mut(insert_index + 1) = count as Remainder;
        }
        self.metadata.num_occupied_slots += ninserts;
    }

    fn shift_remainders(&mut self, insert_index: u64, empty_slot_index: u64, distance: u64) {
        for i in (insert_index..=empty_slot_index).rev() {
            *self.blocks.slot_mut(i + distance) = *self.blocks.slot(i);
        }
    }

    fn shift_runends(&mut self, insert_index: u64, empty_slot_index: u64, distance: u64) {
        for i in (insert_index..=empty_slot_index).rev() {
            self.blocks
                .set_runend(i + distance, self.blocks.is_runend(i));
        }
    }

    fn shift_counts(&mut self, insert_index: u64, empty_slot_index: u64, distance: u64) {
        for i in (insert_index..=empty_slot_index).rev() {
            self.blocks.set_count(i + distance, self.blocks.is_count(i));
        }
    }
}

pub struct U64ConsumingIterator<H: BuildHasher> {
    cqf: U64Cqf<H>,
    current_run_start: u64,
    current_quotient: u64,
    end: u64,
    // num: u64,
}

impl<'a, H: BuildHasher> Iterator for U64RefIterator<'a, H> {
    type Item = (u64, u64);
    fn next(&mut self) -> Option<Self::Item> {
        if self.current_quotient >= self.end {
            // self.cqf.blocks.advise_normal();
            return None;
        }
        let (current_remainder, current_count) =
            self.cqf.blocks.decode_counter(&mut self.current_quotient);
        let current_hash = self
            .cqf
            .build_hash(self.current_run_start, current_remainder);
        if !self.cqf.blocks.is_runend(self.current_quotient) {
            self.current_quotient += 1;
            return Some((current_count, current_hash));
        }
        self.current_quotient += 1;
        let mut block_index = self.current_run_start as usize / SLOTS_PER_BLOCK;
        let mut next_run_slot = ffsv(
            self.cqf.blocks.occupieds_by_block(block_index),
            (self.current_run_start % SLOTS_PER_BLOCK as u64) + 1,
        )
        .unwrap_or(64);
        if next_run_slot == 64 {
            while next_run_slot == 64 && block_index < self.cqf.blocks.len() - 1 {
                block_index += 1;
                next_run_slot = ffs(self.cqf.blocks.occupieds_by_block(block_index)).unwrap_or(64);
            }
        }
        self.current_run_start = block_index as u64 * SLOTS_PER_BLOCK as u64 + next_run_slot;
        self.current_quotient = std::cmp::max(self.current_quotient, self.current_run_start);
        Some((current_count, current_hash))
    }
}

impl<H: BuildHasher> Iterator for U64ConsumingIterator<H> {
    type Item = (u64, u64);
    fn next(&mut self) -> Option<Self::Item> {
        if self.current_quotient >= self.end {
            // self.cqf.blocks.advise_normal();
            return None;
        }
        let (current_remainder, current_count) =
            self.cqf.blocks.decode_counter(&mut self.current_quotient);
        let current_hash = self
            .cqf
            .build_hash(self.current_run_start, current_remainder);
        if !self.cqf.blocks.is_runend(self.current_quotient) {
            self.current_quotient += 1;
            return Some((current_count, current_hash));
        }
        self.current_quotient += 1;
        let mut block_index = self.current_run_start as usize / SLOTS_PER_BLOCK;
        let mut next_run_slot = ffsv(
            self.cqf.blocks.occupieds_by_block(block_index),
            (self.current_run_start % SLOTS_PER_BLOCK as u64) + 1,
        )
        .unwrap_or(64);
        if next_run_slot == 64 {
            while next_run_slot == 64 && block_index < self.cqf.blocks.len() - 1 {
                block_index += 1;
                next_run_slot = ffs(self.cqf.blocks.occupieds_by_block(block_index)).unwrap_or(64);
            }
        }
        self.current_run_start = block_index as u64 * SLOTS_PER_BLOCK as u64 + next_run_slot;
        self.current_quotient = std::cmp::max(self.current_quotient, self.current_run_start);
        Some((current_count, current_hash))
    }
}

impl<H: BuildHasher> CqfIteratorImpl for U64ConsumingIterator<H> {}

pub struct U64RefIterator<'a, H: BuildHasher> {
    cqf: &'a U64Cqf<H>,
    current_run_start: u64,
    current_quotient: u64,
    end: u64,
    // num: u64,
}

impl<H: BuildHasher> U64Cqf<H> {
    pub fn iter(&self) -> U64RefIterator<H> {
        if self.metadata.num_occupied_slots == 0 {
            return U64RefIterator {
                cqf: self,
                current_run_start: 0,
                current_quotient: 1,
                end: 0,
                // num: 0,
            };
        }
        let current_quotient = self.blocks.find_first_occupied_slot();
        let num_slots = self.metadata.num_real_slots;
        U64RefIterator {
            cqf: self,
            current_run_start: current_quotient,
            current_quotient,
            end: num_slots,
            // num: 0,
        }
    }
}

impl<H: BuildHasher> IntoIterator for U64Cqf<H> {
    type Item = (u64, u64);
    type IntoIter = U64ConsumingIterator<H>;

    fn into_iter(self) -> Self::IntoIter {
        if self.metadata.num_occupied_slots == 0 {
            return Self::IntoIter {
                cqf: self,
                current_run_start: 0,
                current_quotient: 1,
                end: 0,
                // num: 0,
            };
        }
        let current_quotient = self.blocks.find_first_occupied_slot();
        let num_slots = self.metadata.num_real_slots;
        Self::IntoIter {
            cqf: self,
            current_run_start: current_quotient,
            current_quotient,
            end: num_slots,
            // num: 0,
        }
    }
}

impl<H: BuildHasher> CqfIteratorImpl for U64RefIterator<'_, H> {}

impl<H: BuildHasher> Drop for U64Cqf<H> {
    fn drop(&mut self) {
        // println!("Dropping U64Cqf");
        let metadata_ptr = self.metadata.0.as_ptr();
        let bytes = self.metadata.total_size_bytes;
        let error = unsafe { libc::munmap(metadata_ptr.cast(), bytes as usize) };
        if error != 0 {
            println!(
                "Error unmapping metadata: {} {:?}",
                error,
                std::io::Error::last_os_error()
            );
        }
    }
}
