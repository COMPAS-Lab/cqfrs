use std::fs::File;
use std::hash::{BuildHasher, Hash};

use crate::SLOTS_PER_BLOCK;

/// Owns Metadata (through a pointer)
struct MetadataWrapper(std::ptr::Unique<Metadata>);

impl MetadataWrapper {
    // FIXME: check if this invariant is correct
    /// # Safety
    /// The pointer must be valid for the lifetime of the wrapper and must be non-null.
    pub unsafe fn from_raw(ptr: *mut Metadata) -> Result<Self, &'static str> {
        std::ptr::Unique::new(ptr)
            .ok_or("Called with null pointer")
            .map(|unique| Self(unique))
    }
    pub fn as_ref(&self) -> &Metadata {
        unsafe { self.0.as_ref() }
    }
    pub fn as_mut(&mut self) -> &mut Metadata {
        unsafe { self.0.as_mut() }
    }
    pub fn as_ptr(&self) -> *const Metadata {
        self.0.as_ptr()
    }
    pub fn as_mut_ptr(&mut self) -> *mut Metadata {
        self.0.as_ptr()
    }
}

impl From<*mut Metadata> for MetadataWrapper {
    /// Create a MetadataWrapper from a *mut Metadata.
    /// The metadata pointer passed in must be valid, this takes ownership of
    /// the metadata object.
    fn from(metadata: *mut Metadata) -> Self {
        unsafe { MetadataWrapper::from_raw(metadata).expect("null pointer") }
    }
}

#[repr(C)]
#[derive(Debug, Copy, Clone)]
/// Metadata for the CQF
struct Metadata {
    pub total_size_bytes: u64,
    pub num_real_slots: u64,
    pub num_occupied_slots: u64,
    pub num_blocks: u64,
    pub quotient_bits: u64,
    pub remainder_bits: u64,
    pub invertable: u64,
    pub largest_offset: u64,
    pub largest_possible_offset: u64,
}

impl std::ops::Deref for MetadataWrapper {
    type Target = Metadata;
    fn deref(&self) -> &Self::Target {
        self.as_ref()
    }
}

impl std::ops::DerefMut for MetadataWrapper {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.as_mut()
    }
}

impl Metadata {
    fn new(quotient_bits: u64, hash_bits: u64, invertable: bool) -> Self {
        let num_slots: u64 = 1u64 << quotient_bits;
        let num_real_slots = (num_slots as f64 + 10_f64 * (num_slots as f64).sqrt()) as u64;
        let num_blocks = (num_real_slots + SLOTS_PER_BLOCK as u64 - 1) / SLOTS_PER_BLOCK as u64;
        let remainder_bits = hash_bits - quotient_bits;
        let invertable = if invertable { 1 } else { 0 };
        let total_size_bytes = std::mem::size_of::<Metadata>() as u64;
        let largest_offset = 0;
        let largest_possible_offset = ((num_slots as f64).sqrt()) as u64;
        Self {
            total_size_bytes,
            num_real_slots,
            num_occupied_slots: 0,
            num_blocks,
            quotient_bits,
            remainder_bits,
            invertable,
            largest_offset,
            largest_possible_offset,
        }
    }

    fn add_size(&mut self, size: u64) {
        self.total_size_bytes += size;
    }

    fn invertable(&self) -> bool {
        self.invertable == 1
    }
}

/// RuntimeData for the CQF
struct RuntimeData<H: BuildHasher> {
    pub file: Option<File>,
    pub hasher: H,
    pub max_occupied_slots: u64,
}

impl<H: BuildHasher> RuntimeData<H> {
    fn new(file: Option<File>, hasher: H, num_real_slots: u64) -> Self {
        Self {
            file,
            hasher,
            max_occupied_slots: ((num_real_slots as f64) * 0.80) as u64,
        }
    }
}

#[derive(Debug)]
pub enum CqfError {
    InvalidArguments,
    FileError,
    MmapError,
    InvalidFile,
    InvalidSize,
    Filled,
}

pub trait CountingQuotientFilter: IntoIterator + Sized {
    type Hasher: BuildHasher;
    type Remainder: Copy + Clone + Default + std::fmt::Debug + Into<u64>;

    /// Makes a new in-memory CQF.
    fn new(
        quotient_bits: u64,
        hash_bits: u64,
        invertable: bool,
        hasher: Self::Hasher,
    ) -> Result<Self, CqfError>;

    /// Makes a new on-disk CQF, using mmap on file.
    fn new_file(
        quotient_bits: u64,
        hash_bits: u64,
        invertable: bool,
        hasher: Self::Hasher,
        file: File,
    ) -> Result<Self, CqfError>;

    /// Loads a file as a CQF, using mmap.
    fn open_file(hasher: Self::Hasher, file: File) -> Result<Self, CqfError>;

    /// Inserts an item-count pair into the CQF.
    /// Returns Ok(()) on successful insert, or a CqfError.
    fn insert<Item: Hash>(&mut self, item: Item, count: u64) -> Result<(), CqfError> {
        let hash = self.calc_hash(item);
        self.insert_by_hash(hash, count)
    }

    /// Returns the (count, hash) of item.
    fn query<Item: Hash>(&self, item: Item) -> (u64, u64) {
        let hash = self.calc_hash(item);
        (self.query_by_hash(hash), hash)
    }

    /// Sets the count of item in the CQF.
    /// Inserts item into the CQF if it was not already present.
    /// Returns Ok(()) on success, or a CqfError.
    fn set_count<Item: Hash>(&mut self, item: Item, count: u64) -> Result<(), CqfError> {
        if self.occupied_slots() >= self.max_occupied_slots() {
            return Err(CqfError::Filled);
        }
        let hash = self.calc_hash(item);
        // self.set_count_by_hash(hash, count)
        match self.set_count_by_hash(hash, count) {
            Ok(_) => Ok(()),
            Err(_) => self.insert_by_hash(hash, count),
        }
    }

    fn quotient_bits(&self) -> u64;

    fn remainder_bits(&self) -> u64;

    // fn set_count_cb<Item: Hash, F: FnMut(u64) -> u64>(&mut self, item: Item, count: u64, cb: F) -> Result<u64, CqfError>;

    // fn iter(&self) -> Self::CqfIterator;

    fn occupied_slots(&self) -> u64;

    fn size_bytes(&self) -> u64;

    fn invertable(&self) -> bool;

    fn insert_by_hash(&mut self, hash: u64, count: u64) -> Result<(), CqfError>;

    fn query_by_hash(&self, hash: u64) -> u64;

    fn set_count_by_hash(&mut self, hash: u64, count: u64) -> Result<(), CqfError>;

    fn max_occupied_slots(&self) -> u64;

    fn quotient_remainder_from_hash(&self, hash: u64) -> (u64, Self::Remainder);

    fn calc_hash<Item: Hash>(&self, item: Item) -> u64;

    fn merge_insert(
        &mut self,
        current_quotient: &mut u64,
        new_quotient: u64,
        next_quotient: u64,
        new_remainder: u64,
        count: u64,
    );

    fn build_hash(&self, quotient: u64, remainder: u64) -> u64;

    fn is_file(&self) -> bool;

    /// Returns the slice of bytes representing the CQF.
    fn serialize_to_bytes(&self) -> &[u8];
}

// fn set_count_by_hash_cb<F: FnMut(u64) -> u64>(&mut self, hash: u64, count: u64, cb: F) -> Result<u64, CqfError>;
// fn check_compatibility(a: Self, b: Self) -> bool;

// fn merge<IterTypeA: CqfIteratorImpl, IterTypeB: CqfIteratorImpl, MergeIntoT: Cqf>(a: IterType, b: IterType) -> Result<Self, CqfError>;

//     fn merge_cb<IterType: CqfIteratorImpl, T: CqfMergeClosure>(a: IterType, b: IterType, cb: &mut T) -> Result<Self, CqfError>;

//     fn merge_file_cb<IterType: CqfIteratorImpl, T: CqfMergeClosure>(a: IterType, b: IterType, file: File, cb: &mut T) -> Result<Self, CqfError>;
// trait CountingQuotientFilterInternal: CountingQuotientFilter {

// }

mod u64_cqf;
pub use u64_cqf::*;
mod u32_cqf;
pub use u32_cqf::*;

pub trait CqfIteratorImpl: Iterator<Item = (u64, u64)> {}

pub trait CqfMergeClosure: Sized {
    fn merge_cb<CqfT: CountingQuotientFilter>(
        &mut self,
        new_cqf: &mut CqfT,
        a_quotient: u64,
        a_remainder: u64,
        a_count: Option<&mut u64>,
        b_quotient: u64,
        b_remainder: u64,
        b_count: Option<&mut u64>,
    );
}

pub struct CqfMerge();

impl CqfMerge {
    pub fn merge<T: CountingQuotientFilter>(
        mut iter_a: impl CqfIteratorImpl,
        mut iter_b: impl CqfIteratorImpl,
        new_cqf: &mut T,
    ) {
        let mut current_a = iter_a.next();
        let mut current_b = iter_b.next();
        let mut merged_cqf_current_quotient = 0u64;
        while current_a.is_some() && current_b.is_some() {
            let Some(&(a_count, a_hash)) = current_a.as_ref() else {
                unreachable!()
            };
            let Some(&(b_count, b_hash)) = current_b.as_ref() else {
                unreachable!()
            };

            let (a_quotient, a_remainder) = {
                let av = new_cqf.quotient_remainder_from_hash(a_hash);
                (av.0, av.1.into())
            };
            let (b_quotient, b_remainder) = {
                let bv = new_cqf.quotient_remainder_from_hash(b_hash);
                (bv.0, bv.1.into())
            };

            let insert_quotient: u64;
            let insert_remainder: u64;
            let insert_count: u64;
            if a_quotient == b_quotient && a_remainder == b_remainder {
                insert_count = a_count + b_count;
                insert_quotient = a_quotient;
                insert_remainder = a_remainder;
                current_a = iter_a.next();
                current_b = iter_b.next();
            } else if a_quotient < b_quotient
                || (a_quotient == b_quotient && a_remainder < b_remainder)
            {
                insert_count = a_count;
                insert_quotient = a_quotient;
                insert_remainder = a_remainder;
                current_a = iter_a.next();
                // current_b = Some(b_val);
            } else {
                insert_count = b_count;
                insert_quotient = b_quotient;
                insert_remainder = b_remainder;
                current_b = iter_b.next();
            }
            let next_quotient_ = Self::next_quotient(
                new_cqf,
                current_a.as_ref(),
                current_b.as_ref(),
                insert_quotient,
            );

            new_cqf.merge_insert(
                &mut merged_cqf_current_quotient,
                insert_quotient,
                next_quotient_,
                insert_remainder,
                insert_count,
            );
        }
        while current_a.is_some() {
            let Some(&(a_count, a_hash)) = current_a.as_ref() else {
                unreachable!()
            };
            let insert_count = a_count;
            let (insert_quotient, insert_remainder) = {
                let av = new_cqf.quotient_remainder_from_hash(a_hash);
                (av.0, av.1.into())
            };
            current_a = iter_a.next();
            let next_quotient_ =
                Self::next_quotient(new_cqf, current_a.as_ref(), None, insert_quotient);
            new_cqf.merge_insert(
                &mut merged_cqf_current_quotient,
                insert_quotient,
                next_quotient_,
                insert_remainder,
                insert_count,
            );
        }
        while current_b.is_some() {
            let Some(&(b_count, b_hash)) = current_b.as_ref() else {
                unreachable!()
            };
            let insert_count = b_count;
            let (insert_quotient, insert_remainder) = {
                let av = new_cqf.quotient_remainder_from_hash(b_hash);
                (av.0, av.1.into())
            };
            current_b = iter_b.next();
            let next_quotient_ =
                Self::next_quotient(new_cqf, current_b.as_ref(), None, insert_quotient);
            new_cqf.merge_insert(
                &mut merged_cqf_current_quotient,
                insert_quotient,
                next_quotient_,
                insert_remainder,
                insert_count,
            );
        }
    }

    pub fn merge_by<T: CountingQuotientFilter>(
        mut iter_a: impl CqfIteratorImpl,
        mut iter_b: impl CqfIteratorImpl,
        new_cqf: &mut T,
        closure: &mut impl CqfMergeClosure,
    ) {
        let mut current_a = iter_a.next();
        let mut current_b = iter_b.next();
        let mut merged_cqf_current_quotient = 0u64;
        while current_a.is_some() && current_b.is_some() {
            //  let mut is_now = false;

            let insert_quotient: u64;
            let insert_remainder: u64;
            let insert_count: u64;
            let next_quotient_: u64;
            {
                let (a_quotient, a_remainder): (u64, u64);
                let (b_quotient, b_remainder): (u64, u64);
                let mut a_count;
                let mut b_count;
                {
                    let a_val = current_a.as_ref().unwrap();
                    let b_val = current_b.as_ref().unwrap();
                    let av = new_cqf.quotient_remainder_from_hash(a_val.1);
                    (a_quotient, a_remainder) = (av.0, av.1.into());
                    let bv = new_cqf.quotient_remainder_from_hash(b_val.1);
                    (b_quotient, b_remainder) = (bv.0, bv.1.into());
                    a_count = a_val.0;
                    b_count = b_val.0;
                }

                closure.merge_cb(
                    new_cqf,
                    a_quotient,
                    a_remainder,
                    Some(&mut a_count),
                    b_quotient,
                    b_remainder,
                    Some(&mut b_count),
                );
                if a_quotient == b_quotient && a_remainder == b_remainder {
                    insert_count = a_count + b_count;
                    insert_quotient = a_quotient;
                    insert_remainder = a_remainder;
                    current_a = iter_a.next();
                    current_b = iter_b.next();
                } else if a_quotient < b_quotient
                    || (a_quotient == b_quotient && a_remainder < b_remainder)
                {
                    insert_count = a_count;
                    insert_quotient = a_quotient;
                    insert_remainder = a_remainder;
                    current_a = iter_a.next();
                    // current_b = Some(b_val);
                } else {
                    insert_count = b_count;
                    insert_quotient = b_quotient;
                    insert_remainder = b_remainder;
                    current_b = iter_b.next();
                }
                next_quotient_ = Self::next_quotient(
                    new_cqf,
                    current_a.as_ref(),
                    current_b.as_ref(),
                    insert_quotient,
                );
            }
            new_cqf.merge_insert(
                &mut merged_cqf_current_quotient,
                insert_quotient,
                next_quotient_,
                insert_remainder,
                insert_count,
            );
        }
        while current_a.is_some() {
            let Some(&(a_count, a_hash)) = current_a.as_ref() else {
                unreachable!()
            };
            let mut insert_count = a_count;
            let (insert_quotient, insert_remainder) = {
                let av = new_cqf.quotient_remainder_from_hash(a_hash);
                (av.0, av.1.into())
            };
            current_a = iter_a.next();
            let next_quotient_ =
                Self::next_quotient(new_cqf, current_a.as_ref(), None, insert_quotient);
            closure.merge_cb(
                new_cqf,
                insert_quotient,
                insert_remainder,
                Some(&mut insert_count),
                u64::MAX,
                u64::MAX,
                None,
            );

            new_cqf.merge_insert(
                &mut merged_cqf_current_quotient,
                insert_quotient,
                next_quotient_,
                insert_remainder,
                insert_count,
            );
        }
        while current_b.is_some() {
            let Some(&(b_count, b_hash)) = current_b.as_ref() else {
                unreachable!()
            };
            let insert_quotient: u64;
            let insert_remainder: u64;
            let mut insert_count = b_count;
            {
                let av = new_cqf.quotient_remainder_from_hash(b_hash);
                (insert_quotient, insert_remainder) = (av.0, av.1.into());
            }
            current_b = iter_b.next();
            let next_quotient_ =
                Self::next_quotient(new_cqf, current_b.as_ref(), None, insert_quotient);
            closure.merge_cb(
                new_cqf,
                u64::MAX,
                u64::MAX,
                None,
                insert_quotient,
                insert_remainder,
                Some(&mut insert_count),
            );

            new_cqf.merge_insert(
                &mut merged_cqf_current_quotient,
                insert_quotient,
                next_quotient_,
                insert_remainder,
                insert_count,
            );
        }
    }

    fn next_quotient(
        new_cqf: &impl CountingQuotientFilter,
        a: Option<&(u64, u64)>,
        b: Option<&(u64, u64)>,
        current_quotient: u64,
    ) -> u64 {
        match (a, b) {
            (Some(&(_, a_hash)), Some(&(_, b_hash))) => {
                let a_quotient = new_cqf.quotient_remainder_from_hash(a_hash).0;
                let b_quotient = new_cqf.quotient_remainder_from_hash(b_hash).0;
                std::cmp::min(a_quotient, b_quotient)
            }
            (Some(&(_, a_hash)), None) => new_cqf.quotient_remainder_from_hash(a_hash).0,
            (None, Some(&(_, b_hash))) => new_cqf.quotient_remainder_from_hash(b_hash).0,
            (None, None) => current_quotient - 1,
        }
    }
}

pub struct ZippedCqfIter<A: CqfIteratorImpl, B: CqfIteratorImpl> {
    iter_a: A,
    iter_b: B,
    current_a: Option<(u64, u64)>,
    current_b: Option<(u64, u64)>,
}

impl<A: CqfIteratorImpl, B: CqfIteratorImpl> ZippedCqfIter<A, B> {
    pub fn new(mut iter_a: A, mut iter_b: B) -> Self {
        let current_a = iter_a.next();
        let current_b = iter_b.next();
        Self {
            iter_a,
            iter_b,
            current_a,
            current_b,
        }
    }
}

pub enum EitherOrBoth<A, B = A> {
    Left(A),
    Right(B),
    Both(A, B),
}

impl<A: CqfIteratorImpl, B: CqfIteratorImpl> Iterator for ZippedCqfIter<A, B> {
    type Item = EitherOrBoth<(u64, u64)>;
    fn next(&mut self) -> Option<Self::Item> {
        match (self.current_a, self.current_b) {
            (None, None) => None,
            (Some(a_val), None) => {
                self.current_a = self.iter_a.next();
                Some(EitherOrBoth::Left(a_val))
            }
            (None, Some(b_val)) => {
                self.current_b = self.iter_b.next();
                Some(EitherOrBoth::Right(b_val))
            }
            (Some(a_val), Some(b_val)) => {
                let a_hash = a_val.1;
                let b_hash = b_val.1;
                if a_hash < b_hash {
                    self.current_a = self.iter_a.next();
                    Some(EitherOrBoth::Left(a_val))
                } else if a_hash > b_hash {
                    self.current_b = self.iter_b.next();
                    Some(EitherOrBoth::Right(b_val))
                } else {
                    self.current_a = self.iter_a.next();
                    self.current_b = self.iter_b.next();
                    Some(EitherOrBoth::Both(a_val, b_val))
                }
            }
        }
    }
}
