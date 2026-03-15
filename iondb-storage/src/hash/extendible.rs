//! Extendible hashing storage engine.
//!
//! Directory-based design with split-on-overflow semantics. Best for workloads
//! with unpredictable key distribution. The directory doubles when a bucket at
//! max local depth overflows.
//!
//! # Buffer layout
//!
//! ```text
//! [Page 0: Metadata] [Page 1: Directory] [Page 2+: Buckets]
//! ```

// Hash engine methods have uniform error conditions (page bounds / capacity).
#![allow(clippy::missing_errors_doc)]

use super::bucket;
use super::hash_key;
use iondb_core::endian;
use iondb_core::error::{Error, Result};
use iondb_core::page::{PageHeader, PageType, PAGE_CHECKSUM_SIZE, PAGE_HEADER_SIZE};
use iondb_core::traits::storage_engine::{EngineStats, StorageEngine};
use iondb_core::types::{PageId, MIN_PAGE_SIZE};

// Metadata page offsets (after 16-byte header)
const META_GLOBAL_DEPTH: usize = PAGE_HEADER_SIZE; // u16
const META_PAGE_COUNT: usize = META_GLOBAL_DEPTH + 2; // u32
const META_KEY_COUNT: usize = META_PAGE_COUNT + 4; // u64
const META_DATA_BYTES: usize = META_KEY_COUNT + 8; // u64

// Directory page: header(16) + entries (u32 each) + CRC(4)
const DIR_ENTRIES_START: usize = PAGE_HEADER_SIZE;

const MAX_BUF: usize = u16::MAX as usize;

/// Extendible hashing storage engine.
///
/// Operates on a caller-provided buffer divided into fixed-size pages.
pub struct ExtendibleHashEngine<'a> {
    buf: &'a mut [u8],
    page_size: usize,
}

impl<'a> ExtendibleHashEngine<'a> {
    /// Create a new extendible hash engine.
    ///
    /// Needs at least 4 pages (metadata + directory + 2 initial buckets).
    pub fn new(buf: &'a mut [u8], page_size: usize) -> Option<Self> {
        if page_size < MIN_PAGE_SIZE
            || !page_size.is_power_of_two()
            || page_size > MAX_BUF
            || buf.len() < page_size * 4
        {
            return None;
        }
        let mut eng = Self { buf, page_size };
        eng.init().ok()?;
        Some(eng)
    }

    fn page(&self, id: PageId) -> Result<&[u8]> {
        let off = (id as usize)
            .checked_mul(self.page_size)
            .ok_or(Error::PageError)?;
        self.buf
            .get(off..off + self.page_size)
            .ok_or(Error::PageError)
    }

    fn page_mut(&mut self, id: PageId) -> Result<&mut [u8]> {
        let ps = self.page_size;
        let off = (id as usize).checked_mul(ps).ok_or(Error::PageError)?;
        self.buf.get_mut(off..off + ps).ok_or(Error::PageError)
    }

    // ── metadata ─────────────────────────────────────────────────────────

    fn init(&mut self) -> Result<()> {
        // Page 0: metadata
        let p0 = self.page_mut(0)?;
        PageHeader::new(PageType::Free, 0).encode(p0)?;
        endian::write_u16_le(&mut p0[META_GLOBAL_DEPTH..], 1)?; // depth 1 → 2 buckets
        endian::write_u32_le(&mut p0[META_PAGE_COUNT..], 4)?;
        endian::write_u64_le(&mut p0[META_KEY_COUNT..], 0)?;
        endian::write_u64_le(&mut p0[META_DATA_BYTES..], 0)?;

        // Page 1: directory with 2 entries → bucket pages 2, 3
        let p1 = self.page_mut(1)?;
        PageHeader::new(PageType::HashDirectory, 1).encode(p1)?;
        endian::write_u32_le(&mut p1[DIR_ENTRIES_START..], 2)?; // bucket for hash & 0 == 0
        endian::write_u32_le(&mut p1[DIR_ENTRIES_START + 4..], 3)?; // bucket for hash & 1 == 1

        // Pages 2, 3: initial buckets
        let p2 = self.page_mut(2)?;
        bucket::bucket_init(p2, 2, 1)?;
        let p3 = self.page_mut(3)?;
        bucket::bucket_init(p3, 3, 1)
    }

    fn global_depth(&self) -> Result<u16> {
        endian::read_u16_le(&self.page(0)?[META_GLOBAL_DEPTH..])
    }

    fn set_global_depth(&mut self, d: u16) -> Result<()> {
        let p = self.page_mut(0)?;
        endian::write_u16_le(&mut p[META_GLOBAL_DEPTH..], d)
    }

    fn page_count(&self) -> Result<u32> {
        endian::read_u32_le(&self.page(0)?[META_PAGE_COUNT..])
    }

    fn set_page_count(&mut self, c: u32) -> Result<()> {
        let p = self.page_mut(0)?;
        endian::write_u32_le(&mut p[META_PAGE_COUNT..], c)
    }

    fn alloc_page(&mut self) -> Result<PageId> {
        let pc = self.page_count()?;
        if pc as usize >= self.buf.len() / self.page_size {
            return Err(Error::CapacityExhausted);
        }
        self.set_page_count(pc + 1)?;
        Ok(pc)
    }

    fn read_key_count(&self) -> Result<u64> {
        endian::read_u64_le(&self.page(0)?[META_KEY_COUNT..])
    }

    fn add_key_count(&mut self, delta: i64) -> Result<()> {
        let c = self.read_key_count()?;
        let n = if delta >= 0 {
            // Sign-loss is safe: delta is non-negative per the branch guard.
            #[allow(clippy::cast_sign_loss)]
            let d = delta as u64;
            c.wrapping_add(d)
        } else {
            c.wrapping_sub(delta.unsigned_abs())
        };
        let p = self.page_mut(0)?;
        endian::write_u64_le(&mut p[META_KEY_COUNT..], n)
    }

    fn read_data_bytes(&self) -> Result<u64> {
        endian::read_u64_le(&self.page(0)?[META_DATA_BYTES..])
    }

    fn add_data_bytes(&mut self, delta: i64) -> Result<()> {
        let c = self.read_data_bytes()?;
        let n = if delta >= 0 {
            // Sign-loss is safe: delta is non-negative per the branch guard.
            #[allow(clippy::cast_sign_loss)]
            let d = delta as u64;
            c.wrapping_add(d)
        } else {
            c.wrapping_sub(delta.unsigned_abs())
        };
        let p = self.page_mut(0)?;
        endian::write_u64_le(&mut p[META_DATA_BYTES..], n)
    }

    // ── directory ────────────────────────────────────────────────────────

    fn dir_get(&self, idx: usize) -> Result<PageId> {
        let off = DIR_ENTRIES_START + idx * 4;
        endian::read_u32_le(&self.page(1)?[off..])
    }

    fn dir_set(&mut self, idx: usize, page_id: PageId) -> Result<()> {
        let off = DIR_ENTRIES_START + idx * 4;
        let p = self.page_mut(1)?;
        endian::write_u32_le(&mut p[off..], page_id)
    }

    fn bucket_for_key(&self, key: &[u8]) -> Result<PageId> {
        let h = hash_key(key);
        let gd = self.global_depth()?;
        let idx = (h as usize) & ((1 << gd) - 1);
        self.dir_get(idx)
    }

    /// Maximum directory entries that fit in a single page.
    fn max_dir_entries(&self) -> usize {
        (self.page_size - PAGE_HEADER_SIZE - PAGE_CHECKSUM_SIZE) / 4
    }

    // ── split ────────────────────────────────────────────────────────────

    fn split_bucket(&mut self, bucket_id: PageId) -> Result<()> {
        let bp = self.page(bucket_id)?;
        let ld = bucket::bucket_local_depth(bp)?;
        let gd = self.global_depth()?;

        if ld == gd {
            // Need to double the directory
            let new_entries = 1usize << (gd + 1);
            if new_entries > self.max_dir_entries() {
                return Err(Error::CapacityExhausted);
            }
            // Double: dir[i + old_count] = dir[i] for all existing entries
            let old_count = 1usize << gd;
            for i in (0..old_count).rev() {
                let pid = self.dir_get(i)?;
                self.dir_set(i + old_count, pid)?;
                // dir[i] stays the same
            }
            self.set_global_depth(gd + 1)?;
        }

        let split_depth = ld + 1;
        let new_id = self.alloc_page()?;
        let new_page = self.page_mut(new_id)?;
        bucket::bucket_init(new_page, new_id, split_depth)?;

        // Update local depth of old bucket
        let old_page = self.page_mut(bucket_id)?;
        bucket::bucket_set_local_depth(old_page, split_depth)?;

        // Collect entries from old bucket, redistribute
        let count = bucket::bucket_count(self.page(bucket_id)?)?;
        let cur_gd = self.global_depth()?;
        let mask = (1u32 << split_depth) - 1;

        // Determine which hash bit distinguishes old vs new
        // Old bucket keeps entries where (hash & mask) matches its directory index
        let new_bit = 1u32 << ld;

        // Collect all entries (keys + values) from old bucket
        let mut keys: [[u8; 256]; 32] = [[0u8; 256]; 32];
        let mut vals: [[u8; 256]; 32] = [[0u8; 256]; 32];
        let mut klens = [0usize; 32];
        let mut vlens = [0usize; 32];

        let bp = self.page(bucket_id)?;
        for i in 0..count {
            let k = bucket::bucket_key_at(bp, i)?;
            let v = bucket::bucket_value_at(bp, i)?;
            klens[i] = k.len();
            vlens[i] = v.len();
            keys[i][..k.len()].copy_from_slice(k);
            vals[i][..v.len()].copy_from_slice(v);
        }

        // Reinitialise old bucket
        let old = self.page_mut(bucket_id)?;
        bucket::bucket_init(old, bucket_id, split_depth)?;

        // Redistribute entries
        for i in 0..count {
            let h = hash_key(&keys[i][..klens[i]]) & mask;
            let target = if h & new_bit != 0 { new_id } else { bucket_id };
            let tp = self.page(target)?;
            let pos = match bucket::bucket_search(tp, &keys[i][..klens[i]])? {
                Ok(p) | Err(p) => p,
            };
            let tp_m = self.page_mut(target)?;
            bucket::bucket_insert_at(tp_m, pos, &keys[i][..klens[i]], &vals[i][..vlens[i]])?;
        }

        // Update directory: all entries that pointed to old bucket and have the
        // new bit set should now point to new bucket
        let dir_count = 1usize << cur_gd;
        for i in 0..dir_count {
            let pid = self.dir_get(i)?;
            // Directory index fits in u32 (max entries bounded by page size).
            #[allow(clippy::cast_possible_truncation)]
            let i_u32 = i as u32;
            if pid == bucket_id && (i_u32 & new_bit) != 0 {
                self.dir_set(i, new_id)?;
            }
        }

        Ok(())
    }
}

impl StorageEngine for ExtendibleHashEngine<'_> {
    fn get(&self, key: &[u8]) -> Result<Option<&[u8]>> {
        let bid = self.bucket_for_key(key)?;
        let bp = self.page(bid)?;
        match bucket::bucket_search(bp, key)? {
            Ok(i) => Ok(Some(bucket::bucket_value_at(bp, i)?)),
            Err(_) => Ok(None),
        }
    }

    // Key/value lengths are bounded by page size and will never exceed i64::MAX.
    #[allow(clippy::cast_possible_wrap)]
    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        let bid = self.bucket_for_key(key)?;

        // Check for existing key
        let bp = self.page(bid)?;
        if let Ok(i) = bucket::bucket_search(bp, key)? {
            let old_sz = bucket::bucket_entry_size(bp, i)?;
            let bm = self.page_mut(bid)?;
            bucket::bucket_delete_at(bm, i)?;
            let bp2 = self.page(bid)?;
            if bucket::bucket_has_space(bp2, key.len(), value.len())? {
                let pos = match bucket::bucket_search(bp2, key)? {
                    Ok(p) | Err(p) => p,
                };
                let bm2 = self.page_mut(bid)?;
                bucket::bucket_insert_at(bm2, pos, key, value)?;
                let delta = (key.len() + value.len()) as i64 - old_sz as i64;
                return self.add_data_bytes(delta);
            }
            // Need split even for update
            self.split_bucket(bid)?;
            let new_bid = self.bucket_for_key(key)?;
            let bp3 = self.page(new_bid)?;
            let pos = match bucket::bucket_search(bp3, key)? {
                Ok(p) | Err(p) => p,
            };
            let bm3 = self.page_mut(new_bid)?;
            bucket::bucket_insert_at(bm3, pos, key, value)?;
            let delta = (key.len() + value.len()) as i64 - old_sz as i64;
            return self.add_data_bytes(delta);
        }

        // New key
        let bp = self.page(bid)?;
        if bucket::bucket_has_space(bp, key.len(), value.len())? {
            let pos = match bucket::bucket_search(bp, key)? {
                Ok(p) | Err(p) => p,
            };
            let bm = self.page_mut(bid)?;
            bucket::bucket_insert_at(bm, pos, key, value)?;
            self.add_key_count(1)?;
            return self.add_data_bytes((key.len() + value.len()) as i64);
        }

        // Split and retry
        self.split_bucket(bid)?;
        let new_bid = self.bucket_for_key(key)?;
        let bp2 = self.page(new_bid)?;
        if !bucket::bucket_has_space(bp2, key.len(), value.len())? {
            return Err(Error::CapacityExhausted);
        }
        let pos = match bucket::bucket_search(bp2, key)? {
            Ok(p) | Err(p) => p,
        };
        let bm2 = self.page_mut(new_bid)?;
        bucket::bucket_insert_at(bm2, pos, key, value)?;
        self.add_key_count(1)?;
        self.add_data_bytes((key.len() + value.len()) as i64)
    }

    // Key/value lengths are bounded by page size and will never exceed i64::MAX.
    #[allow(clippy::cast_possible_wrap)]
    fn delete(&mut self, key: &[u8]) -> Result<bool> {
        let bid = self.bucket_for_key(key)?;
        let bp = self.page(bid)?;
        match bucket::bucket_search(bp, key)? {
            Ok(i) => {
                let sz = bucket::bucket_entry_size(bp, i)?;
                let bm = self.page_mut(bid)?;
                bucket::bucket_delete_at(bm, i)?;
                self.add_key_count(-1)?;
                self.add_data_bytes(-(sz as i64))?;
                Ok(true)
            }
            Err(_) => Ok(false),
        }
    }

    fn flush(&mut self) -> Result<()> {
        Ok(())
    }

    fn stats(&self) -> EngineStats {
        EngineStats {
            key_count: self.read_key_count().unwrap_or(0),
            data_bytes: self.read_data_bytes().unwrap_or(0),
            page_count: self.page_count().unwrap_or(1),
        }
    }
}

#[cfg(test)]
// Tests use unwrap for brevity; panics are acceptable in test code.
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn make(buf: &mut [u8]) -> ExtendibleHashEngine<'_> {
        ExtendibleHashEngine::new(buf, 128).unwrap()
    }

    #[test]
    fn new_valid() {
        let mut buf = [0u8; 2048];
        assert!(ExtendibleHashEngine::new(&mut buf, 128).is_some());
    }

    #[test]
    fn new_invalid() {
        let mut buf = [0u8; 128];
        assert!(ExtendibleHashEngine::new(&mut buf, 128).is_none()); // need 4 pages
    }

    #[test]
    fn put_and_get() {
        let mut buf = [0u8; 4096];
        let mut e = make(&mut buf);
        assert_eq!(e.put(b"hello", b"world"), Ok(()));
        assert_eq!(e.get(b"hello"), Ok(Some(b"world".as_slice())));
    }

    #[test]
    fn get_missing() {
        let mut buf = [0u8; 2048];
        let e = make(&mut buf);
        assert_eq!(e.get(b"nope"), Ok(None));
    }

    #[test]
    fn put_overwrite() {
        let mut buf = [0u8; 4096];
        let mut e = make(&mut buf);
        assert_eq!(e.put(b"k", b"v1"), Ok(()));
        assert_eq!(e.put(b"k", b"v2"), Ok(()));
        assert_eq!(e.get(b"k"), Ok(Some(b"v2".as_slice())));
        assert_eq!(e.stats().key_count, 1);
    }

    #[test]
    fn delete_existing() {
        let mut buf = [0u8; 4096];
        let mut e = make(&mut buf);
        assert_eq!(e.put(b"k", b"v"), Ok(()));
        assert_eq!(e.delete(b"k"), Ok(true));
        assert_eq!(e.get(b"k"), Ok(None));
    }

    #[test]
    fn delete_missing() {
        let mut buf = [0u8; 2048];
        let mut e = make(&mut buf);
        assert_eq!(e.delete(b"nope"), Ok(false));
    }

    #[test]
    fn many_keys_with_splits() {
        // Large buffer needed to exercise multiple directory doublings.
        #[allow(clippy::large_stack_arrays)]
        let mut buf = [0u8; 65535];
        let mut e = ExtendibleHashEngine::new(&mut buf, 256).unwrap();
        for i in 0u16..50 {
            let k = i.to_be_bytes();
            assert_eq!(e.put(&k, &k), Ok(()), "insert {i} failed");
        }
        for i in 0u16..50 {
            let k = i.to_be_bytes();
            assert_eq!(e.get(&k), Ok(Some(k.as_slice())), "get {i} failed");
        }
        assert_eq!(e.stats().key_count, 50);
    }

    #[test]
    fn stats_accuracy() {
        let mut buf = [0u8; 4096];
        let mut e = make(&mut buf);
        assert_eq!(e.stats().key_count, 0);
        assert_eq!(e.put(b"ab", b"cd"), Ok(()));
        assert_eq!(e.stats().key_count, 1);
        assert_eq!(e.stats().data_bytes, 4);
    }
}
