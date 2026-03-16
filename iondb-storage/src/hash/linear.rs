//! Linear hashing storage engine.
//!
//! Deterministic split order with controlled load factor. Best for steady-growth
//! workloads with predictable insertion rates. Splits happen at a controlled
//! pointer position rather than on overflow.
//!
//! # Buffer layout
//!
//! ```text
//! [Page 0: Metadata] [Page 1+: Buckets (page_id = bucket_index + 1)]
//! ```

// Hash engine methods have uniform error conditions (page bounds / capacity).
#![allow(clippy::missing_errors_doc)]

use super::bucket;
use super::hash_key;
use iondb_core::endian;
use iondb_core::error::{Error, Result};
use iondb_core::page::{PageHeader, PageType, PAGE_HEADER_SIZE};
use iondb_core::traits::storage_engine::{EngineStats, StorageEngine};
use iondb_core::types::{PageId, MIN_PAGE_SIZE};

// Metadata offsets
const META_LEVEL: usize = PAGE_HEADER_SIZE; // u16 — current level L
const META_SPLIT_PTR: usize = META_LEVEL + 2; // u32 — split pointer p
const META_INITIAL_BUCKETS: usize = META_SPLIT_PTR + 4; // u32 — N (initial bucket count)
const META_BUCKET_COUNT: usize = META_INITIAL_BUCKETS + 4; // u32 — total active buckets
const META_PAGE_COUNT: usize = META_BUCKET_COUNT + 4; // u32
const META_KEY_COUNT: usize = META_PAGE_COUNT + 4; // u64
const META_DATA_BYTES: usize = META_KEY_COUNT + 8; // u64

/// Default load factor threshold (numerator / 256). 75% → 192/256.
const LOAD_THRESHOLD_NUM: u64 = 192;
const LOAD_THRESHOLD_DEN: u64 = 256;

const MAX_BUF: usize = u16::MAX as usize;

/// Linear hashing storage engine.
pub struct LinearHashEngine<'a> {
    buf: &'a mut [u8],
    page_size: usize,
}

impl<'a> LinearHashEngine<'a> {
    /// Create a new linear hash engine with `initial_buckets` starting buckets.
    ///
    /// Needs at least `1 + initial_buckets` pages. `initial_buckets` must be >= 2
    /// and a power of 2.
    pub fn new(buf: &'a mut [u8], page_size: usize, initial_buckets: u32) -> Option<Self> {
        if page_size < MIN_PAGE_SIZE
            || !page_size.is_power_of_two()
            || page_size > MAX_BUF
            || initial_buckets < 2
            || !initial_buckets.is_power_of_two()
            || buf.len() < page_size * (1 + initial_buckets as usize)
        {
            return None;
        }
        let mut eng = Self { buf, page_size };
        eng.init(initial_buckets).ok()?;
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

    fn init(&mut self, n: u32) -> Result<()> {
        let p = self.page_mut(0)?;
        PageHeader::new(PageType::Free, 0).encode(p)?;
        endian::write_u16_le(&mut p[META_LEVEL..], 0)?;
        endian::write_u32_le(&mut p[META_SPLIT_PTR..], 0)?;
        endian::write_u32_le(&mut p[META_INITIAL_BUCKETS..], n)?;
        endian::write_u32_le(&mut p[META_BUCKET_COUNT..], n)?;
        endian::write_u32_le(&mut p[META_PAGE_COUNT..], 1 + n)?;
        endian::write_u64_le(&mut p[META_KEY_COUNT..], 0)?;
        endian::write_u64_le(&mut p[META_DATA_BYTES..], 0)?;

        for i in 0..n {
            let bp = self.page_mut(1 + i)?;
            bucket::bucket_init(bp, 1 + i, 0)?;
        }
        Ok(())
    }

    fn level(&self) -> Result<u16> {
        endian::read_u16_le(&self.page(0)?[META_LEVEL..])
    }

    fn split_ptr(&self) -> Result<u32> {
        endian::read_u32_le(&self.page(0)?[META_SPLIT_PTR..])
    }

    fn initial_buckets(&self) -> Result<u32> {
        endian::read_u32_le(&self.page(0)?[META_INITIAL_BUCKETS..])
    }

    fn bucket_count(&self) -> Result<u32> {
        endian::read_u32_le(&self.page(0)?[META_BUCKET_COUNT..])
    }

    fn page_count(&self) -> Result<u32> {
        endian::read_u32_le(&self.page(0)?[META_PAGE_COUNT..])
    }

    fn read_key_count(&self) -> Result<u64> {
        endian::read_u64_le(&self.page(0)?[META_KEY_COUNT..])
    }

    fn read_data_bytes(&self) -> Result<u64> {
        endian::read_u64_le(&self.page(0)?[META_DATA_BYTES..])
    }

    fn set_meta_u16(&mut self, off: usize, v: u16) -> Result<()> {
        let p = self.page_mut(0)?;
        endian::write_u16_le(&mut p[off..], v)
    }

    fn set_meta_u32(&mut self, off: usize, v: u32) -> Result<()> {
        let p = self.page_mut(0)?;
        endian::write_u32_le(&mut p[off..], v)
    }

    fn add_key_count(&mut self, delta: i64) -> Result<()> {
        let c = self.read_key_count()?;
        let n = if delta >= 0 {
            // delta is non-negative, so the cast preserves the value.
            #[allow(clippy::cast_sign_loss)]
            c.wrapping_add(delta as u64)
        } else {
            c.wrapping_sub(delta.unsigned_abs())
        };
        let p = self.page_mut(0)?;
        endian::write_u64_le(&mut p[META_KEY_COUNT..], n)
    }

    fn add_data_bytes(&mut self, delta: i64) -> Result<()> {
        let c = self.read_data_bytes()?;
        let n = if delta >= 0 {
            // delta is non-negative, so the cast preserves the value.
            #[allow(clippy::cast_sign_loss)]
            c.wrapping_add(delta as u64)
        } else {
            c.wrapping_sub(delta.unsigned_abs())
        };
        let p = self.page_mut(0)?;
        endian::write_u64_le(&mut p[META_DATA_BYTES..], n)
    }

    // ── hashing ──────────────────────────────────────────────────────────

    /// Map a key to its bucket page id.
    fn bucket_for_key(&self, key: &[u8]) -> Result<PageId> {
        let h = hash_key(key);
        let n = self.initial_buckets()?;
        let l = self.level()?;
        let p = self.split_ptr()?;
        let nl = n << l;
        let mut idx = h % nl;
        if idx < p {
            idx = h % (nl << 1);
        }
        Ok(1 + idx) // page_id = bucket_index + 1
    }

    // ── split ────────────────────────────────────────────────────────────

    fn should_split(&self) -> Result<bool> {
        let kc = self.read_key_count()?;
        let bc = u64::from(self.bucket_count()?);
        // Load factor = keys / buckets > threshold
        Ok(kc * LOAD_THRESHOLD_DEN > bc * LOAD_THRESHOLD_NUM)
    }

    fn do_split(&mut self) -> Result<()> {
        let p = self.split_ptr()?;
        let n = self.initial_buckets()?;
        let l = self.level()?;
        let old_id = 1 + p; // page being split

        // Allocate new bucket
        let bc = self.bucket_count()?;
        let new_idx = bc;
        let new_id = 1 + new_idx;
        if (new_id as usize + 1) * self.page_size > self.buf.len() {
            return Ok(()); // no room, skip split
        }
        // new_id always equals page_count (buckets are allocated sequentially).
        self.set_meta_u32(META_PAGE_COUNT, new_id + 1)?;
        self.set_meta_u32(META_BUCKET_COUNT, bc + 1)?;

        let new_page = self.page_mut(new_id)?;
        bucket::bucket_init(new_page, new_id, 0)?;

        // Advance split pointer
        let nl = n << l;
        if p + 1 >= nl {
            self.set_meta_u16(META_LEVEL, l + 1)?;
            self.set_meta_u32(META_SPLIT_PTR, 0)?;
        } else {
            self.set_meta_u32(META_SPLIT_PTR, p + 1)?;
        }

        // Redistribute entries from old bucket
        let count = bucket::bucket_count(self.page(old_id)?)?;
        if count == 0 {
            return Ok(());
        }

        let mut keys: [[u8; 256]; 32] = [[0u8; 256]; 32];
        let mut vals: [[u8; 256]; 32] = [[0u8; 256]; 32];
        let mut klens = [0usize; 32];
        let mut vlens = [0usize; 32];

        let bp = self.page(old_id)?;
        for i in 0..count {
            let k = bucket::bucket_key_at(bp, i)?;
            let v = bucket::bucket_value_at(bp, i)?;
            klens[i] = k.len();
            vlens[i] = v.len();
            keys[i][..k.len()].copy_from_slice(k);
            vals[i][..v.len()].copy_from_slice(v);
        }

        // Reinit old bucket
        let old_page = self.page_mut(old_id)?;
        bucket::bucket_init(old_page, old_id, 0)?;

        // Re-insert each entry into the correct bucket
        for i in 0..count {
            let target = self.bucket_for_key(&keys[i][..klens[i]])?;
            let tp = self.page(target)?;
            let pos = match bucket::bucket_search(tp, &keys[i][..klens[i]])? {
                Ok(j) | Err(j) => j,
            };
            let tm = self.page_mut(target)?;
            bucket::bucket_insert_at(tm, pos, &keys[i][..klens[i]], &vals[i][..vlens[i]])?;
        }
        Ok(())
    }
}

impl StorageEngine for LinearHashEngine<'_> {
    fn get(&self, key: &[u8]) -> Result<Option<&[u8]>> {
        let bid = self.bucket_for_key(key)?;
        let bp = self.page(bid)?;
        match bucket::bucket_search(bp, key)? {
            Ok(i) => Ok(Some(bucket::bucket_value_at(bp, i)?)),
            Err(_) => Ok(None),
        }
    }

    // Key and value lengths are bounded by page size (≤ 64 KiB), so usize→i64 never wraps.
    #[allow(clippy::cast_possible_wrap)]
    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        let bid = self.bucket_for_key(key)?;

        // Update existing
        let bp = self.page(bid)?;
        if let Ok(i) = bucket::bucket_search(bp, key)? {
            let old_sz = bucket::bucket_entry_size(bp, i)?;
            let bm = self.page_mut(bid)?;
            bucket::bucket_delete_at(bm, i)?;
            let bp2 = self.page(bid)?;
            let pos = match bucket::bucket_search(bp2, key)? {
                Ok(j) | Err(j) => j,
            };
            if !bucket::bucket_has_space(bp2, key.len(), value.len())? {
                return Err(Error::CapacityExhausted);
            }
            let bm2 = self.page_mut(bid)?;
            bucket::bucket_insert_at(bm2, pos, key, value)?;
            let delta = (key.len() + value.len()) as i64 - old_sz as i64;
            return self.add_data_bytes(delta);
        }

        // New key
        let bp = self.page(bid)?;
        if !bucket::bucket_has_space(bp, key.len(), value.len())? {
            return Err(Error::CapacityExhausted);
        }
        let pos = match bucket::bucket_search(bp, key)? {
            Ok(j) | Err(j) => j,
        };
        let bm = self.page_mut(bid)?;
        bucket::bucket_insert_at(bm, pos, key, value)?;
        self.add_key_count(1)?;
        self.add_data_bytes((key.len() + value.len()) as i64)?;

        // Check load factor and split if needed
        if self.should_split()? {
            let _ = self.do_split();
        }
        Ok(())
    }

    // Entry size is bounded by page size (≤ 64 KiB), so usize→i64 never wraps.
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

    fn make(buf: &mut [u8]) -> LinearHashEngine<'_> {
        LinearHashEngine::new(buf, 128, 4).unwrap()
    }

    #[test]
    fn new_valid() {
        let mut buf = [0u8; 2048];
        assert!(LinearHashEngine::new(&mut buf, 128, 4).is_some());
    }

    #[test]
    fn new_invalid() {
        let mut buf = [0u8; 256];
        assert!(LinearHashEngine::new(&mut buf, 128, 4).is_none()); // not enough pages
        let mut buf2 = [0u8; 2048];
        assert!(LinearHashEngine::new(&mut buf2, 128, 3).is_none()); // not power of 2
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
        let mut buf = [0u8; 65535];
        let mut e = make(&mut buf);
        for i in 0u16..40 {
            let k = i.to_be_bytes();
            assert_eq!(e.put(&k, &k), Ok(()), "insert {i} failed");
        }
        for i in 0u16..40 {
            let k = i.to_be_bytes();
            assert_eq!(e.get(&k), Ok(Some(k.as_slice())), "get {i} failed");
        }
        assert_eq!(e.stats().key_count, 40);
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

    #[test]
    fn flush_is_noop() {
        let mut buf = [0u8; 2048];
        let mut e = make(&mut buf);
        assert_eq!(e.flush(), Ok(()));
    }

    #[test]
    fn capacity_exhaustion() {
        // Small buffer: 3 pages of 128 bytes (1 meta + 2 buckets)
        let mut buf = [0u8; 384];
        let mut e = LinearHashEngine::new(&mut buf, 128, 2).unwrap();
        let mut i = 0u16;
        loop {
            let k = i.to_le_bytes();
            if e.put(&k, b"val").is_err() {
                break;
            }
            i += 1;
            if i > 200 {
                break; // safety net
            }
        }
        assert!(i > 0); // at least some keys were inserted
    }

    #[test]
    fn many_keys_with_splits_small_initial() {
        // Larger buffer with small initial bucket count to trigger many splits.
        #[allow(clippy::large_stack_arrays)]
        let mut buf = [0u8; 8192];
        let mut e = LinearHashEngine::new(&mut buf, 128, 2).unwrap();
        for i in 0u16..40 {
            let k = i.to_be_bytes();
            assert_eq!(e.put(&k, &k), Ok(()), "insert {i} failed");
        }
        for i in 0u16..40 {
            let k = i.to_be_bytes();
            assert_eq!(e.get(&k), Ok(Some(k.as_slice())), "get {i} failed");
        }
        assert_eq!(e.stats().key_count, 40);
    }

    #[test]
    fn update_oversize_value_fails() {
        // With 64-byte pages each bucket has only ~36 usable bytes
        // (header=24, CRC=4). Insert a key with a small value, then try
        // to update it with a value that exceeds the bucket capacity.
        // The put-update path should return CapacityExhausted (line 285)
        // because after deleting the old entry the new value still won't fit.
        let mut buf = [0u8; 256];
        let mut e = LinearHashEngine::new(&mut buf, 64, 2).unwrap();

        // Insert a key with a small (1-byte) value.
        assert_eq!(e.put(b"ab", &[1]), Ok(()));
        assert_eq!(e.get(b"ab"), Ok(Some([1].as_slice())));

        // Try to update with a value far too large for the bucket.
        // Usable space is ~36 bytes; key is 2 bytes, value is 30 bytes,
        // plus 8 bytes for the slot = 40 > 36. This must fail.
        let big = [0xFFu8; 30];
        assert_eq!(e.put(b"ab", &big), Err(Error::CapacityExhausted));

        // Original value should be gone because the update path deletes
        // before checking space, but the key count is unchanged.
    }
}
