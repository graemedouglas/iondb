//! B+ tree storage engine — Phase 0 sorted-array placeholder.
//!
//! This module provides a minimal [`StorageEngine`] implementation backed by a
//! sorted array in a flat buffer. It validates the trait contract end-to-end
//! and is sufficient for wiring the `sensor-log` dogfood app.
//!
//! **This will be replaced with a proper page-based B+ tree in Phase 1.**
//!
//! # Buffer layout
//!
//! ```text
//! [entry_count: 2 bytes LE] [data_offset: 2 bytes LE]
//! [index: entry_count * 8 bytes]  (sorted by key)
//! [... free space ...]
//! [data: key-value pairs packed from the end of the buffer backward]
//! ```
//!
//! Each index entry is 8 bytes:
//! `[key_offset: u16 LE] [key_len: u16 LE] [val_offset: u16 LE] [val_len: u16 LE]`
//!
//! Data is packed from the end of the buffer backward. The index grows forward.
//! When index and data regions would overlap, the engine is full.

use iondb_core::error::{Error, Result};
use iondb_core::traits::storage_engine::{EngineStats, StorageEngine};

/// Size of the metadata header (count + offset fields).
const HEADER_SIZE: usize = 4;
/// Size of each index entry.
const INDEX_ENTRY_SIZE: usize = 8;
/// Maximum buffer size (limited by u16 offsets).
const MAX_BUF_SIZE: usize = u16::MAX as usize;

/// A minimal sorted-array storage engine for Phase 0 validation.
///
/// Stores key-value pairs in a fixed-size caller-provided buffer using sorted
/// insertion with binary search for lookups.
pub struct BTreeEngine<'a> {
    buf: &'a mut [u8],
}

/// Internal index entry (offsets and lengths within the buffer).
struct IndexEntry {
    key_offset: usize,
    key_len: usize,
    val_offset: usize,
    val_len: usize,
}

impl<'a> BTreeEngine<'a> {
    /// Create a new engine backed by the given buffer.
    ///
    /// The buffer must be at least 4 bytes and at most 65535 bytes (u16 offsets).
    /// Returns `None` if the buffer is too small or too large.
    pub fn new(buf: &'a mut [u8]) -> Option<Self> {
        if buf.len() < HEADER_SIZE || buf.len() > MAX_BUF_SIZE {
            return None;
        }
        let len_u16 = truncate_to_u16(buf.len());
        write_u16(&mut buf[0..], 0);
        write_u16(&mut buf[2..], len_u16);
        Some(Self { buf })
    }

    fn entry_count(&self) -> usize {
        usize::from(read_u16(&self.buf[0..]))
    }

    fn set_entry_count(&mut self, count: usize) {
        write_u16(&mut self.buf[0..], truncate_to_u16(count));
    }

    fn data_offset(&self) -> usize {
        usize::from(read_u16(&self.buf[2..]))
    }

    fn set_data_offset(&mut self, offset: usize) {
        write_u16(&mut self.buf[2..], truncate_to_u16(offset));
    }

    fn index_offset(i: usize) -> usize {
        HEADER_SIZE + i * INDEX_ENTRY_SIZE
    }

    fn read_entry(&self, i: usize) -> IndexEntry {
        let off = Self::index_offset(i);
        IndexEntry {
            key_offset: usize::from(read_u16(&self.buf[off..])),
            key_len: usize::from(read_u16(&self.buf[off + 2..])),
            val_offset: usize::from(read_u16(&self.buf[off + 4..])),
            val_len: usize::from(read_u16(&self.buf[off + 6..])),
        }
    }

    fn write_entry(&mut self, i: usize, entry: &IndexEntry) {
        let off = Self::index_offset(i);
        write_u16(&mut self.buf[off..], truncate_to_u16(entry.key_offset));
        write_u16(&mut self.buf[off + 2..], truncate_to_u16(entry.key_len));
        write_u16(&mut self.buf[off + 4..], truncate_to_u16(entry.val_offset));
        write_u16(&mut self.buf[off + 6..], truncate_to_u16(entry.val_len));
    }

    fn key_at(&self, i: usize) -> &[u8] {
        let e = self.read_entry(i);
        &self.buf[e.key_offset..e.key_offset + e.key_len]
    }

    /// Binary search for `key`. Returns `Ok(index)` if found, `Err(index)` for
    /// the insertion point.
    fn search(&self, key: &[u8]) -> core::result::Result<usize, usize> {
        let count = self.entry_count();
        let mut lo = 0usize;
        let mut hi = count;
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            match self.key_at(mid).cmp(key) {
                core::cmp::Ordering::Equal => return Ok(mid),
                core::cmp::Ordering::Less => lo = mid + 1,
                core::cmp::Ordering::Greater => hi = mid,
            }
        }
        Err(lo)
    }

    fn free_space(&self) -> usize {
        let index_end = Self::index_offset(self.entry_count());
        self.data_offset().saturating_sub(index_end)
    }

    /// Insert key/value at the given sorted position.
    fn insert_at_position(&mut self, key: &[u8], value: &[u8], pos: usize) -> Result<()> {
        let data_size = key.len() + value.len();
        let needed = INDEX_ENTRY_SIZE + data_size;
        if self.free_space() < needed {
            return Err(Error::CapacityExhausted);
        }

        let count = self.entry_count();

        // Allocate data space (grows downward)
        let new_data_offset = self.data_offset() - data_size;
        let key_offset = new_data_offset;
        self.buf[key_offset..key_offset + key.len()].copy_from_slice(key);
        let val_offset = key_offset + key.len();
        self.buf[val_offset..val_offset + value.len()].copy_from_slice(value);

        // Shift index entries after `pos` to make room (back to front)
        for i in (pos..count).rev() {
            let e = self.read_entry(i);
            self.write_entry(i + 1, &e);
        }

        self.write_entry(
            pos,
            &IndexEntry {
                key_offset,
                key_len: key.len(),
                val_offset,
                val_len: value.len(),
            },
        );

        self.set_entry_count(count + 1);
        self.set_data_offset(new_data_offset);
        Ok(())
    }

    /// Delete the entry at index `i` and compact the index.
    fn delete_at(&mut self, i: usize) {
        let count = self.entry_count();
        for j in i..count - 1 {
            let e = self.read_entry(j + 1);
            self.write_entry(j, &e);
        }
        self.set_entry_count(count - 1);
        // Note: data space is not reclaimed (fragmentation). Acceptable for
        // Phase 0; a real B+ tree manages pages properly.
    }
}

impl StorageEngine for BTreeEngine<'_> {
    fn get(&self, key: &[u8]) -> Result<Option<&[u8]>> {
        match self.search(key) {
            Ok(i) => {
                let e = self.read_entry(i);
                Ok(Some(&self.buf[e.val_offset..e.val_offset + e.val_len]))
            }
            Err(_) => Ok(None),
        }
    }

    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        let data_size = key.len() + value.len();

        match self.search(key) {
            Ok(i) => {
                // Key exists — remove then re-insert (simple, O(n) but fine for Phase 0)
                self.delete_at(i);
                self.insert_at_position(key, value, i)
            }
            Err(pos) => {
                let needed = INDEX_ENTRY_SIZE + data_size;
                if self.free_space() < needed {
                    return Err(Error::CapacityExhausted);
                }
                self.insert_at_position(key, value, pos)
            }
        }
    }

    fn delete(&mut self, key: &[u8]) -> Result<bool> {
        match self.search(key) {
            Ok(i) => {
                self.delete_at(i);
                Ok(true)
            }
            Err(_) => Ok(false),
        }
    }

    fn flush(&mut self) -> Result<()> {
        Ok(())
    }

    fn stats(&self) -> EngineStats {
        let count = self.entry_count();
        let mut data_bytes = 0u64;
        for i in 0..count {
            let e = self.read_entry(i);
            data_bytes += (e.key_len + e.val_len) as u64;
        }
        EngineStats {
            key_count: count as u64,
            data_bytes,
            page_count: 1,
        }
    }
}

// Internal LE u16 helpers — no `Result` overhead for hot paths.

fn read_u16(buf: &[u8]) -> u16 {
    u16::from_le_bytes([buf[0], buf[1]])
}

fn write_u16(buf: &mut [u8], val: u16) {
    let bytes = val.to_le_bytes();
    buf[0] = bytes[0];
    buf[1] = bytes[1];
}

/// Truncate a `usize` to `u16`. Safe because buffer size is validated in `new()`.
#[allow(clippy::cast_possible_truncation)]
fn truncate_to_u16(val: usize) -> u16 {
    val as u16
}

#[cfg(test)]
// Tests use unwrap for brevity; panics are acceptable in test code.
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn new_valid() {
        let mut buf = [0u8; 256];
        assert!(BTreeEngine::new(&mut buf).is_some());
    }

    #[test]
    fn new_too_small() {
        let mut buf = [0u8; 2];
        assert!(BTreeEngine::new(&mut buf).is_none());
    }

    #[test]
    fn put_and_get() {
        let mut buf = [0u8; 256];
        let mut engine = BTreeEngine::new(&mut buf).unwrap(); // OK in tests
        assert_eq!(engine.put(b"hello", b"world"), Ok(()));
        assert_eq!(engine.get(b"hello"), Ok(Some(b"world".as_slice())));
    }

    #[test]
    fn get_missing_key() {
        let mut buf = [0u8; 256];
        let engine = BTreeEngine::new(&mut buf).unwrap(); // OK in tests
        assert_eq!(engine.get(b"missing"), Ok(None));
    }

    #[test]
    fn put_overwrite() {
        let mut buf = [0u8; 512];
        let mut engine = BTreeEngine::new(&mut buf).unwrap(); // OK in tests
        assert_eq!(engine.put(b"key", b"val1"), Ok(()));
        assert_eq!(engine.put(b"key", b"val2"), Ok(()));
        assert_eq!(engine.get(b"key"), Ok(Some(b"val2".as_slice())));
        assert_eq!(engine.stats().key_count, 1);
    }

    #[test]
    fn delete_existing() {
        let mut buf = [0u8; 256];
        let mut engine = BTreeEngine::new(&mut buf).unwrap(); // OK in tests
        assert_eq!(engine.put(b"key", b"val"), Ok(()));
        assert_eq!(engine.delete(b"key"), Ok(true));
        assert_eq!(engine.get(b"key"), Ok(None));
    }

    #[test]
    fn delete_missing() {
        let mut buf = [0u8; 256];
        let mut engine = BTreeEngine::new(&mut buf).unwrap(); // OK in tests
        assert_eq!(engine.delete(b"missing"), Ok(false));
    }

    #[test]
    fn sorted_order() {
        let mut buf = [0u8; 512];
        let mut engine = BTreeEngine::new(&mut buf).unwrap(); // OK in tests
        assert_eq!(engine.put(b"cherry", b"3"), Ok(()));
        assert_eq!(engine.put(b"apple", b"1"), Ok(()));
        assert_eq!(engine.put(b"banana", b"2"), Ok(()));
        assert_eq!(engine.get(b"apple"), Ok(Some(b"1".as_slice())));
        assert_eq!(engine.get(b"banana"), Ok(Some(b"2".as_slice())));
        assert_eq!(engine.get(b"cherry"), Ok(Some(b"3".as_slice())));
    }

    #[test]
    fn capacity_exhaustion() {
        let mut buf = [0u8; 32];
        let mut engine = BTreeEngine::new(&mut buf).unwrap(); // OK in tests
        let result1 = engine.put(b"a", b"x");
        if result1.is_ok() {
            let mut i = b'b';
            loop {
                let key = [i];
                let result = engine.put(&key, b"value_data");
                if result.is_err() {
                    assert_eq!(result, Err(Error::CapacityExhausted));
                    break;
                }
                i += 1;
                if i > b'z' {
                    break;
                }
            }
        }
    }

    #[test]
    fn stats_accuracy() {
        let mut buf = [0u8; 512];
        let mut engine = BTreeEngine::new(&mut buf).unwrap(); // OK in tests
        assert_eq!(engine.stats().key_count, 0);
        assert_eq!(engine.stats().data_bytes, 0);
        assert_eq!(engine.put(b"ab", b"cd"), Ok(()));
        assert_eq!(engine.stats().key_count, 1);
        assert_eq!(engine.stats().data_bytes, 4);
        assert_eq!(engine.put(b"ef", b"gh"), Ok(()));
        assert_eq!(engine.stats().key_count, 2);
        assert_eq!(engine.stats().data_bytes, 8);
        assert_eq!(engine.stats().page_count, 1);
    }

    #[test]
    fn flush_is_noop() {
        let mut buf = [0u8; 64];
        let mut engine = BTreeEngine::new(&mut buf).unwrap(); // OK in tests
        assert_eq!(engine.flush(), Ok(()));
    }

    #[test]
    fn multiple_deletes() {
        let mut buf = [0u8; 512];
        let mut engine = BTreeEngine::new(&mut buf).unwrap(); // OK in tests
        assert_eq!(engine.put(b"a", b"1"), Ok(()));
        assert_eq!(engine.put(b"b", b"2"), Ok(()));
        assert_eq!(engine.put(b"c", b"3"), Ok(()));
        assert_eq!(engine.delete(b"b"), Ok(true));
        assert_eq!(engine.get(b"a"), Ok(Some(b"1".as_slice())));
        assert_eq!(engine.get(b"b"), Ok(None));
        assert_eq!(engine.get(b"c"), Ok(Some(b"3".as_slice())));
        assert_eq!(engine.stats().key_count, 2);
    }

    #[test]
    fn empty_key_and_value() {
        let mut buf = [0u8; 256];
        let mut engine = BTreeEngine::new(&mut buf).unwrap(); // OK in tests
        assert_eq!(engine.put(b"", b""), Ok(()));
        assert_eq!(engine.get(b""), Ok(Some(b"".as_slice())));
    }
}
