//! Page-based B+ tree storage engine.
//!
//! Stores key-value pairs in a caller-provided flat buffer divided into
//! fixed-size pages. Supports splitting, range scans via leaf sibling pointers,
//! and configurable page sizes (powers of 2, minimum 64 bytes).
//!
//! # Buffer layout
//!
//! ```text
//! [Page 0: Metadata] [Page 1+: tree nodes (leaf / internal)]
//! ```
// Engine methods have uniform error conditions (page bounds / capacity / corruption).
#![allow(clippy::missing_errors_doc)]

pub mod node;

use iondb_core::endian;
use iondb_core::error::{Error, Result};
use iondb_core::page::{PageType, PAGE_HEADER_SIZE};
use iondb_core::traits::storage_engine::{EngineStats, StorageEngine};
use iondb_core::types::{PageId, MIN_PAGE_SIZE};
use node::NO_PAGE;

const META_ROOT: usize = PAGE_HEADER_SIZE; // u32
const META_PAGE_COUNT: usize = META_ROOT + 4; // u32
const META_KEY_COUNT: usize = META_PAGE_COUNT + 4; // u64
const META_DATA_BYTES: usize = META_KEY_COUNT + 8; // u64

/// Maximum tree height (stack depth for traversal).
const MAX_HEIGHT: usize = 16;

/// Maximum buffer size (u16 offsets inside pages).
const MAX_BUF: usize = u16::MAX as usize;

/// A page-based B+ tree storage engine.
///
/// Operates on a caller-provided `&mut [u8]` buffer divided into fixed-size
/// pages. `no_std` compatible, zero heap allocation.
pub struct BTreeEngine<'a> {
    buf: &'a mut [u8],
    page_size: usize,
}

impl<'a> BTreeEngine<'a> {
    /// Create a new B+ tree engine.
    ///
    /// `page_size` must be a power of 2, at least [`MIN_PAGE_SIZE`] (64),
    /// and the buffer must hold at least 2 pages. Returns `None` on
    /// invalid parameters.
    pub fn new(buf: &'a mut [u8], page_size: usize) -> Option<Self> {
        if page_size < MIN_PAGE_SIZE
            || !page_size.is_power_of_two()
            || page_size > MAX_BUF
            || buf.len() < page_size * 2
        {
            return None;
        }
        let mut eng = Self { buf, page_size };
        eng.init_metadata().ok()?;
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

    /// Borrow two distinct pages mutably at the same time.
    fn two_pages_mut(&mut self, a: PageId, b: PageId) -> Result<(&mut [u8], &mut [u8])> {
        if a == b {
            return Err(Error::PageError);
        }
        let ps = self.page_size;
        let (lo, hi) = if a < b { (a, b) } else { (b, a) };
        let hi_off = (hi as usize).checked_mul(ps).ok_or(Error::PageError)?;
        if hi_off + ps > self.buf.len() {
            return Err(Error::PageError);
        }
        let (left, right) = self.buf.split_at_mut(hi_off);
        let lo_off = lo as usize * ps;
        let lo_page = left.get_mut(lo_off..lo_off + ps).ok_or(Error::PageError)?;
        let hi_page = right.get_mut(..ps).ok_or(Error::PageError)?;
        if a < b {
            Ok((lo_page, hi_page))
        } else {
            Ok((hi_page, lo_page))
        }
    }

    fn init_metadata(&mut self) -> Result<()> {
        let p = self.page_mut(0)?;
        iondb_core::page::PageHeader::new(PageType::Free, 0).encode(p)?;
        endian::write_u32_le(&mut p[META_ROOT..], NO_PAGE)?;
        endian::write_u32_le(&mut p[META_PAGE_COUNT..], 1)?; // metadata page
        endian::write_u64_le(&mut p[META_KEY_COUNT..], 0)?;
        endian::write_u64_le(&mut p[META_DATA_BYTES..], 0)
    }

    fn root_id(&self) -> Result<PageId> {
        endian::read_u32_le(&self.page(0)?[META_ROOT..])
    }

    fn set_root_id(&mut self, id: PageId) -> Result<()> {
        let p = self.page_mut(0)?;
        endian::write_u32_le(&mut p[META_ROOT..], id)
    }

    fn page_count(&self) -> Result<u32> {
        endian::read_u32_le(&self.page(0)?[META_PAGE_COUNT..])
    }

    fn set_page_count(&mut self, c: u32) -> Result<()> {
        let p = self.page_mut(0)?;
        endian::write_u32_le(&mut p[META_PAGE_COUNT..], c)
    }

    fn read_key_count(&self) -> Result<u64> {
        endian::read_u64_le(&self.page(0)?[META_KEY_COUNT..])
    }

    fn add_key_count(&mut self, delta: i64) -> Result<()> {
        let cur = self.read_key_count()?;
        let new = if delta >= 0 {
            // Guard above ensures non-negative; cast is safe.
            #[allow(clippy::cast_sign_loss)]
            cur.wrapping_add(delta as u64)
        } else {
            cur.wrapping_sub(delta.unsigned_abs())
        };
        let p = self.page_mut(0)?;
        endian::write_u64_le(&mut p[META_KEY_COUNT..], new)
    }

    fn read_data_bytes(&self) -> Result<u64> {
        endian::read_u64_le(&self.page(0)?[META_DATA_BYTES..])
    }

    fn add_data_bytes(&mut self, delta: i64) -> Result<()> {
        let cur = self.read_data_bytes()?;
        let new = if delta >= 0 {
            // Guard above ensures non-negative; cast is safe.
            #[allow(clippy::cast_sign_loss)]
            cur.wrapping_add(delta as u64)
        } else {
            cur.wrapping_sub(delta.unsigned_abs())
        };
        let p = self.page_mut(0)?;
        endian::write_u64_le(&mut p[META_DATA_BYTES..], new)
    }

    fn alloc_page(&mut self) -> Result<PageId> {
        let pc = self.page_count()?;
        let max = self.buf.len() / self.page_size;
        if pc as usize >= max {
            return Err(Error::CapacityExhausted);
        }
        self.set_page_count(pc + 1)?;
        Ok(pc)
    }

    /// Walk from root to the leaf containing `key`. Returns `(leaf_id, parent_stack, depth)`.
    fn find_leaf(&self, key: &[u8]) -> Result<(PageId, [PageId; MAX_HEIGHT], usize)> {
        let mut stack = [NO_PAGE; MAX_HEIGHT];
        let mut depth = 0usize;
        let mut cur = self.root_id()?;
        loop {
            let p = self.page(cur)?;
            let pt = iondb_core::page::PageType::from_byte(iondb_core::endian::read_u8(&p[0..])?)?;
            if pt == PageType::BTreeLeaf {
                return Ok((cur, stack, depth));
            }
            if depth >= MAX_HEIGHT {
                return Err(Error::Corruption);
            }
            stack[depth] = cur;
            depth += 1;
            cur = node::internal_find_child(p, key)?;
        }
    }

    fn split_leaf_and_insert(
        &mut self,
        leaf_id: PageId,
        key: &[u8],
        value: &[u8],
    ) -> Result<(PageId, PageId)> {
        let new_id = self.alloc_page()?;
        let (left, right) = self.two_pages_mut(leaf_id, new_id)?;
        node::leaf_init(right, new_id)?;
        let n = node::leaf_count(left)?;
        let mid = n / 2;
        // Copy upper half to right page
        for i in mid..n {
            let k = node::leaf_key_at(left, i)?;
            let v = node::leaf_value_at(left, i)?;
            let pos = i - mid;
            node::leaf_insert_at(right, pos, k, v)?;
        }
        node::leaf_set_count(left, mid)?;
        // Fix sibling pointers
        let old_next = node::leaf_next(left)?;
        node::leaf_set_next(left, new_id)?;
        node::leaf_set_prev(right, leaf_id)?;
        node::leaf_set_next(right, old_next)?;
        // Insert the new key-value into the correct page
        let first_right_key = node::leaf_key_at(right, 0)?;
        if key < first_right_key {
            let pos = match node::leaf_search(left, key)? {
                Ok(i) | Err(i) => i,
            };
            node::leaf_insert_at(left, pos, key, value)?;
        } else {
            let pos = match node::leaf_search(right, key)? {
                Ok(i) | Err(i) => i,
            };
            node::leaf_insert_at(right, pos, key, value)?;
        }
        // old-next prev pointer fixed after two_pages_mut borrow is dropped.
        Ok((leaf_id, new_id))
    }

    fn fix_prev_pointer(&mut self, page_id: PageId, new_prev: PageId) -> Result<()> {
        if page_id != NO_PAGE {
            let p = self.page_mut(page_id)?;
            node::leaf_set_prev(p, new_prev)?;
        }
        Ok(())
    }

    /// Propagate a split upward through internal nodes.
    fn propagate_split(
        &mut self,
        stack: &[PageId; MAX_HEIGHT],
        depth: usize,
        mut sep_key_buf: [u8; 256],
        mut sep_len: usize,
        mut right_child: PageId,
    ) -> Result<()> {
        let mut d = depth;
        while d > 0 {
            d -= 1;
            let parent_id = stack[d];
            let parent = self.page(parent_id)?;
            if node::internal_has_space(parent, sep_len)? {
                let p = self.page_mut(parent_id)?;
                return node::internal_insert(p, &sep_key_buf[..sep_len], right_child);
            }
            // Split this internal node
            let new_id = self.alloc_page()?;
            let promoted = self.split_internal_and_insert(
                parent_id,
                new_id,
                &sep_key_buf[..sep_len],
                right_child,
            )?;
            sep_key_buf[..promoted.1].copy_from_slice(&promoted.0[..promoted.1]);
            sep_len = promoted.1;
            right_child = new_id;
        }
        // Reached the root — create a new root
        let new_root = self.alloc_page()?;
        let old_root = self.root_id()?;
        let p = self.page_mut(new_root)?;
        node::internal_init(p, new_root, old_root)?;
        node::internal_insert(p, &sep_key_buf[..sep_len], right_child)?;
        self.set_root_id(new_root)
    }

    /// Split an internal node and insert (key, `right_child`). Returns promoted key.
    fn split_internal_and_insert(
        &mut self,
        node_id: PageId,
        new_id: PageId,
        key: &[u8],
        right_child: PageId,
    ) -> Result<([u8; 256], usize)> {
        // First insert into the node (temporarily overfull handled by new page)
        let pg = self.page_mut(node_id)?;
        // We need to collect all entries, add the new one, then redistribute
        let n = node::internal_count(pg)?;

        // Collect all entries + the new one into a temp array
        let mut keys: [[u8; 256]; 32] = [[0u8; 256]; 32];
        let mut key_lens = [0usize; 32];
        let mut children = [NO_PAGE; 33];
        children[0] = node::internal_left_child(pg)?;

        let mut total = 0usize;
        let mut inserted = false;
        for i in 0..n {
            let ek = node::internal_key_at(pg, i)?;
            if !inserted && key < ek {
                keys[total][..key.len()].copy_from_slice(key);
                key_lens[total] = key.len();
                children[total + 1] = right_child;
                total += 1;
                inserted = true;
            }
            let kl = ek.len();
            keys[total][..kl].copy_from_slice(ek);
            key_lens[total] = kl;
            children[total + 1] = node::internal_child_at(pg, i)?;
            total += 1;
        }
        if !inserted {
            keys[total][..key.len()].copy_from_slice(key);
            key_lens[total] = key.len();
            children[total + 1] = right_child;
            total += 1;
        }

        let mid = total / 2;
        let promoted_len = key_lens[mid];
        let mut promoted = [0u8; 256];
        promoted[..promoted_len].copy_from_slice(&keys[mid][..promoted_len]);

        // Rebuild left node with entries [0..mid]
        let (left, right_pg) = self.two_pages_mut(node_id, new_id)?;
        node::internal_init(left, node_id, children[0])?;
        for i in 0..mid {
            node::internal_insert(left, &keys[i][..key_lens[i]], children[i + 1])?;
        }

        // Build right node with entries [mid+1..total], left_child = children[mid+1]
        node::internal_init(right_pg, new_id, children[mid + 1])?;
        for i in (mid + 1)..total {
            node::internal_insert(right_pg, &keys[i][..key_lens[i]], children[i + 1])?;
        }

        Ok((promoted, promoted_len))
    }

    /// Scan key-value pairs in `[start, end)`. Calls `f` for each; stop if it returns `false`.
    pub fn range<F>(&self, start: &[u8], end: &[u8], mut f: F) -> Result<()>
    where
        F: FnMut(&[u8], &[u8]) -> bool,
    {
        if self.root_id()? == NO_PAGE {
            return Ok(());
        }
        let (leaf_id, _, _) = self.find_leaf(start)?;
        let mut cur = leaf_id;
        'outer: loop {
            let pg = self.page(cur)?;
            let n = node::leaf_count(pg)?;
            for i in 0..n {
                let k = node::leaf_key_at(pg, i)?;
                if k >= end {
                    break 'outer;
                }
                if k >= start {
                    let v = node::leaf_value_at(pg, i)?;
                    if !f(k, v) {
                        break 'outer;
                    }
                }
            }
            cur = node::leaf_next(pg)?;
            if cur == NO_PAGE {
                break;
            }
        }
        Ok(())
    }
}

// All size→delta casts are bounded by page size (≤ 64 KiB), never wraps.
#[allow(clippy::cast_possible_wrap)]
impl StorageEngine for BTreeEngine<'_> {
    fn get(&self, key: &[u8]) -> Result<Option<&[u8]>> {
        if self.root_id()? == NO_PAGE {
            return Ok(None);
        }
        let (leaf_id, _, _) = self.find_leaf(key)?;
        let pg = self.page(leaf_id)?;
        match node::leaf_search(pg, key)? {
            Ok(i) => Ok(Some(node::leaf_value_at(pg, i)?)),
            Err(_) => Ok(None),
        }
    }

    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        if self.root_id()? == NO_PAGE {
            let id = self.alloc_page()?;
            let p = self.page_mut(id)?;
            node::leaf_init(p, id)?;
            node::leaf_insert_at(p, 0, key, value)?;
            self.set_root_id(id)?;
            self.add_key_count(1)?;
            return self.add_data_bytes((key.len() + value.len()) as i64);
        }

        let (leaf_id, stack, depth) = self.find_leaf(key)?;

        // Check for existing key (update case)
        let pg = self.page(leaf_id)?;
        if let Ok(i) = node::leaf_search(pg, key)? {
            let old_size = node::leaf_entry_data_size(pg, i)?;
            let p = self.page_mut(leaf_id)?;
            node::leaf_delete_at(p, i)?;
            // Re-insert: may need split if new value is larger
            let p2 = self.page(leaf_id)?;
            if node::leaf_has_space(p2, key.len(), value.len())? {
                let p3 = self.page_mut(leaf_id)?;
                let pos = match node::leaf_search(p3, key)? {
                    Ok(j) | Err(j) => j,
                };
                node::leaf_insert_at(p3, pos, key, value)?;
                let delta = (key.len() + value.len()) as i64 - old_size as i64;
                return self.add_data_bytes(delta);
            }
            // Doesn't fit — fall through to split path
            // (key count unchanged, data_bytes adjusted after split)
            let data_delta = (key.len() + value.len()) as i64 - old_size as i64;
            let (_, new_id) = self.split_leaf_and_insert(leaf_id, key, value)?;
            // Fix old-next prev pointer
            let old_next = node::leaf_next(self.page(new_id)?)?;
            self.fix_prev_pointer(old_next, new_id)?;
            let sep = node::leaf_key_at(self.page(new_id)?, 0)?;
            let mut sep_buf = [0u8; 256];
            let sep_len = sep.len();
            sep_buf[..sep_len].copy_from_slice(sep);
            self.propagate_split(&stack, depth, sep_buf, sep_len, new_id)?;
            return self.add_data_bytes(data_delta);
        }

        // New key — try direct insert
        let p = self.page(leaf_id)?;
        if node::leaf_has_space(p, key.len(), value.len())? {
            let p2 = self.page_mut(leaf_id)?;
            let pos = match node::leaf_search(p2, key)? {
                Ok(j) | Err(j) => j,
            };
            node::leaf_insert_at(p2, pos, key, value)?;
            self.add_key_count(1)?;
            return self.add_data_bytes((key.len() + value.len()) as i64);
        }

        // Split required
        let (_, new_id) = self.split_leaf_and_insert(leaf_id, key, value)?;
        let old_next = node::leaf_next(self.page(new_id)?)?;
        self.fix_prev_pointer(old_next, new_id)?;
        let sep = node::leaf_key_at(self.page(new_id)?, 0)?;
        let mut sep_buf = [0u8; 256];
        let sep_len = sep.len();
        sep_buf[..sep_len].copy_from_slice(sep);
        self.propagate_split(&stack, depth, sep_buf, sep_len, new_id)?;
        self.add_key_count(1)?;
        self.add_data_bytes((key.len() + value.len()) as i64)
    }

    fn delete(&mut self, key: &[u8]) -> Result<bool> {
        if self.root_id()? == NO_PAGE {
            return Ok(false);
        }
        let (leaf_id, _, _) = self.find_leaf(key)?;
        let pg = self.page(leaf_id)?;
        match node::leaf_search(pg, key)? {
            Ok(i) => {
                let sz = node::leaf_entry_data_size(pg, i)?;
                let p = self.page_mut(leaf_id)?;
                node::leaf_delete_at(p, i)?;
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
        let kc = self.read_key_count().unwrap_or(0);
        let db = self.read_data_bytes().unwrap_or(0);
        let pc = self.page_count().unwrap_or(1);
        EngineStats {
            key_count: kc,
            data_bytes: db,
            page_count: pc,
        }
    }
}

#[cfg(test)]
// Tests use unwrap for brevity; panics are acceptable in test code.
#[allow(clippy::unwrap_used)]
mod tests;
