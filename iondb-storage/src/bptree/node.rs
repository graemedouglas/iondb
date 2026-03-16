//! B+ tree node page operations for leaf and internal pages.
//!
//! All functions operate on raw page byte slices. No heap allocation.
//! On-disk layout uses little-endian byte order throughout.

// Page-level helpers have uniform error conditions (buffer bounds / corruption).
#![allow(clippy::missing_errors_doc)]

use iondb_core::endian;
use iondb_core::error::{Error, Result};
use iondb_core::page::{PageHeader, PageType, PAGE_CHECKSUM_SIZE, PAGE_HEADER_SIZE};
use iondb_core::types::PageId;

/// Sentinel value indicating no linked page.
pub const NO_PAGE: PageId = u32::MAX;

// ─── Shared Helpers ──────────────────────────────────────────────────────────

/// Truncate `usize` to `u16`. Callers guarantee the value fits.
// Page sizes are validated to be <= u16::MAX at engine construction.
#[allow(clippy::cast_possible_truncation)]
fn to_u16(v: usize) -> u16 {
    v as u16
}

struct KvSlot {
    key_off: usize,
    key_len: usize,
    val_off: usize,
    val_len: usize,
}

struct KeySlot {
    child: PageId,
    key_off: usize,
    key_len: usize,
}

// ─── Leaf Node ───────────────────────────────────────────────────────────────
// Layout: [PageHeader:16][count:2][data_end:2][next:4][prev:4][slots…][…data][CRC:4]

/// Total size of the leaf header (before index slots).
pub const LEAF_HDR: usize = PAGE_HEADER_SIZE + 2 + 2 + 4 + 4; // 28
/// Size of one leaf index slot.
pub const LEAF_SLOT: usize = 8;

/// Minimum page size that can hold at least one small leaf entry.
pub const LEAF_MIN_PAGE: usize = LEAF_HDR + LEAF_SLOT + 2 + PAGE_CHECKSUM_SIZE; // 42

/// Initialise a page as an empty leaf node.
pub fn leaf_init(page: &mut [u8], page_id: PageId) -> Result<()> {
    PageHeader::new(PageType::BTreeLeaf, page_id).encode(page)?;
    let de = page.len() - PAGE_CHECKSUM_SIZE;
    endian::write_u16_le(&mut page[16..], 0)?;
    endian::write_u16_le(&mut page[18..], to_u16(de))?;
    endian::write_u32_le(&mut page[20..], NO_PAGE)?;
    endian::write_u32_le(&mut page[24..], NO_PAGE)
}

/// Number of entries in a leaf page.
pub fn leaf_count(page: &[u8]) -> Result<usize> {
    Ok(usize::from(endian::read_u16_le(&page[16..])?))
}

/// Set the entry count for a leaf page.
pub fn leaf_set_count(page: &mut [u8], c: usize) -> Result<()> {
    endian::write_u16_le(&mut page[16..], to_u16(c))
}

fn leaf_data_end(page: &[u8]) -> Result<usize> {
    Ok(usize::from(endian::read_u16_le(&page[18..])?))
}

fn leaf_set_data_end(page: &mut [u8], v: usize) -> Result<()> {
    endian::write_u16_le(&mut page[18..], to_u16(v))
}

/// Page-id of the next leaf (or [`NO_PAGE`]).
pub fn leaf_next(page: &[u8]) -> Result<PageId> {
    endian::read_u32_le(&page[20..])
}

/// Page-id of the previous leaf (or [`NO_PAGE`]).
pub fn leaf_prev(page: &[u8]) -> Result<PageId> {
    endian::read_u32_le(&page[24..])
}

/// Set the next-leaf pointer.
pub fn leaf_set_next(page: &mut [u8], id: PageId) -> Result<()> {
    endian::write_u32_le(&mut page[20..], id)
}

/// Set the previous-leaf pointer.
pub fn leaf_set_prev(page: &mut [u8], id: PageId) -> Result<()> {
    endian::write_u32_le(&mut page[24..], id)
}

fn read_kv_slot(page: &[u8], base: usize) -> Result<KvSlot> {
    Ok(KvSlot {
        key_off: usize::from(endian::read_u16_le(&page[base..])?),
        key_len: usize::from(endian::read_u16_le(&page[base + 2..])?),
        val_off: usize::from(endian::read_u16_le(&page[base + 4..])?),
        val_len: usize::from(endian::read_u16_le(&page[base + 6..])?),
    })
}

fn write_kv_slot(page: &mut [u8], base: usize, s: &KvSlot) -> Result<()> {
    endian::write_u16_le(&mut page[base..], to_u16(s.key_off))?;
    endian::write_u16_le(&mut page[base + 2..], to_u16(s.key_len))?;
    endian::write_u16_le(&mut page[base + 4..], to_u16(s.val_off))?;
    endian::write_u16_le(&mut page[base + 6..], to_u16(s.val_len))
}

fn leaf_slot(i: usize) -> usize {
    LEAF_HDR + i * LEAF_SLOT
}

/// Return the key at entry `i`.
pub fn leaf_key_at(page: &[u8], i: usize) -> Result<&[u8]> {
    let s = read_kv_slot(page, leaf_slot(i))?;
    page.get(s.key_off..s.key_off + s.key_len)
        .ok_or(Error::Corruption)
}

/// Return the value at entry `i`.
pub fn leaf_value_at(page: &[u8], i: usize) -> Result<&[u8]> {
    let s = read_kv_slot(page, leaf_slot(i))?;
    page.get(s.val_off..s.val_off + s.val_len)
        .ok_or(Error::Corruption)
}

/// Binary search. `Ok(i)` = found, `Err(i)` = insertion point.
pub fn leaf_search(page: &[u8], key: &[u8]) -> Result<core::result::Result<usize, usize>> {
    let n = leaf_count(page)?;
    let (mut lo, mut hi) = (0, n);
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        match leaf_key_at(page, mid)?.cmp(key) {
            core::cmp::Ordering::Equal => return Ok(Ok(mid)),
            core::cmp::Ordering::Less => lo = mid + 1,
            core::cmp::Ordering::Greater => hi = mid,
        }
    }
    Ok(Err(lo))
}

/// Check whether `key_len + val_len` bytes plus one index slot fit.
pub fn leaf_has_space(page: &[u8], key_len: usize, val_len: usize) -> Result<bool> {
    let n = leaf_count(page)?;
    let de = leaf_data_end(page)?;
    let idx_end = LEAF_HDR + (n + 1) * LEAF_SLOT;
    let data_need = key_len + val_len;
    Ok(de >= idx_end + data_need)
}

/// Insert a key-value pair at position `pos`, shifting later slots right.
pub fn leaf_insert_at(page: &mut [u8], pos: usize, key: &[u8], value: &[u8]) -> Result<()> {
    let n = leaf_count(page)?;
    let de = leaf_data_end(page)?;
    let new_de = de - key.len() - value.len();

    // Write data (key then value, packed downward)
    let ko = new_de;
    let vo = ko + key.len();
    page[ko..ko + key.len()].copy_from_slice(key);
    page[vo..vo + value.len()].copy_from_slice(value);

    // Shift index slots [pos..n] → [pos+1..n+1]
    let mut i = n;
    while i > pos {
        let src = leaf_slot(i - 1);
        let dst = leaf_slot(i);
        let s = read_kv_slot(page, src)?;
        write_kv_slot(page, dst, &s)?;
        i -= 1;
    }

    write_kv_slot(
        page,
        leaf_slot(pos),
        &KvSlot {
            key_off: ko,
            key_len: key.len(),
            val_off: vo,
            val_len: value.len(),
        },
    )?;
    leaf_set_count(page, n + 1)?;
    leaf_set_data_end(page, new_de)
}

/// Delete entry at index `i`, shifting later slots left.
pub fn leaf_delete_at(page: &mut [u8], i: usize) -> Result<()> {
    let n = leaf_count(page)?;
    let mut j = i;
    while j + 1 < n {
        let s = read_kv_slot(page, leaf_slot(j + 1))?;
        write_kv_slot(page, leaf_slot(j), &s)?;
        j += 1;
    }
    leaf_set_count(page, n - 1)
}

/// Total key+value bytes for entry `i`.
pub fn leaf_entry_data_size(page: &[u8], i: usize) -> Result<usize> {
    let s = read_kv_slot(page, leaf_slot(i))?;
    Ok(s.key_len + s.val_len)
}

// ─── Internal Node ───────────────────────────────────────────────────────────
// Layout: [PageHeader:16][count:2][data_end:2][left_child:4][slots…][…data][CRC:4]
// Slot:   [right_child:4][key_off:2][key_len:2]

/// Total size of the internal-node header.
pub const INTL_HDR: usize = PAGE_HEADER_SIZE + 2 + 2 + 4; // 24
/// Size of one internal index slot.
pub const INTL_SLOT: usize = 8;

/// Initialise a page as an internal node with a leftmost child.
pub fn internal_init(page: &mut [u8], page_id: PageId, left_child: PageId) -> Result<()> {
    PageHeader::new(PageType::BTreeInternal, page_id).encode(page)?;
    let de = page.len() - PAGE_CHECKSUM_SIZE;
    endian::write_u16_le(&mut page[16..], 0)?;
    endian::write_u16_le(&mut page[18..], to_u16(de))?;
    endian::write_u32_le(&mut page[20..], left_child)
}

/// Number of separator keys in an internal node.
pub fn internal_count(page: &[u8]) -> Result<usize> {
    Ok(usize::from(endian::read_u16_le(&page[16..])?))
}

fn internal_set_count(page: &mut [u8], c: usize) -> Result<()> {
    endian::write_u16_le(&mut page[16..], to_u16(c))
}

fn internal_data_end(page: &[u8]) -> Result<usize> {
    Ok(usize::from(endian::read_u16_le(&page[18..])?))
}

fn internal_set_data_end(page: &mut [u8], v: usize) -> Result<()> {
    endian::write_u16_le(&mut page[18..], to_u16(v))
}

/// The leftmost child pointer (for keys less than the first separator).
pub fn internal_left_child(page: &[u8]) -> Result<PageId> {
    endian::read_u32_le(&page[20..])
}

/// Set the leftmost child pointer.
pub fn internal_set_left_child(page: &mut [u8], id: PageId) -> Result<()> {
    endian::write_u32_le(&mut page[20..], id)
}

fn intl_slot(i: usize) -> usize {
    INTL_HDR + i * INTL_SLOT
}

fn read_key_slot(page: &[u8], base: usize) -> Result<KeySlot> {
    Ok(KeySlot {
        child: endian::read_u32_le(&page[base..])?,
        key_off: usize::from(endian::read_u16_le(&page[base + 4..])?),
        key_len: usize::from(endian::read_u16_le(&page[base + 6..])?),
    })
}

fn write_key_slot(page: &mut [u8], base: usize, s: &KeySlot) -> Result<()> {
    endian::write_u32_le(&mut page[base..], s.child)?;
    endian::write_u16_le(&mut page[base + 4..], to_u16(s.key_off))?;
    endian::write_u16_le(&mut page[base + 6..], to_u16(s.key_len))
}

/// Return the separator key at index `i`.
pub fn internal_key_at(page: &[u8], i: usize) -> Result<&[u8]> {
    let s = read_key_slot(page, intl_slot(i))?;
    page.get(s.key_off..s.key_off + s.key_len)
        .ok_or(Error::Corruption)
}

/// Return the right-child pointer for separator `i`.
pub fn internal_child_at(page: &[u8], i: usize) -> Result<PageId> {
    let s = read_key_slot(page, intl_slot(i))?;
    Ok(s.child)
}

/// Find which child to follow for `key` (binary search).
pub fn internal_find_child(page: &[u8], key: &[u8]) -> Result<PageId> {
    let n = internal_count(page)?;
    let (mut lo, mut hi) = (0usize, n);
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        if key >= internal_key_at(page, mid)? {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    if lo == 0 {
        internal_left_child(page)
    } else {
        internal_child_at(page, lo - 1)
    }
}

/// Check whether a new separator of `key_len` bytes fits.
pub fn internal_has_space(page: &[u8], key_len: usize) -> Result<bool> {
    let n = internal_count(page)?;
    let de = internal_data_end(page)?;
    let idx_end = INTL_HDR + (n + 1) * INTL_SLOT;
    Ok(de >= idx_end + key_len)
}

/// Insert `(key, right_child)` at sorted position, shifting later slots.
pub fn internal_insert(page: &mut [u8], key: &[u8], right_child: PageId) -> Result<()> {
    let n = internal_count(page)?;

    // Binary search for insertion point
    let mut pos = 0;
    while pos < n {
        if key < internal_key_at(page, pos)? {
            break;
        }
        pos += 1;
    }

    let de = internal_data_end(page)?;
    let new_de = de - key.len();
    page[new_de..new_de + key.len()].copy_from_slice(key);

    // Shift slots [pos..n] → [pos+1..n+1]
    let mut i = n;
    while i > pos {
        let s = read_key_slot(page, intl_slot(i - 1))?;
        write_key_slot(page, intl_slot(i), &s)?;
        i -= 1;
    }

    write_key_slot(
        page,
        intl_slot(pos),
        &KeySlot {
            child: right_child,
            key_off: new_de,
            key_len: key.len(),
        },
    )?;
    internal_set_count(page, n + 1)?;
    internal_set_data_end(page, new_de)
}

#[cfg(test)]
// Tests use unwrap for brevity; panics are acceptable in test code.
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn leaf_insert_tiny_page_errors() {
        // Page too small for slot area — write_kv_slot fails, exercising
        // the error propagation path in leaf_insert_at.
        let mut page = [0u8; 30]; // LEAF_HDR(28) + 2 bytes, not enough for slot
        leaf_init(&mut page, 0).unwrap();
        assert!(leaf_insert_at(&mut page, 0, &[1], &[]).is_err());
    }

    #[test]
    fn internal_insert_tiny_page_errors() {
        // Page too small for slot area — write_key_slot fails, exercising
        // the error propagation path in internal_insert.
        let mut page = [0u8; 26]; // INTL_HDR(24) + 2 bytes, not enough for slot
        internal_init(&mut page, 0, 1).unwrap();
        assert!(internal_insert(&mut page, &[1], 2).is_err());
    }
}
