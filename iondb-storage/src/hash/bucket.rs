//! Hash bucket page operations.
//!
//! A bucket stores sorted key-value entries within a single page.
//! Shared by both extendible and linear hash engines.
//!
//! # Layout
//!
//! ```text
//! [PageHeader:16][count:2][data_end:2][local_depth:2][pad:2][slots…][…data][CRC:4]
//! ```
//!
//! Each slot is 8 bytes: `[key_off:2][key_len:2][val_off:2][val_len:2]`

// Page-level helpers have uniform error conditions (buffer bounds / corruption).
#![allow(clippy::missing_errors_doc)]

use iondb_core::endian;
use iondb_core::error::{Error, Result};
use iondb_core::page::{PageHeader, PageType, PAGE_CHECKSUM_SIZE, PAGE_HEADER_SIZE};
use iondb_core::types::PageId;

/// Total bucket header size.
pub const BUCKET_HDR: usize = PAGE_HEADER_SIZE + 2 + 2 + 2 + 2; // 24
/// Size of one bucket index slot.
pub const BUCKET_SLOT: usize = 8;

/// Truncate usize to u16.
// Page sizes validated at engine construction to fit u16.
#[allow(clippy::cast_possible_truncation)]
fn to_u16(v: usize) -> u16 {
    v as u16
}

struct Slot {
    key_off: usize,
    key_len: usize,
    val_off: usize,
    val_len: usize,
}

fn slot_base(i: usize) -> usize {
    BUCKET_HDR + i * BUCKET_SLOT
}

fn read_slot(page: &[u8], base: usize) -> Result<Slot> {
    Ok(Slot {
        key_off: usize::from(endian::read_u16_le(&page[base..])?),
        key_len: usize::from(endian::read_u16_le(&page[base + 2..])?),
        val_off: usize::from(endian::read_u16_le(&page[base + 4..])?),
        val_len: usize::from(endian::read_u16_le(&page[base + 6..])?),
    })
}

fn write_slot(page: &mut [u8], base: usize, s: &Slot) -> Result<()> {
    endian::write_u16_le(&mut page[base..], to_u16(s.key_off))?;
    endian::write_u16_le(&mut page[base + 2..], to_u16(s.key_len))?;
    endian::write_u16_le(&mut page[base + 4..], to_u16(s.val_off))?;
    endian::write_u16_le(&mut page[base + 6..], to_u16(s.val_len))
}

/// Initialise a page as an empty hash bucket.
pub fn bucket_init(page: &mut [u8], page_id: PageId, local_depth: u16) -> Result<()> {
    PageHeader::new(PageType::HashBucket, page_id).encode(page)?;
    let de = page.len() - PAGE_CHECKSUM_SIZE;
    endian::write_u16_le(&mut page[16..], 0)?; // count
    endian::write_u16_le(&mut page[18..], to_u16(de))?; // data_end
    endian::write_u16_le(&mut page[20..], local_depth)?; // local_depth
    endian::write_u16_le(&mut page[22..], 0) // padding
}

/// Number of entries.
pub fn bucket_count(page: &[u8]) -> Result<usize> {
    Ok(usize::from(endian::read_u16_le(&page[16..])?))
}

fn bucket_set_count(page: &mut [u8], c: usize) -> Result<()> {
    endian::write_u16_le(&mut page[16..], to_u16(c))
}

pub fn bucket_data_end(page: &[u8]) -> Result<usize> {
    Ok(usize::from(endian::read_u16_le(&page[18..])?))
}

fn bucket_set_data_end(page: &mut [u8], v: usize) -> Result<()> {
    endian::write_u16_le(&mut page[18..], to_u16(v))
}

/// Local depth (for extendible hashing).
#[cfg(feature = "storage-hash-ext")]
pub fn bucket_local_depth(page: &[u8]) -> Result<u16> {
    endian::read_u16_le(&page[20..])
}

/// Set local depth.
#[cfg(feature = "storage-hash-ext")]
pub fn bucket_set_local_depth(page: &mut [u8], d: u16) -> Result<()> {
    endian::write_u16_le(&mut page[20..], d)
}

/// Return the key at entry `i`.
pub fn bucket_key_at(page: &[u8], i: usize) -> Result<&[u8]> {
    let s = read_slot(page, slot_base(i))?;
    page.get(s.key_off..s.key_off + s.key_len)
        .ok_or(Error::Corruption)
}

/// Return the value at entry `i`.
pub fn bucket_value_at(page: &[u8], i: usize) -> Result<&[u8]> {
    let s = read_slot(page, slot_base(i))?;
    page.get(s.val_off..s.val_off + s.val_len)
        .ok_or(Error::Corruption)
}

/// Binary search for `key`. `Ok(i)` = found, `Err(i)` = insertion point.
pub fn bucket_search(page: &[u8], key: &[u8]) -> Result<core::result::Result<usize, usize>> {
    let n = bucket_count(page)?;
    let (mut lo, mut hi) = (0, n);
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        match bucket_key_at(page, mid)?.cmp(key) {
            core::cmp::Ordering::Equal => return Ok(Ok(mid)),
            core::cmp::Ordering::Less => lo = mid + 1,
            core::cmp::Ordering::Greater => hi = mid,
        }
    }
    Ok(Err(lo))
}

/// Check if key+value fits.
pub fn bucket_has_space(page: &[u8], key_len: usize, val_len: usize) -> Result<bool> {
    let n = bucket_count(page)?;
    let de = bucket_data_end(page)?;
    let idx_end = BUCKET_HDR + (n + 1) * BUCKET_SLOT;
    Ok(de >= idx_end + key_len + val_len)
}

/// Insert key-value at sorted position `pos`.
pub fn bucket_insert_at(page: &mut [u8], pos: usize, key: &[u8], value: &[u8]) -> Result<()> {
    let n = bucket_count(page)?;
    let de = bucket_data_end(page)?;
    let new_de = de - key.len() - value.len();

    page[new_de..new_de + key.len()].copy_from_slice(key);
    let vo = new_de + key.len();
    page[vo..vo + value.len()].copy_from_slice(value);

    let mut i = n;
    while i > pos {
        let s = read_slot(page, slot_base(i - 1))?;
        write_slot(page, slot_base(i), &s)?;
        i -= 1;
    }

    write_slot(
        page,
        slot_base(pos),
        &Slot {
            key_off: new_de,
            key_len: key.len(),
            val_off: vo,
            val_len: value.len(),
        },
    )?;
    bucket_set_count(page, n + 1)?;
    bucket_set_data_end(page, new_de)
}

/// Delete entry at `i`.
pub fn bucket_delete_at(page: &mut [u8], i: usize) -> Result<()> {
    let n = bucket_count(page)?;
    let mut j = i;
    while j + 1 < n {
        let s = read_slot(page, slot_base(j + 1))?;
        write_slot(page, slot_base(j), &s)?;
        j += 1;
    }
    bucket_set_count(page, n - 1)
}

/// Total data bytes for entry `i`.
pub fn bucket_entry_size(page: &[u8], i: usize) -> Result<usize> {
    let s = read_slot(page, slot_base(i))?;
    Ok(s.key_len + s.val_len)
}

#[cfg(test)]
// Tests use unwrap for brevity; panics are acceptable in test code.
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn bucket_insert_tiny_page_errors() {
        // Page too small for slot area — write_slot fails, exercising
        // the error propagation path in bucket_insert_at.
        let mut page = [0u8; 26]; // BUCKET_HDR(24) + 2 bytes
        bucket_init(&mut page, 0, 0).unwrap();
        assert!(bucket_insert_at(&mut page, 0, &[1], &[]).is_err());
    }
}
