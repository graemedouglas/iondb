//! Page format: header layout, page types, and checksum integration.
//!
//! Every page in `IonDB` starts with a fixed-size [`PageHeader`] and ends with
//! a CRC-32 checksum. The header is encoded in little-endian byte order for
//! portability across architectures.
//!
//! # On-disk layout (16-byte header)
//!
//! ```text
//! Offset  Size  Field
//! ------  ----  -----
//!  0      1     page_type (PageType as u8)
//!  1      1     reserved (must be 0)
//!  2      2     flags (little-endian u16)
//!  4      4     page_id (little-endian u32)
//!  8      8     lsn (little-endian u64)
//! ```
//!
//! The last 4 bytes of every page hold a CRC-32 checksum computed over all
//! preceding bytes (header + payload).

use crate::crc;
use crate::endian;
use crate::error::{Error, Result};
use crate::types::{Lsn, PageId};

/// Size of the page header in bytes.
pub const PAGE_HEADER_SIZE: usize = 16;

/// Size of the trailing CRC-32 checksum in bytes.
pub const PAGE_CHECKSUM_SIZE: usize = 4;

/// Minimum usable payload per page (header + checksum overhead).
pub const PAGE_OVERHEAD: usize = PAGE_HEADER_SIZE + PAGE_CHECKSUM_SIZE;

/// Page type tag stored in the first byte of the header.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum PageType {
    /// Unallocated / free page.
    Free = 0,
    /// B+ tree internal (index) node.
    BTreeInternal = 1,
    /// B+ tree leaf node.
    BTreeLeaf = 2,
    /// Hash table bucket.
    HashBucket = 3,
    /// Hash table directory.
    HashDirectory = 4,
    /// Overflow page for large keys/values.
    Overflow = 5,
    /// WAL segment page.
    WalSegment = 6,
}

impl PageType {
    /// Convert a raw byte to a [`PageType`].
    ///
    /// # Errors
    ///
    /// Returns [`Error::PageError`] if the byte does not map to a known type.
    pub fn from_byte(b: u8) -> Result<Self> {
        match b {
            0 => Ok(Self::Free),
            1 => Ok(Self::BTreeInternal),
            2 => Ok(Self::BTreeLeaf),
            3 => Ok(Self::HashBucket),
            4 => Ok(Self::HashDirectory),
            5 => Ok(Self::Overflow),
            6 => Ok(Self::WalSegment),
            _ => Err(Error::PageError),
        }
    }

    /// Convert to the raw byte representation.
    #[must_use]
    pub fn as_byte(self) -> u8 {
        self as u8
    }
}

/// Fixed-size page header (16 bytes, little-endian on disk).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PageHeader {
    /// The type of this page.
    pub page_type: PageType,
    /// Bit flags (reserved for future use; currently 0).
    pub flags: u16,
    /// Unique page identifier within the storage file.
    pub page_id: PageId,
    /// Log sequence number of the last modification.
    pub lsn: Lsn,
}

impl PageHeader {
    /// Create a new page header.
    #[must_use]
    pub fn new(page_type: PageType, page_id: PageId) -> Self {
        Self {
            page_type,
            flags: 0,
            page_id,
            lsn: 0,
        }
    }

    /// Encode this header into `buf` (must be at least [`PAGE_HEADER_SIZE`] bytes).
    ///
    /// # Errors
    ///
    /// Returns [`Error::PageError`] if `buf` is too small.
    pub fn encode(&self, buf: &mut [u8]) -> Result<()> {
        if buf.len() < PAGE_HEADER_SIZE {
            return Err(Error::PageError);
        }
        endian::write_u8(&mut buf[0..], self.page_type.as_byte())?;
        endian::write_u8(&mut buf[1..], 0)?; // reserved
        endian::write_u16_le(&mut buf[2..], self.flags)?;
        endian::write_u32_le(&mut buf[4..], self.page_id)?;
        endian::write_u64_le(&mut buf[8..], self.lsn)?;
        Ok(())
    }

    /// Decode a header from `buf` (must be at least [`PAGE_HEADER_SIZE`] bytes).
    ///
    /// # Errors
    ///
    /// Returns [`Error::PageError`] if `buf` is too small or contains an
    /// invalid page type.
    pub fn decode(buf: &[u8]) -> Result<Self> {
        if buf.len() < PAGE_HEADER_SIZE {
            return Err(Error::PageError);
        }
        let page_type = PageType::from_byte(endian::read_u8(&buf[0..])?)?;
        // buf[1] is reserved — ignore on read
        let flags = endian::read_u16_le(&buf[2..])?;
        let page_id = endian::read_u32_le(&buf[4..])?;
        let lsn = endian::read_u64_le(&buf[8..])?;

        Ok(Self {
            page_type,
            flags,
            page_id,
            lsn,
        })
    }
}

/// Write a CRC-32 checksum into the last 4 bytes of `page`.
///
/// The checksum covers all bytes except the last 4 (the checksum slot itself).
///
/// # Errors
///
/// Returns [`Error::PageError`] if `page` is smaller than [`PAGE_OVERHEAD`].
pub fn write_page_checksum(page: &mut [u8]) -> Result<()> {
    if page.len() < PAGE_OVERHEAD {
        return Err(Error::PageError);
    }
    let data_len = page.len() - PAGE_CHECKSUM_SIZE;
    let checksum = crc::crc32(&page[..data_len]);
    endian::write_u32_le(&mut page[data_len..], checksum)
}

/// Verify the CRC-32 checksum in the last 4 bytes of `page`.
///
/// # Errors
///
/// Returns [`Error::Corruption`] if the checksum does not match.
/// Returns [`Error::PageError`] if `page` is smaller than [`PAGE_OVERHEAD`].
pub fn verify_page_checksum(page: &[u8]) -> Result<()> {
    if page.len() < PAGE_OVERHEAD {
        return Err(Error::PageError);
    }
    let data_len = page.len() - PAGE_CHECKSUM_SIZE;
    let expected = crc::crc32(&page[..data_len]);
    let stored = endian::read_u32_le(&page[data_len..])?;
    if expected == stored {
        Ok(())
    } else {
        Err(Error::Corruption)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn page_type_round_trip() {
        let types = [
            PageType::Free,
            PageType::BTreeInternal,
            PageType::BTreeLeaf,
            PageType::HashBucket,
            PageType::HashDirectory,
            PageType::Overflow,
            PageType::WalSegment,
        ];
        for pt in types {
            assert_eq!(PageType::from_byte(pt.as_byte()), Ok(pt));
        }
    }

    #[test]
    fn page_type_invalid_byte() {
        assert_eq!(PageType::from_byte(7), Err(Error::PageError));
        assert_eq!(PageType::from_byte(255), Err(Error::PageError));
    }

    #[test]
    fn header_encode_decode_round_trip() {
        let header = PageHeader {
            page_type: PageType::BTreeLeaf,
            flags: 0x00FF,
            page_id: 42,
            lsn: 12345,
        };
        let mut buf = [0u8; PAGE_HEADER_SIZE];
        assert_eq!(header.encode(&mut buf), Ok(()));
        let decoded = PageHeader::decode(&buf);
        assert_eq!(decoded, Ok(header));
    }

    #[test]
    fn header_new_defaults() {
        let h = PageHeader::new(PageType::Free, 0);
        assert_eq!(h.page_type, PageType::Free);
        assert_eq!(h.flags, 0);
        assert_eq!(h.page_id, 0);
        assert_eq!(h.lsn, 0);
    }

    #[test]
    fn header_encode_too_small() {
        let header = PageHeader::new(PageType::Free, 0);
        let mut buf = [0u8; PAGE_HEADER_SIZE - 1];
        assert_eq!(header.encode(&mut buf), Err(Error::PageError));
    }

    #[test]
    fn header_decode_too_small() {
        let buf = [0u8; PAGE_HEADER_SIZE - 1];
        assert_eq!(PageHeader::decode(&buf), Err(Error::PageError));
    }

    #[test]
    fn header_byte_layout() {
        let header = PageHeader {
            page_type: PageType::BTreeInternal,
            flags: 0,
            page_id: 1,
            lsn: 0,
        };
        let mut buf = [0u8; PAGE_HEADER_SIZE];
        assert_eq!(header.encode(&mut buf), Ok(()));
        // page_type at offset 0
        assert_eq!(buf[0], 1);
        // reserved at offset 1
        assert_eq!(buf[1], 0);
        // flags at offset 2..4 (LE)
        assert_eq!(buf[2], 0);
        assert_eq!(buf[3], 0);
        // page_id at offset 4..8 (LE)
        assert_eq!(buf[4], 1);
        assert_eq!(buf[5], 0);
        assert_eq!(buf[6], 0);
        assert_eq!(buf[7], 0);
    }

    #[test]
    fn page_checksum_write_and_verify() {
        let mut page = [0u8; 64];
        // Write a header
        let header = PageHeader::new(PageType::BTreeLeaf, 7);
        assert_eq!(header.encode(&mut page), Ok(()));
        // Fill some payload
        page[PAGE_HEADER_SIZE] = 0xAA;
        page[PAGE_HEADER_SIZE + 1] = 0xBB;

        assert_eq!(write_page_checksum(&mut page), Ok(()));
        assert_eq!(verify_page_checksum(&page), Ok(()));
    }

    #[test]
    fn page_checksum_detects_corruption() {
        let mut page = [0u8; 64];
        let header = PageHeader::new(PageType::Free, 0);
        assert_eq!(header.encode(&mut page), Ok(()));
        assert_eq!(write_page_checksum(&mut page), Ok(()));

        // Corrupt a byte
        page[PAGE_HEADER_SIZE] ^= 0xFF;
        assert_eq!(verify_page_checksum(&page), Err(Error::Corruption));
    }

    #[test]
    fn page_checksum_too_small() {
        let mut tiny = [0u8; PAGE_OVERHEAD - 1];
        assert_eq!(write_page_checksum(&mut tiny), Err(Error::PageError));
        assert_eq!(verify_page_checksum(&tiny), Err(Error::PageError));
    }
}
