//! Shared cursor logic for flat and paged WAL recovery readers.
//!
//! [`Cursor`] captures mutable iteration state so that the
//! "advance to next valid record" logic can be separated from the borrow of
//! the read buffer. Both [`RawRecoveryReader`] and [`CommittedRecoveryReader`]
//! delegate their low-level reads to this module.
//!
//! [`RawRecoveryReader`]: crate::recovery::RawRecoveryReader
//! [`CommittedRecoveryReader`]: crate::recovery::CommittedRecoveryReader

use iondb_core::{
    error::{Error, Result},
    page::{PAGE_CHECKSUM_SIZE, PAGE_HEADER_SIZE},
    traits::io_backend::IoBackend,
};

use crate::flat;
use crate::record::{self, RECORD_HEADER_SIZE};

// ── Cursor ───────────────────────────────────────────────────────────────────

/// Cursor state shared by both flat and paged readers.
///
/// This struct captures the mutable iteration state so that the "advance to
/// next valid record" logic can be separated from the borrow of the read
/// buffer.
#[derive(Debug, Clone)]
pub(crate) struct Cursor {
    /// Current read offset (flat layout) / absolute position tracker.
    pub(crate) offset: u64,
    /// End-of-log offset.
    pub(crate) end: u64,
    /// Current page start offset (paged layout only).
    pub(crate) page_offset: u64,
    /// Position within the current page (paged layout only).
    pub(crate) pos_in_page: usize,
    /// `true` once the reader has reached end-of-log.
    pub(crate) done: bool,
}

// ── Advance ──────────────────────────────────────────────────────────────────

/// Outcome of attempting to advance the cursor to the next record.
#[derive(Debug)]
pub(crate) enum Advance {
    /// A valid record was deserialized; the cursor has been updated.
    Record,
    /// End-of-log reached; cursor is done.
    Done,
}

// ── impl Cursor ──────────────────────────────────────────────────────────────

impl Cursor {
    /// Advance the flat-layout cursor, reading the next record into `buf`.
    ///
    /// On success, the record bytes are in `buf[..total]` and the cursor has
    /// been advanced past the record. On corruption, the cursor skips via
    /// magic scan and tries again.
    pub(crate) fn advance_flat<I: IoBackend>(
        &mut self,
        backend: &I,
        buf: &mut [u8],
    ) -> Result<Advance> {
        loop {
            if self.offset >= self.end {
                self.done = true;
                return Ok(Advance::Done);
            }

            if buf.len() < RECORD_HEADER_SIZE {
                return Err(Error::WalError);
            }

            let n = backend.read(self.offset, &mut buf[..RECORD_HEADER_SIZE])?;
            if n < RECORD_HEADER_SIZE {
                self.done = true;
                return Ok(Advance::Done);
            }

            // No magic — end of written data.
            if buf[0] != record::MAGIC[0] || buf[1] != record::MAGIC[1] {
                self.done = true;
                return Ok(Advance::Done);
            }

            // Parse key_len/val_len to know total size.
            let key_len = usize::from(iondb_core::endian::read_u16_le(&buf[23..25])?);
            let val_len = iondb_core::endian::read_u32_le(&buf[25..29])? as usize;
            let total = record::record_size(key_len, val_len);

            if buf.len() < total {
                self.done = true;
                return Ok(Advance::Done);
            }

            // Read the full record.
            let n = backend.read(self.offset, &mut buf[..total])?;
            if n < total {
                self.done = true;
                return Ok(Advance::Done);
            }

            // Validate CRC. We do a manual check instead of `deserialize_from`
            // so that we can handle corruption without holding a borrow on buf.
            let stored_crc = iondb_core::endian::read_u32_le(&buf[2..6])?;
            let computed_crc = iondb_core::crc::crc32(&buf[6..total]);

            if computed_crc != stored_crc {
                // Corruption — scan for next magic.
                if let Some(next) = flat::scan_for_magic(backend, self.offset + 1, self.end)? {
                    self.offset = next;
                    continue;
                }
                self.done = true;
                return Ok(Advance::Done);
            }

            // CRC passed; advance cursor.
            self.offset += total as u64;
            return Ok(Advance::Record);
        }
    }

    /// Advance the paged-layout cursor, reading the next record into `buf`.
    pub(crate) fn advance_paged<I: IoBackend>(
        &mut self,
        backend: &I,
        page_size: usize,
        buf: &mut [u8],
    ) -> Result<Advance> {
        let usable_end = page_size - PAGE_CHECKSUM_SIZE;

        loop {
            let abs_offset = self.page_offset + self.pos_in_page as u64;
            if abs_offset >= self.end {
                self.done = true;
                return Ok(Advance::Done);
            }

            // Advance past checksum slot.
            if self.pos_in_page >= usable_end {
                self.page_offset += page_size as u64;
                self.pos_in_page = PAGE_HEADER_SIZE;
                continue;
            }

            // Not enough space for a header — skip to next page.
            if self.pos_in_page + RECORD_HEADER_SIZE > usable_end {
                self.page_offset += page_size as u64;
                self.pos_in_page = PAGE_HEADER_SIZE;
                continue;
            }

            if buf.len() < RECORD_HEADER_SIZE {
                return Err(Error::WalError);
            }

            let abs_cur = self.page_offset + self.pos_in_page as u64;
            let n = backend.read(abs_cur, &mut buf[..RECORD_HEADER_SIZE])?;
            if n < RECORD_HEADER_SIZE {
                self.done = true;
                return Ok(Advance::Done);
            }

            // No magic — rest of page is empty/padding.
            if buf[0] != record::MAGIC[0] || buf[1] != record::MAGIC[1] {
                self.page_offset += page_size as u64;
                self.pos_in_page = PAGE_HEADER_SIZE;
                continue;
            }

            // Parse lengths.
            let key_len = usize::from(iondb_core::endian::read_u16_le(&buf[23..25])?);
            let val_len = iondb_core::endian::read_u32_le(&buf[25..29])? as usize;
            let total = record::record_size(key_len, val_len);

            // Check record fits in page.
            if self.pos_in_page + total > usable_end {
                self.page_offset += page_size as u64;
                self.pos_in_page = PAGE_HEADER_SIZE;
                continue;
            }

            if buf.len() < total {
                return Err(Error::WalError);
            }

            // Read full record.
            let n = backend.read(abs_cur, &mut buf[..total])?;
            if n < total {
                self.done = true;
                return Ok(Advance::Done);
            }

            // Validate CRC manually.
            let stored_crc = iondb_core::endian::read_u32_le(&buf[2..6])?;
            let computed_crc = iondb_core::crc::crc32(&buf[6..total]);

            if computed_crc != stored_crc {
                // Corruption — skip to the next page.
                self.page_offset += page_size as u64;
                self.pos_in_page = PAGE_HEADER_SIZE;
                continue;
            }

            // CRC passed; advance cursor within the page.
            self.pos_in_page += total;
            return Ok(Advance::Record);
        }
    }
}
