//! Page-segmented WAL layout: page packing, page-boundary recovery.
//!
//! Records are packed into fixed-size [`WalSegment`] pages. Each page has a
//! 16-byte [`PageHeader`] at the front and a 4-byte CRC-32 checksum at the
//! back. Records never span page boundaries: if a record does not fit in the
//! remaining space of the current page, the current page is finalised and a
//! fresh page is started.
//!
//! # Page layout
//!
//! ```text
//! [16 bytes: PageHeader] [records …] [padding] [4 bytes: CRC-32]
//! ```
//!
//! `usable_space = page_size - PAGE_OVERHEAD` (`PAGE_OVERHEAD` = 20).
//!
//! [`WalSegment`]: iondb_core::page::PageType::WalSegment

use iondb_core::{
    crc,
    error::{Error, Result},
    page::{PageHeader, PageType, PAGE_CHECKSUM_SIZE, PAGE_HEADER_SIZE, PAGE_OVERHEAD},
    traits::io_backend::IoBackend,
    types::PageId,
};

use crate::record::{self, RECORD_HEADER_SIZE};

// ── PagedWriter ───────────────────────────────────────────────────────────────

/// Sequential writer that packs WAL records into fixed-size pages.
///
/// Records do not span page boundaries. When a record does not fit in the
/// remaining space of the current page, the current page is finalised (CRC
/// written) and a new page is started automatically.
///
/// Use [`PagedWriter::new`] for a fresh WAL and [`PagedWriter::resume`] when
/// reopening an existing WAL with [`crate::wal::Wal::open`].
#[derive(Debug)]
pub struct PagedWriter {
    /// Size of each page in bytes (including header and checksum).
    page_size: usize,
    /// Byte offset in the backend where the current page starts.
    page_offset: u64,
    /// Write cursor within the current page (starts at [`PAGE_HEADER_SIZE`]).
    pos_in_page: usize,
    /// Page identifier to assign to the next new page.
    next_page_id: PageId,
}

impl PagedWriter {
    /// Create a new [`PagedWriter`] for a fresh WAL segment.
    ///
    /// `start_offset` is the byte offset in the backend where the first page
    /// will be written. `pos_in_page` is initialised to [`PAGE_HEADER_SIZE`]
    /// so that the first [`write_record`][Self::write_record] call will trigger
    /// a new-page write.
    #[must_use]
    pub fn new(page_size: usize, start_offset: u64) -> Self {
        Self {
            page_size,
            page_offset: start_offset,
            pos_in_page: PAGE_HEADER_SIZE,
            next_page_id: 0,
        }
    }

    /// Resume a [`PagedWriter`] from a previously persisted cursor position.
    ///
    /// Used by [`crate::wal::Wal::open`] to reconstruct the writer state after
    /// reopening an existing WAL without replaying every record.
    #[must_use]
    pub fn resume(
        page_size: usize,
        page_offset: u64,
        pos_in_page: usize,
        next_page_id: PageId,
    ) -> Self {
        Self {
            page_size,
            page_offset,
            pos_in_page,
            next_page_id,
        }
    }

    // ── Getters ───────────────────────────────────────────────────────────────

    /// Return the byte offset of the start of the current page.
    #[must_use]
    pub fn page_offset(&self) -> u64 {
        self.page_offset
    }

    /// Return the write cursor within the current page.
    #[must_use]
    pub fn pos_in_page(&self) -> usize {
        self.pos_in_page
    }

    /// Return the page identifier that will be assigned to the next new page.
    #[must_use]
    pub fn next_page_id(&self) -> PageId {
        self.next_page_id
    }

    /// Return the absolute byte offset of the current write position.
    ///
    /// Equivalent to `page_offset + pos_in_page`.
    #[must_use]
    pub fn current_offset(&self) -> u64 {
        self.page_offset + self.pos_in_page as u64
    }

    // ── Core operations ───────────────────────────────────────────────────────

    /// Write `record_data` to the WAL, starting a new page if necessary.
    ///
    /// Returns the absolute byte offset at which the record was written.
    ///
    /// If `record_data` is larger than the usable space per page
    /// (`page_size - PAGE_OVERHEAD`) the record is rejected with
    /// [`Error::WalError`] — records that would never fit are not retried on
    /// a new page.
    ///
    /// # Errors
    ///
    /// - [`Error::WalError`] if `record_data` exceeds usable space.
    /// - [`Error::Io`] if a backend write fails.
    pub fn write_record(
        &mut self,
        backend: &mut impl IoBackend,
        record_data: &[u8],
    ) -> Result<u64> {
        let usable = self.page_size - PAGE_OVERHEAD;

        // Reject records that will never fit in any page.
        if record_data.len() > usable {
            return Err(Error::WalError);
        }

        // Start the very first page if this is the initial write.
        let is_initial = self.pos_in_page == PAGE_HEADER_SIZE
            && self.page_offset == 0
            && self.next_page_id == 0;
        if is_initial {
            self.start_new_page(backend)?;
        }

        // Determine remaining usable bytes in the current page.
        let usable_end = self.page_size - PAGE_CHECKSUM_SIZE;
        let remaining = usable_end.saturating_sub(self.pos_in_page);

        // If the record doesn't fit, finalise the current page and open a new one.
        if record_data.len() > remaining {
            self.finalize_page(backend)?;
            self.start_new_page(backend)?;
        }

        // Write the record at the current position.
        let record_offset = self.current_offset();
        let written = backend.write(record_offset, record_data)?;
        if written != record_data.len() {
            return Err(Error::Io);
        }
        self.pos_in_page += record_data.len();

        Ok(record_offset)
    }

    /// Finalise the current page by computing and writing its CRC-32 checksum.
    ///
    /// The checksum covers every byte of the page from offset 0 up to (but not
    /// including) the 4-byte checksum slot at the end. The CRC is computed
    /// incrementally in 64-byte chunks to avoid allocating a full-page buffer.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Io`] if any backend read or write fails.
    pub fn finalize_page(&mut self, backend: &mut impl IoBackend) -> Result<()> {
        let checksum_offset = self.page_offset + (self.page_size - PAGE_CHECKSUM_SIZE) as u64;
        let data_len = self.page_size - PAGE_CHECKSUM_SIZE;

        // Compute CRC incrementally in 64-byte chunks.
        let mut crc_state = 0xFFFF_FFFFu32;
        let mut read_pos: u64 = self.page_offset;
        let mut remaining = data_len;

        let mut chunk = [0u8; 64];
        while remaining > 0 {
            let to_read = remaining.min(chunk.len());
            let n = backend.read(read_pos, &mut chunk[..to_read])?;
            if n == 0 {
                break;
            }
            crc_state = crc::crc32_update(crc_state, &chunk[..n]);
            read_pos += n as u64;
            remaining -= n;
        }

        let checksum = crc_state ^ 0xFFFF_FFFF;

        // Write the 4-byte checksum at the end of the page.
        let cs_bytes = checksum.to_le_bytes();
        let written = backend.write(checksum_offset, &cs_bytes)?;
        if written != PAGE_CHECKSUM_SIZE {
            return Err(Error::Io);
        }

        Ok(())
    }

    /// Start a new page: advance `page_offset` to the next page boundary,
    /// write a [`PageHeader`] with [`PageType::WalSegment`], and reset the
    /// internal write cursor.
    ///
    /// On the very first call (`page_id` 0) the offset is not advanced because
    /// no previous page exists yet.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Io`] if the backend write fails.
    fn start_new_page(&mut self, backend: &mut impl IoBackend) -> Result<()> {
        // Advance to the next page boundary only when this is not the very
        // first page (next_page_id > 0 means we already wrote at least one).
        if self.next_page_id > 0 {
            self.page_offset += self.page_size as u64;
        }

        let header = PageHeader::new(PageType::WalSegment, self.next_page_id);
        let mut header_buf = [0u8; PAGE_HEADER_SIZE];
        header.encode(&mut header_buf)?;

        let written = backend.write(self.page_offset, &header_buf)?;
        if written != PAGE_HEADER_SIZE {
            return Err(Error::Io);
        }

        self.next_page_id = self.next_page_id.wrapping_add(1);
        self.pos_in_page = PAGE_HEADER_SIZE;

        Ok(())
    }
}

// ── read_record_paged ─────────────────────────────────────────────────────────

/// Read the next WAL record from a paged layout.
///
/// Returns `Ok(Some((record, new_page_offset, new_pos_in_page)))` on success,
/// where the two cursor values indicate where to continue reading. Returns
/// `Ok(None)` when the end of the log is reached. Returns
/// [`Error::Corruption`] when a record header is present but the CRC-32 does
/// not match (the caller should skip to the next page).
///
/// # Page-boundary rules
///
/// - If `pos_in_page >= usable_end` (i.e., inside the checksum slot), advance
///   to the next page.
/// - If fewer than [`RECORD_HEADER_SIZE`] bytes remain before the checksum
///   slot, treat the trailing bytes as padding and advance to the next page.
/// - If no WAL magic is found at the current position, treat the rest of the
///   page as empty/padding and advance to the next page.
///
/// # Errors
///
/// Returns [`Error::Corruption`] for CRC mismatches on otherwise valid
/// records. Returns [`Error::Io`] for backend failures.
pub fn read_record_paged<'buf>(
    backend: &impl IoBackend,
    page_offset: u64,
    pos_in_page: usize,
    page_size: usize,
    end_offset: u64,
    buf: &'buf mut [u8],
) -> Result<Option<(record::WalRecord<'buf>, u64, usize)>> {
    let usable_end = page_size - PAGE_CHECKSUM_SIZE;

    let mut cur_page = page_offset;
    let mut cur_pos = pos_in_page;

    loop {
        let abs_offset = cur_page + cur_pos as u64;

        // End of log.
        if abs_offset >= end_offset {
            return Ok(None);
        }

        // Advance past the checksum slot (and any inter-page gap).
        if cur_pos >= usable_end {
            cur_page += page_size as u64;
            cur_pos = PAGE_HEADER_SIZE;
            continue;
        }

        // Not enough space for even a record header — treat as padding.
        if cur_pos + RECORD_HEADER_SIZE > usable_end {
            cur_page += page_size as u64;
            cur_pos = PAGE_HEADER_SIZE;
            continue;
        }

        // Read the record header to check magic.
        if buf.len() < RECORD_HEADER_SIZE {
            return Err(Error::WalError);
        }
        let abs_cur = cur_page + cur_pos as u64;
        let n = backend.read(abs_cur, &mut buf[..RECORD_HEADER_SIZE])?;
        if n < RECORD_HEADER_SIZE {
            // Truncated — end of written data.
            return Ok(None);
        }

        // No magic at this position → rest of page is empty/padding.
        if buf[0] != record::MAGIC[0] || buf[1] != record::MAGIC[1] {
            cur_page += page_size as u64;
            cur_pos = PAGE_HEADER_SIZE;
            continue;
        }

        // Parse key_len and val_len to determine total record size.
        let key_len = usize::from(iondb_core::endian::read_u16_le(&buf[23..25])?);
        let val_len = iondb_core::endian::read_u32_le(&buf[25..29])? as usize;
        let total = record::record_size(key_len, val_len);

        // Ensure the full record fits in the page (before the checksum).
        if cur_pos + total > usable_end {
            // Record claims to extend beyond the usable area — skip page.
            cur_page += page_size as u64;
            cur_pos = PAGE_HEADER_SIZE;
            continue;
        }

        // Ensure our read buffer is large enough.
        if buf.len() < total {
            return Err(Error::WalError);
        }

        // Read the full record.
        let n = backend.read(abs_cur, &mut buf[..total])?;
        if n < total {
            return Ok(None);
        }

        // Deserialise and validate CRC — propagate Corruption to caller.
        let rec = record::deserialize_from(&buf[..total])?;
        let new_pos = cur_pos + total;

        return Ok(Some((rec, cur_page, new_pos)));
    }
}

// ── verify_page ───────────────────────────────────────────────────────────────

/// Verify the CRC-32 checksum of a single page.
///
/// Reads `page_size` bytes from `backend` starting at `page_offset`,
/// computes the CRC-32 over the first `page_size - 4` bytes, and compares it
/// with the 4-byte little-endian checksum stored in the last 4 bytes.
///
/// # Errors
///
/// - [`Error::Corruption`] if the stored and computed checksums differ.
/// - [`Error::WalError`] if the page cannot be fully read.
/// - [`Error::Io`] for backend failures.
pub fn verify_page(
    backend: &impl IoBackend,
    page_offset: u64,
    page_size: usize,
) -> Result<()> {
    // Read in 64-byte chunks to avoid large stack allocations.
    let data_len = page_size - PAGE_CHECKSUM_SIZE;
    let mut crc_state = 0xFFFF_FFFFu32;
    let mut read_pos = page_offset;
    let mut remaining = data_len;

    let mut chunk = [0u8; 64];
    while remaining > 0 {
        let to_read = remaining.min(chunk.len());
        let n = backend.read(read_pos, &mut chunk[..to_read])?;
        if n == 0 {
            return Err(Error::WalError);
        }
        crc_state = crc::crc32_update(crc_state, &chunk[..n]);
        read_pos += n as u64;
        remaining -= n;
    }

    let computed = crc_state ^ 0xFFFF_FFFF;

    // Read the stored checksum.
    let checksum_offset = page_offset + data_len as u64;
    let mut cs_buf = [0u8; PAGE_CHECKSUM_SIZE];
    let n = backend.read(checksum_offset, &mut cs_buf)?;
    if n < PAGE_CHECKSUM_SIZE {
        return Err(Error::WalError);
    }

    let stored = u32::from_le_bytes(cs_buf);
    if computed == stored {
        Ok(())
    } else {
        Err(Error::Corruption)
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
// Tests use unwrap for brevity; panics are acceptable in test code.
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::record::{serialize_into, RecordType};
    use iondb_io::memory::MemoryIoBackend;

    /// Serialise a Put record into a stack buffer and return the serialised bytes.
    fn make_put_record(lsn: u64, key: &[u8], value: &[u8]) -> ([u8; 512], usize) {
        let mut buf = [0u8; 512];
        let n = serialize_into(&mut buf, lsn, 0, RecordType::Put, key, value).unwrap();
        (buf, n)
    }

    /// Write a single Put record, finalise the page, then read it back.
    #[test]
    fn write_and_read_single_record() {
        let mut storage = [0u8; 4096];
        let mut backend = MemoryIoBackend::new(&mut storage);

        let page_size = 256usize;
        let mut writer = PagedWriter::new(page_size, 0);

        let (buf, n) = make_put_record(1, b"hello", b"world");
        let record_offset = writer.write_record(&mut backend, &buf[..n]).unwrap();
        writer.finalize_page(&mut backend).unwrap();

        // The record should start after the page header.
        assert_eq!(record_offset, PAGE_HEADER_SIZE as u64);

        // Read it back.
        let end = writer.current_offset();
        let mut read_buf = [0u8; 512];
        let result = read_record_paged(
            &backend,
            0,
            PAGE_HEADER_SIZE,
            page_size,
            end,
            &mut read_buf,
        )
        .unwrap();

        let (rec, _, _) = result.unwrap();
        assert_eq!(rec.lsn, 1);
        assert_eq!(rec.record_type, RecordType::Put);
        assert_eq!(rec.key, b"hello");
        assert_eq!(rec.value, b"world");
    }

    /// A record that exceeds the usable space per page is rejected with
    /// [`Error::WalError`].
    #[test]
    fn record_too_large_for_page_rejected() {
        let mut storage = [0u8; 4096];
        let mut backend = MemoryIoBackend::new(&mut storage);

        // page_size = 64 → usable = 44 bytes.
        let page_size = 64usize;
        let mut writer = PagedWriter::new(page_size, 0);

        // Build a record that is definitely larger than 44 bytes.
        let key = [0u8; 30];
        let value = [0u8; 30];
        let (buf, n) = make_put_record(1, &key, &value);

        let result = writer.write_record(&mut backend, &buf[..n]);
        assert_eq!(result, Err(Error::WalError));
    }

    /// Writing records across multiple pages (auto page-flip) round-trips
    /// correctly for all records.
    #[test]
    fn auto_new_page_when_full() {
        let mut storage = [0u8; 4096];
        let mut backend = MemoryIoBackend::new(&mut storage);

        let page_size = 128usize;
        let mut writer = PagedWriter::new(page_size, 0);

        // Each record: RECORD_HEADER_SIZE (29) + 1 + 1 = 31 bytes.
        // Usable per page: 128 - 20 = 108 bytes → fits 3 records per page
        // (3 * 31 = 93 ≤ 108). Ten records → 4 pages.
        let mut offsets: [u64; 10] = [0; 10];
        for i in 0u8..10 {
            let key = [i; 1];
            let value = [i + 100; 1];
            let (buf, n) = make_put_record(u64::from(i), &key, &value);
            offsets[usize::from(i)] = writer.write_record(&mut backend, &buf[..n]).unwrap();
        }
        writer.finalize_page(&mut backend).unwrap();

        let end = writer.current_offset();

        // Read all records back.
        let mut count = 0usize;
        let mut cur_page: u64 = 0;
        let mut cur_pos: usize = PAGE_HEADER_SIZE;
        let mut read_buf = [0u8; 512];

        loop {
            match read_record_paged(
                &backend,
                cur_page,
                cur_pos,
                page_size,
                end,
                &mut read_buf,
            )
            .unwrap()
            {
                None => break,
                Some((rec, np, npos)) => {
                    assert_eq!(rec.lsn, count as u64);
                    assert_eq!(rec.key, &[count as u8]);
                    assert_eq!(rec.value, &[count as u8 + 100]);
                    cur_page = np;
                    cur_pos = npos;
                    count += 1;
                }
            }
        }

        assert_eq!(count, 10);
    }

    /// `verify_page` returns [`Error::Corruption`] after a byte is corrupted.
    #[test]
    fn corruption_in_page_detected() {
        let mut storage = [0u8; 4096];
        let mut backend = MemoryIoBackend::new(&mut storage);

        let page_size = 256usize;
        let mut writer = PagedWriter::new(page_size, 0);

        let (buf, n) = make_put_record(99, b"key", b"val");
        let _record_offset = writer.write_record(&mut backend, &buf[..n]).unwrap();
        writer.finalize_page(&mut backend).unwrap();

        // Page should verify cleanly before corruption.
        verify_page(&backend, 0, page_size).unwrap();

        // Corrupt a byte in the payload area.
        let _written = backend.write(PAGE_HEADER_SIZE as u64, &[0xFF]).unwrap();

        // Verification must now report corruption.
        let result = verify_page(&backend, 0, page_size);
        assert_eq!(result, Err(Error::Corruption));
    }
}
