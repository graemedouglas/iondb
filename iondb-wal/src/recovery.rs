//! Recovery readers: raw and committed-only, with zero-alloc `no_std` support.
//!
//! [`RawRecoveryReader`] replays every valid record from a checkpoint forward,
//! including uncommitted transactions. [`CommittedRecoveryReader`] performs a
//! two-pass scan: the first pass (at construction time) identifies committed
//! transaction IDs, and the second pass (via [`CommittedRecoveryReader::next_record`])
//! yields only records belonging to those transactions.
//!
//! Both readers work with flat and page-segmented WAL layouts. Corruption in
//! flat mode triggers a magic-byte scan to skip to the next record; in paged
//! mode, corruption skips to the next page boundary.

use iondb_core::{
    error::{Error, Result},
    page::{PAGE_CHECKSUM_SIZE, PAGE_HEADER_SIZE},
    traits::io_backend::IoBackend,
    types::TxnId,
};

use crate::config::WalLayout;
use crate::flat;
use crate::record::{self, RecordType, WalRecord, RECORD_HEADER_SIZE};

// ── Shared cursor logic ─────────────────────────────────────────────────────

/// Cursor state shared by both flat and paged readers.
///
/// This struct captures the mutable iteration state so that the "advance to
/// next valid record" logic can be separated from the borrow of the read
/// buffer.
#[derive(Debug, Clone)]
struct Cursor {
    /// Current read offset (flat layout) / absolute position tracker.
    offset: u64,
    /// End-of-log offset.
    end: u64,
    /// Current page start offset (paged layout only).
    page_offset: u64,
    /// Position within the current page (paged layout only).
    pos_in_page: usize,
    /// `true` once the reader has reached end-of-log.
    done: bool,
}

/// Outcome of attempting to advance the cursor to the next record.
#[derive(Debug)]
enum Advance {
    /// A valid record was deserialized; the cursor has been updated.
    Record,
    /// End-of-log reached; cursor is done.
    Done,
}

impl Cursor {
    /// Advance the flat-layout cursor, reading the next record into `buf`.
    ///
    /// On success, the record bytes are in `buf[..total]` and the cursor has
    /// been advanced past the record. On corruption, the cursor skips via
    /// magic scan and tries again.
    fn advance_flat<I: IoBackend>(
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
    fn advance_paged<I: IoBackend>(
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

// ── RawRecoveryReader ───────────────────────────────────────────────────────

/// Reads ALL valid records from `start` to `end`, including uncommitted
/// transactions.
///
/// For flat layouts, corruption triggers [`flat::scan_for_magic`] to find the
/// next record. For paged layouts, corruption skips to the next page boundary.
pub struct RawRecoveryReader<'a, I: IoBackend> {
    /// The underlying I/O backend.
    backend: &'a I,
    /// WAL storage layout (flat or page-segmented).
    layout: &'a WalLayout,
    /// Mutable iteration state.
    cursor: Cursor,
}

impl<I: IoBackend> core::fmt::Debug for RawRecoveryReader<'_, I> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("RawRecoveryReader")
            .field("layout", &self.layout)
            .field("cursor", &self.cursor)
            .finish_non_exhaustive()
    }
}

impl<'a, I: IoBackend> RawRecoveryReader<'a, I> {
    /// Create a new raw recovery reader scanning from `start` to `end`.
    #[must_use]
    pub fn new(backend: &'a I, layout: &'a WalLayout, start: u64, end: u64) -> Self {
        Self {
            backend,
            layout,
            cursor: Cursor {
                offset: start,
                end,
                page_offset: start,
                pos_in_page: PAGE_HEADER_SIZE,
                done: start >= end,
            },
        }
    }

    /// Read the next valid record.
    ///
    /// Returns `Ok(None)` when the end of the log is reached. The returned
    /// [`WalRecord`] borrows its key/value slices from `buf`.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Io`] for backend failures. Corruption is handled
    /// internally by skipping to the next candidate record.
    pub fn next_record<'buf>(
        &mut self,
        buf: &'buf mut [u8],
    ) -> Result<Option<WalRecord<'buf>>> {
        if self.cursor.done {
            return Ok(None);
        }

        // Phase 1: advance cursor, filling buf with valid record bytes.
        // The advance methods handle corruption recovery internally without
        // holding any borrow on buf across iterations.
        let advance = match *self.layout {
            WalLayout::Flat => self.cursor.advance_flat(self.backend, buf)?,
            WalLayout::PageSegmented { page_size } => {
                self.cursor.advance_paged(self.backend, page_size, buf)?
            }
        };

        match advance {
            Advance::Done => Ok(None),
            // Phase 2: buf now contains a CRC-validated record. Deserialize it.
            // This is guaranteed to succeed since we already validated the CRC.
            Advance::Record => {
                let rec = record::deserialize_from(buf)?;
                Ok(Some(rec))
            }
        }
    }
}

// ── CommittedRecoveryReader ─────────────────────────────────────────────────

/// Reads only records belonging to committed transactions.
///
/// Construction performs a first pass over the WAL to identify committed
/// transaction IDs (stored in the caller-provided `scratch` buffer). The
/// [`next_record`][CommittedRecoveryReader::next_record] method then performs a
/// second pass, yielding only records whose `txn_id` appears in the committed
/// set.
pub struct CommittedRecoveryReader<'a, I: IoBackend> {
    /// The underlying I/O backend.
    backend: &'a I,
    /// WAL storage layout (flat or page-segmented).
    layout: &'a WalLayout,
    /// Mutable iteration state.
    cursor: Cursor,
    /// Scratch buffer holding committed `TxnId`s.
    scratch: &'a [TxnId],
    /// Number of committed `TxnId`s in `scratch[..committed_count]`.
    committed_count: usize,
}

impl<I: IoBackend> core::fmt::Debug for CommittedRecoveryReader<'_, I> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("CommittedRecoveryReader")
            .field("layout", &self.layout)
            .field("cursor", &self.cursor)
            .field("committed_count", &self.committed_count)
            .finish_non_exhaustive()
    }
}

impl<'a, I: IoBackend> CommittedRecoveryReader<'a, I> {
    /// Create a new committed recovery reader.
    ///
    /// Performs a first pass over the WAL from `start` to `end` to identify
    /// all committed transaction IDs. The IDs are stored in `scratch`.
    ///
    /// # Errors
    ///
    /// Returns [`Error::WalError`] if there are more committed transactions
    /// than `scratch` slots.
    pub fn new(
        backend: &'a I,
        layout: &'a WalLayout,
        start: u64,
        end: u64,
        scratch: &'a mut [TxnId],
    ) -> Result<Self> {
        let committed_count = Self::scan_committed(backend, layout, start, end, scratch)?;

        Ok(Self {
            backend,
            layout,
            cursor: Cursor {
                offset: start,
                end,
                page_offset: start,
                pos_in_page: PAGE_HEADER_SIZE,
                done: start >= end,
            },
            scratch,
            committed_count,
        })
    }

    /// Read the next committed record.
    ///
    /// Skips records whose `txn_id` is not in the committed set. Returns
    /// `Ok(None)` when the end of the log is reached.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Io`] for backend failures.
    pub fn next_record<'buf>(
        &mut self,
        buf: &'buf mut [u8],
    ) -> Result<Option<WalRecord<'buf>>> {
        loop {
            if self.cursor.done {
                return Ok(None);
            }

            let advance = match *self.layout {
                WalLayout::Flat => self.cursor.advance_flat(self.backend, buf)?,
                WalLayout::PageSegmented { page_size } => {
                    self.cursor.advance_paged(self.backend, page_size, buf)?
                }
            };

            match advance {
                Advance::Done => return Ok(None),
                Advance::Record => {
                    // Peek at the txn_id from the header without full
                    // deserialization to decide whether to skip.
                    let txn_id = iondb_core::endian::read_u64_le(&buf[14..22])?;
                    if self.is_committed(txn_id) {
                        let rec = record::deserialize_from(buf)?;
                        return Ok(Some(rec));
                    }
                    // Not committed — loop to advance past this record.
                }
            }
        }
    }

    /// Check whether `txn_id` is in the committed set.
    fn is_committed(&self, txn_id: TxnId) -> bool {
        self.scratch[..self.committed_count].contains(&txn_id)
    }

    /// First-pass scan: collect committed `TxnId`s into `scratch`.
    ///
    /// Returns the number of committed transactions found.
    fn scan_committed(
        backend: &I,
        layout: &WalLayout,
        start: u64,
        end: u64,
        scratch: &mut [TxnId],
    ) -> Result<usize> {
        let mut count = 0usize;

        match *layout {
            WalLayout::Flat => {
                Self::scan_committed_flat(backend, start, end, scratch, &mut count)?;
            }
            WalLayout::PageSegmented { page_size } => {
                Self::scan_committed_paged(
                    backend, start, end, page_size, scratch, &mut count,
                )?;
            }
        }

        Ok(count)
    }

    /// Flat-layout first-pass scan for Commit records.
    fn scan_committed_flat(
        backend: &I,
        start: u64,
        end: u64,
        scratch: &mut [TxnId],
        count: &mut usize,
    ) -> Result<()> {
        let mut offset = start;
        let mut header_buf = [0u8; RECORD_HEADER_SIZE];

        while offset < end {
            let n = backend.read(offset, &mut header_buf)?;
            if n < RECORD_HEADER_SIZE {
                break;
            }

            match record::read_header(&header_buf) {
                Ok((_lsn, txn_id, record_type, key_len, val_len)) => {
                    let total =
                        record::record_size(usize::from(key_len), val_len as usize);

                    if record_type == RecordType::Commit {
                        if *count >= scratch.len() {
                            return Err(Error::WalError);
                        }
                        scratch[*count] = txn_id;
                        *count += 1;
                    }

                    offset += total as u64;
                }
                Err(_) => {
                    match flat::scan_for_magic(backend, offset + 1, end)? {
                        Some(next) => offset = next,
                        None => break,
                    }
                }
            }
        }

        Ok(())
    }

    /// Paged-layout first-pass scan for Commit records.
    fn scan_committed_paged(
        backend: &I,
        start: u64,
        end: u64,
        page_size: usize,
        scratch: &mut [TxnId],
        count: &mut usize,
    ) -> Result<()> {
        let usable_end = page_size - PAGE_CHECKSUM_SIZE;
        let mut page_offset = start;
        let mut pos_in_page = PAGE_HEADER_SIZE;
        let mut header_buf = [0u8; RECORD_HEADER_SIZE];

        loop {
            let abs_offset = page_offset + pos_in_page as u64;
            if abs_offset >= end {
                break;
            }

            if pos_in_page >= usable_end {
                page_offset += page_size as u64;
                pos_in_page = PAGE_HEADER_SIZE;
                continue;
            }

            if pos_in_page + RECORD_HEADER_SIZE > usable_end {
                page_offset += page_size as u64;
                pos_in_page = PAGE_HEADER_SIZE;
                continue;
            }

            let abs_cur = page_offset + pos_in_page as u64;
            let n = backend.read(abs_cur, &mut header_buf)?;
            if n < RECORD_HEADER_SIZE {
                break;
            }

            // No magic — rest of page is empty/padding.
            if header_buf[0] != record::MAGIC[0] || header_buf[1] != record::MAGIC[1] {
                page_offset += page_size as u64;
                pos_in_page = PAGE_HEADER_SIZE;
                continue;
            }

            if let Ok((_lsn, txn_id, record_type, key_len, val_len)) =
                record::read_header(&header_buf)
            {
                let total =
                    record::record_size(usize::from(key_len), val_len as usize);

                if pos_in_page + total > usable_end {
                    page_offset += page_size as u64;
                    pos_in_page = PAGE_HEADER_SIZE;
                    continue;
                }

                if record_type == RecordType::Commit {
                    if *count >= scratch.len() {
                        return Err(Error::WalError);
                    }
                    scratch[*count] = txn_id;
                    *count += 1;
                }

                pos_in_page += total;
            } else {
                page_offset += page_size as u64;
                pos_in_page = PAGE_HEADER_SIZE;
            }
        }

        Ok(())
    }
}

// ── OwnedWalRecord ──────────────────────────────────────────────────────────

/// Heap-allocated version of [`WalRecord`], available with the `alloc` feature.
///
/// Useful when records must outlive the read buffer (e.g. collecting all
/// committed records into a `Vec`).
#[cfg(feature = "alloc")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwnedWalRecord {
    /// Log sequence number.
    pub lsn: iondb_core::types::Lsn,
    /// Transaction identifier.
    pub txn_id: TxnId,
    /// Record type.
    pub record_type: RecordType,
    /// Key payload (heap-allocated copy).
    pub key: alloc::vec::Vec<u8>,
    /// Value payload (heap-allocated copy).
    pub value: alloc::vec::Vec<u8>,
}

#[cfg(feature = "alloc")]
impl OwnedWalRecord {
    /// Create an owned copy of a borrowed [`WalRecord`].
    #[must_use]
    pub fn from_borrowed(rec: &WalRecord<'_>) -> Self {
        Self {
            lsn: rec.lsn,
            txn_id: rec.txn_id,
            record_type: rec.record_type,
            key: alloc::vec::Vec::from(rec.key),
            value: alloc::vec::Vec::from(rec.value),
        }
    }
}

// ── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
#[allow(clippy::unwrap_used, unused_results)]
mod tests {
    use super::*;
    use crate::config::{SyncPolicy, TruncationMode, WalConfig};
    use crate::wal::Wal;
    use iondb_io::memory::MemoryIoBackend;

    /// Helper: create a flat WAL config with no sync and logical truncation.
    fn flat_config() -> WalConfig {
        WalConfig {
            layout: WalLayout::Flat,
            sync_policy: SyncPolicy::None,
            truncation: TruncationMode::Logical,
        }
    }

    // ── raw_recovery_empty_wal ──────────────────────────────────────────────

    /// An empty WAL yields no records via the raw recovery reader.
    #[test]
    fn raw_recovery_empty_wal() {
        let mut storage = [0u8; 4096];
        let backend = MemoryIoBackend::new(&mut storage);
        let wal = Wal::new(backend, flat_config()).unwrap();

        let mut reader = wal.recover().unwrap();
        let mut buf = [0u8; 512];
        assert!(reader.next_record(&mut buf).unwrap().is_none());
    }

    // ── raw_recovery_reads_all_records ──────────────────────────────────────

    /// Raw reader returns ALL records including uncommitted ones.
    #[test]
    fn raw_recovery_reads_all_records() {
        let mut storage = [0u8; 4096];
        let backend = MemoryIoBackend::new(&mut storage);
        let mut wal = Wal::new(backend, flat_config()).unwrap();

        // txn1: begin, put, commit
        wal.begin_tx(1).unwrap();
        wal.put(1, b"k1", b"v1").unwrap();
        wal.commit_tx(1).unwrap();

        // txn2: begin, put (no commit)
        wal.begin_tx(2).unwrap();
        wal.put(2, b"k2", b"v2").unwrap();

        let mut reader = wal.recover().unwrap();
        let mut buf = [0u8; 512];
        let mut count = 0;
        while reader.next_record(&mut buf).unwrap().is_some() {
            count += 1;
        }
        assert_eq!(count, 5);
    }

    // ── committed_recovery_filters_uncommitted ──────────────────────────────

    /// Committed reader returns only records from committed transactions.
    #[test]
    fn committed_recovery_filters_uncommitted() {
        let mut storage = [0u8; 4096];
        let backend = MemoryIoBackend::new(&mut storage);
        let mut wal = Wal::new(backend, flat_config()).unwrap();

        // txn1: begin, put, commit
        wal.begin_tx(1).unwrap();
        wal.put(1, b"k1", b"v1").unwrap();
        wal.commit_tx(1).unwrap();

        // txn2: begin, put (no commit)
        wal.begin_tx(2).unwrap();
        wal.put(2, b"k2", b"v2").unwrap();

        let mut scratch = [0u64; 16];
        let mut reader = wal.recover_committed(&mut scratch).unwrap();
        let mut buf = [0u8; 512];
        let mut count = 0;
        let mut txn_ids = [0u64; 16];
        while let Some(rec) = reader.next_record(&mut buf).unwrap() {
            txn_ids[count] = rec.txn_id;
            count += 1;
        }
        // Only txn1's 3 records (begin, put, commit).
        assert_eq!(count, 3);
        for i in 0..count {
            assert_eq!(txn_ids[i], 1);
        }
    }

    // ── committed_recovery_interleaved_transactions ─────────────────────────

    /// Interleaved transactions: only the committed one's records are returned.
    #[test]
    fn committed_recovery_interleaved_transactions() {
        let mut storage = [0u8; 4096];
        let backend = MemoryIoBackend::new(&mut storage);
        let mut wal = Wal::new(backend, flat_config()).unwrap();

        // Interleave txn1 and txn2.
        wal.begin_tx(1).unwrap();
        wal.begin_tx(2).unwrap();
        wal.put(1, b"k1", b"v1").unwrap();
        wal.put(2, b"k2", b"v2").unwrap();
        wal.commit_tx(1).unwrap();
        wal.rollback_tx(2).unwrap();

        let mut scratch = [0u64; 16];
        let mut reader = wal.recover_committed(&mut scratch).unwrap();
        let mut buf = [0u8; 512];
        let mut count = 0;
        while let Some(rec) = reader.next_record(&mut buf).unwrap() {
            assert_eq!(rec.txn_id, 1);
            count += 1;
        }
        // txn1: begin, put, commit = 3 records.
        assert_eq!(count, 3);
    }

    // ── scratch_buffer_exhaustion ───────────────────────────────────────────

    /// Providing a scratch buffer that is too small returns WalError.
    #[test]
    fn scratch_buffer_exhaustion() {
        let mut storage = [0u8; 8192];
        let backend = MemoryIoBackend::new(&mut storage);
        let mut wal = Wal::new(backend, flat_config()).unwrap();

        // Commit 3 different transactions.
        for txn_id in 1..=3u64 {
            wal.begin_tx(txn_id).unwrap();
            wal.put(txn_id, b"k", b"v").unwrap();
            wal.commit_tx(txn_id).unwrap();
        }

        // Provide scratch of size 2 (but 3 committed txns).
        let mut scratch = [0u64; 2];
        let result = wal.recover_committed(&mut scratch);
        assert_eq!(result.unwrap_err(), Error::WalError);
    }

    // ── recovery_lsn_order ──────────────────────────────────────────────────

    /// Recovered records have monotonically increasing LSNs.
    #[test]
    fn recovery_lsn_order() {
        let mut storage = [0u8; 4096];
        let backend = MemoryIoBackend::new(&mut storage);
        let mut wal = Wal::new(backend, flat_config()).unwrap();

        wal.begin_tx(1).unwrap();
        wal.put(1, b"k1", b"v1").unwrap();
        wal.put(1, b"k2", b"v2").unwrap();
        wal.commit_tx(1).unwrap();

        let mut reader = wal.recover().unwrap();
        let mut buf = [0u8; 512];
        let mut prev_lsn: Option<iondb_core::types::Lsn> = None;
        while let Some(rec) = reader.next_record(&mut buf).unwrap() {
            if let Some(prev) = prev_lsn {
                assert!(rec.lsn > prev, "LSNs must be monotonically increasing");
            }
            prev_lsn = Some(rec.lsn);
        }
        // Ensure we actually read some records.
        assert!(prev_lsn.is_some());
    }
}
