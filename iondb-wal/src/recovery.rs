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
//!
//! Cursor iteration logic is in the internal `cursor` module.

use iondb_core::{
    error::{Error, Result},
    page::{PAGE_CHECKSUM_SIZE, PAGE_HEADER_SIZE},
    traits::io_backend::IoBackend,
    types::TxnId,
};

use crate::config::WalLayout;
use crate::cursor::{Advance, Cursor};
use crate::flat;
use crate::record::{self, RecordType, WalRecord, RECORD_HEADER_SIZE};

#[cfg(test)]
#[path = "recovery_tests.rs"]
mod tests;

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
    pub fn next_record<'buf>(&mut self, buf: &'buf mut [u8]) -> Result<Option<WalRecord<'buf>>> {
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
    pub fn next_record<'buf>(&mut self, buf: &'buf mut [u8]) -> Result<Option<WalRecord<'buf>>> {
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
                Self::scan_committed_paged(backend, start, end, page_size, scratch, &mut count)?;
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
                    let total = record::record_size(usize::from(key_len), val_len as usize);

                    if record_type == RecordType::Commit {
                        if *count >= scratch.len() {
                            return Err(Error::WalError);
                        }
                        scratch[*count] = txn_id;
                        *count += 1;
                    }

                    offset += total as u64;
                }
                Err(_) => match flat::scan_for_magic(backend, offset + 1, end)? {
                    Some(next) => offset = next,
                    None => break,
                },
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
                let total = record::record_size(usize::from(key_len), val_len as usize);

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
