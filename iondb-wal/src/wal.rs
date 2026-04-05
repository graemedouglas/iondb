//! Core WAL implementation: construction, append, sync, checkpoint, queries.
//!
//! The [`Wal`] struct is the main entry point for the write-ahead log. It
//! supports three storage layouts (flat, page-segmented, circular) and four
//! sync policies. Records are appended sequentially and assigned monotonically
//! increasing LSNs.

use iondb_core::{
    error::{Error, Result},
    page::PAGE_HEADER_SIZE,
    traits::io_backend::IoBackend,
    types::{Lsn, TxnId},
};

use crate::config::{SyncPolicy, TruncationMode, WalConfig, WalLayout};
use crate::flat::{self, CircularHeader, CIRCULAR_HEADER_SIZE};
use crate::paged::{self, PagedWriter};
use crate::record::{self, RecordType};

// ── Constants ────────────────────────────────────────────────────────────────

/// Maximum size of a serialized record buffer on the stack.
///
/// Records larger than this are rejected with [`Error::WalError`].
const MAX_RECORD_BUF: usize = 512;

// ── LayoutState ──────────────────────────────────────────────────────────────

/// Internal write-position state, determined by the [`WalLayout`].
#[derive(Debug)]
enum LayoutState {
    /// Flat layout: records are written back-to-back at sequential offsets.
    Flat {
        /// Byte offset where the next record will be written.
        write_offset: u64,
    },
    /// Page-segmented layout: records are packed into fixed-size pages.
    Paged {
        /// The paged writer that manages page boundaries.
        writer: PagedWriter,
    },
}

// ── Wal ──────────────────────────────────────────────────────────────────────

/// Core WAL struct parameterised over an [`IoBackend`].
///
/// Use [`Wal::new`] to create a fresh WAL or [`Wal::open`] to reopen an
/// existing one.
pub struct Wal<I: IoBackend> {
    /// The underlying I/O backend.
    backend: I,
    /// Configuration for this WAL instance.
    config: WalConfig,
    /// Internal layout state tracking the write position.
    layout: LayoutState,
    /// The next LSN to assign.
    next_lsn: Lsn,
    /// The LSN of the most recent checkpoint.
    checkpoint_lsn: Lsn,
    /// Number of records appended since the last sync.
    records_since_sync: u32,
}

impl<I: IoBackend> core::fmt::Debug for Wal<I> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("Wal")
            .field("config", &self.config)
            .field("layout", &self.layout)
            .field("next_lsn", &self.next_lsn)
            .field("checkpoint_lsn", &self.checkpoint_lsn)
            .field("records_since_sync", &self.records_since_sync)
            .finish_non_exhaustive()
    }
}

impl<I: IoBackend> Wal<I> {
    // ── Construction ─────────────────────────────────────────────────────────

    /// Create a fresh WAL on `backend` with the given `config`.
    ///
    /// For circular truncation, an initial [`CircularHeader`] is written and
    /// synced. For page-segmented layouts, a [`PagedWriter`] is initialised.
    ///
    /// # Errors
    ///
    /// Returns [`Error::WalError`] if config validation fails.
    /// Returns [`Error::Io`] if the backend write fails.
    pub fn new(mut backend: I, config: WalConfig) -> Result<Self> {
        config.validate()?;

        let layout = match config.layout {
            WalLayout::Flat => {
                let write_offset = match config.truncation {
                    TruncationMode::Circular { .. } => {
                        // Write initial circular header.
                        let header = CircularHeader {
                            head_offset: CIRCULAR_HEADER_SIZE as u64,
                            tail_offset: CIRCULAR_HEADER_SIZE as u64,
                            checkpoint_lsn: 0,
                        };
                        flat::write_circular_header(&mut backend, &header)?;
                        backend.sync()?;
                        CIRCULAR_HEADER_SIZE as u64
                    }
                    TruncationMode::Logical => 0,
                    #[cfg(feature = "std")]
                    TruncationMode::Physical => 0,
                };
                LayoutState::Flat { write_offset }
            }
            WalLayout::PageSegmented { page_size } => {
                let writer = PagedWriter::new(page_size, 0);
                LayoutState::Paged { writer }
            }
        };

        Ok(Self {
            backend,
            config,
            layout,
            next_lsn: 0,
            checkpoint_lsn: 0,
            records_since_sync: 0,
        })
    }

    /// Open an existing WAL from `backend` with the given `config`.
    ///
    /// Validates the configuration and scans the backend to restore the write
    /// position, `next_lsn`, and `checkpoint_lsn`.
    ///
    /// # Errors
    ///
    /// Returns [`Error::WalError`] if the backend is empty (size == 0) or
    /// config validation fails.
    pub fn open(backend: I, config: WalConfig) -> Result<Self> {
        config.validate()?;

        let size = backend.size()?;
        if size == 0 {
            return Err(Error::WalError);
        }

        let layout = match config.layout {
            WalLayout::Flat => LayoutState::Flat { write_offset: 0 },
            WalLayout::PageSegmented { page_size } => LayoutState::Paged {
                writer: PagedWriter::new(page_size, 0),
            },
        };

        let mut wal = Self {
            backend,
            config,
            layout,
            next_lsn: 0,
            checkpoint_lsn: 0,
            records_since_sync: 0,
        };

        match wal.config.layout {
            WalLayout::Flat => wal.scan_flat()?,
            WalLayout::PageSegmented { page_size } => wal.scan_paged(page_size)?,
        }

        Ok(wal)
    }

    // ── Scanning (private) ──────────────────────────────────────────────────

    /// Scan a flat-layout WAL to find the write position, max LSN, and
    /// checkpoint LSN.
    fn scan_flat(&mut self) -> Result<()> {
        let size = self.backend.size()?;

        // For circular layout, read the circular header first.
        let start_offset = match self.config.truncation {
            TruncationMode::Circular { .. } => {
                let header = flat::read_circular_header(&self.backend)?;
                self.checkpoint_lsn = header.checkpoint_lsn;
                header.tail_offset
            }
            TruncationMode::Logical => 0,
            #[cfg(feature = "std")]
            TruncationMode::Physical => 0,
        };

        let mut offset = start_offset;
        let mut max_lsn: Option<Lsn> = None;
        let mut buf = [0u8; MAX_RECORD_BUF];

        loop {
            match flat::read_record(&self.backend, offset, size, &mut buf)? {
                None => break,
                Some((rec, next_offset)) => {
                    if rec.record_type == RecordType::Checkpoint {
                        self.checkpoint_lsn = rec.lsn;
                    }
                    max_lsn = Some(match max_lsn {
                        Some(prev) if prev > rec.lsn => prev,
                        _ => rec.lsn,
                    });
                    offset = next_offset;
                }
            }
        }

        if let Some(lsn) = max_lsn {
            self.next_lsn = lsn + 1;
        }

        self.layout = LayoutState::Flat {
            write_offset: offset,
        };

        Ok(())
    }

    /// Scan a paged WAL to find the write position, max LSN, and checkpoint
    /// LSN, then create a [`PagedWriter`] at the correct resume position.
    fn scan_paged(&mut self, page_size: usize) -> Result<()> {
        let size = self.backend.size()?;

        let mut page_offset: u64 = 0;
        let mut pos_in_page = PAGE_HEADER_SIZE;
        let mut max_lsn: Option<Lsn> = None;
        let mut last_page_offset: u64 = 0;
        let mut last_pos_in_page = PAGE_HEADER_SIZE;
        let mut page_count: u32 = 0;
        let mut buf = [0u8; MAX_RECORD_BUF];

        loop {
            match paged::read_record_paged(
                &self.backend,
                page_offset,
                pos_in_page,
                page_size,
                size,
                &mut buf,
            )? {
                None => break,
                Some((rec, new_page_offset, new_pos_in_page)) => {
                    if rec.record_type == RecordType::Checkpoint {
                        self.checkpoint_lsn = rec.lsn;
                    }
                    max_lsn = Some(match max_lsn {
                        Some(prev) if prev > rec.lsn => prev,
                        _ => rec.lsn,
                    });

                    // Track page transitions for page_count.
                    if new_page_offset != last_page_offset {
                        page_count += 1;
                    }
                    last_page_offset = new_page_offset;
                    last_pos_in_page = new_pos_in_page;
                    page_offset = new_page_offset;
                    pos_in_page = new_pos_in_page;
                }
            }
        }

        if let Some(lsn) = max_lsn {
            self.next_lsn = lsn + 1;
            // We found at least one record: add 1 for the initial page.
            page_count += 1;
        }

        let writer = PagedWriter::resume(page_size, last_page_offset, last_pos_in_page, page_count);

        self.layout = LayoutState::Paged { writer };

        Ok(())
    }

    // ── Append ──────────────────────────────────────────────────────────────

    /// Append a `Begin` record for transaction `txn_id`.
    ///
    /// Returns the assigned LSN.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Io`] if the backend write fails, or [`Error::WalError`]
    /// if the record is too large.
    pub fn begin_tx(&mut self, txn_id: TxnId) -> Result<Lsn> {
        self.append_record(txn_id, RecordType::Begin, &[], &[])
    }

    /// Append a `Put` record for transaction `txn_id`.
    ///
    /// Returns the assigned LSN.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Io`] if the backend write fails, or [`Error::WalError`]
    /// if the record is too large.
    pub fn put(&mut self, txn_id: TxnId, key: &[u8], value: &[u8]) -> Result<Lsn> {
        self.append_record(txn_id, RecordType::Put, key, value)
    }

    /// Append a `Delete` record for transaction `txn_id`.
    ///
    /// Returns the assigned LSN.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Io`] if the backend write fails, or [`Error::WalError`]
    /// if the record is too large.
    pub fn delete(&mut self, txn_id: TxnId, key: &[u8]) -> Result<Lsn> {
        self.append_record(txn_id, RecordType::Delete, key, &[])
    }

    /// Append a `Commit` record for transaction `txn_id`.
    ///
    /// Returns the assigned LSN.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Io`] if the backend write fails, or [`Error::WalError`]
    /// if the record is too large.
    pub fn commit_tx(&mut self, txn_id: TxnId) -> Result<Lsn> {
        self.append_record(txn_id, RecordType::Commit, &[], &[])
    }

    /// Append a `Rollback` record for transaction `txn_id`.
    ///
    /// Returns the assigned LSN.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Io`] if the backend write fails, or [`Error::WalError`]
    /// if the record is too large.
    pub fn rollback_tx(&mut self, txn_id: TxnId) -> Result<Lsn> {
        self.append_record(txn_id, RecordType::Rollback, &[], &[])
    }

    /// Core append: serialize a record and write it to the backend.
    ///
    /// 1. Assigns the next LSN.
    /// 2. Serializes the record into a stack buffer.
    /// 3. For flat layout: checks circular capacity, writes via
    ///    [`flat::write_record`], and updates the write offset.
    /// 4. For paged layout: writes via [`PagedWriter::write_record`].
    /// 5. Increments `next_lsn` and `records_since_sync`.
    /// 6. Calls [`maybe_sync`][Self::maybe_sync].
    ///
    /// # Errors
    ///
    /// - [`Error::WalError`] if the serialized record exceeds [`MAX_RECORD_BUF`].
    /// - [`Error::CapacityExhausted`] if the circular buffer is full.
    /// - [`Error::Io`] if the backend write fails.
    fn append_record(
        &mut self,
        txn_id: TxnId,
        record_type: RecordType,
        key: &[u8],
        value: &[u8],
    ) -> Result<Lsn> {
        let lsn = self.next_lsn;

        // Serialize into a stack buffer.
        let mut buf = [0u8; MAX_RECORD_BUF];
        let total = record::serialize_into(&mut buf, lsn, txn_id, record_type, key, value)?;

        match &mut self.layout {
            LayoutState::Flat { write_offset } => {
                // For circular: check capacity.
                if let TruncationMode::Circular { capacity } = self.config.truncation {
                    let head = *write_offset;
                    // Tail is after the circular header for circular mode.
                    let tail = CIRCULAR_HEADER_SIZE as u64;
                    let free = circular_free_space(head, tail, capacity);
                    if total > free {
                        return Err(Error::CapacityExhausted);
                    }
                }

                let new_offset =
                    flat::write_record(&mut self.backend, *write_offset, &buf[..total])?;
                *write_offset = new_offset;
            }
            LayoutState::Paged { writer } => {
                let _record_offset = writer.write_record(&mut self.backend, &buf[..total])?;
            }
        }

        self.next_lsn = lsn + 1;
        self.records_since_sync += 1;
        self.maybe_sync(record_type)?;

        Ok(lsn)
    }

    // ── Sync ────────────────────────────────────────────────────────────────

    /// Force a sync of the backend.
    ///
    /// Resets `records_since_sync` to zero.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Io`] if the backend sync fails.
    pub fn sync(&mut self) -> Result<()> {
        self.backend.sync()?;
        self.records_since_sync = 0;
        Ok(())
    }

    /// Conditionally sync based on the configured [`SyncPolicy`].
    fn maybe_sync(&mut self, record_type: RecordType) -> Result<()> {
        let should_sync = match self.config.sync_policy {
            SyncPolicy::EveryRecord => true,
            SyncPolicy::EveryTransaction => {
                matches!(record_type, RecordType::Commit | RecordType::Rollback)
            }
            SyncPolicy::Periodic(n) => self.records_since_sync >= n,
            SyncPolicy::None => false,
        };

        if should_sync {
            self.sync()?;
        }

        Ok(())
    }

    // ── Checkpoint ──────────────────────────────────────────────────────────

    /// Append a checkpoint record and update internal checkpoint state.
    ///
    /// For circular truncation, the circular header is updated with the new
    /// checkpoint LSN. For physical truncation (behind `#[cfg(feature = "std")]`),
    /// a compaction pass is run.
    ///
    /// # Errors
    ///
    /// Propagates any error from the append or backend operations.
    pub fn checkpoint(&mut self, up_to_lsn: Lsn) -> Result<()> {
        // Append a Checkpoint record with txn_id=0.
        let _checkpoint_record_lsn =
            self.append_record(0, RecordType::Checkpoint, &[], &[])?;
        self.checkpoint_lsn = up_to_lsn;

        match self.config.truncation {
            TruncationMode::Circular { .. } => {
                // Update the circular header with the new checkpoint LSN.
                let write_offset = match &self.layout {
                    LayoutState::Flat { write_offset } => *write_offset,
                    LayoutState::Paged { .. } => {
                        // Should not happen: circular + paged is rejected by validation.
                        return Err(Error::WalError);
                    }
                };
                let header = CircularHeader {
                    head_offset: write_offset,
                    tail_offset: CIRCULAR_HEADER_SIZE as u64,
                    checkpoint_lsn: up_to_lsn,
                };
                flat::write_circular_header(&mut self.backend, &header)?;
                self.backend.sync()?;
            }
            #[cfg(feature = "std")]
            TruncationMode::Physical => {
                self.physical_compact()?;
            }
            TruncationMode::Logical => {
                // No-op: checkpoint_lsn is tracked in memory and discovered
                // by scan on open.
            }
        }

        Ok(())
    }

    /// Compact the WAL by copying live data to the beginning of the backend.
    ///
    /// Only available with the `std` feature.
    #[cfg(feature = "std")]
    fn physical_compact(&mut self) -> Result<()> {
        let size = self.backend.size()?;
        let mut offset = 0u64;
        let mut first_live_offset: Option<u64> = None;
        let mut buf = [0u8; MAX_RECORD_BUF];

        // Scan from start to find first record with LSN > checkpoint_lsn.
        loop {
            match flat::read_record(&self.backend, offset, size, &mut buf)? {
                None => break,
                Some((rec, next_offset)) => {
                    if rec.lsn > self.checkpoint_lsn && first_live_offset.is_none() {
                        first_live_offset = Some(offset);
                    }
                    offset = next_offset;
                }
            }
        }

        let end_offset = offset;

        let Some(live_start) = first_live_offset else {
            // All records are checkpointed; reset to empty.
            if let LayoutState::Flat { write_offset } = &mut self.layout {
                *write_offset = 0;
            }
            self.backend.sync()?;
            return Ok(());
        };

        // Write checkpoint pointer via circular header format at offset 0 as
        // crash safety commit point, then sync.
        let header = CircularHeader {
            head_offset: live_start,
            tail_offset: 0,
            checkpoint_lsn: self.checkpoint_lsn,
        };
        flat::write_circular_header(&mut self.backend, &header)?;
        self.backend.sync()?;

        // Copy live data to beginning in 256-byte chunks.
        let live_len = end_offset - live_start;
        let mut src = live_start;
        let mut dst = 0u64;
        let mut remaining = live_len;
        let mut chunk = [0u8; 256];

        while remaining > 0 {
            // remaining is bounded by the backend size which fits in memory.
            #[allow(clippy::cast_possible_truncation)]
            let to_copy = (remaining as usize).min(chunk.len());
            let n = self.backend.read(src, &mut chunk[..to_copy])?;
            if n == 0 {
                break;
            }
            let written = self.backend.write(dst, &chunk[..n])?;
            if written != n {
                return Err(Error::Io);
            }
            src += n as u64;
            dst += n as u64;
            remaining -= n as u64;
        }

        if let LayoutState::Flat { write_offset } = &mut self.layout {
            *write_offset = dst;
        }

        self.backend.sync()?;
        Ok(())
    }

    // ── Recovery ────────────────────────────────────────────────────────────

    /// Create a raw recovery reader from [`recovery_start`][Self::recovery_start]
    /// to [`write_end`][Self::write_end].
    ///
    /// The returned reader yields every valid record, including those from
    /// uncommitted transactions.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Io`] if the backend cannot be read.
    pub fn recover(&self) -> Result<crate::recovery::RawRecoveryReader<'_, I>> {
        Ok(crate::recovery::RawRecoveryReader::new(
            &self.backend,
            &self.config.layout,
            self.recovery_start(),
            self.write_end(),
        ))
    }

    /// Create a committed recovery reader.
    ///
    /// Performs a first pass over the WAL to identify committed transaction IDs,
    /// storing them in the caller-provided `scratch` buffer. The second pass
    /// (via [`CommittedRecoveryReader::next_record`]) yields only records from
    /// committed transactions.
    ///
    /// [`CommittedRecoveryReader::next_record`]: crate::recovery::CommittedRecoveryReader::next_record
    ///
    /// # Errors
    ///
    /// Returns [`Error::WalError`] if there are more committed transactions
    /// than `scratch` slots.
    pub fn recover_committed<'a>(
        &'a self,
        scratch: &'a mut [TxnId],
    ) -> Result<crate::recovery::CommittedRecoveryReader<'a, I>> {
        crate::recovery::CommittedRecoveryReader::new(
            &self.backend,
            &self.config.layout,
            self.recovery_start(),
            self.write_end(),
            scratch,
        )
    }

    /// Recover all committed records into a [`Vec`].
    ///
    /// Convenience wrapper around [`recover_committed`][Self::recover_committed]
    /// that collects all committed records into heap-allocated
    /// [`OwnedWalRecord`] values.
    ///
    /// [`OwnedWalRecord`]: crate::recovery::OwnedWalRecord
    ///
    /// # Errors
    ///
    /// Propagates any error from the underlying recovery reader.
    #[cfg(feature = "alloc")]
    pub fn recover_committed_to_vec(
        &self,
    ) -> Result<alloc::vec::Vec<crate::recovery::OwnedWalRecord>> {
        let mut scratch_buf = alloc::vec![0u64; 256];
        let mut reader = self.recover_committed(&mut scratch_buf)?;
        let mut buf = alloc::vec![0u8; 512];
        let mut records = alloc::vec::Vec::new();
        while let Some(rec) = reader.next_record(&mut buf)? {
            records.push(crate::recovery::OwnedWalRecord::from_borrowed(&rec));
        }
        Ok(records)
    }

    // ── Queries ─────────────────────────────────────────────────────────────

    /// Return the current (next-to-assign) LSN.
    #[must_use]
    pub fn current_lsn(&self) -> Lsn {
        self.next_lsn
    }

    /// Return the LSN of the most recent checkpoint.
    #[must_use]
    pub fn checkpoint_lsn(&self) -> Lsn {
        self.checkpoint_lsn
    }

    /// Return the remaining free space for circular WALs, or `None` for other
    /// layouts.
    #[must_use]
    pub fn remaining(&self) -> Option<usize> {
        match self.config.truncation {
            TruncationMode::Circular { capacity } => {
                let head = match &self.layout {
                    LayoutState::Flat { write_offset } => *write_offset,
                    LayoutState::Paged { .. } => return None,
                };
                let tail = CIRCULAR_HEADER_SIZE as u64;
                Some(circular_free_space(head, tail, capacity))
            }
            TruncationMode::Logical => None,
            #[cfg(feature = "std")]
            TruncationMode::Physical => None,
        }
    }

    /// Borrow the underlying I/O backend.
    #[must_use]
    pub fn backend(&self) -> &I {
        &self.backend
    }

    /// Return the current write-end offset.
    #[must_use]
    pub fn write_end(&self) -> u64 {
        match &self.layout {
            LayoutState::Flat { write_offset } => *write_offset,
            LayoutState::Paged { writer } => writer.current_offset(),
        }
    }

    /// Return a reference to the configured [`WalLayout`].
    #[must_use]
    pub fn layout(&self) -> &WalLayout {
        &self.config.layout
    }

    /// Return the recovery start offset.
    ///
    /// For circular WALs this is the tail offset from the circular header;
    /// for all other layouts it is 0.
    pub fn recovery_start(&self) -> u64 {
        if let TruncationMode::Circular { .. } = self.config.truncation {
            // Read the tail from the circular header if possible.
            if let Ok(header) = flat::read_circular_header(&self.backend) {
                return header.tail_offset;
            }
        }
        0
    }
}

// ── Helpers ──────────────────────────────────────────────────────────────────

/// Compute the free space in a circular buffer.
///
/// `head` is the next write position, `tail` is the oldest live record,
/// and `capacity` is the total buffer size in bytes.
///
/// The capacity comes from a `usize` and the head/tail offsets are bounded
/// by the capacity, so the result always fits in `usize`.
#[must_use]
#[allow(clippy::cast_possible_truncation)]
pub fn circular_free_space(head: u64, tail: u64, capacity: usize) -> usize {
    let cap = capacity as u64;
    if head >= tail {
        // Linear case: free space is capacity - (head - tail).
        (cap - (head - tail)) as usize
    } else {
        // Wrapped case: free space is tail - head.
        (tail - head) as usize
    }
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
#[allow(clippy::unwrap_used, unused_results)]
mod tests {
    use super::*;
    use crate::config::{SyncPolicy, TruncationMode, WalConfig, WalLayout};
    use iondb_io::memory::MemoryIoBackend;

    /// Helper: create a flat/logical config with no auto-sync.
    fn flat_logical() -> WalConfig {
        WalConfig {
            layout: WalLayout::Flat,
            sync_policy: SyncPolicy::None,
            truncation: TruncationMode::Logical,
        }
    }

    /// Helper: create a flat/circular config.
    fn flat_circular(capacity: usize) -> WalConfig {
        WalConfig {
            layout: WalLayout::Flat,
            sync_policy: SyncPolicy::None,
            truncation: TruncationMode::Circular { capacity },
        }
    }

    /// Helper: create a paged/logical config.
    fn paged_logical(page_size: usize) -> WalConfig {
        WalConfig {
            layout: WalLayout::PageSegmented { page_size },
            sync_policy: SyncPolicy::None,
            truncation: TruncationMode::Logical,
        }
    }

    // ── new_flat_logical ─────────────────────────────────────────────────────

    #[test]
    fn new_flat_logical() {
        let mut storage = [0u8; 4096];
        let backend = MemoryIoBackend::new(&mut storage);
        let wal = Wal::new(backend, flat_logical()).unwrap();

        assert_eq!(wal.current_lsn(), 0);
        assert_eq!(wal.checkpoint_lsn(), 0);
        assert!(wal.remaining().is_none());
    }

    // ── append_and_lsn_increments ────────────────────────────────────────────

    #[test]
    fn append_and_lsn_increments() {
        let mut storage = [0u8; 4096];
        let backend = MemoryIoBackend::new(&mut storage);
        let mut wal = Wal::new(backend, flat_logical()).unwrap();

        let lsn0 = wal.begin_tx(1).unwrap();
        let lsn1 = wal.put(1, b"key", b"value").unwrap();
        let lsn2 = wal.commit_tx(1).unwrap();

        assert_eq!(lsn0, 0);
        assert_eq!(lsn1, 1);
        assert_eq!(lsn2, 2);
        assert_eq!(wal.current_lsn(), 3);
    }

    // ── open_empty_backend_fails ─────────────────────────────────────────────

    #[test]
    fn open_empty_backend_fails() {
        let mut storage = [0u8; 4096];
        let backend = MemoryIoBackend::new(&mut storage);
        let result = Wal::open(backend, flat_logical());
        assert_eq!(result.unwrap_err(), Error::WalError);
    }

    // ── open_restores_position ───────────────────────────────────────────────

    #[test]
    fn open_restores_position() {
        let mut storage = [0u8; 4096];

        // Phase 1: write some records.
        let written_lsn;
        let total_bytes;
        {
            let backend = MemoryIoBackend::new(&mut storage);
            let mut wal = Wal::new(backend, flat_logical()).unwrap();
            wal.begin_tx(1).unwrap();
            wal.put(1, b"k1", b"v1").unwrap();
            wal.commit_tx(1).unwrap();
            written_lsn = wal.current_lsn();
            total_bytes = wal.write_end();
        }

        // Phase 2: reopen and verify position is restored.
        {
            let backend =
                MemoryIoBackend::with_len(&mut storage, total_bytes).unwrap();
            let wal = Wal::open(backend, flat_logical()).unwrap();
            assert_eq!(wal.current_lsn(), written_lsn);
        }
    }

    // ── all_record_types ─────────────────────────────────────────────────────

    #[test]
    fn all_record_types() {
        let mut storage = [0u8; 4096];
        let backend = MemoryIoBackend::new(&mut storage);
        let mut wal = Wal::new(backend, flat_logical()).unwrap();

        let lsn0 = wal.begin_tx(1).unwrap();
        let lsn1 = wal.put(1, b"key", b"value").unwrap();
        let lsn2 = wal.delete(1, b"key").unwrap();
        let lsn3 = wal.rollback_tx(1).unwrap();

        assert_eq!(lsn0, 0);
        assert_eq!(lsn1, 1);
        assert_eq!(lsn2, 2);
        assert_eq!(lsn3, 3);
        assert_eq!(wal.current_lsn(), 4);
    }

    // ── new_circular ─────────────────────────────────────────────────────────

    #[test]
    fn new_circular() {
        let mut storage = [0u8; 4096];
        let backend = MemoryIoBackend::new(&mut storage);
        let wal = Wal::new(backend, flat_circular(1024)).unwrap();

        assert!(wal.remaining().is_some());
        // Capacity = 1024, write_offset = 32 (circular header), tail = 32
        // Free = 1024 - (32 - 32) = 1024
        assert_eq!(wal.remaining(), Some(1024));
    }

    // ── new_paged ────────────────────────────────────────────────────────────

    #[test]
    fn new_paged() {
        let mut storage = [0u8; 4096];
        let backend = MemoryIoBackend::new(&mut storage);
        let mut wal = Wal::new(backend, paged_logical(256)).unwrap();

        let lsn0 = wal.begin_tx(1).unwrap();
        let lsn1 = wal.put(1, b"key", b"val").unwrap();

        assert_eq!(lsn0, 0);
        assert_eq!(lsn1, 1);
    }

    // ── checkpoint_updates_lsn ───────────────────────────────────────────────

    #[test]
    fn checkpoint_updates_lsn() {
        let mut storage = [0u8; 4096];
        let backend = MemoryIoBackend::new(&mut storage);
        let mut wal = Wal::new(backend, flat_logical()).unwrap();

        wal.begin_tx(1).unwrap();
        wal.put(1, b"k", b"v").unwrap();
        wal.commit_tx(1).unwrap();

        assert_eq!(wal.checkpoint_lsn(), 0);
        wal.checkpoint(2).unwrap();
        assert_eq!(wal.checkpoint_lsn(), 2);
    }

    // ── invalid_config_rejected ──────────────────────────────────────────────

    #[test]
    fn invalid_config_rejected() {
        let mut storage = [0u8; 4096];
        let backend = MemoryIoBackend::new(&mut storage);
        let config = WalConfig {
            layout: WalLayout::PageSegmented { page_size: 256 },
            sync_policy: SyncPolicy::None,
            truncation: TruncationMode::Circular { capacity: 1024 },
        };
        let result = Wal::new(backend, config);
        assert_eq!(result.unwrap_err(), Error::WalError);
    }

    // ── circular_free_space tests ────────────────────────────────────────────

    #[test]
    fn circular_free_space_empty() {
        assert_eq!(circular_free_space(32, 32, 1024), 1024);
    }

    #[test]
    fn circular_free_space_partial() {
        // head = 100, tail = 32 => used = 68, free = 1024 - 68 = 956
        assert_eq!(circular_free_space(100, 32, 1024), 956);
    }

    #[test]
    fn circular_free_space_wrapped() {
        // head = 10, tail = 100 => free = 100 - 10 = 90
        assert_eq!(circular_free_space(10, 100, 1024), 90);
    }

    // ── write_end tracks position ────────────────────────────────────────────

    #[test]
    fn write_end_advances() {
        let mut storage = [0u8; 4096];
        let backend = MemoryIoBackend::new(&mut storage);
        let mut wal = Wal::new(backend, flat_logical()).unwrap();

        assert_eq!(wal.write_end(), 0);
        wal.begin_tx(1).unwrap();
        assert!(wal.write_end() > 0);
    }

    // ── sync_policy_every_record ─────────────────────────────────────────────

    #[test]
    fn sync_policy_every_record_works() {
        let mut storage = [0u8; 4096];
        let backend = MemoryIoBackend::new(&mut storage);
        let config = WalConfig {
            layout: WalLayout::Flat,
            sync_policy: SyncPolicy::EveryRecord,
            truncation: TruncationMode::Logical,
        };
        let mut wal = Wal::new(backend, config).unwrap();
        // Should not panic even with EveryRecord sync.
        wal.begin_tx(1).unwrap();
        wal.put(1, b"k", b"v").unwrap();
        wal.commit_tx(1).unwrap();
    }

    // ── layout returns reference ─────────────────────────────────────────────

    #[test]
    fn layout_returns_correct_ref() {
        let mut storage = [0u8; 4096];
        let backend = MemoryIoBackend::new(&mut storage);
        let wal = Wal::new(backend, flat_logical()).unwrap();
        assert_eq!(*wal.layout(), WalLayout::Flat);
    }

    // ── recovery_start for non-circular ──────────────────────────────────────

    #[test]
    fn recovery_start_flat_logical() {
        let mut storage = [0u8; 4096];
        let backend = MemoryIoBackend::new(&mut storage);
        let wal = Wal::new(backend, flat_logical()).unwrap();
        assert_eq!(wal.recovery_start(), 0);
    }
}
