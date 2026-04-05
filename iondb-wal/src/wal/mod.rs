//! Core WAL implementation: construction, append, sync, checkpoint.
//!
//! The [`Wal`] struct is the main entry point for the write-ahead log. It
//! supports three storage layouts (flat, page-segmented, circular) and four
//! sync policies. Records are appended sequentially and assigned monotonically
//! increasing LSNs.
//!
//! Implementation is split across submodules to stay within the 500-line limit:
//! - `scan`: open-time backend scanning
//! - `compact`: physical compaction (std feature only)
//! - `queries`: recovery readers and read-only accessors

mod compact;
mod queries;
mod scan;

#[cfg(test)]
mod tests;

use iondb_core::{
    error::{Error, Result},
    traits::io_backend::IoBackend,
    types::{Lsn, TxnId},
};

use crate::config::{SyncPolicy, TruncationMode, WalConfig, WalLayout};
use crate::flat::{self, CircularHeader, CIRCULAR_HEADER_SIZE};
use crate::paged::PagedWriter;
use crate::record::{self, RecordType};

// ── Constants ────────────────────────────────────────────────────────────────

/// Maximum size of a serialized record buffer on the stack.
///
/// Records larger than this are rejected with [`Error::WalError`].
pub(super) const MAX_RECORD_BUF: usize = 512;

// ── LayoutState ──────────────────────────────────────────────────────────────

/// Internal write-position state, determined by the [`WalLayout`].
#[derive(Debug)]
pub(super) enum LayoutState {
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
    pub(super) backend: I,
    /// Configuration for this WAL instance.
    pub(super) config: WalConfig,
    /// Internal layout state tracking the write position.
    pub(super) layout: LayoutState,
    /// The next LSN to assign.
    pub(super) next_lsn: Lsn,
    /// The LSN of the most recent checkpoint.
    pub(super) checkpoint_lsn: Lsn,
    /// Number of records appended since the last sync.
    pub(super) records_since_sync: u32,
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

        match config.layout {
            WalLayout::Flat => {
                let (write_offset, next_lsn, checkpoint_lsn) = scan::scan_flat(&backend, &config)?;
                Ok(Self {
                    backend,
                    config,
                    layout: LayoutState::Flat { write_offset },
                    next_lsn,
                    checkpoint_lsn,
                    records_since_sync: 0,
                })
            }
            WalLayout::PageSegmented { page_size } => {
                let (writer, next_lsn, checkpoint_lsn) = scan::scan_paged(&backend, page_size)?;
                Ok(Self {
                    backend,
                    config,
                    layout: LayoutState::Paged { writer },
                    next_lsn,
                    checkpoint_lsn,
                    records_since_sync: 0,
                })
            }
        }
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
        let _checkpoint_record_lsn = self.append_record(0, RecordType::Checkpoint, &[], &[])?;
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
                let new_write_offset =
                    compact::physical_compact(&mut self.backend, self.checkpoint_lsn)?;
                if let LayoutState::Flat { write_offset } = &mut self.layout {
                    *write_offset = new_write_offset;
                }
            }
            TruncationMode::Logical => {
                // No-op: checkpoint_lsn is tracked in memory and discovered
                // by scan on open.
            }
        }

        Ok(())
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
// head and tail are bounded by capacity (a usize), so the casts are safe.
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
