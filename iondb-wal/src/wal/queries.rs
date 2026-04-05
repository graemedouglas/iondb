//! Recovery accessors and read-only queries on the WAL.
//!
//! These `impl Wal<I>` methods provide the public read-only surface of the
//! WAL (recovery readers, LSN accessors, layout info). Separated here to keep
//! `wal/mod.rs` under the 500-line limit.

use iondb_core::{
    error::Result,
    traits::io_backend::IoBackend,
    types::{Lsn, TxnId},
};

use crate::config::{TruncationMode, WalLayout};
use crate::flat;

use super::{circular_free_space, LayoutState, Wal, CIRCULAR_HEADER_SIZE};

impl<I: IoBackend> Wal<I> {
    // ── Recovery ────────────────────────────────────────────────────────────

    /// Create a raw recovery reader from [`recovery_start`][Self::recovery_start]
    /// to [`write_end`][Self::write_end].
    ///
    /// The returned reader yields every valid record, including those from
    /// uncommitted transactions.
    ///
    /// # Errors
    ///
    /// Returns [`iondb_core::error::Error::Io`] if the backend cannot be read.
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
    /// Returns [`iondb_core::error::Error::WalError`] if there are more committed transactions
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
