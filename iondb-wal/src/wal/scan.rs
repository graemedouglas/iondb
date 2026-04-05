//! WAL open-time scan: restore write position, next LSN, and checkpoint LSN.
//!
//! These free functions implement the scanning half of [`Wal::open`], separated
//! here to keep the main module under the 500-line limit.

use iondb_core::{
    error::Result, page::PAGE_HEADER_SIZE, traits::io_backend::IoBackend, types::Lsn,
};

use crate::config::{TruncationMode, WalConfig};
use crate::flat;
use crate::paged::{self, PagedWriter};
use crate::record::RecordType;

use super::MAX_RECORD_BUF;

/// Scan a flat-layout WAL backend to recover write position and LSN state.
///
/// Returns `(write_offset, next_lsn, checkpoint_lsn)`.
pub(super) fn scan_flat<I: IoBackend>(backend: &I, config: &WalConfig) -> Result<(u64, Lsn, Lsn)> {
    let size = backend.size()?;

    let mut checkpoint_lsn: Lsn = 0;

    // For circular layout, read the circular header first.
    let start_offset = match config.truncation {
        TruncationMode::Circular { .. } => {
            let header = flat::read_circular_header(backend)?;
            checkpoint_lsn = header.checkpoint_lsn;
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
        match flat::read_record(backend, offset, size, &mut buf)? {
            None => break,
            Some((rec, next_offset)) => {
                if rec.record_type == RecordType::Checkpoint {
                    checkpoint_lsn = rec.lsn;
                }
                max_lsn = Some(match max_lsn {
                    Some(prev) if prev > rec.lsn => prev,
                    _ => rec.lsn,
                });
                offset = next_offset;
            }
        }
    }

    let next_lsn = max_lsn.map_or(0, |lsn| lsn + 1);

    Ok((offset, next_lsn, checkpoint_lsn))
}

/// Scan a paged-layout WAL backend to recover write position and LSN state.
///
/// Returns `(PagedWriter, next_lsn, checkpoint_lsn)`.
pub(super) fn scan_paged<I: IoBackend>(
    backend: &I,
    page_size: usize,
) -> Result<(PagedWriter, Lsn, Lsn)> {
    let size = backend.size()?;

    let mut page_offset: u64 = 0;
    let mut pos_in_page = PAGE_HEADER_SIZE;
    let mut max_lsn: Option<Lsn> = None;
    let mut last_page_offset: u64 = 0;
    let mut last_pos_in_page = PAGE_HEADER_SIZE;
    let mut page_count: u32 = 0;
    let mut checkpoint_lsn: Lsn = 0;
    let mut buf = [0u8; MAX_RECORD_BUF];

    loop {
        match paged::read_record_paged(
            backend,
            page_offset,
            pos_in_page,
            page_size,
            size,
            &mut buf,
        )? {
            None => break,
            Some((rec, new_page_offset, new_pos_in_page)) => {
                if rec.record_type == RecordType::Checkpoint {
                    checkpoint_lsn = rec.lsn;
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

    if max_lsn.is_some() {
        // We found at least one record: add 1 for the initial page.
        page_count += 1;
    }

    let next_lsn = max_lsn.map_or(0, |lsn| lsn + 1);

    let writer = PagedWriter::resume(page_size, last_page_offset, last_pos_in_page, page_count);

    Ok((writer, next_lsn, checkpoint_lsn))
}
