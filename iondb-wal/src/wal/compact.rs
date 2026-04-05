//! Physical WAL compaction: copy live records to the start of the backend.
//!
//! Only compiled with the `std` feature. Separated here to keep the main
//! module under the 500-line limit.

#[cfg(feature = "std")]
use iondb_core::{
    error::{Error, Result},
    traits::io_backend::IoBackend,
    types::Lsn,
};

#[cfg(feature = "std")]
use crate::flat::{self, CircularHeader};

#[cfg(feature = "std")]
use super::MAX_RECORD_BUF;

/// Compact the WAL backend in place by copying live records to offset 0.
///
/// Returns the new write offset (the byte immediately after the last copied
/// record), or `0` if all records were checkpointed.
///
/// # Errors
///
/// Returns [`Error::Io`] if any backend read or write fails.
#[cfg(feature = "std")]
pub(super) fn physical_compact<I: IoBackend>(backend: &mut I, checkpoint_lsn: Lsn) -> Result<u64> {
    let size = backend.size()?;
    let mut offset = 0u64;
    let mut first_live_offset: Option<u64> = None;
    let mut buf = [0u8; MAX_RECORD_BUF];

    // Scan from start to find first record with LSN > checkpoint_lsn.
    loop {
        match flat::read_record(backend, offset, size, &mut buf)? {
            None => break,
            Some((rec, next_offset)) => {
                if rec.lsn > checkpoint_lsn && first_live_offset.is_none() {
                    first_live_offset = Some(offset);
                }
                offset = next_offset;
            }
        }
    }

    let end_offset = offset;

    let Some(live_start) = first_live_offset else {
        // All records are checkpointed; backend can be reset to empty.
        backend.sync()?;
        return Ok(0);
    };

    // Write checkpoint pointer via circular header format at offset 0 as
    // crash safety commit point, then sync.
    let header = CircularHeader {
        head_offset: live_start,
        tail_offset: 0,
        checkpoint_lsn,
    };
    flat::write_circular_header(backend, &header)?;
    backend.sync()?;

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
        let n = backend.read(src, &mut chunk[..to_copy])?;
        if n == 0 {
            break;
        }
        let written = backend.write(dst, &chunk[..n])?;
        if written != n {
            return Err(Error::Io);
        }
        src += n as u64;
        dst += n as u64;
        remaining -= n as u64;
    }

    backend.sync()?;
    Ok(dst)
}
