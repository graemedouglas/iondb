//! Flat (sequential) WAL layout: write, read, magic-byte scan, circular header.
//!
//! Records are stored back-to-back at sequential offsets. The circular header
//! lives at offset 0 and tracks the head/tail of a circular region as well as
//! the latest checkpoint LSN.

use iondb_core::{
    crc, endian,
    error::{Error, Result},
    traits::io_backend::IoBackend,
    types::Lsn,
};

use crate::record::{self, MAGIC, RECORD_HEADER_SIZE};

// ── CircularHeader constants ──────────────────────────────────────────────────

/// Size of the serialized [`CircularHeader`] in bytes.
pub const CIRCULAR_HEADER_SIZE: usize = 32;

/// Magic bytes identifying a valid [`CircularHeader`] (`"WLCR"`).
pub const CIRCULAR_MAGIC: [u8; 4] = [0x57, 0x4C, 0x43, 0x52];

// ── CircularHeader ────────────────────────────────────────────────────────────

/// Fixed-size header for the circular WAL region.
///
/// The header occupies the first [`CIRCULAR_HEADER_SIZE`] bytes of the
/// WAL file.  The layout on disk is:
///
/// ```text
/// Offset  Size  Field
/// ------  ----  -----
///  0       4    magic (0x57 0x4C 0x43 0x52, "WLCR")
///  4       4    crc32 (IEEE, over bytes 8..32)
///  8       8    head_offset  (little-endian u64)
/// 16       8    tail_offset  (little-endian u64)
/// 24       8    checkpoint_lsn (little-endian u64)
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CircularHeader {
    /// Offset of the next write position (the "head" of the circular log).
    pub head_offset: u64,
    /// Offset of the oldest live record (the "tail" of the circular log).
    pub tail_offset: u64,
    /// LSN of the most recent checkpoint.
    pub checkpoint_lsn: Lsn,
}

impl CircularHeader {
    /// Encode this header into `buf`.
    ///
    /// Writes magic at 0..4, a CRC-32 (over bytes 8..32) at 4..8, and the
    /// three fields at 8..32.
    ///
    /// # Errors
    ///
    /// Returns [`Error::WalError`] if `buf.len() < CIRCULAR_HEADER_SIZE`.
    pub fn encode(&self, buf: &mut [u8]) -> Result<()> {
        if buf.len() < CIRCULAR_HEADER_SIZE {
            return Err(Error::WalError);
        }

        // Magic — offsets 0..4
        buf[0] = CIRCULAR_MAGIC[0];
        buf[1] = CIRCULAR_MAGIC[1];
        buf[2] = CIRCULAR_MAGIC[2];
        buf[3] = CIRCULAR_MAGIC[3];

        // CRC placeholder — offsets 4..8 (filled after writing the fields)
        buf[4] = 0;
        buf[5] = 0;
        buf[6] = 0;
        buf[7] = 0;

        // head_offset — offsets 8..16
        endian::write_u64_le(&mut buf[8..16], self.head_offset)?;

        // tail_offset — offsets 16..24
        endian::write_u64_le(&mut buf[16..24], self.tail_offset)?;

        // checkpoint_lsn — offsets 24..32
        endian::write_u64_le(&mut buf[24..32], self.checkpoint_lsn)?;

        // CRC over bytes 8..32 written at 4..8
        let checksum = crc::crc32(&buf[8..CIRCULAR_HEADER_SIZE]);
        endian::write_u32_le(&mut buf[4..8], checksum)?;

        Ok(())
    }

    /// Decode a [`CircularHeader`] from `buf`.
    ///
    /// Verifies the magic bytes and the CRC-32 before returning.
    ///
    /// # Errors
    ///
    /// - Returns [`Error::WalError`] if `buf.len() < CIRCULAR_HEADER_SIZE` or
    ///   the magic bytes do not match.
    /// - Returns [`Error::Corruption`] if the CRC-32 does not match.
    pub fn decode(buf: &[u8]) -> Result<Self> {
        if buf.len() < CIRCULAR_HEADER_SIZE {
            return Err(Error::WalError);
        }

        // Verify magic
        if buf[0] != CIRCULAR_MAGIC[0]
            || buf[1] != CIRCULAR_MAGIC[1]
            || buf[2] != CIRCULAR_MAGIC[2]
            || buf[3] != CIRCULAR_MAGIC[3]
        {
            return Err(Error::WalError);
        }

        // Verify CRC (stored at 4..8, covers 8..32)
        let stored_crc = endian::read_u32_le(&buf[4..8])?;
        let computed_crc = crc::crc32(&buf[8..CIRCULAR_HEADER_SIZE]);
        if computed_crc != stored_crc {
            return Err(Error::Corruption);
        }

        let head_offset = endian::read_u64_le(&buf[8..16])?;
        let tail_offset = endian::read_u64_le(&buf[16..24])?;
        let checkpoint_lsn = endian::read_u64_le(&buf[24..32])?;

        Ok(Self {
            head_offset,
            tail_offset,
            checkpoint_lsn,
        })
    }
}

// ── Record I/O ────────────────────────────────────────────────────────────────

/// Write `data` to `backend` at `offset`.
///
/// Returns the next offset immediately following the written bytes.
///
/// # Errors
///
/// Returns [`Error::Io`] if the number of bytes written does not equal
/// `data.len()`.
pub fn write_record(backend: &mut impl IoBackend, offset: u64, data: &[u8]) -> Result<u64> {
    let written = backend.write(offset, data)?;
    if written != data.len() {
        return Err(Error::Io);
    }
    Ok(offset + data.len() as u64)
}

/// Read one WAL record from `backend` starting at `offset`.
///
/// Returns `Ok(None)` when:
/// - `offset >= end` (logical end of log), or
/// - the first bytes do not carry the WAL magic (end of written data), or
/// - the record is truncated (partially written).
///
/// Returns `Ok(Some((record, next_offset)))` on success, where `next_offset`
/// is the byte position immediately following this record.
///
/// # Errors
///
/// Returns [`Error::Corruption`] when the magic is present but the CRC-32
/// fails — the caller should use [`scan_for_magic`] to skip to the next
/// candidate.
pub fn read_record<'buf>(
    backend: &impl IoBackend,
    offset: u64,
    end: u64,
    buf: &'buf mut [u8],
) -> Result<Option<(record::WalRecord<'buf>, u64)>> {
    if offset >= end {
        return Ok(None);
    }

    // Read the fixed-size record header
    if buf.len() < RECORD_HEADER_SIZE {
        return Err(Error::WalError);
    }
    let n = backend.read(offset, &mut buf[..RECORD_HEADER_SIZE])?;
    if n < RECORD_HEADER_SIZE {
        // Truncated — treat as end of log
        return Ok(None);
    }

    // Check magic bytes; absence means we've reached unwritten space
    if buf[0] != MAGIC[0] || buf[1] != MAGIC[1] {
        return Ok(None);
    }

    // Parse lengths from the header to know how much more to read
    let key_len = usize::from(endian::read_u16_le(&buf[23..25])?);
    let val_len = endian::read_u32_le(&buf[25..29])? as usize;
    let total = record::record_size(key_len, val_len);

    if buf.len() < total {
        // Caller-provided buffer is too small — truncated view
        return Ok(None);
    }

    // Read the full record (header already in buf; re-read from offset for
    // convenience since we need one contiguous slice for deserialize_from)
    let n = backend.read(offset, &mut buf[..total])?;
    if n < total {
        // Truncated record — treat as end of log
        return Ok(None);
    }

    // Deserialize with CRC validation; Corruption propagates to caller
    let rec = record::deserialize_from(&buf[..total])?;
    let next_offset = offset + total as u64;

    Ok(Some((rec, next_offset)))
}

/// Scan `backend` byte-by-byte from `start` to `end` for the WAL magic
/// bytes `0x57 0x4C`.
///
/// Returns the offset of the first match, or `None` if the magic is not found.
///
/// This is used after a [`Error::Corruption`] to locate the next potential
/// record boundary.
///
/// # Errors
///
/// Returns [`Error::Io`] if the backend read fails.
pub fn scan_for_magic(backend: &impl IoBackend, start: u64, end: u64) -> Result<Option<u64>> {
    let mut offset = start;
    let mut prev_byte: Option<u8> = None;

    while offset < end {
        let mut byte = [0u8; 1];
        let n = backend.read(offset, &mut byte)?;
        if n == 0 {
            break;
        }

        // Check if previous byte + current byte form the magic sequence
        if let Some(pb) = prev_byte {
            if pb == MAGIC[0] && byte[0] == MAGIC[1] {
                return Ok(Some(offset - 1));
            }
        }

        prev_byte = Some(byte[0]);
        offset += 1;
    }

    Ok(None)
}

// ── Circular header I/O ───────────────────────────────────────────────────────

/// Encode and write `header` to `backend` at offset 0.
///
/// # Errors
///
/// Propagates any error from [`CircularHeader::encode`] or the backend write.
pub fn write_circular_header(backend: &mut impl IoBackend, header: &CircularHeader) -> Result<()> {
    let mut buf = [0u8; CIRCULAR_HEADER_SIZE];
    header.encode(&mut buf)?;
    let written = backend.write(0, &buf)?;
    if written != CIRCULAR_HEADER_SIZE {
        return Err(Error::Io);
    }
    Ok(())
}

/// Read and decode the [`CircularHeader`] from offset 0 of `backend`.
///
/// # Errors
///
/// Propagates any error from the backend read or [`CircularHeader::decode`].
pub fn read_circular_header(backend: &impl IoBackend) -> Result<CircularHeader> {
    let mut buf = [0u8; CIRCULAR_HEADER_SIZE];
    let n = backend.read(0, &mut buf)?;
    if n < CIRCULAR_HEADER_SIZE {
        return Err(Error::WalError);
    }
    CircularHeader::decode(&buf)
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
// Tests use unwrap/expect for brevity; panics are acceptable in test code.
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::record::{serialize_into, RecordType};
    use iondb_io::memory::MemoryIoBackend;

    /// Helper: serialize a Put record into a fresh vec-like fixed buffer and
    /// write it via `write_record`. Returns the record byte length.
    fn write_put_record(
        backend: &mut MemoryIoBackend<'_>,
        offset: u64,
        lsn: u64,
        txn_id: u64,
        key: &[u8],
        value: &[u8],
    ) -> u64 {
        let total = record::record_size(key.len(), value.len());
        let mut tmp = [0u8; 512];
        let written = serialize_into(&mut tmp, lsn, txn_id, RecordType::Put, key, value).unwrap();
        assert_eq!(written, total);
        write_record(backend, offset, &tmp[..written]).unwrap()
    }

    /// Write a Put record, read it back, and verify all fields.
    #[test]
    fn write_then_read_record() {
        let mut storage = [0u8; 512];
        let mut backend = MemoryIoBackend::new(&mut storage);

        let key = b"hello";
        let value = b"world";
        let next = write_put_record(&mut backend, 0, 42, 7, key, value);

        let mut buf = [0u8; 512];
        let result = read_record(&backend, 0, next, &mut buf).unwrap();
        let (rec, after) = result.unwrap();

        assert_eq!(rec.lsn, 42);
        assert_eq!(rec.txn_id, 7);
        assert_eq!(rec.record_type, RecordType::Put);
        assert_eq!(rec.key, key);
        assert_eq!(rec.value, value);
        assert_eq!(after, next);
    }

    /// Reading from an empty backend (offset == end == 0) returns None.
    #[test]
    fn read_at_end_returns_none() {
        let mut storage = [0u8; 512];
        let backend = MemoryIoBackend::new(&mut storage);

        let mut buf = [0u8; 512];
        let result = read_record(&backend, 0, 0, &mut buf).unwrap();
        assert!(result.is_none());
    }

    /// Write two records, corrupt the CRC of the first, then scan_for_magic
    /// from byte 1 — it should return the offset of the second record.
    #[test]
    fn magic_scan_finds_second_record() {
        let mut storage = [0u8; 1024];
        let mut backend = MemoryIoBackend::new(&mut storage);

        let key1 = b"key1";
        let val1 = b"val1";
        let key2 = b"key2";
        let val2 = b"val2";

        let end1 = write_put_record(&mut backend, 0, 1, 1, key1, val1);
        let end2 = write_put_record(&mut backend, end1, 2, 2, key2, val2);

        // Corrupt the CRC of the first record (bytes 2..6)
        let _n = backend.write(2, &[0xFF, 0xFF, 0xFF, 0xFF]).unwrap();

        // scan_for_magic starting at byte 1 should find the start of record 2
        let found = scan_for_magic(&backend, 1, end2).unwrap();
        assert_eq!(found, Some(end1));
    }

    /// A CircularHeader round-trips through encode/decode.
    #[test]
    fn circular_header_round_trip() {
        let hdr = CircularHeader {
            head_offset: 0x0102_0304_0506_0708,
            tail_offset: 0x0807_0605_0403_0201,
            checkpoint_lsn: 0xDEAD_BEEF_CAFE_BABE,
        };

        let mut buf = [0u8; CIRCULAR_HEADER_SIZE];
        hdr.encode(&mut buf).unwrap();
        let decoded = CircularHeader::decode(&buf).unwrap();
        assert_eq!(decoded, hdr);
    }

    /// Corrupting any byte of a written circular header causes decode to return
    /// Error::Corruption (for bytes in the CRC-protected region) or
    /// Error::WalError (for the magic region).
    #[test]
    fn circular_header_corruption_detected() {
        let mut storage = [0u8; 256];
        let mut backend = MemoryIoBackend::new(&mut storage);

        let hdr = CircularHeader {
            head_offset: 100,
            tail_offset: 0,
            checkpoint_lsn: 5,
        };
        write_circular_header(&mut backend, &hdr).unwrap();

        // Corrupt a byte inside the CRC-protected region (offset 10 is in
        // head_offset at bytes 8..16)
        let _n = backend.write(10, &[0xFF]).unwrap();

        let err = read_circular_header(&backend).unwrap_err();
        assert_eq!(err, Error::Corruption);
    }

    /// Write five sequential Put records and read them all back in order;
    /// the next read after the last record returns None.
    #[test]
    fn multiple_records_sequential() {
        let mut storage = [0u8; 4096];
        let mut backend = MemoryIoBackend::new(&mut storage);

        let records: &[(&[u8], &[u8])] = &[
            (b"k1", b"v1"),
            (b"k2", b"v2"),
            (b"k3", b"v3"),
            (b"k4", b"v4"),
            (b"k5", b"v5"),
        ];

        let mut offset = 0u64;
        for (i, (k, v)) in records.iter().enumerate() {
            offset = write_put_record(&mut backend, offset, i as u64, 0, k, v);
        }
        let end = offset;

        // Read all five records back
        let mut offset = 0u64;
        for (i, (k, v)) in records.iter().enumerate() {
            let mut buf = [0u8; 512];
            let result = read_record(&backend, offset, end, &mut buf).unwrap();
            let (rec, next) = result.unwrap();
            assert_eq!(rec.lsn, i as u64);
            assert_eq!(rec.key, *k);
            assert_eq!(rec.value, *v);
            offset = next;
        }

        // One more read should return None
        let mut buf = [0u8; 512];
        assert!(read_record(&backend, offset, end, &mut buf)
            .unwrap()
            .is_none());
    }
}
