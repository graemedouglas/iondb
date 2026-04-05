//! WAL record format: serialization, deserialization, CRC integrity.
//!
//! Every WAL record has a 29-byte fixed header followed by a variable-length
//! key and value payload:
//!
//! ```text
//! Offset  Size  Field
//! ------  ----  -----
//!  0      2     magic (0x57 0x4C, "WL")
//!  2      4     crc32 (IEEE, over bytes 6..end)
//!  6      8     lsn (little-endian u64)
//! 14      8     txn_id (little-endian u64)
//! 22      1     record_type (u8)
//! 23      2     key_len (little-endian u16)
//! 25      4     val_len (little-endian u32)
//! 29      var   key (key_len bytes)
//! 29+K    var   value (val_len bytes)
//! ```

use iondb_core::{
    crc, endian,
    error::{Error, Result},
    types::{Lsn, TxnId},
};

// ── Constants ────────────────────────────────────────────────────────────────

/// Magic bytes that identify the start of a WAL record (`"WL"`).
pub const MAGIC: [u8; 2] = [0x57, 0x4C];

/// Size of the fixed WAL record header in bytes.
pub const RECORD_HEADER_SIZE: usize = 29;

// ── RecordType ───────────────────────────────────────────────────────────────

/// The type of a WAL record, encoded as a single byte.
///
/// Each variant maps to a distinct discriminant so records can be
/// distinguished without parsing the full payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum RecordType {
    /// Marks the beginning of a transaction.
    Begin = 0,
    /// A key/value write within a transaction.
    Put = 1,
    /// A key deletion within a transaction.
    Delete = 2,
    /// Marks a successful transaction commit.
    Commit = 3,
    /// Marks a transaction rollback (abort).
    Rollback = 4,
    /// A WAL checkpoint record.
    Checkpoint = 5,
}

impl RecordType {
    /// Decode a `RecordType` from its byte representation.
    ///
    /// # Errors
    ///
    /// Returns [`Error::WalError`] if `byte` does not correspond to any
    /// known variant.
    pub fn from_byte(byte: u8) -> Result<Self> {
        match byte {
            0 => Ok(Self::Begin),
            1 => Ok(Self::Put),
            2 => Ok(Self::Delete),
            3 => Ok(Self::Commit),
            4 => Ok(Self::Rollback),
            5 => Ok(Self::Checkpoint),
            _ => Err(Error::WalError),
        }
    }

    /// Encode this `RecordType` as a single byte.
    #[must_use]
    pub fn as_byte(self) -> u8 {
        self as u8
    }
}

// ── WalRecord ────────────────────────────────────────────────────────────────

/// A borrowed view of a single WAL record.
///
/// The key and value slices borrow directly from the buffer passed to
/// [`deserialize_from`], so no heap allocation is required.
#[derive(Debug, PartialEq, Eq)]
pub struct WalRecord<'a> {
    /// Log sequence number uniquely identifying this record in the log.
    pub lsn: Lsn,
    /// Transaction identifier this record belongs to.
    pub txn_id: TxnId,
    /// The type of this record.
    pub record_type: RecordType,
    /// The key payload (may be empty for `Begin`, `Commit`, `Rollback`, `Checkpoint`).
    pub key: &'a [u8],
    /// The value payload (may be empty).
    pub value: &'a [u8],
}

// ── Public helpers ────────────────────────────────────────────────────────────

/// Return the total serialized size of a record with the given key and value
/// lengths.
///
/// This is `RECORD_HEADER_SIZE + key_len + val_len`.
#[must_use]
pub fn record_size(key_len: usize, val_len: usize) -> usize {
    RECORD_HEADER_SIZE + key_len + val_len
}

/// Serialize a WAL record into `buf`.
///
/// The CRC-32 field at offset 2 covers all bytes from offset 6 through the
/// end of the value (i.e. `buf[6..total]`).
///
/// # Errors
///
/// Returns [`Error::WalError`] if `buf` is smaller than
/// `record_size(key.len(), value.len())`.
pub fn serialize_into(
    buf: &mut [u8],
    lsn: Lsn,
    txn_id: TxnId,
    record_type: RecordType,
    key: &[u8],
    value: &[u8],
) -> Result<usize> {
    let total = record_size(key.len(), value.len());
    if buf.len() < total {
        return Err(Error::WalError);
    }

    // Magic bytes — offsets 0..2
    buf[0] = MAGIC[0];
    buf[1] = MAGIC[1];

    // CRC placeholder — offsets 2..6 (filled in after computing over payload)
    buf[2] = 0;
    buf[3] = 0;
    buf[4] = 0;
    buf[5] = 0;

    // LSN — offsets 6..14
    endian::write_u64_le(&mut buf[6..14], lsn)?;

    // TxnId — offsets 14..22
    endian::write_u64_le(&mut buf[14..22], txn_id)?;

    // RecordType — offset 22
    endian::write_u8(&mut buf[22..23], record_type.as_byte())?;

    // key_len — offsets 23..25
    let key_len_u16 = u16::try_from(key.len()).map_err(|_| Error::WalError)?;
    endian::write_u16_le(&mut buf[23..25], key_len_u16)?;

    // val_len — offsets 25..29
    let val_len_u32 = u32::try_from(value.len()).map_err(|_| Error::WalError)?;
    endian::write_u32_le(&mut buf[25..29], val_len_u32)?;

    // Key payload — offsets 29..29+key_len
    let key_end = RECORD_HEADER_SIZE + key.len();
    buf[RECORD_HEADER_SIZE..key_end].copy_from_slice(key);

    // Value payload — offsets key_end..total
    buf[key_end..total].copy_from_slice(value);

    // Compute CRC over bytes 6..total and write at offsets 2..6
    let checksum = crc::crc32(&buf[6..total]);
    endian::write_u32_le(&mut buf[2..6], checksum)?;

    Ok(total)
}

/// Deserialize a WAL record from `buf`, verifying magic and CRC.
///
/// The returned [`WalRecord`] borrows its key and value slices from `buf`.
///
/// # Errors
///
/// - Returns [`Error::WalError`] if `buf` is shorter than
///   [`RECORD_HEADER_SIZE`] or the magic bytes do not match.
/// - Returns [`Error::Corruption`] if the CRC-32 checksum does not match the
///   payload.
/// - Returns [`Error::WalError`] if `buf` is shorter than the full record
///   indicated by the header lengths.
pub fn deserialize_from(buf: &[u8]) -> Result<WalRecord<'_>> {
    // Need at least the fixed header
    if buf.len() < RECORD_HEADER_SIZE {
        return Err(Error::WalError);
    }

    // Verify magic bytes
    if buf[0] != MAGIC[0] || buf[1] != MAGIC[1] {
        return Err(Error::WalError);
    }

    // Read CRC from header
    let stored_crc = endian::read_u32_le(&buf[2..6])?;

    // Read lengths from header to determine full record size
    let key_len = usize::from(endian::read_u16_le(&buf[23..25])?);
    let val_len = endian::read_u32_le(&buf[25..29])? as usize;
    let total = record_size(key_len, val_len);

    // Ensure buffer contains the full record
    if buf.len() < total {
        return Err(Error::WalError);
    }

    // Verify CRC over bytes 6..total
    let computed_crc = crc::crc32(&buf[6..total]);
    if computed_crc != stored_crc {
        return Err(Error::Corruption);
    }

    // Parse header fields
    let lsn = endian::read_u64_le(&buf[6..14])?;
    let txn_id = endian::read_u64_le(&buf[14..22])?;
    let record_type = RecordType::from_byte(endian::read_u8(&buf[22..23])?)?;

    // Borrow key and value slices directly from buf
    let key_start = RECORD_HEADER_SIZE;
    let key_end = key_start + key_len;
    let val_end = key_end + val_len;

    Ok(WalRecord {
        lsn,
        txn_id,
        record_type,
        key: &buf[key_start..key_end],
        value: &buf[key_end..val_end],
    })
}

/// Read only the 29-byte fixed header from `buf` without CRC validation.
///
/// Returns `(lsn, txn_id, record_type, key_len, val_len)`.
///
/// This is used by the first pass of [`CommittedRecoveryReader`] to scan
/// the log and locate record boundaries without validating integrity.
///
/// [`CommittedRecoveryReader`]: crate::recovery::CommittedRecoveryReader
///
/// # Errors
///
/// Returns [`Error::WalError`] if `buf.len() < RECORD_HEADER_SIZE`, if the
/// magic bytes are wrong, or if the `record_type` byte is unrecognised.
pub fn read_header(buf: &[u8]) -> Result<(Lsn, TxnId, RecordType, u16, u32)> {
    if buf.len() < RECORD_HEADER_SIZE {
        return Err(Error::WalError);
    }

    if buf[0] != MAGIC[0] || buf[1] != MAGIC[1] {
        return Err(Error::WalError);
    }

    let lsn = endian::read_u64_le(&buf[6..14])?;
    let txn_id = endian::read_u64_le(&buf[14..22])?;
    let record_type = RecordType::from_byte(endian::read_u8(&buf[22..23])?)?;
    let key_len = endian::read_u16_le(&buf[23..25])?;
    let val_len = endian::read_u32_le(&buf[25..29])?;

    Ok((lsn, txn_id, record_type, key_len, val_len))
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
// Tests use unwrap for brevity; panics are acceptable in test code.
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    /// All six `RecordType` variants survive a `from_byte(as_byte())` round-trip.
    #[test]
    fn record_type_round_trip() {
        let types = [
            RecordType::Begin,
            RecordType::Put,
            RecordType::Delete,
            RecordType::Commit,
            RecordType::Rollback,
            RecordType::Checkpoint,
        ];
        for rt in types {
            assert_eq!(RecordType::from_byte(rt.as_byte()), Ok(rt));
        }
    }

    /// Bytes 6 and 255 are not valid `RecordType` discriminants.
    #[test]
    fn record_type_invalid() {
        assert_eq!(RecordType::from_byte(6), Err(Error::WalError));
        assert_eq!(RecordType::from_byte(255), Err(Error::WalError));
    }

    /// A `Put` record with key `"hello"` and value `"world"` round-trips.
    #[test]
    fn serialize_deserialize_round_trip() {
        let key = b"hello";
        let value = b"world";
        let total = record_size(key.len(), value.len());
        let mut buf = [0u8; 64];

        let written = serialize_into(&mut buf, 1, 42, RecordType::Put, key, value).unwrap();
        assert_eq!(written, total);

        let rec = deserialize_from(&buf[..total]).unwrap();
        assert_eq!(rec.lsn, 1);
        assert_eq!(rec.txn_id, 42);
        assert_eq!(rec.record_type, RecordType::Put);
        assert_eq!(rec.key, key);
        assert_eq!(rec.value, value);
    }

    /// Magic bytes `0x57 0x4C` are present at offsets 0 and 1 after serialization.
    #[test]
    fn magic_bytes_present() {
        let mut buf = [0u8; 64];
        let _ = serialize_into(&mut buf, 0, 0, RecordType::Begin, b"", b"").unwrap();
        assert_eq!(buf[0], 0x57);
        assert_eq!(buf[1], 0x4C);
    }

    /// Flipping a bit in the key causes `deserialize_from` to return
    /// `Error::Corruption`.
    #[test]
    fn crc_detects_corruption() {
        let key = b"hello";
        let value = b"world";
        let total = record_size(key.len(), value.len());
        let mut buf = [0u8; 64];
        let _ = serialize_into(&mut buf, 1, 1, RecordType::Put, key, value).unwrap();

        // Flip a bit in the key region (offset 29 is the first key byte)
        buf[29] ^= 0x01;

        assert_eq!(deserialize_from(&buf[..total]), Err(Error::Corruption));
    }

    /// A `Commit` record with empty key and value survives a round-trip.
    #[test]
    fn empty_key_and_value() {
        let total = record_size(0, 0);
        let mut buf = [0u8; RECORD_HEADER_SIZE];
        let written = serialize_into(&mut buf, 99, 7, RecordType::Commit, b"", b"").unwrap();
        assert_eq!(written, total);

        let rec = deserialize_from(&buf).unwrap();
        assert_eq!(rec.lsn, 99);
        assert_eq!(rec.txn_id, 7);
        assert_eq!(rec.record_type, RecordType::Commit);
        assert_eq!(rec.key, b"");
        assert_eq!(rec.value, b"");
    }

    /// `serialize_into` returns `Error::WalError` when the buffer is too small.
    #[test]
    fn buffer_too_small_for_serialize() {
        let mut buf = [0u8; RECORD_HEADER_SIZE - 1];
        let result = serialize_into(&mut buf, 0, 0, RecordType::Begin, b"", b"");
        assert_eq!(result, Err(Error::WalError));
    }

    /// `deserialize_from` returns `Error::WalError` when the buffer is too small
    /// for the header.
    #[test]
    fn buffer_too_small_for_deserialize() {
        let buf = [0u8; RECORD_HEADER_SIZE - 1];
        assert_eq!(deserialize_from(&buf), Err(Error::WalError));
    }

    /// A buffer with incorrect magic bytes is rejected by `deserialize_from`.
    #[test]
    fn wrong_magic_rejected() {
        let mut buf = [0u8; 64];
        let _ = serialize_into(&mut buf, 0, 0, RecordType::Begin, b"", b"").unwrap();
        // Corrupt the magic
        buf[0] = 0xFF;
        assert_eq!(deserialize_from(&buf), Err(Error::WalError));
    }

    /// `record_size` returns the expected totals for various key/value length
    /// combinations.
    #[test]
    fn record_size_calculation() {
        assert_eq!(record_size(0, 0), 29);
        assert_eq!(record_size(5, 10), 44);
        assert_eq!(record_size(256, 65536), 65821);
    }

    /// `read_header` returns the same LSN, `TxnId`, `RecordType`, and lengths as
    /// a full `deserialize_from`.
    #[test]
    fn read_header_matches_full_deserialize() {
        let key = b"key";
        let value = b"value";
        let total = record_size(key.len(), value.len());
        let mut buf = [0u8; 64];
        let _ = serialize_into(&mut buf, 5, 10, RecordType::Delete, key, value).unwrap();

        let (lsn, txn_id, rt, klen, vlen) = read_header(&buf[..total]).unwrap();
        let rec = deserialize_from(&buf[..total]).unwrap();

        assert_eq!(lsn, rec.lsn);
        assert_eq!(txn_id, rec.txn_id);
        assert_eq!(rt, rec.record_type);
        assert_eq!(usize::from(klen), rec.key.len());
        assert_eq!(vlen as usize, rec.value.len());
    }

    /// All six record types round-trip through serialize/deserialize.
    #[test]
    fn all_record_types_round_trip() {
        let cases: &[(RecordType, &[u8], &[u8])] = &[
            (RecordType::Begin, b"", b""),
            (RecordType::Put, b"k", b"v"),
            (RecordType::Delete, b"k", b""),
            (RecordType::Commit, b"", b""),
            (RecordType::Rollback, b"", b""),
            (RecordType::Checkpoint, b"ckpt", b"data"),
        ];

        for &(rt, key, value) in cases {
            let total = record_size(key.len(), value.len());
            let mut buf = [0u8; 128];
            let written = serialize_into(&mut buf, 0, 0, rt, key, value).unwrap();
            assert_eq!(written, total);
            let rec = deserialize_from(&buf[..total]).unwrap();
            assert_eq!(rec.record_type, rt);
            assert_eq!(rec.key, key);
            assert_eq!(rec.value, value);
        }
    }
}
