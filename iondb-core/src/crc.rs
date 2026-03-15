//! CRC-32 checksum for data integrity verification.
//!
//! Uses the IEEE/PKZIP polynomial (`0xEDB8_8320`, reflected). The lookup table
//! is generated at compile time via `const fn`, so there is zero runtime cost
//! for table initialization and no external dependencies.
//!
//! # Usage
//!
//! ```ignore
//! let checksum = crc32(b"hello");
//! assert_eq!(checksum, 0x3610_A686);
//! ```

/// IEEE/PKZIP CRC-32 polynomial (reflected form).
const CRC32_POLYNOMIAL: u32 = 0xEDB8_8320;

/// Precomputed CRC-32 lookup table (256 entries).
const CRC32_TABLE: [u32; 256] = build_crc32_table();

/// Build the CRC-32 lookup table at compile time.
const fn build_crc32_table() -> [u32; 256] {
    let mut table = [0u32; 256];
    let mut i: u32 = 0;
    while i < 256 {
        let mut crc = i;
        let mut j = 0;
        while j < 8 {
            if crc & 1 != 0 {
                crc = (crc >> 1) ^ CRC32_POLYNOMIAL;
            } else {
                crc >>= 1;
            }
            j += 1;
        }
        table[i as usize] = crc;
        i += 1;
    }
    table
}

/// Compute the CRC-32 checksum of `data`.
#[must_use]
pub fn crc32(data: &[u8]) -> u32 {
    crc32_update(0xFFFF_FFFF, data) ^ 0xFFFF_FFFF
}

/// Update a running CRC-32 with additional data.
///
/// To compute incrementally, start with `crc = 0xFFFF_FFFF`, call
/// `crc32_update` for each chunk, then XOR the final result with
/// `0xFFFF_FFFF`.
///
/// For a single buffer, prefer [`crc32`] which handles initialization
/// and finalization.
#[must_use]
pub fn crc32_update(mut crc: u32, data: &[u8]) -> u32 {
    for &byte in data {
        let index = ((crc ^ u32::from(byte)) & 0xFF) as usize;
        crc = (crc >> 8) ^ CRC32_TABLE[index];
    }
    crc
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_data() {
        assert_eq!(crc32(&[]), 0x0000_0000);
    }

    #[test]
    fn known_vector_hello() {
        // "hello" => 0x3610A686 (standard CRC-32)
        assert_eq!(crc32(b"hello"), 0x3610_A686);
    }

    #[test]
    fn known_vector_check_string() {
        // The canonical CRC-32 check value for "123456789"
        assert_eq!(crc32(b"123456789"), 0xCBF4_3926);
    }

    #[test]
    fn single_byte() {
        // CRC-32 of a single zero byte
        assert_eq!(crc32(&[0x00]), 0xD202_EF8D);
    }

    #[test]
    fn incremental_matches_single_pass() {
        let data = b"hello, world!";
        let single = crc32(data);

        // Split into two chunks and compute incrementally
        let mid = 5;
        let mut crc = 0xFFFF_FFFF;
        crc = crc32_update(crc, &data[..mid]);
        crc = crc32_update(crc, &data[mid..]);
        let incremental = crc ^ 0xFFFF_FFFF;

        assert_eq!(single, incremental);
    }

    #[test]
    fn different_data_different_checksums() {
        assert_ne!(crc32(b"aaa"), crc32(b"bbb"));
        assert_ne!(crc32(b"abc"), crc32(b"cba"));
    }

    #[test]
    fn table_is_const() {
        // Verify the table was built at compile time by checking known entries.
        assert_eq!(CRC32_TABLE[0], 0x0000_0000);
        // CRC32_TABLE[128] = polynomial (byte 0x80 has bit 7 set, shifts once).
        assert_eq!(CRC32_TABLE[128], CRC32_POLYNOMIAL);
    }
}
