//! Little-endian read/write helpers for alignment-safe, portable I/O.
//!
//! All on-disk formats in `IonDB` use little-endian byte order. These helpers
//! provide bounds-checked conversions between native types and byte slices
//! without any `unsafe` code or alignment requirements.

use crate::error::{Error, Result};

/// Read a `u8` from `buf`.
///
/// # Errors
///
/// Returns [`Error::PageError`] if `buf` is empty.
pub fn read_u8(buf: &[u8]) -> Result<u8> {
    buf.first().copied().ok_or(Error::PageError)
}

/// Write a `u8` to `buf`.
///
/// # Errors
///
/// Returns [`Error::PageError`] if `buf` is empty.
pub fn write_u8(buf: &mut [u8], val: u8) -> Result<()> {
    let dest = buf.first_mut().ok_or(Error::PageError)?;
    *dest = val;
    Ok(())
}

/// Read a little-endian `u16` from `buf`.
///
/// # Errors
///
/// Returns [`Error::PageError`] if `buf.len() < 2`.
pub fn read_u16_le(buf: &[u8]) -> Result<u16> {
    let bytes: [u8; 2] = buf
        .get(..2)
        .ok_or(Error::PageError)?
        .try_into()
        .map_err(|_| Error::PageError)?;
    Ok(u16::from_le_bytes(bytes))
}

/// Write a little-endian `u16` to `buf`.
///
/// # Errors
///
/// Returns [`Error::PageError`] if `buf.len() < 2`.
pub fn write_u16_le(buf: &mut [u8], val: u16) -> Result<()> {
    let dest = buf.get_mut(..2).ok_or(Error::PageError)?;
    dest.copy_from_slice(&val.to_le_bytes());
    Ok(())
}

/// Read a little-endian `u32` from `buf`.
///
/// # Errors
///
/// Returns [`Error::PageError`] if `buf.len() < 4`.
pub fn read_u32_le(buf: &[u8]) -> Result<u32> {
    let bytes: [u8; 4] = buf
        .get(..4)
        .ok_or(Error::PageError)?
        .try_into()
        .map_err(|_| Error::PageError)?;
    Ok(u32::from_le_bytes(bytes))
}

/// Write a little-endian `u32` to `buf`.
///
/// # Errors
///
/// Returns [`Error::PageError`] if `buf.len() < 4`.
pub fn write_u32_le(buf: &mut [u8], val: u32) -> Result<()> {
    let dest = buf.get_mut(..4).ok_or(Error::PageError)?;
    dest.copy_from_slice(&val.to_le_bytes());
    Ok(())
}

/// Read a little-endian `u64` from `buf`.
///
/// # Errors
///
/// Returns [`Error::PageError`] if `buf.len() < 8`.
pub fn read_u64_le(buf: &[u8]) -> Result<u64> {
    let bytes: [u8; 8] = buf
        .get(..8)
        .ok_or(Error::PageError)?
        .try_into()
        .map_err(|_| Error::PageError)?;
    Ok(u64::from_le_bytes(bytes))
}

/// Write a little-endian `u64` to `buf`.
///
/// # Errors
///
/// Returns [`Error::PageError`] if `buf.len() < 8`.
pub fn write_u64_le(buf: &mut [u8], val: u64) -> Result<()> {
    let dest = buf.get_mut(..8).ok_or(Error::PageError)?;
    dest.copy_from_slice(&val.to_le_bytes());
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn u8_round_trip() {
        let mut buf = [0u8; 1];
        assert_eq!(write_u8(&mut buf, 0xAB), Ok(()));
        assert_eq!(read_u8(&buf), Ok(0xAB));
    }

    #[test]
    fn u16_round_trip() {
        let mut buf = [0u8; 2];
        assert_eq!(write_u16_le(&mut buf, 0x1234), Ok(()));
        assert_eq!(buf, [0x34, 0x12]); // little-endian
        assert_eq!(read_u16_le(&buf), Ok(0x1234));
    }

    #[test]
    fn u32_round_trip() {
        let mut buf = [0u8; 4];
        assert_eq!(write_u32_le(&mut buf, 0xDEAD_BEEF), Ok(()));
        assert_eq!(buf, [0xEF, 0xBE, 0xAD, 0xDE]);
        assert_eq!(read_u32_le(&buf), Ok(0xDEAD_BEEF));
    }

    #[test]
    fn u64_round_trip() {
        let mut buf = [0u8; 8];
        assert_eq!(write_u64_le(&mut buf, 0x0102_0304_0506_0708), Ok(()));
        assert_eq!(buf, [0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01]);
        assert_eq!(read_u64_le(&buf), Ok(0x0102_0304_0506_0708));
    }

    #[test]
    fn boundary_values() {
        let mut buf = [0u8; 8];
        assert_eq!(write_u16_le(&mut buf, u16::MAX), Ok(()));
        assert_eq!(read_u16_le(&buf), Ok(u16::MAX));

        assert_eq!(write_u32_le(&mut buf, u32::MAX), Ok(()));
        assert_eq!(read_u32_le(&buf), Ok(u32::MAX));

        assert_eq!(write_u64_le(&mut buf, u64::MAX), Ok(()));
        assert_eq!(read_u64_le(&buf), Ok(u64::MAX));

        // Zero
        assert_eq!(write_u64_le(&mut buf, 0), Ok(()));
        assert_eq!(read_u64_le(&buf), Ok(0));
    }

    #[test]
    fn buffer_too_small() {
        let buf = [0u8; 0];
        assert_eq!(read_u8(&buf), Err(Error::PageError));
        assert_eq!(read_u16_le(&buf), Err(Error::PageError));
        assert_eq!(read_u32_le(&buf), Err(Error::PageError));
        assert_eq!(read_u64_le(&buf), Err(Error::PageError));

        let buf = [0u8; 1];
        assert_eq!(read_u16_le(&buf), Err(Error::PageError));

        let buf = [0u8; 3];
        assert_eq!(read_u32_le(&buf), Err(Error::PageError));

        let buf = [0u8; 7];
        assert_eq!(read_u64_le(&buf), Err(Error::PageError));
    }

    #[test]
    fn write_buffer_too_small() {
        let mut buf = [0u8; 0];
        assert_eq!(write_u8(&mut buf, 1), Err(Error::PageError));
        assert_eq!(write_u16_le(&mut buf, 1), Err(Error::PageError));
        assert_eq!(write_u32_le(&mut buf, 1), Err(Error::PageError));
        assert_eq!(write_u64_le(&mut buf, 1), Err(Error::PageError));
    }
}
