//! In-memory I/O backend.
//!
//! RAM-backed buffer useful for testing and volatile caches. This is the
//! default backend for all unit and integration tests — zero disk I/O,
//! deterministic, and fast.
//!
//! The backend is `no_std` compatible: it takes a `&mut [u8]` slice from the
//! caller rather than allocating with `Vec`. The caller decides where the
//! buffer lives (stack, static, or heap).

use iondb_core::error::{Error, Result};
use iondb_core::IoBackend;

/// In-memory I/O backend backed by a caller-provided byte slice.
///
/// Tracks a logical size (high-water mark) that grows with writes.
pub struct MemoryIoBackend<'a> {
    buf: &'a mut [u8],
    /// Logical size: the highest offset written to.
    len: u64,
}

impl<'a> MemoryIoBackend<'a> {
    /// Create a new in-memory backend from the given buffer.
    ///
    /// The buffer capacity determines the maximum storage size.
    /// Initial logical size is zero.
    pub fn new(buf: &'a mut [u8]) -> Self {
        Self { buf, len: 0 }
    }

    /// Create a new in-memory backend with an initial logical size.
    ///
    /// Useful when the buffer contains pre-existing data.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Io`] if `initial_len` exceeds `buf.len()`.
    pub fn with_len(buf: &'a mut [u8], initial_len: u64) -> Result<Self> {
        if initial_len > buf.len() as u64 {
            return Err(Error::Io);
        }
        Ok(Self {
            buf,
            len: initial_len,
        })
    }

    /// Return the capacity of the underlying buffer.
    #[must_use]
    pub fn capacity(&self) -> usize {
        self.buf.len()
    }
}

impl IoBackend for MemoryIoBackend<'_> {
    // Buffer length fits in usize by construction; offset is bounds-checked.
    #[allow(clippy::cast_possible_truncation)]
    fn read(&self, offset: u64, buf: &mut [u8]) -> Result<usize> {
        let off = offset as usize;
        if off >= self.buf.len() {
            return Ok(0);
        }
        let available = self.buf.len() - off;
        let n = buf.len().min(available);
        buf[..n].copy_from_slice(&self.buf[off..off + n]);
        Ok(n)
    }

    // Buffer length fits in usize by construction; offset is bounds-checked.
    #[allow(clippy::cast_possible_truncation)]
    fn write(&mut self, offset: u64, buf: &[u8]) -> Result<usize> {
        let off = offset as usize;
        let end = off.checked_add(buf.len()).ok_or(Error::Io)?;
        if end > self.buf.len() {
            return Err(Error::Io);
        }
        self.buf[off..end].copy_from_slice(buf);
        let new_end = end as u64;
        if new_end > self.len {
            self.len = new_end;
        }
        Ok(buf.len())
    }

    fn sync(&mut self) -> Result<()> {
        Ok(())
    }

    fn size(&self) -> Result<u64> {
        Ok(self.len)
    }
}

#[cfg(test)]
// Tests use unwrap for brevity; panics are acceptable in test code.
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn new_empty() {
        let mut buf = [0u8; 64];
        let backend = MemoryIoBackend::new(&mut buf);
        assert_eq!(backend.size(), Ok(0));
        assert_eq!(backend.capacity(), 64);
    }

    #[test]
    fn write_read_round_trip() {
        let mut buf = [0u8; 64];
        let mut backend = MemoryIoBackend::new(&mut buf);

        let data = [0xAA, 0xBB, 0xCC, 0xDD];
        assert_eq!(backend.write(0, &data), Ok(4));
        assert_eq!(backend.size(), Ok(4));

        let mut read_buf = [0u8; 4];
        assert_eq!(backend.read(0, &mut read_buf), Ok(4));
        assert_eq!(read_buf, data);
    }

    #[test]
    fn write_at_offset() {
        let mut buf = [0u8; 64];
        let mut backend = MemoryIoBackend::new(&mut buf);

        assert_eq!(backend.write(10, &[1, 2, 3]), Ok(3));
        assert_eq!(backend.size(), Ok(13));

        let mut read_buf = [0u8; 3];
        assert_eq!(backend.read(10, &mut read_buf), Ok(3));
        assert_eq!(read_buf, [1, 2, 3]);
    }

    #[test]
    fn write_past_capacity_fails() {
        let mut buf = [0u8; 8];
        let mut backend = MemoryIoBackend::new(&mut buf);
        assert_eq!(backend.write(5, &[1, 2, 3, 4]), Err(Error::Io));
    }

    #[test]
    fn read_past_end_returns_zero_bytes() {
        let mut buf = [0u8; 16];
        let backend = MemoryIoBackend::new(&mut buf);

        let mut read_buf = [0u8; 4];
        assert_eq!(backend.read(100, &mut read_buf), Ok(0));
    }

    #[test]
    fn read_partial_at_boundary() {
        let mut buf = [0u8; 8];
        let mut backend = MemoryIoBackend::new(&mut buf);
        assert_eq!(backend.write(0, &[1, 2, 3, 4, 5, 6, 7, 8]), Ok(8));

        let mut read_buf = [0u8; 4];
        assert_eq!(backend.read(6, &mut read_buf), Ok(2));
        assert_eq!(read_buf[0], 7);
        assert_eq!(read_buf[1], 8);
    }

    #[test]
    fn sync_is_noop() {
        let mut buf = [0u8; 8];
        let mut backend = MemoryIoBackend::new(&mut buf);
        assert_eq!(backend.sync(), Ok(()));
    }

    #[test]
    fn size_tracks_high_water_mark() {
        let mut buf = [0u8; 64];
        let mut backend = MemoryIoBackend::new(&mut buf);

        assert_eq!(backend.write(10, &[1, 2]), Ok(2));
        assert_eq!(backend.size(), Ok(12));

        assert_eq!(backend.write(0, &[3, 4]), Ok(2));
        assert_eq!(backend.size(), Ok(12));

        assert_eq!(backend.write(20, &[5]), Ok(1));
        assert_eq!(backend.size(), Ok(21));
    }

    #[test]
    fn with_len_valid() {
        let mut buf = [0u8; 64];
        buf[0] = 0xAA;
        let backend = MemoryIoBackend::with_len(&mut buf, 32).unwrap(); // OK in tests
        assert_eq!(backend.size(), Ok(32));

        let mut read_buf = [0u8; 1];
        assert_eq!(backend.read(0, &mut read_buf), Ok(1));
        assert_eq!(read_buf[0], 0xAA);
    }

    #[test]
    fn with_len_too_large() {
        let mut buf = [0u8; 8];
        let result = MemoryIoBackend::with_len(&mut buf, 100);
        assert!(result.is_err());
    }

    #[test]
    fn overwrite_existing_data() {
        let mut buf = [0u8; 32];
        let mut backend = MemoryIoBackend::new(&mut buf);

        assert_eq!(backend.write(0, &[1, 2, 3, 4]), Ok(4));
        assert_eq!(backend.write(0, &[5, 6]), Ok(2));

        let mut read_buf = [0u8; 4];
        assert_eq!(backend.read(0, &mut read_buf), Ok(4));
        assert_eq!(read_buf, [5, 6, 3, 4]);
    }
}
