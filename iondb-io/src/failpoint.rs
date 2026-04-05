//! Crash simulation via I/O fault injection.
//!
//! The `FailpointIoBackend` wraps any `IoBackend` and injects faults at
//! configurable points. Used for testing recovery paths:
//!
//! - WAL replay after crash mid-write.
//! - Partial checkpoint detection and recovery.
//! - Corrupted storage file detection via checksums.
//! - Incomplete transaction rollback.
//!
//! # Example
//!
//! ```ignore
//! let inner = MemoryIoBackend::new(4096);
//! let mut failpoint = FailpointIoBackend::new(inner);
//! failpoint.set_fault(Fault::ErrorBeforeWrite);
//! assert!(failpoint.write(0, &[1, 2, 3]).is_err());
//! ```

use iondb_core::error::{Error, Result};
use iondb_core::traits::io_backend::IoBackend;

/// Types of faults that can be injected.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Fault {
    /// I/O error before the write reaches storage.
    ErrorBeforeWrite,
    /// I/O error after write but before sync.
    ErrorBeforeSync,
    /// Partial write: only `n` bytes are written.
    PartialWrite(usize),
    /// Sync failure.
    SyncFailure,
    /// Read corruption: flip a bit at the given byte offset within the read buffer.
    ReadCorruption(usize),
}

/// A wrapper around any `IoBackend` that injects configurable faults.
///
/// Designed for deterministic, reproducible crash simulation testing.
pub struct FailpointIoBackend<T: IoBackend> {
    inner: T,
    fault: Option<Fault>,
    write_count: u64,
    trigger_after: Option<u64>,
}

impl<T: IoBackend> FailpointIoBackend<T> {
    /// Create a new failpoint wrapper around the given backend.
    pub fn new(inner: T) -> Self {
        Self {
            inner,
            fault: None,
            write_count: 0,
            trigger_after: None,
        }
    }

    /// Set the fault to inject on the next applicable operation.
    pub fn set_fault(&mut self, fault: Fault) {
        self.fault = Some(fault);
    }

    /// Set the fault to trigger after N write operations.
    pub fn set_fault_after(&mut self, fault: Fault, after: u64) {
        self.fault = Some(fault);
        self.trigger_after = Some(after);
    }

    /// Clear any pending fault.
    pub fn clear_fault(&mut self) {
        self.fault = None;
        self.trigger_after = None;
    }

    /// Return the number of successful write operations.
    pub fn write_count(&self) -> u64 {
        self.write_count
    }

    /// Consume the wrapper and return the inner backend.
    pub fn into_inner(self) -> T {
        self.inner
    }

    fn should_trigger(&self) -> bool {
        match self.trigger_after {
            Some(n) => self.write_count >= n,
            None => self.fault.is_some(),
        }
    }
}

impl<T: IoBackend> IoBackend for FailpointIoBackend<T> {
    fn read(&self, offset: u64, buf: &mut [u8]) -> Result<usize> {
        let n = self.inner.read(offset, buf)?;
        if let Some(Fault::ReadCorruption(byte_offset)) = self.fault {
            if byte_offset < buf.len() {
                buf[byte_offset] ^= 0xFF;
            }
        }
        Ok(n)
    }

    fn write(&mut self, offset: u64, buf: &[u8]) -> Result<usize> {
        if self.should_trigger() {
            match self.fault {
                Some(Fault::ErrorBeforeWrite) => return Err(Error::Io),
                Some(Fault::PartialWrite(n)) => {
                    let to_write = if n < buf.len() { n } else { buf.len() };
                    let result = self.inner.write(offset, &buf[..to_write]);
                    self.write_count += 1;
                    return result;
                }
                _ => {}
            }
        }
        let result = self.inner.write(offset, buf);
        if result.is_ok() {
            self.write_count += 1;
        }
        if self.should_trigger() {
            if let Some(Fault::ErrorBeforeSync) = self.fault {
                // Write succeeded but we'll fail on sync
            }
        }
        result
    }

    fn sync(&mut self) -> Result<()> {
        if self.should_trigger() {
            if let Some(Fault::SyncFailure | Fault::ErrorBeforeSync) = self.fault {
                return Err(Error::Io);
            }
        }
        self.inner.sync()
    }

    fn size(&self) -> Result<u64> {
        self.inner.size()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // A minimal in-memory backend for testing the failpoint wrapper.
    struct TestBackend {
        data: [u8; 256],
        len: u64,
    }

    impl TestBackend {
        fn new() -> Self {
            Self {
                data: [0u8; 256],
                len: 256,
            }
        }
    }

    impl IoBackend for TestBackend {
        fn read(&self, offset: u64, buf: &mut [u8]) -> Result<usize> {
            let off = offset as usize;
            let end = core::cmp::min(off + buf.len(), self.data.len());
            if off >= self.data.len() {
                return Ok(0);
            }
            let n = end - off;
            buf[..n].copy_from_slice(&self.data[off..end]);
            Ok(n)
        }

        fn write(&mut self, offset: u64, buf: &[u8]) -> Result<usize> {
            let off = offset as usize;
            let end = core::cmp::min(off + buf.len(), self.data.len());
            if off >= self.data.len() {
                return Err(Error::Io);
            }
            let n = end - off;
            self.data[off..end].copy_from_slice(&buf[..n]);
            Ok(n)
        }

        fn sync(&mut self) -> Result<()> {
            Ok(())
        }

        fn size(&self) -> Result<u64> {
            Ok(self.len)
        }
    }

    #[test]
    fn no_fault_passthrough() {
        let mut fp = FailpointIoBackend::new(TestBackend::new());
        let written = fp.write(0, &[1, 2, 3]).unwrap();
        assert_eq!(written, 3);
        assert_eq!(fp.write_count(), 1);

        let mut buf = [0u8; 3];
        let read = fp.read(0, &mut buf).unwrap();
        assert_eq!(read, 3);
        assert_eq!(buf, [1, 2, 3]);
    }

    #[test]
    fn error_before_write() {
        let mut fp = FailpointIoBackend::new(TestBackend::new());
        fp.set_fault(Fault::ErrorBeforeWrite);
        assert_eq!(fp.write(0, &[1, 2, 3]), Err(Error::Io));
    }

    #[test]
    fn partial_write() {
        let mut fp = FailpointIoBackend::new(TestBackend::new());
        fp.set_fault(Fault::PartialWrite(2));
        let written = fp.write(0, &[1, 2, 3]).unwrap();
        assert_eq!(written, 2);
    }

    #[test]
    fn sync_failure() {
        let mut fp = FailpointIoBackend::new(TestBackend::new());
        fp.set_fault(Fault::SyncFailure);
        assert_eq!(fp.sync(), Err(Error::Io));
    }

    #[test]
    fn read_corruption() {
        let mut fp = FailpointIoBackend::new(TestBackend::new());
        let _ = fp.write(0, &[0xAA, 0xBB, 0xCC]);
        fp.set_fault(Fault::ReadCorruption(1));

        let mut buf = [0u8; 3];
        let _ = fp.read(0, &mut buf);
        assert_eq!(buf[0], 0xAA);
        assert_eq!(buf[1], 0xBB ^ 0xFF); // corrupted
        assert_eq!(buf[2], 0xCC);
    }

    #[test]
    fn fault_after_n_writes() {
        let mut fp = FailpointIoBackend::new(TestBackend::new());
        fp.set_fault_after(Fault::ErrorBeforeWrite, 3);

        // First 3 writes succeed (count reaches 0, 1, 2 which are < 3)
        assert!(fp.write(0, &[1]).is_ok());
        assert!(fp.write(1, &[2]).is_ok());
        assert!(fp.write(2, &[3]).is_ok());
        // 4th write fails (count is now 3 which == trigger_after)
        assert_eq!(fp.write(3, &[4]), Err(Error::Io));
    }

    #[test]
    fn clear_fault() {
        let mut fp = FailpointIoBackend::new(TestBackend::new());
        fp.set_fault(Fault::ErrorBeforeWrite);
        fp.clear_fault();
        assert!(fp.write(0, &[1]).is_ok());
    }

    #[test]
    fn into_inner() {
        let fp = FailpointIoBackend::new(TestBackend::new());
        let inner = fp.into_inner();
        assert_eq!(inner.size().unwrap(), 256);
    }

    #[test]
    fn sync_fault_does_not_affect_write() {
        let mut fp = FailpointIoBackend::new(TestBackend::new());
        fp.set_fault(Fault::SyncFailure);
        // Write should succeed — SyncFailure only triggers on sync()
        let written = fp.write(0, &[1, 2, 3]).unwrap();
        assert_eq!(written, 3);
        // Sync should fail
        assert_eq!(fp.sync(), Err(Error::Io));
    }

    #[test]
    fn error_before_sync_fault() {
        let mut fp = FailpointIoBackend::new(TestBackend::new());
        fp.set_fault(Fault::ErrorBeforeSync);
        // Write succeeds (data reaches storage)
        let written = fp.write(0, &[4, 5, 6]).unwrap();
        assert_eq!(written, 3);
        // Sync fails
        assert_eq!(fp.sync(), Err(Error::Io));
    }

    #[test]
    fn size_delegates() {
        let fp = FailpointIoBackend::new(TestBackend::new());
        assert_eq!(fp.size().unwrap(), 256);
    }

    #[test]
    fn sync_without_fault() {
        let mut fp = FailpointIoBackend::new(TestBackend::new());
        // No fault set — sync delegates to inner backend
        assert!(fp.sync().is_ok());
    }

    #[test]
    fn read_out_of_bounds() {
        let fp = FailpointIoBackend::new(TestBackend::new());
        let mut buf = [0u8; 4];
        // Offset at end of backing store — TestBackend returns Ok(0)
        let n = fp.read(256, &mut buf).unwrap();
        assert_eq!(n, 0);
    }

    #[test]
    fn write_out_of_bounds() {
        let mut fp = FailpointIoBackend::new(TestBackend::new());
        // Offset past backing store — TestBackend returns Err
        assert_eq!(fp.write(256, &[1, 2]), Err(Error::Io));
    }

    #[test]
    fn sync_with_write_fault_succeeds() {
        let mut fp = FailpointIoBackend::new(TestBackend::new());
        // Set a write-related fault — sync should still succeed
        // (covers the `_ => {}` branch in sync, line 134).
        fp.set_fault(Fault::ErrorBeforeWrite);
        assert!(fp.sync().is_ok());
    }
}
