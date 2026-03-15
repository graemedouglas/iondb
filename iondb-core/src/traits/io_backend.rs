//! The `IoBackend` trait — pluggable I/O interface.

use crate::error;

/// Pluggable I/O backend interface.
///
/// Implementations include in-memory, file-based, and raw flash backends.
/// Users may implement this trait for custom storage media.
pub trait IoBackend {
    /// Read bytes from the given offset into `buf`.
    ///
    /// Returns the number of bytes read.
    ///
    /// # Errors
    ///
    /// Returns `Error::Io` if the read operation fails.
    fn read(&self, offset: u64, buf: &mut [u8]) -> error::Result<usize>;

    /// Write bytes from `buf` to the given offset.
    ///
    /// Returns the number of bytes written.
    ///
    /// # Errors
    ///
    /// Returns `Error::Io` if the write operation fails.
    fn write(&mut self, offset: u64, buf: &[u8]) -> error::Result<usize>;

    /// Sync all buffered writes to the underlying storage medium.
    ///
    /// # Errors
    ///
    /// Returns `Error::Io` if the sync operation fails.
    fn sync(&mut self) -> error::Result<()>;

    /// Return the total size of the storage medium in bytes.
    ///
    /// # Errors
    ///
    /// Returns `Error::Io` if the size cannot be determined.
    fn size(&self) -> error::Result<u64>;
}

#[cfg(test)]
mod tests {
    #[test]
    fn trait_is_defined() {
        // Verifies the trait compiles. Actual I/O backend tests live in iondb-io.
    }
}
