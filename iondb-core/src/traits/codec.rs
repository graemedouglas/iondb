//! The `Codec` trait — pluggable serialization interface.

use crate::error;

/// Pluggable serialization interface for keys and values.
///
/// All on-disk formats explicitly define byte order (little-endian default).
/// Implementations must support `no_std`.
pub trait Codec: Sized {
    /// Encode this value into the provided buffer.
    ///
    /// Returns the number of bytes written.
    ///
    /// # Errors
    ///
    /// Returns `Error::CodecError` if encoding fails (e.g., buffer too small).
    fn encode(&self, buf: &mut [u8]) -> error::Result<usize>;

    /// Decode a value from the provided buffer.
    ///
    /// Returns the decoded value and the number of bytes consumed.
    ///
    /// # Errors
    ///
    /// Returns `Error::CodecError` if decoding fails (e.g., invalid data).
    fn decode(buf: &[u8]) -> error::Result<(Self, usize)>;

    /// Return the maximum encoded size in bytes.
    ///
    /// Used for buffer pre-allocation when possible.
    fn max_encoded_size(&self) -> usize;
}

#[cfg(test)]
mod tests {
    #[test]
    fn trait_is_defined() {
        // Verifies the trait compiles. Actual codec tests live in their
        // respective crate implementations.
    }
}
