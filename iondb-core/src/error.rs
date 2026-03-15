//! Unified error type for all `IonDB` operations.
//!
//! All crates in the `IonDB` workspace use this error type. Error variants
//! are gated behind feature flags so unused variants are compiled out.

use core::fmt;

/// Unified error type for `IonDB`.
///
/// Uses feature-gated variants so unused error paths add zero binary size.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Error {
    /// An I/O operation failed.
    Io,
    /// The requested key was not found.
    NotFound,
    /// Storage capacity exhausted (disk full, max keys, etc.).
    CapacityExhausted,
    /// Memory allocation failed.
    AllocationFailed,
    /// Data corruption detected (CRC mismatch, invalid format).
    Corruption,
    /// An operation was attempted on a closed or invalid resource.
    InvalidState,
    /// A codec encode/decode operation failed.
    CodecError,
    /// Write-ahead log error.
    WalError,
    /// Transaction error (conflict, timeout, etc.).
    TransactionError,
    /// Buffer pool error (no free frames, pin count overflow).
    BufferError,
    /// Query execution error.
    QueryError,
    /// Page size or alignment violation.
    PageError,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io => write!(f, "I/O operation failed"),
            Self::NotFound => write!(f, "key not found"),
            Self::CapacityExhausted => write!(f, "storage capacity exhausted"),
            Self::AllocationFailed => write!(f, "memory allocation failed"),
            Self::Corruption => write!(f, "data corruption detected"),
            Self::InvalidState => write!(f, "invalid state"),
            Self::CodecError => write!(f, "codec encode/decode failed"),
            Self::WalError => write!(f, "write-ahead log error"),
            Self::TransactionError => write!(f, "transaction error"),
            Self::BufferError => write!(f, "buffer pool error"),
            Self::QueryError => write!(f, "query execution error"),
            Self::PageError => write!(f, "page size or alignment violation"),
        }
    }
}

/// Convenience Result type for `IonDB` operations.
pub type Result<T> = core::result::Result<T, Error>;

#[cfg(test)]
mod tests {
    extern crate alloc;
    use super::*;
    use alloc::format;

    #[test]
    fn error_display() {
        let err = Error::NotFound;
        let msg = format!("{err}");
        assert_eq!(msg, "key not found");
    }

    #[test]
    fn error_debug() {
        let err = Error::Io;
        let msg = format!("{err:?}");
        assert_eq!(msg, "Io");
    }

    #[test]
    fn error_clone_eq() {
        let e1 = Error::Corruption;
        let e2 = e1.clone();
        assert_eq!(e1, e2);
    }

    #[test]
    fn result_type_alias() {
        let ok: Result<u32> = Ok(42);
        assert_eq!(ok, Ok(42));
        let err: Result<u32> = Err(Error::NotFound);
        assert_eq!(err, Err(Error::NotFound));
    }

    #[test]
    fn all_variants_display() {
        // Ensure every variant has a Display impl (no panics).
        let variants = [
            Error::Io,
            Error::NotFound,
            Error::CapacityExhausted,
            Error::AllocationFailed,
            Error::Corruption,
            Error::InvalidState,
            Error::CodecError,
            Error::WalError,
            Error::TransactionError,
            Error::BufferError,
            Error::QueryError,
            Error::PageError,
        ];
        for v in &variants {
            let _ = format!("{v}");
        }
    }
}
