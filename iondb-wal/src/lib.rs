//! # iondb-wal
//!
//! Write-ahead log for `IonDB`.
//!
//! Provides sequential log append, CRC validation, crash recovery,
//! and checkpoint support. `no_std` compatible.

#![no_std]
#![forbid(unsafe_code)]
#![deny(missing_docs)]

#[cfg(feature = "alloc")]
extern crate alloc;

#[cfg(feature = "std")]
extern crate std;

/// Re-export core dependency.
pub use iondb_core;

#[cfg(test)]
mod tests {
    #[test]
    fn crate_compiles() {
        // Placeholder: WAL tests will go here.
    }
}
