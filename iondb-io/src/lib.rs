//! # iondb-io
//!
//! I/O backend implementations for `IonDB`.
//!
//! This crate provides in-memory, file-based, and raw flash I/O backends,
//! all implementing `iondb_core::IoBackend`. Also includes the
//! `FailpointIoBackend` for crash simulation testing (behind `failpoint` feature).
//!
//! `no_std` by default. `unsafe` code is permitted here for I/O operations.

#![no_std]
#![deny(missing_docs)]

#[cfg(feature = "alloc")]
extern crate alloc;

#[cfg(feature = "std")]
extern crate std;

#[cfg(feature = "failpoint")]
pub mod failpoint;

#[cfg(feature = "io-mem")]
pub mod memory;

/// Re-export core dependency.
pub use iondb_core;

#[cfg(test)]
mod tests {
    #[test]
    fn crate_compiles() {
        // Placeholder: I/O backend tests will go here.
    }
}
