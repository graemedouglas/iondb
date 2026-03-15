//! # iondb-tx
//!
//! Transaction manager, MVCC, and isolation for `IonDB`.
//!
//! This is the cross-cutting integration crate. It depends on `iondb-core`
//! and coordinates across storage, WAL, and buffer pool via their traits.

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
        // Placeholder: transaction tests will go here.
    }
}
