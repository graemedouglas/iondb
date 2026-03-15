//! # iondb-query
//!
//! LINQ-style embedded query DSL for `IonDB`.
//!
//! Provides a zero-allocation query path for Tier 1 targets and
//! richer heap-backed operations behind the `query-alloc` feature.

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
        // Placeholder: query DSL tests will go here.
    }
}
