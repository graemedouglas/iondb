//! # iondb-buffer
//!
//! Buffer pool / page cache for `IonDB`.
//!
//! Provides LRU and Clock eviction policies, dirty-page tracking,
//! pin/unpin semantics, and write-ahead integration.

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
        // Placeholder: buffer pool tests will go here.
    }
}
