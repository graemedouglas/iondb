//! # `IonDB`
//!
//! An embedded database engine for the most constrained environments.
//!
//! `IonDB` is a library (not a daemon) that compiles for targets from 2 KB RAM
//! microcontrollers (`no_std`) through full embedded Linux systems (`std`).
//!
//! ## Quick start
//!
//! Add to your `Cargo.toml`:
//!
//! ```toml
//! [dependencies]
//! iondb = { version = "0.1", features = ["profile-minimal"] }
//! ```
//!
//! ## Feature profiles
//!
//! - `profile-minimal` — Tier 1 (Cortex-M0): static alloc, B+ tree, in-memory I/O.
//! - `profile-embedded` — Tier 2 (ESP32): bump alloc, all storage engines, WAL, buffer pool.
//! - `profile-full` — Tier 3 (Linux): everything enabled.
//!
//! ## Crate re-exports

#![no_std]
#![forbid(unsafe_code)]
#![deny(missing_docs)]

#[cfg(feature = "alloc")]
extern crate alloc;

#[cfg(feature = "std")]
extern crate std;

// Re-export all workspace crates for convenience.
pub use iondb_core as core;

/// Allocator implementations.
pub mod alloc_impl {
    pub use iondb_alloc::*;
}

/// Storage engine implementations.
pub mod storage {
    pub use iondb_storage::*;
}

/// I/O backend implementations.
pub mod io {
    pub use iondb_io::*;
}

/// Write-ahead log.
pub mod wal {
    pub use iondb_wal::*;
}

/// Transaction management.
pub mod tx {
    pub use iondb_tx::*;
}

/// Buffer pool / page cache.
pub mod buffer {
    pub use iondb_buffer::*;
}

/// Query DSL.
pub mod query {
    pub use iondb_query::*;
}

// Re-export core traits at the top level for convenience.
pub use iondb_core::Codec;
pub use iondb_core::Error;
pub use iondb_core::IoBackend;
pub use iondb_core::MemoryAllocator;
pub use iondb_core::StorageEngine;

#[cfg(test)]
mod tests {
    #[test]
    fn facade_compiles() {
        // Verifies all re-exports compile correctly.
    }
}
