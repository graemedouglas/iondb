//! # iondb-core
//!
//! Core traits, error types, page format, and shared types for `IonDB`.
//!
//! This crate is `no_std` by default. Enable the `std` feature for standard
//! library support, or `alloc` for heap allocation support.
//!
//! ## Key types
//!
//! - [`Error`] — Unified error type for all `IonDB` operations.
//! - [`StorageEngine`] — Trait for pluggable storage backends (B+ tree, hash, etc.).
//! - [`MemoryAllocator`] — Trait for pluggable memory allocators.
//! - [`IoBackend`] — Trait for pluggable I/O backends (memory, file, raw flash).
//! - [`Codec`] — Trait for key/value serialization.

#![no_std]
#![forbid(unsafe_code)]
#![deny(missing_docs)]

#[cfg(feature = "alloc")]
extern crate alloc;

#[cfg(feature = "std")]
extern crate std;

pub mod crc;
pub mod endian;
pub mod error;
pub mod page;
#[cfg(test)]
pub mod test_utils;
pub mod traits;
pub mod types;

pub use error::Error;
pub use traits::codec::Codec;
pub use traits::io_backend::IoBackend;
pub use traits::memory_allocator::MemoryAllocator;
pub use traits::storage_engine::StorageEngine;
