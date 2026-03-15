//! # iondb-storage
//!
//! Storage engine implementations for `IonDB`.
//!
//! This crate provides the B+ tree, extendible hash, and linear hash
//! storage engines, all implementing `iondb_core::StorageEngine`.
//!
//! `no_std` by default. No `unsafe` code permitted.

#![no_std]
#![forbid(unsafe_code)]
#![deny(missing_docs)]

#[cfg(feature = "alloc")]
extern crate alloc;

#[cfg(feature = "std")]
extern crate std;

#[cfg(feature = "storage-bptree")]
pub mod bptree;

/// Re-export core dependency.
pub use iondb_core;
