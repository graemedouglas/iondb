//! Hash table storage engines.
//!
//! Two hash table variants are provided:
//!
//! - **Extendible hashing** (`storage-hash-ext`): Directory-based, split-on-overflow.
//! - **Linear hashing** (`storage-hash-linear`): Deterministic split order, controlled load.

#[cfg(any(feature = "storage-hash-ext", feature = "storage-hash-linear"))]
pub(crate) mod bucket;

#[cfg(feature = "storage-hash-ext")]
pub mod extendible;

#[cfg(feature = "storage-hash-linear")]
pub mod linear;

/// Hash a byte slice using CRC-32 (deterministic, `no_std`, zero-alloc).
pub(crate) fn hash_key(key: &[u8]) -> u32 {
    iondb_core::crc::crc32(key)
}
