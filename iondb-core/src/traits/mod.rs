//! Core traits that define `IonDB`'s pluggable architecture.
//!
//! All extension points go through these traits. No concrete type coupling
//! across crate boundaries — implementations live in their respective crates.

pub mod codec;
pub mod io_backend;
pub mod memory_allocator;
pub mod storage_engine;
