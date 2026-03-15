//! # iondb-alloc
//!
//! Pluggable memory allocator implementations for `IonDB`.
//!
//! This crate is `no_std` by default. `unsafe` code is permitted here
//! for allocator implementations — it is the only crate (along with
//! `iondb-io`) where `unsafe` is allowed.
//!
//! ## Allocators
//!
//! - **Static pool** (`alloc-static`): Fixed-size arena from a `static` array. Zero heap.
//! - **Bump allocator** (`alloc-bump`): Fast monotonic allocator with bulk-free.
//! - **System allocator** (`alloc-system`): Delegates to `std::alloc::GlobalAlloc`.

#![no_std]
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
        // Placeholder: allocator implementation tests will go here.
    }
}
