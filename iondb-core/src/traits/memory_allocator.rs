//! The `MemoryAllocator` trait — pluggable memory allocation interface.

use crate::error;
use core::alloc::Layout;

/// Pluggable memory allocator interface.
///
/// Implementations include static pool, bump allocator, and system allocator.
/// Users may implement this trait for RTOS heaps, slab allocators, or
/// external SRAM drivers.
///
/// # Implementation safety
///
/// Implementors must ensure that:
/// - `allocate` returns a pointer aligned to `layout.align()`.
/// - `deallocate` is only called with pointers returned by `allocate`.
/// - `reallocate` preserves the content of the original allocation up to
///   `min(old_layout.size(), new_layout.size())` bytes.
///
/// The methods are safe at the trait boundary because the actual `unsafe`
/// operations are confined to the `iondb-alloc` crate where `unsafe` is
/// permitted. Callers use higher-level safe wrappers.
pub trait MemoryAllocator {
    /// Allocate memory with the given layout.
    ///
    /// Returns a raw pointer to the allocated memory, or an error.
    /// The caller is responsible for proper deallocation.
    ///
    /// # Errors
    ///
    /// Returns `Error::AllocationFailed` if the allocation cannot be satisfied.
    fn allocate(&mut self, layout: Layout) -> error::Result<*mut u8>;

    /// Deallocate memory previously allocated with `allocate`.
    ///
    /// The caller must ensure `ptr` was returned by a prior call to `allocate`
    /// with a compatible layout.
    fn deallocate(&mut self, ptr: *mut u8, layout: Layout);

    /// Reallocate memory to a new layout.
    ///
    /// The caller must ensure `ptr` was returned by a prior call to `allocate`.
    ///
    /// # Errors
    ///
    /// Returns `Error::AllocationFailed` if the reallocation cannot be satisfied.
    fn reallocate(
        &mut self,
        ptr: *mut u8,
        old_layout: Layout,
        new_layout: Layout,
    ) -> error::Result<*mut u8>;

    /// Return the remaining capacity in bytes, if known.
    fn available(&self) -> Option<usize>;
}

#[cfg(test)]
mod tests {
    use core::alloc::Layout;

    #[test]
    fn trait_is_defined() {
        // Verify the trait compiles. Actual allocator tests live in iondb-alloc.
        let _layout = Layout::from_size_align(64, 8);
    }
}
