//! Test utilities for `IonDB`.
//!
//! These are available only in test builds. They provide instrumented wrappers
//! for verifying allocation behavior and other invariants.

use crate::error::Result;
use crate::traits::memory_allocator::MemoryAllocator;
use core::alloc::Layout;

/// An allocator wrapper that counts allocations and deallocations.
///
/// Used in tests to verify zero-allocation guarantees and track memory usage
/// patterns.
///
/// # Example (in tests)
///
/// ```ignore
/// let mut inner = StaticPoolAllocator::new(&mut buf, 64).unwrap();
/// let mut counting = CountingAllocator::new(&mut inner);
/// counting.allocate(layout)?;
/// assert_eq!(counting.alloc_count(), 1);
/// ```
pub struct CountingAllocator<'a, A: MemoryAllocator> {
    inner: &'a mut A,
    alloc_count: usize,
    dealloc_count: usize,
    bytes_allocated: usize,
    bytes_deallocated: usize,
}

impl<'a, A: MemoryAllocator> CountingAllocator<'a, A> {
    /// Wrap an existing allocator with counting instrumentation.
    pub fn new(inner: &'a mut A) -> Self {
        Self {
            inner,
            alloc_count: 0,
            dealloc_count: 0,
            bytes_allocated: 0,
            bytes_deallocated: 0,
        }
    }

    /// Number of successful allocations.
    #[must_use]
    pub fn alloc_count(&self) -> usize {
        self.alloc_count
    }

    /// Number of deallocations.
    #[must_use]
    pub fn dealloc_count(&self) -> usize {
        self.dealloc_count
    }

    /// Total bytes successfully allocated.
    #[must_use]
    pub fn bytes_allocated(&self) -> usize {
        self.bytes_allocated
    }

    /// Total bytes deallocated.
    #[must_use]
    pub fn bytes_deallocated(&self) -> usize {
        self.bytes_deallocated
    }

    /// Net bytes currently allocated (allocated - deallocated).
    #[must_use]
    pub fn bytes_in_use(&self) -> usize {
        self.bytes_allocated.saturating_sub(self.bytes_deallocated)
    }

    /// Reset all counters to zero.
    pub fn reset_counts(&mut self) {
        self.alloc_count = 0;
        self.dealloc_count = 0;
        self.bytes_allocated = 0;
        self.bytes_deallocated = 0;
    }
}

impl<A: MemoryAllocator> MemoryAllocator for CountingAllocator<'_, A> {
    fn allocate(&mut self, layout: Layout) -> Result<*mut u8> {
        let ptr = self.inner.allocate(layout)?;
        self.alloc_count += 1;
        self.bytes_allocated += layout.size();
        Ok(ptr)
    }

    fn deallocate(&mut self, ptr: *mut u8, layout: Layout) {
        self.inner.deallocate(ptr, layout);
        self.dealloc_count += 1;
        self.bytes_deallocated += layout.size();
    }

    fn reallocate(
        &mut self,
        ptr: *mut u8,
        old_layout: Layout,
        new_layout: Layout,
    ) -> Result<*mut u8> {
        let new_ptr = self.inner.reallocate(ptr, old_layout, new_layout)?;
        // Reallocate counts as a dealloc + alloc
        self.dealloc_count += 1;
        self.bytes_deallocated += old_layout.size();
        self.alloc_count += 1;
        self.bytes_allocated += new_layout.size();
        Ok(new_ptr)
    }

    fn available(&self) -> Option<usize> {
        self.inner.available()
    }
}

#[cfg(test)]
// Tests use unwrap for brevity; panics are acceptable in test code.
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::error::Error;

    /// Minimal test allocator that tracks a single allocation.
    struct TestAllocator {
        buf: [u8; 128],
        allocated: bool,
    }

    impl TestAllocator {
        fn new() -> Self {
            Self {
                buf: [0u8; 128],
                allocated: false,
            }
        }
    }

    impl MemoryAllocator for TestAllocator {
        fn allocate(&mut self, _layout: Layout) -> Result<*mut u8> {
            if self.allocated {
                return Err(Error::AllocationFailed);
            }
            self.allocated = true;
            Ok(self.buf.as_mut_ptr())
        }

        fn deallocate(&mut self, _ptr: *mut u8, _layout: Layout) {
            self.allocated = false;
        }

        fn reallocate(
            &mut self,
            _ptr: *mut u8,
            _old_layout: Layout,
            _new_layout: Layout,
        ) -> Result<*mut u8> {
            Ok(self.buf.as_mut_ptr())
        }

        fn available(&self) -> Option<usize> {
            if self.allocated {
                Some(0)
            } else {
                Some(128)
            }
        }
    }

    #[test]
    fn counts_allocations() {
        let mut inner = TestAllocator::new();
        let mut counting = CountingAllocator::new(&mut inner);
        let layout = Layout::from_size_align(16, 8).unwrap(); // OK in tests

        assert_eq!(counting.alloc_count(), 0);
        let ptr = counting.allocate(layout);
        assert!(ptr.is_ok());
        assert_eq!(counting.alloc_count(), 1);
        assert_eq!(counting.bytes_allocated(), 16);
    }

    #[test]
    fn counts_deallocations() {
        let mut inner = TestAllocator::new();
        let mut counting = CountingAllocator::new(&mut inner);
        let layout = Layout::from_size_align(16, 8).unwrap(); // OK in tests

        let ptr = counting.allocate(layout).unwrap(); // OK in tests
        counting.deallocate(ptr, layout);
        assert_eq!(counting.dealloc_count(), 1);
        assert_eq!(counting.bytes_deallocated(), 16);
        assert_eq!(counting.bytes_in_use(), 0);
    }

    #[test]
    fn reset_counts() {
        let mut inner = TestAllocator::new();
        let mut counting = CountingAllocator::new(&mut inner);
        let layout = Layout::from_size_align(8, 8).unwrap(); // OK in tests

        let _ptr = counting.allocate(layout);
        counting.reset_counts();
        assert_eq!(counting.alloc_count(), 0);
        assert_eq!(counting.dealloc_count(), 0);
        assert_eq!(counting.bytes_allocated(), 0);
    }

    #[test]
    fn delegates_available() {
        let mut inner = TestAllocator::new();
        let counting = CountingAllocator::new(&mut inner);
        assert_eq!(counting.available(), Some(128));
    }
}
