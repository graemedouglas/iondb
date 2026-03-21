//! Bump allocator — fast monotonic allocation with bulk-free.
//!
//! Allocations grow forward through a caller-provided buffer. Individual
//! deallocations are no-ops; memory is reclaimed all at once via [`BumpAllocator::reset`].
//!
//! Ideal for transaction-scoped scratch memory where all allocations share
//! a single lifetime.
//!
//! # Layout
//!
//! ```text
//! [allocated region →] [free space] [← end of buffer]
//! ```

use core::alloc::Layout;
use iondb_core::error::{Error, Result};
use iondb_core::MemoryAllocator;

/// A bump (arena) allocator backed by a caller-provided buffer.
///
/// Allocations advance a cursor forward. Individual `deallocate` calls are
/// no-ops. Call [`reset`](Self::reset) to reclaim all memory at once.
///
/// # Alignment
///
/// Each allocation is aligned to the requested `Layout::align()`. The cursor
/// is bumped past any alignment padding.
pub struct BumpAllocator<'a> {
    buf: &'a mut [u8],
    /// Current allocation cursor (byte offset into buf).
    cursor: usize,
    /// Number of outstanding allocations (for bookkeeping).
    alloc_count: usize,
}

impl<'a> BumpAllocator<'a> {
    /// Create a new bump allocator from the given buffer.
    ///
    /// The buffer must be at least 8 bytes. Returns `None` if too small.
    pub fn new(buf: &'a mut [u8]) -> Option<Self> {
        if buf.len() < 8 {
            return None;
        }
        Some(Self {
            buf,
            cursor: 0,
            alloc_count: 0,
        })
    }

    /// Reset the allocator, reclaiming all memory.
    ///
    /// All previously returned pointers become invalid after this call.
    /// The caller is responsible for ensuring no references to allocated
    /// memory are held.
    pub fn reset(&mut self) {
        self.cursor = 0;
        self.alloc_count = 0;
    }

    /// Return the number of bytes still available.
    #[must_use]
    pub fn remaining(&self) -> usize {
        self.buf.len().saturating_sub(self.cursor)
    }

    /// Return the number of outstanding allocations.
    #[must_use]
    pub fn alloc_count(&self) -> usize {
        self.alloc_count
    }

    /// Compute the aligned cursor position for the given alignment.
    fn aligned_cursor(&self, align: usize) -> usize {
        let base = self.buf.as_ptr() as usize + self.cursor;
        let aligned = (base + align - 1) & !(align - 1);
        self.cursor + (aligned - base)
    }
}

impl MemoryAllocator for BumpAllocator<'_> {
    fn allocate(&mut self, layout: Layout) -> Result<*mut u8> {
        let aligned = self.aligned_cursor(layout.align());
        let end = aligned
            .checked_add(layout.size())
            .ok_or(Error::AllocationFailed)?;
        if end > self.buf.len() {
            return Err(Error::AllocationFailed);
        }
        self.cursor = end;
        self.alloc_count += 1;
        // SAFETY: `aligned` is within buf bounds (checked above) and properly
        // aligned. The pointer is valid for `layout.size()` bytes.
        Ok(unsafe { self.buf.as_mut_ptr().add(aligned) })
    }

    fn deallocate(&mut self, _ptr: *mut u8, _layout: Layout) {
        // Bump allocators don't free individual allocations.
        // Memory is reclaimed in bulk via `reset()`.
        if self.alloc_count > 0 {
            self.alloc_count -= 1;
        }
    }

    // Trait method is safe; callers uphold pointer validity invariants.
    #[allow(clippy::not_unsafe_ptr_arg_deref)]
    fn reallocate(
        &mut self,
        ptr: *mut u8,
        old_layout: Layout,
        new_layout: Layout,
    ) -> Result<*mut u8> {
        // If the new size fits in the old allocation, return same pointer
        if new_layout.size() <= old_layout.size() && new_layout.align() <= old_layout.align() {
            return Ok(ptr);
        }

        // Allocate new block and copy data
        let new_ptr = self.allocate(new_layout)?;
        let copy_len = old_layout.size().min(new_layout.size());
        // SAFETY: Both pointers are valid, non-overlapping (bump only grows
        // forward), and within the buffer.
        unsafe {
            core::ptr::copy_nonoverlapping(ptr, new_ptr, copy_len);
        }
        // Don't deallocate old — bump allocator can't reclaim individual blocks
        Ok(new_ptr)
    }

    fn available(&self) -> Option<usize> {
        Some(self.remaining())
    }
}

#[cfg(test)]
// Tests use unwrap for brevity; panics are acceptable in test code.
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn new_valid() {
        let mut buf = [0u8; 256];
        assert!(BumpAllocator::new(&mut buf).is_some());
    }

    #[test]
    fn new_too_small() {
        let mut buf = [0u8; 4];
        assert!(BumpAllocator::new(&mut buf).is_none());
    }

    #[test]
    fn allocate_basic() {
        let mut buf = [0u8; 256];
        let mut bump = BumpAllocator::new(&mut buf).unwrap();
        let layout = Layout::from_size_align(16, 8).unwrap();
        let ptr = bump.allocate(layout);
        assert!(ptr.is_ok());
        assert_eq!(bump.alloc_count(), 1);
    }

    #[test]
    fn allocate_alignment() {
        let mut buf = [0u8; 512];
        let mut bump = BumpAllocator::new(&mut buf).unwrap();

        // Allocate 1 byte with alignment 1
        let l1 = Layout::from_size_align(1, 1).unwrap();
        let p1 = bump.allocate(l1).unwrap();

        // Allocate with alignment 16
        let l2 = Layout::from_size_align(8, 16).unwrap();
        let p2 = bump.allocate(l2).unwrap();

        assert_eq!((p2 as usize) % 16, 0);
        assert!(p2 as usize > p1 as usize);
    }

    #[test]
    fn allocate_until_full() {
        let mut buf = [0u8; 64];
        let mut bump = BumpAllocator::new(&mut buf).unwrap();
        let layout = Layout::from_size_align(16, 8).unwrap();

        let mut count = 0;
        while bump.allocate(layout).is_ok() {
            count += 1;
        }
        assert!(count > 0);
        assert!(count <= 4); // 64 bytes / 16 bytes per alloc
    }

    #[test]
    fn reset_reclaims_memory() {
        let mut buf = [0u8; 64];
        let mut bump = BumpAllocator::new(&mut buf).unwrap();
        let layout = Layout::from_size_align(32, 8).unwrap();

        let _ = bump.allocate(layout).unwrap();
        assert!(bump.remaining() < 64);

        bump.reset();
        assert_eq!(bump.remaining(), 64);
        assert_eq!(bump.alloc_count(), 0);

        // Can allocate again
        assert!(bump.allocate(layout).is_ok());
    }

    #[test]
    fn deallocate_is_noop_for_memory() {
        let mut buf = [0u8; 256];
        let mut bump = BumpAllocator::new(&mut buf).unwrap();
        let layout = Layout::from_size_align(16, 8).unwrap();

        let before = bump.remaining();
        let ptr = bump.allocate(layout).unwrap();
        let after_alloc = bump.remaining();

        bump.deallocate(ptr, layout);
        // Memory NOT reclaimed (bump semantics)
        assert_eq!(bump.remaining(), after_alloc);
        assert!(after_alloc < before);
    }

    #[test]
    fn available_tracking() {
        let mut buf = [0u8; 256];
        let mut bump = BumpAllocator::new(&mut buf).unwrap();
        assert_eq!(bump.available(), Some(256));

        let layout = Layout::from_size_align(32, 8).unwrap();
        let _ = bump.allocate(layout).unwrap();
        assert!(bump.available().unwrap() < 256);
    }

    #[test]
    fn reallocate_in_place() {
        let mut buf = [0u8; 256];
        let mut bump = BumpAllocator::new(&mut buf).unwrap();
        let l1 = Layout::from_size_align(32, 8).unwrap();
        let ptr = bump.allocate(l1).unwrap();

        unsafe { *ptr = 0xAA };

        // Shrink — should return same pointer
        let l2 = Layout::from_size_align(16, 8).unwrap();
        let p2 = bump.reallocate(ptr, l1, l2).unwrap();
        assert_eq!(p2, ptr);
        assert_eq!(unsafe { *p2 }, 0xAA);
    }

    #[test]
    fn reallocate_grows() {
        let mut buf = [0u8; 256];
        let mut bump = BumpAllocator::new(&mut buf).unwrap();
        let l1 = Layout::from_size_align(8, 8).unwrap();
        let ptr = bump.allocate(l1).unwrap();

        unsafe { *ptr = 0xBB };

        let l2 = Layout::from_size_align(32, 8).unwrap();
        let p2 = bump.reallocate(ptr, l1, l2).unwrap();
        // Data preserved
        assert_eq!(unsafe { *p2 }, 0xBB);
    }

    #[test]
    // Loop index always fits in u8.
    #[allow(clippy::cast_possible_truncation)]
    fn write_and_read_back() {
        let mut buf = [0u8; 256];
        let mut bump = BumpAllocator::new(&mut buf).unwrap();
        let layout = Layout::from_size_align(16, 8).unwrap();
        let ptr = bump.allocate(layout).unwrap();

        unsafe {
            for i in 0..16 {
                *ptr.add(i) = i as u8;
            }
            for i in 0..16 {
                assert_eq!(*ptr.add(i), i as u8);
            }
        }
    }
}
