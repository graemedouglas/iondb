//! Static pool allocator — fixed-size block allocator for `no_std` environments.
//!
//! Carves a caller-provided buffer into equal-size blocks tracked by a bitmap.
//! Zero heap allocation. Suitable for Tier 1 targets (Cortex-M0, 2 KB RAM).
//!
//! # Layout
//!
//! The provided buffer is split into two regions:
//! - **Bitmap region** (front): 1 bit per block, rounded up to whole bytes.
//! - **Pool region** (remainder): contiguous block storage.
//!
//! ```text
//! [bitmap: ceil(block_count / 8) bytes] [pool: block_count * block_size bytes]
//! ```

use core::alloc::Layout;
use iondb_core::error::{Error, Result};
use iondb_core::MemoryAllocator;

/// A fixed-size block allocator backed by a caller-provided buffer.
///
/// The buffer is partitioned into a bitmap and a pool of equal-size blocks.
/// All metadata lives inside the buffer — no external allocation.
///
/// # Block size requirements
///
/// - Must be a power of 2.
/// - Must be at least 8 bytes (to satisfy common alignment needs).
pub struct StaticPoolAllocator<'a> {
    /// Full backing buffer (bitmap + pool).
    buf: &'a mut [u8],
    /// Size of each allocatable block in bytes.
    block_size: usize,
    /// Total number of blocks in the pool.
    block_count: usize,
    /// Byte offset where the pool region starts (after bitmap).
    pool_offset: usize,
    /// Number of currently allocated blocks.
    allocated_count: usize,
}

impl<'a> StaticPoolAllocator<'a> {
    /// Create a new static pool allocator from the given buffer.
    ///
    /// `block_size` must be a power of 2 and >= 8. The buffer is partitioned
    /// into a bitmap and pool region. Returns `None` if the buffer is too
    /// small for even one block, or if `block_size` is invalid.
    pub fn new(buf: &'a mut [u8], block_size: usize) -> Option<Self> {
        if block_size < 8 || !block_size.is_power_of_two() {
            return None;
        }

        // Calculate how many blocks fit:
        // bitmap_bytes = ceil(block_count / 8)
        // We need: bitmap_bytes + block_count * block_size <= buf.len()
        // Solve: block_count * block_size + ceil(block_count / 8) <= buf.len()
        // Approximate: block_count * (block_size + 1) <= buf.len() * 8
        //   block_count <= (buf.len() * 8) / (block_size * 8 + 1)
        let block_count = (buf.len() * 8) / (block_size * 8 + 1);
        if block_count == 0 {
            return None;
        }

        let bitmap_bytes = block_count.div_ceil(8);
        // Align pool region so that block pointers satisfy block_size alignment.
        // We need (buf_base + pool_offset) to be aligned to block_size.
        let buf_addr = buf.as_ptr() as usize;
        let min_offset = bitmap_bytes;
        let abs_start = buf_addr + min_offset;
        let aligned_abs = (abs_start + block_size - 1) & !(block_size - 1);
        let pool_offset = aligned_abs - buf_addr;

        // Recalculate block_count with aligned pool_offset
        let remaining = buf.len().checked_sub(pool_offset)?;
        let block_count = remaining / block_size;
        if block_count == 0 {
            return None;
        }

        // Verify bitmap still fits
        let bitmap_needed = block_count.div_ceil(8);
        if bitmap_needed > pool_offset {
            return None;
        }

        // Verify it fits
        let total_needed = pool_offset + block_count * block_size;
        if total_needed > buf.len() {
            return None;
        }

        // Zero the bitmap (all blocks free)
        for b in &mut buf[..bitmap_bytes] {
            *b = 0;
        }

        Some(Self {
            buf,
            block_size,
            block_count,
            pool_offset,
            allocated_count: 0,
        })
    }

    /// Return the block size.
    #[must_use]
    pub fn block_size(&self) -> usize {
        self.block_size
    }

    /// Return the total number of blocks.
    #[must_use]
    pub fn block_count(&self) -> usize {
        self.block_count
    }

    /// Return the number of allocated blocks.
    #[must_use]
    pub fn allocated_count(&self) -> usize {
        self.allocated_count
    }

    /// Check if a specific block index is allocated.
    fn is_allocated(&self, index: usize) -> bool {
        let byte = index / 8;
        let bit = index % 8;
        (self.buf[byte] & (1 << bit)) != 0
    }

    /// Mark a block as allocated in the bitmap.
    fn mark_allocated(&mut self, index: usize) {
        let byte = index / 8;
        let bit = index % 8;
        self.buf[byte] |= 1 << bit;
    }

    /// Mark a block as free in the bitmap.
    fn mark_free(&mut self, index: usize) {
        let byte = index / 8;
        let bit = index % 8;
        self.buf[byte] &= !(1 << bit);
    }

    /// Find the first free block and return its index, or `None`.
    fn find_free_block(&self) -> Option<usize> {
        let bitmap_bytes = self.block_count.div_ceil(8);
        for byte_idx in 0..bitmap_bytes {
            let byte = self.buf[byte_idx];
            if byte != 0xFF {
                // At least one free bit in this byte
                for bit in 0..8u8 {
                    let index = byte_idx * 8 + bit as usize;
                    if index >= self.block_count {
                        return None;
                    }
                    if (byte & (1 << bit)) == 0 {
                        return Some(index);
                    }
                }
            }
        }
        None
    }

    /// Get a pointer to the start of block `index`.
    fn block_ptr(&mut self, index: usize) -> *mut u8 {
        let offset = self.pool_offset + index * self.block_size;
        // SAFETY: offset is within buf bounds (guaranteed by construction).
        unsafe { self.buf.as_mut_ptr().add(offset) }
    }

    /// Convert a pointer back to a block index, or `None` if invalid.
    fn ptr_to_index(&self, ptr: *mut u8) -> Option<usize> {
        let base = self.buf.as_ptr() as usize + self.pool_offset;
        let addr = ptr as usize;
        if addr < base {
            return None;
        }
        let offset = addr - base;
        if !offset.is_multiple_of(self.block_size) {
            return None;
        }
        let index = offset / self.block_size;
        if index >= self.block_count {
            return None;
        }
        Some(index)
    }
}

impl MemoryAllocator for StaticPoolAllocator<'_> {
    fn allocate(&mut self, layout: Layout) -> Result<*mut u8> {
        // Check that the requested size and alignment fit within a block
        if layout.size() > self.block_size || layout.align() > self.block_size {
            return Err(Error::AllocationFailed);
        }

        let index = self.find_free_block().ok_or(Error::AllocationFailed)?;
        self.mark_allocated(index);
        self.allocated_count += 1;
        Ok(self.block_ptr(index))
    }

    fn deallocate(&mut self, ptr: *mut u8, _layout: Layout) {
        if let Some(index) = self.ptr_to_index(ptr) {
            if self.is_allocated(index) {
                self.mark_free(index);
                self.allocated_count -= 1;
            }
        }
    }

    fn reallocate(
        &mut self,
        ptr: *mut u8,
        _old_layout: Layout,
        new_layout: Layout,
    ) -> Result<*mut u8> {
        // If the new size still fits in one block, just return the same pointer
        if new_layout.size() <= self.block_size && new_layout.align() <= self.block_size {
            return Ok(ptr);
        }

        // Fixed block sizes — cannot grow beyond block_size.
        // allocate() rejects the same conditions, so the copy path is unreachable.
        Err(Error::AllocationFailed)
    }

    fn available(&self) -> Option<usize> {
        Some((self.block_count - self.allocated_count) * self.block_size)
    }
}

#[cfg(test)]
// Tests use unwrap for brevity; panics are acceptable in test code.
#[allow(clippy::unwrap_used)]
#[path = "static_pool_tests.rs"]
mod tests;
