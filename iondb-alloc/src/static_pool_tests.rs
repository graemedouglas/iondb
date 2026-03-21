use super::*;

#[test]
fn new_valid() {
    let mut buf = [0u8; 256];
    let pool = StaticPoolAllocator::new(&mut buf, 8);
    assert!(pool.is_some());
    let pool = pool.unwrap(); // OK in tests
    assert!(pool.block_count() > 0);
    assert_eq!(pool.allocated_count(), 0);
}

#[test]
fn new_invalid_block_size() {
    let mut buf = [0u8; 256];
    // Not power of 2
    assert!(StaticPoolAllocator::new(&mut buf, 7).is_none());
    // Too small
    assert!(StaticPoolAllocator::new(&mut buf, 4).is_none());
    // Zero
    assert!(StaticPoolAllocator::new(&mut buf, 0).is_none());
}

#[test]
fn new_buffer_too_small() {
    let mut buf = [0u8; 4];
    assert!(StaticPoolAllocator::new(&mut buf, 8).is_none());
}

#[test]
fn allocate_and_deallocate() {
    let mut buf = [0u8; 256];
    let mut pool = StaticPoolAllocator::new(&mut buf, 16).unwrap(); // OK in tests
    let layout = Layout::from_size_align(8, 8).unwrap(); // OK in tests

    let ptr = pool.allocate(layout);
    assert!(ptr.is_ok());
    assert_eq!(pool.allocated_count(), 1);

    pool.deallocate(ptr.unwrap(), layout); // OK in tests
    assert_eq!(pool.allocated_count(), 0);
}

#[test]
fn allocate_until_full() {
    let mut buf = [0u8; 128];
    let mut pool = StaticPoolAllocator::new(&mut buf, 8).unwrap(); // OK in tests
    let count = pool.block_count();
    let layout = Layout::from_size_align(8, 8).unwrap(); // OK in tests

    for _ in 0..count {
        assert!(pool.allocate(layout).is_ok());
    }
    // Next allocation should fail
    assert_eq!(pool.allocate(layout), Err(Error::AllocationFailed));
}

#[test]
fn deallocate_then_reallocate() {
    let mut buf = [0u8; 128];
    let mut pool = StaticPoolAllocator::new(&mut buf, 8).unwrap(); // OK in tests
    let layout = Layout::from_size_align(8, 8).unwrap(); // OK in tests
    let count = pool.block_count();

    // Fill up
    let mut ptrs = [core::ptr::null_mut(); 32];
    for p in ptrs.iter_mut().take(count) {
        *p = pool.allocate(layout).unwrap(); // OK in tests
    }

    // Free one
    pool.deallocate(ptrs[0], layout);
    assert_eq!(pool.allocated_count(), count - 1);

    // Allocate again — should succeed
    assert!(pool.allocate(layout).is_ok());
    assert_eq!(pool.allocated_count(), count);
}

#[test]
fn available_tracking() {
    let mut buf = [0u8; 256];
    let mut pool = StaticPoolAllocator::new(&mut buf, 16).unwrap(); // OK in tests
    let total = pool.available();
    let layout = Layout::from_size_align(8, 8).unwrap(); // OK in tests

    let ptr = pool.allocate(layout).unwrap(); // OK in tests
    assert_eq!(
        pool.available(),
        Some(total.unwrap() - pool.block_size()) // OK in tests
    );

    pool.deallocate(ptr, layout);
    assert_eq!(pool.available(), total);
}

#[test]
fn alignment_is_respected() {
    let mut buf = [0u8; 512];
    let mut pool = StaticPoolAllocator::new(&mut buf, 64).unwrap(); // OK in tests
    let layout = Layout::from_size_align(32, 8).unwrap(); // OK in tests

    let ptr = pool.allocate(layout).unwrap(); // OK in tests

    // Block size is 64, which is aligned to 64 (and thus to 8)
    assert_eq!((ptr as usize) % layout.align(), 0);
}

#[test]
fn oversize_allocation_fails() {
    let mut buf = [0u8; 256];
    let mut pool = StaticPoolAllocator::new(&mut buf, 16).unwrap(); // OK in tests

    // Request larger than block size
    let layout = Layout::from_size_align(32, 8).unwrap(); // OK in tests
    assert_eq!(pool.allocate(layout), Err(Error::AllocationFailed));
}

#[test]
fn reallocate_within_block() {
    let mut buf = [0u8; 256];
    let mut pool = StaticPoolAllocator::new(&mut buf, 16).unwrap(); // OK in tests
    let layout = Layout::from_size_align(8, 8).unwrap(); // OK in tests

    let ptr = pool.allocate(layout).unwrap(); // OK in tests

    // Write some data
    unsafe { *ptr = 0xAA };

    // Reallocate to a slightly larger size that still fits in one block
    let new_layout = Layout::from_size_align(12, 4).unwrap(); // OK in tests
    let new_ptr = pool.reallocate(ptr, layout, new_layout).unwrap(); // OK in tests

    // Should return the same pointer (fits in same block)
    assert_eq!(new_ptr, ptr);
    // Data preserved
    assert_eq!(unsafe { *new_ptr }, 0xAA);
}

#[test]
fn deallocate_invalid_pointer() {
    let mut buf = [0u8; 256];
    let mut pool = StaticPoolAllocator::new(&mut buf, 16).unwrap(); // OK in tests
    let layout = Layout::from_size_align(8, 8).unwrap(); // OK in tests

    // Allocate one block so allocated_count is 1
    let _ptr = pool.allocate(layout).unwrap(); // OK in tests
    assert_eq!(pool.allocated_count(), 1);

    // Deallocate with a null pointer — should be silently ignored
    pool.deallocate(core::ptr::null_mut(), layout);
    assert_eq!(pool.allocated_count(), 1);
}

#[test]
fn allocate_all_blocks_bitmap_full() {
    // Use a buffer large enough that block_count is a multiple of 8,
    // so every bitmap byte becomes 0xFF when all blocks are allocated.
    let mut buf = [0u8; 72];
    let mut pool = StaticPoolAllocator::new(&mut buf, 8).unwrap(); // OK in tests
    let count = pool.block_count();
    let layout = Layout::from_size_align(8, 8).unwrap(); // OK in tests

    for _ in 0..count {
        assert!(pool.allocate(layout).is_ok());
    }
    // All bitmap bytes are 0xFF — find_free_block scans every byte
    // and returns None (line 164).
    assert_eq!(pool.allocate(layout), Err(Error::AllocationFailed));
}

#[test]
fn reallocate_larger_than_block() {
    let mut buf = [0u8; 256];
    let mut pool = StaticPoolAllocator::new(&mut buf, 8).unwrap(); // OK in tests
    let layout = Layout::from_size_align(8, 8).unwrap(); // OK in tests

    let ptr = pool.allocate(layout).unwrap(); // OK in tests

    // Reallocate with a layout larger than block_size — should fail
    let big_layout = Layout::from_size_align(64, 8).unwrap(); // OK in tests
    assert_eq!(
        pool.reallocate(ptr, layout, big_layout),
        Err(Error::AllocationFailed)
    );
}

#[test]
fn deallocate_misaligned_pointer() {
    let mut buf = [0u8; 256];
    let mut pool = StaticPoolAllocator::new(&mut buf, 16).unwrap(); // OK in tests
    let layout = Layout::from_size_align(8, 8).unwrap(); // OK in tests

    let ptr = pool.allocate(layout).unwrap(); // OK in tests
    assert_eq!(pool.allocated_count(), 1);

    // Offset the pointer by 1 byte — not aligned to block_size
    let bad_ptr = unsafe { ptr.add(1) };
    pool.deallocate(bad_ptr, layout);
    // Deallocate should be silently ignored
    assert_eq!(pool.allocated_count(), 1);
}

#[test]
fn deallocate_out_of_range_pointer() {
    let mut buf = [0u8; 256];
    let mut pool = StaticPoolAllocator::new(&mut buf, 16).unwrap(); // OK in tests
    let layout = Layout::from_size_align(8, 8).unwrap(); // OK in tests

    let ptr = pool.allocate(layout).unwrap(); // OK in tests
    assert_eq!(pool.allocated_count(), 1);

    // Point to a completely separate allocation that is definitely outside the pool.
    // Using ptr.add(N) to go out-of-bounds is UB even without dereferencing, so we
    // instead take the address of an unrelated local buffer.
    let mut other_buf = [0u8; 8];
    let far_ptr = other_buf.as_mut_ptr();
    pool.deallocate(far_ptr, layout);
    // Deallocate should be silently ignored
    assert_eq!(pool.allocated_count(), 1);
}

#[test]
fn reallocate_too_large() {
    let mut buf = [0u8; 256];
    let mut pool = StaticPoolAllocator::new(&mut buf, 8).unwrap(); // OK in tests
    let layout = Layout::from_size_align(8, 8).unwrap(); // OK in tests

    // Fill all blocks so allocate inside reallocate will fail
    let count = pool.block_count();
    let mut ptrs = [core::ptr::null_mut(); 32];
    for p in ptrs.iter_mut().take(count) {
        *p = pool.allocate(layout).unwrap(); // OK in tests
    }

    // Reallocate with a layout larger than block_size — should return Err
    let big_layout = Layout::from_size_align(16, 8).unwrap(); // OK in tests
    assert_eq!(
        pool.reallocate(ptrs[0], layout, big_layout),
        Err(Error::AllocationFailed)
    );
}

#[test]
// Loop index is 0..16, always fits in u8.
#[allow(clippy::cast_possible_truncation)]
fn write_and_read_back() {
    let mut buf = [0u8; 256];
    let mut pool = StaticPoolAllocator::new(&mut buf, 16).unwrap(); // OK in tests
    let layout = Layout::from_size_align(16, 8).unwrap(); // OK in tests

    let ptr = pool.allocate(layout).unwrap(); // OK in tests

    // Write pattern
    unsafe {
        for i in 0..16 {
            *ptr.add(i) = i as u8;
        }
    }

    // Read back
    unsafe {
        for i in 0..16 {
            assert_eq!(*ptr.add(i), i as u8);
        }
    }
}

#[test]
fn new_borderline_sizes() {
    // Exercise the validation paths in new() with various edge-case sizes.
    // The exact path hit depends on runtime buffer alignment, so we sweep
    // a range of sizes and just verify we don't panic.
    for size in 5..20 {
        let mut buf = [0u8; 64];
        let _ = StaticPoolAllocator::new(&mut buf[..size], 8);
    }
    for size in 9..35 {
        let mut buf = [0u8; 64];
        let _ = StaticPoolAllocator::new(&mut buf[..size], 16);
    }
    for size in 17..70 {
        let mut buf = [0u8; 128];
        let _ = StaticPoolAllocator::new(&mut buf[..size], 32);
    }
}

#[test]
fn bitmap_overflow_alignment_edge_case() {
    // The bitmap_needed > pool_offset guard (line 83) triggers when alignment
    // makes pool_offset small enough that the recalculated block_count needs
    // more bitmap bytes than pool_offset can hold. This depends on the buffer's
    // memory address. Sweep all alignments via sub-slices to guarantee coverage.
    let mut big = [0u8; 256];
    for start in 0..8 {
        let sub = &mut big[start..start + 73];
        // With block_size=8, orig_bc=8, bitmap_bytes=1. If (buf_addr + 1) is
        // aligned to 8, pool_offset=1, recalc_bc=9, bitmap_needed=2 > 1.
        let _ = StaticPoolAllocator::new(sub, 8);
    }
}
