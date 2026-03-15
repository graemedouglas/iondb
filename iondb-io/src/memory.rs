//! In-memory I/O backend.
//!
//! RAM-backed buffer useful for testing and volatile caches. This is the
//! default backend for all unit and integration tests — zero disk I/O,
//! deterministic, and fast.

// This module requires alloc for Vec-based storage.
// For no_std without alloc, a fixed-size variant would be needed.

#[cfg(test)]
mod tests {
    #[test]
    fn module_compiles() {
        // Placeholder: MemoryIoBackend implementation and tests will be
        // added when the storage layer is built in Phase 0.
    }
}
