# Phase 0 — Foundation Completion Report

**Date:** 2026-03-15
**Phase:** 0 (Foundation)
**Status:** Complete

## Summary

Phase 0 implements the foundational layer of IonDB: core utilities (page format,
CRC-32, endian helpers), a static pool allocator, an in-memory I/O backend, and
a sorted-array storage engine placeholder. The `sensor-log` dogfood app
validates the full stack end-to-end.

## What Was Built

### `iondb-core` — Foundation modules

| Module | Purpose | Tests |
|---|---|---|
| `crc.rs` | CRC-32 (IEEE polynomial), const-generated table, incremental API | 7 |
| `endian.rs` | Little-endian read/write helpers with bounds checking | 7 |
| `page.rs` | Page header (16 bytes), page types, CRC write/verify | 10 |
| `test_utils.rs` | `CountingAllocator` — tracks alloc/dealloc counts and bytes | 4 |

### `iondb-alloc` — Static pool allocator

| Module | Purpose | Tests |
|---|---|---|
| `static_pool.rs` | Bitmap-based fixed-block allocator from `&mut [u8]` | 11 |

- Blocks: power-of-2 sizes, minimum 8 bytes
- Bitmap carved from buffer front, pool from remainder
- Pool region aligned to block_size accounting for buffer base address
- Zero heap allocation — suitable for Tier 1 (Cortex-M0, 2 KB RAM)
- Miri-validated: zero undefined behavior

### `iondb-io` — In-memory I/O backend

| Module | Purpose | Tests |
|---|---|---|
| `memory.rs` | RAM-backed `IoBackend` from `&mut [u8]` | 11 |

- High-water mark tracking for logical size
- Bounds-checked reads and writes
- `no_std` compatible (no `Vec`, no heap)

### `iondb-storage` — B+ tree placeholder

| Module | Purpose | Tests |
|---|---|---|
| `bptree.rs` | Sorted-array `StorageEngine` (Phase 0 placeholder) | 13 |

- Binary search for O(log n) lookups
- Sorted insertion with index shifting
- Buffer layout: header + index entries + data packed backward
- u16 offsets (max 64 KB buffer)
- **Will be replaced with a proper page-based B+ tree in Phase 1**

### `apps/sensor-log` — Dogfood application

- Demonstrates `StorageEngine::put()`/`get()` with `BTreeEngine`
- Simulates timestamped sensor readings
- 2 integration tests

## Test Results

| Suite | Count | Status |
|---|---|---|
| `iondb-core` | 41 | Pass |
| `iondb-alloc` | 11 | Pass |
| `iondb-io` | 12 | Pass |
| `iondb-storage` | 13 | Pass |
| `sensor-log` | 2 | Pass |
| Structural tests | 9 | Pass |
| Other crates | 9 | Pass |
| **Total** | **97** | **All pass** |

## Verification Checklist

| Check | Result |
|---|---|
| `cargo fmt --all -- --check` | Clean |
| `cargo clippy --workspace --all-targets -- -D warnings` | Zero warnings |
| `cargo test --workspace` | 97 pass, 0 fail |
| `cargo build -p iondb-core --no-default-features --target thumbv6m-none-eabi` | Compiles |
| `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps` | Clean |
| `cargo +nightly miri test -p iondb-alloc` | 11 pass, zero UB |
| Structural: no horizontal deps | Pass |
| Structural: no `#[allow]` without justification | Pass |
| Structural: no `Box<dyn Error>` | Pass |
| Structural: all files < 500 lines | Pass |

## Architecture Decisions

1. **Caller-provided buffers everywhere** — All structures (`StaticPoolAllocator`,
   `MemoryIoBackend`, `BTreeEngine`) take `&mut [u8]` from the caller. No heap,
   no `static mut`, fully `no_std` compatible.

2. **Address-aware alignment** — The static pool aligner accounts for the
   buffer's actual address (not just internal offset) to guarantee correct
   pointer alignment. Validated by Miri.

3. **Sorted-array placeholder** — Phase 0 uses a simple sorted array instead of
   a real B+ tree. This validates the `StorageEngine` trait contract without the
   complexity of page management.

4. **CRC-32 with const table** — The lookup table is generated at compile time
   via `const fn`, so zero runtime initialization cost.

## Next Steps (Phase 1)

1. **Page-based B+ tree** — Replace `bptree.rs` sorted-array with a proper
   page-oriented B+ tree supporting splitting, merging, and multi-page storage.

2. **Buffer pool / page cache** — Implement `iondb-buffer` with page
   eviction policies (LRU or clock).

3. **Hash table engines** — Implement extendible hash and linear hash in
   `iondb-storage`.

4. **Bump allocator** — Add `iondb-alloc` bump allocator (behind `alloc-bump`
   feature) for monotonic allocation with bulk-free.

5. **WAL skeleton** — Begin `iondb-wal` with log record format and write path.

6. **`no_std` sensor-log** — Convert `sensor-log` to a true `no_std` entry
   point with `cortex-m-rt` when QEMU runner is activated.

7. **File I/O backend** — Implement `io-file` feature in `iondb-io` for
   filesystem-backed storage.
