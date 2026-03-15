# iondb-alloc

**Layer**: Implementation
**Depends on**: `iondb-core`
**Depended on by**: `iondb-facade`

## Role

Provides pluggable memory allocator implementations. This is one of only two crates where `unsafe` code is permitted (the other is `iondb-io`).

## Allocators

| Allocator | Feature | Description |
|---|---|---|
| Static pool | `alloc-static` | Fixed-size arena from static array. Zero heap. Tier 1. |
| Bump | `alloc-bump` | Fast monotonic allocator with bulk-free. Requires `alloc`. |
| System | `alloc-system` | Delegates to `std::alloc::GlobalAlloc`. Requires `std`. |

## Feature flags

| Flag | Effect |
|---|---|
| `alloc-static` | Static pool allocator (default) |
| `alloc-bump` | Bump allocator (requires `alloc`) |
| `alloc-system` | System allocator (requires `std`) |

## Target tier compatibility

- **Tier 1**: `alloc-static` only.
- **Tier 2**: `alloc-static` + `alloc-bump`.
- **Tier 3**: All allocators including `alloc-system`.

## Constraints

- `unsafe` code is allowed but must be reviewed/audited.
- Run `cargo +nightly miri test -p iondb-alloc` to detect UB.
- Every returned pointer must satisfy the requested alignment.
- All allocators implement `iondb_core::MemoryAllocator`.
