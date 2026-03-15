# iondb-core

**Layer**: Foundation (leaf of the dependency tree)
**Depends on**: Nothing (except `core`/`alloc`)
**Depended on by**: Every other crate in the workspace

## Role

Defines the shared traits, error types, page format, and foundational types that all other IonDB crates depend on. This is the API contract for the entire system.

## Key traits

| Trait | File | Purpose |
|---|---|---|
| `StorageEngine` | `src/traits/storage_engine.rs` | Pluggable storage backends |
| `MemoryAllocator` | `src/traits/memory_allocator.rs` | Pluggable memory allocation |
| `IoBackend` | `src/traits/io_backend.rs` | Pluggable I/O backends |
| `Codec` | `src/traits/codec.rs` | Pluggable serialization |

## Feature flags

| Flag | Effect |
|---|---|
| `std` | Enables `std` facilities |
| `alloc` | Enables heap allocation via `alloc` crate |

## Target tier compatibility

- **Tier 1** (`no_std`): Full support — this crate is `#![no_std]` by default.
- **Tier 2** (`no_std` + `alloc`): With `alloc` feature.
- **Tier 3** (`std`): With `std` feature.

## Constraints

- `#![forbid(unsafe_code)]` — no unsafe code permitted.
- Must compile for `thumbv6m-none-eabi` with no features.
- The `Error` enum is the single error type for the entire workspace.
- No `format!` or `panic!` with formatting in the default code path.
