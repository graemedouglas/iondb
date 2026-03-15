# iondb-io

**Layer**: Implementation
**Depends on**: `iondb-core`
**Depended on by**: `iondb-facade`

## Role

I/O backend implementations. This is one of only two crates where `unsafe` code is permitted (the other is `iondb-alloc`).

Also provides `FailpointIoBackend` (behind `failpoint` feature) for crash simulation testing.

## Backends

| Backend | Feature | Description |
|---|---|---|
| In-memory | `io-mem` | RAM-backed buffer. Default for testing. |
| File | `io-file` | Filesystem I/O via `std::fs`. Requires `std`. |
| Raw flash | `io-raw` | NOR/NAND flash adapter with wear-leveling. |
| Failpoint | `failpoint` | Fault injection wrapper for crash testing. |

## Feature flags

| Flag | Effect |
|---|---|
| `io-mem` | In-memory backend (default) |
| `io-file` | File-based backend (requires `std`) |
| `io-raw` | Raw flash backend |
| `async-io` | Async `IoBackend` variant |
| `failpoint` | `FailpointIoBackend` for crash simulation |

## Target tier compatibility

- **Tier 1**: `io-mem`, `io-raw`.
- **Tier 2**: `io-mem`, `io-raw`.
- **Tier 3**: All backends including `io-file`.

## Constraints

- `unsafe` code allowed but must be audited.
- All buffer accesses through safe accessors — no unaligned reads/writes.
- Use `read_unaligned`/`write_unaligned` or align the buffer.
- All on-disk formats: little-endian default.
