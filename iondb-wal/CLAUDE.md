# iondb-wal

**Layer**: Implementation
**Depends on**: `iondb-core`
**Depended on by**: `iondb-facade`

## Role

Write-ahead log: sequential append, CRC validation, crash recovery, and checkpointing.

## Key invariant

**No dirty page is flushed before its log record is synced.** This is the write-ahead invariant that guarantees durability.

## WAL record format

```
[CRC32] [TxnID: u64] [Type: u8] [KeyLen: u16] [ValLen: u32] [Key] [Value]
```

## Feature flags

| Flag | Effect |
|---|---|
| `std` | Standard library support |
| `alloc` | Heap allocation support |

## Target tier compatibility

- **Tier 1**: `no_std` compatible with static buffers.
- **Tier 2/3**: Full support with dynamic allocation.

## Constraints

- `#![forbid(unsafe_code)]`.
- Must NOT depend on any other implementation crate.
- CRC validation on read — corrupted records detected and skipped.
- Sync policy configurable: `EveryRecord`, `EveryTransaction`, `Periodic`, `None`.
