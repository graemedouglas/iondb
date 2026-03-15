# iondb-buffer

**Layer**: Implementation
**Depends on**: `iondb-core`
**Depended on by**: `iondb-facade`

## Role

Buffer pool / page cache with configurable eviction policies, dirty-page tracking, and write-ahead integration.

## Key invariant

**No dirty page is flushed before its WAL record is synced** (write-ahead rule).

## Feature flags

| Flag | Effect |
|---|---|
| `buffer-pool` | Enable the buffer pool |
| `eviction-lru` | LRU eviction policy |
| `eviction-clock` | Clock eviction policy |

## Target tier compatibility

- **Tier 1**: Buffer pool with as few as 2 pages.
- **Tier 2/3**: Larger page cache with configurable size.

## Constraints

- `#![forbid(unsafe_code)]`.
- Must NOT depend on any other implementation crate.
- Pin/unpin semantics: pinned pages are never evicted.
- Dirty-page tracking for efficient checkpointing.
- Page count can be as small as 2 for Tier 1.
