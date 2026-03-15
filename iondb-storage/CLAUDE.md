# iondb-storage

**Layer**: Implementation
**Depends on**: `iondb-core`
**Depended on by**: `iondb-facade`

## Role

Storage engine implementations: B+ tree, extendible hash, and linear hash. All implement `iondb_core::StorageEngine`.

## Engines

| Engine | Feature | Description |
|---|---|---|
| B+ tree | `storage-bptree` | Balanced tree for sequential/range queries. Default. |
| Extendible hash | `storage-hash-ext` | Directory-based, split-on-overflow. |
| Linear hash | `storage-hash-linear` | Deterministic split order, controlled load factor. |

## Feature flags

| Flag | Effect |
|---|---|
| `storage-bptree` | B+ tree engine (default) |
| `storage-hash-ext` | Extendible hashing engine |
| `storage-hash-linear` | Linear hashing engine |

## Target tier compatibility

- **Tier 1**: B+ tree with static allocation.
- **Tier 2/3**: All engines available.

## Constraints

- `#![forbid(unsafe_code)]` — no unsafe code.
- Must NOT depend on any other implementation crate.
- Configurable page size (powers of 2, minimum 64 bytes).
- All structural invariants (tree balance, hash load factor) must be testable.
