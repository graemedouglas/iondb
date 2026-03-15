# iondb-tx

**Layer**: Cross-cutting integration
**Depends on**: `iondb-core`
**Depended on by**: `iondb-facade`

## Role

Transaction manager, MVCC isolation, and savepoints. This is the cross-cutting integration crate — it coordinates across storage, WAL, and buffer pool via their `iondb-core` traits.

## Key patterns

**Typestate pattern** for transaction lifecycle:
```
Active → Committed
Active → RolledBack
```
Enforced at compile time — you cannot call `put()` on a committed transaction.

## Feature flags

| Flag | Effect |
|---|---|
| `acid` | Full ACID transaction support |
| `concurrency` | MVCC with SSI (requires `alloc`) |
| `savepoints` | Nested transactions via savepoints |

## Target tier compatibility

- **Tier 1**: Sequential transactions (no MVCC).
- **Tier 2**: Transactions with optional concurrency.
- **Tier 3**: Full MVCC with concurrency.

## Constraints

- `#![forbid(unsafe_code)]`.
- Depends ONLY on `iondb-core` — not on implementation crates.
- Cross-crate integration happens here, not in impl crates.
- Auto-rollback on drop if not explicitly committed.
- Use Loom for thread interleaving tests when `concurrency` is enabled.
