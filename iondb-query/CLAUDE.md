# iondb-query

**Layer**: Implementation
**Depends on**: `iondb-core`
**Depended on by**: `iondb-facade`

## Role

LINQ-style embedded query DSL with zero-allocation query path for Tier 1 and richer heap-backed operations for higher tiers.

## Syntax styles

**Builder pattern** (method chaining):
```rust
db.query().from("sensors").where_(|r| r.field("temp").gt(30)).take(10)
```

**Macro syntax** (behind `query-macro` feature):
```rust
query! { from "sensors" where temp > 30 take 10 }
```

## Feature flags

| Flag | Effect |
|---|---|
| `query` | Enable query DSL |
| `query-alloc` | Heap operations: group-by, join (requires `alloc`) |
| `query-macro` | `query!{}` macro syntax (requires `query`) |

## Target tier compatibility

- **Tier 1**: Zero-allocation iterator-based queries.
- **Tier 2/3**: Full query DSL with heap operations.

## Constraints

- `#![forbid(unsafe_code)]`.
- Must NOT depend on any other implementation crate.
- Zero-allocation path verified via counting `#[global_allocator]` in tests.
- Predicate pushdown: filters at storage-engine level when possible.
- Macro syntax desugars to builder pattern at compile time.
