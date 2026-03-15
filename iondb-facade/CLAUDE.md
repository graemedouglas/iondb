# iondb (facade)

**Layer**: Facade (top of the dependency tree)
**Depends on**: All implementation crates
**Published as**: `iondb` on crates.io

## Role

Re-exports all workspace crates through a single dependency. Users add `iondb` to their `Cargo.toml` with a profile feature to get the right set of functionality.

## Feature profiles

| Profile | Target | Flags |
|---|---|---|
| `profile-minimal` | Tier 1 (Cortex-M0) | `alloc-static`, `storage-bptree`, `io-mem` |
| `profile-embedded` | Tier 2 (ESP32) | alloc, bump, all storage, `io-raw`, WAL, buffer pool, query |
| `profile-full` | Tier 3 (Linux) | Everything |

## Constraints

- `#![forbid(unsafe_code)]` — facade has no logic, only re-exports.
- Feature flags propagate to the appropriate sub-crates.
- This is the only crate (along with `iondb-tx`) that may depend on multiple impl crates.
