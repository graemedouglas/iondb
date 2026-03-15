//! # fleet-telemetry — Tier 3 Dogfood Application
//!
//! A `std` Linux application skeleton that ingests telemetry from multiple
//! simulated devices, stores it in `IonDB`, and serves queries.
//!
//! ## Validation focus
//!
//! - Concurrent writes with MVCC isolation
//! - Query DSL under realistic data volumes
//! - `io-file` backend with crash recovery
//! - Serde round-trip
//!
//! ## Current status
//!
//! Phase 0 skeleton. Will evolve through Phase 6 (hardening).

fn main() {
    // Phase 3 skeleton: validates compilation and linking.
}

#[cfg(test)]
mod tests {
    #[test]
    fn skeleton_compiles() {
        // Validates the dogfood app compiles in CI.
    }
}
