//! # edge-config — Tier 2 Dogfood Application
//!
//! A `no_std + alloc` application skeleton for an ESP32-class target that
//! stores and retrieves device configuration key-value pairs with
//! transactional updates.
//!
//! ## Validation focus
//!
//! - Transactions, WAL recovery after simulated crash
//! - Hash table for O(1) config lookups
//! - Heap usage within budget
//!
//! ## Current status
//!
//! Phase 0 skeleton. Will evolve through Phase 3 (transactions).

fn main() {
    // Phase 1 skeleton: validates compilation and linking.
}

#[cfg(test)]
mod tests {
    #[test]
    fn skeleton_compiles() {
        // Validates the dogfood app compiles in CI.
    }
}
