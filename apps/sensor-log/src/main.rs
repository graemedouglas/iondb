//! # sensor-log — Tier 1 Dogfood Application
//!
//! A bare-metal `no_std` firmware skeleton that logs timestamped sensor
//! readings to `IonDB` on a Cortex-M0 target.
//!
//! ## Validation focus
//!
//! - `no_std` compilation
//! - Zero-heap operation
//! - Static memory budget compliance
//! - Binary size < 32 KB
//!
//! ## Current status
//!
//! Phase 0 skeleton. Compiles as a host-native binary for CI validation.
//! Will be converted to a `no_std` entry point when the runtime harness
//! is configured.

fn main() {
    // Phase 0 skeleton: validates that the app compiles and links.
    // Will be replaced with no_std entry point + StorageEngine usage.
}

#[cfg(test)]
mod tests {
    #[test]
    fn skeleton_compiles() {
        // Validates the dogfood app compiles in CI.
    }
}
