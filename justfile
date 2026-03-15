# IonDB Development Harness — Justfile
#
# All development commands for building, testing, linting, and verifying IonDB.
# Run `just --list` to see all available commands.

# Default: run full inner-loop check
default: check

# === Inner Loop (Agent Session) ===

# Run all inner-loop quality gates
check: fmt-check clippy test doc-check

# Compile (host-native)
build:
    cargo build --workspace

# Compile (no_std — Cortex-M0, the ultimate architectural constraint)
build-nostd:
    cargo build -p iondb-core --no-default-features --target thumbv6m-none-eabi
    cargo build -p iondb-alloc --no-default-features --target thumbv6m-none-eabi
    cargo build -p iondb-storage --no-default-features --target thumbv6m-none-eabi
    cargo build -p iondb-wal --no-default-features --target thumbv6m-none-eabi
    cargo build -p iondb-buffer --no-default-features --target thumbv6m-none-eabi

# Run all tests (host-native)
test:
    cargo test --workspace

# Run structural tests only
test-structural:
    cargo test -p iondb-structural-tests

# Run Clippy with all targets and features, warnings as errors
clippy:
    cargo clippy --workspace --all-targets -- -D warnings

# Check formatting
fmt-check:
    cargo fmt --all -- --check

# Auto-format
fmt:
    cargo fmt --all

# Check documentation (warnings as errors)
doc-check:
    RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps

# Generate documentation
doc:
    cargo doc --workspace --no-deps --open

# === Middle Loop (PR / CI) ===

# Run the full CI verification stack
ci: check build-nostd test-structural feature-matrix

# Run feature matrix tests
feature-matrix:
    @echo "Running feature matrix..."
    ./scripts/feature-matrix.sh

# Check binary size budget for profile-minimal
check-binary-size:
    ./scripts/check-binary-size.sh

# Check RAM budget
check-ram-budget:
    ./scripts/check-ram-budget.sh

# === Specific Crate Commands ===

# Run tests for a specific crate
test-crate crate:
    cargo test -p {{crate}}

# Run Miri on iondb-alloc (unsafe code UB detection)
miri:
    cargo +nightly miri test -p iondb-alloc

# === Dogfood Apps ===

# Build all dogfood applications
build-apps:
    cargo build -p sensor-log
    cargo build -p edge-config
    cargo build -p fleet-telemetry

# Test all dogfood applications
test-apps:
    cargo test -p sensor-log
    cargo test -p edge-config
    cargo test -p fleet-telemetry

# === Maintenance ===

# Clean build artifacts
clean:
    cargo clean

# Update dependencies
update:
    cargo update
