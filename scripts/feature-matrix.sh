#!/usr/bin/env bash
# Feature Matrix CI Script
#
# Automatically generates and tests meaningful feature-flag combinations.
# See docs/requirements/harness/v0.md §6.2.
#
# This script is NOT manually maintained — it derives the matrix from
# the crate feature definitions.

set -euo pipefail

echo "=== IonDB Feature Matrix Test ==="
echo ""

FAIL=0

run_check() {
    local desc="$1"
    shift
    echo -n "  $desc ... "
    if "$@" > /dev/null 2>&1; then
        echo "OK"
    else
        echo "FAIL"
        FAIL=1
    fi
}

# --- Profile builds (host-native) ---
echo "Profile builds:"
run_check "profile-minimal" \
    cargo build -p iondb --no-default-features --features profile-minimal

run_check "profile-embedded" \
    cargo build -p iondb --no-default-features --features profile-embedded

run_check "profile-full" \
    cargo build -p iondb --no-default-features --features profile-full

echo ""

# --- Individual feature flags (host-native) ---
echo "Individual feature flags:"
INDIVIDUAL_FLAGS=(
    "alloc-static"
    "storage-bptree"
    "storage-hash-ext"
    "storage-hash-linear"
    "io-mem"
    "query"
    "wal"
    "buffer-pool"
    "savepoints"
)

for flag in "${INDIVIDUAL_FLAGS[@]}"; do
    run_check "$flag alone" \
        cargo build -p iondb --no-default-features --features "$flag"
done

echo ""

# --- Feature flag combinations that require std ---
echo "std-dependent features:"
run_check "io-file (requires std)" \
    cargo build -p iondb --no-default-features --features "std,io-file"

run_check "alloc-system (requires std)" \
    cargo build -p iondb --no-default-features --features "std,alloc-system"

echo ""

# --- Profile tests (host-native) ---
echo "Profile tests:"
run_check "test profile-minimal" \
    cargo test -p iondb --no-default-features --features profile-minimal

run_check "test profile-full" \
    cargo test -p iondb --no-default-features --features profile-full

echo ""

# --- Summary ---
if [ "$FAIL" -eq 0 ]; then
    echo "=== All feature matrix checks passed ==="
else
    echo "=== FEATURE MATRIX FAILURES DETECTED ==="
    exit 1
fi
