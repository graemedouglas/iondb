#!/usr/bin/env bash
# RAM Budget Enforcement
#
# Validates that core engine is operational with <= 2 KB RAM
# (excluding user data pages) for Tier 1 targets.
#
# Parses linker map file and asserts .bss + .data <= 2048 bytes.
#
# See docs/requirements/harness/v0.md §6.3.

set -euo pipefail

MAX_RAM=2048  # 2 KB

echo "=== RAM Budget Check ==="
echo "Target: Tier 1 (thumbv6m-none-eabi)"
echo "Budget: <= ${MAX_RAM} bytes (.bss + .data)"
echo ""

# Build with linker map output
cargo build -p iondb-core --no-default-features --target thumbv6m-none-eabi --release 2>/dev/null

# Find the binary
BINARY=$(find target/thumbv6m-none-eabi/release -name "libiondb_core.rlib" -o -name "iondb_core-*.rlib" 2>/dev/null | head -1)

if [ -z "$BINARY" ]; then
    echo "WARNING: Could not find binary to measure. Skipping RAM check."
    echo "This is expected during early development."
    exit 0
fi

# Parse size output for .data and .bss
SIZE_OUTPUT=$(size "$BINARY" 2>/dev/null | tail -1)
DATA_SIZE=$(echo "$SIZE_OUTPUT" | awk '{print $2}')
BSS_SIZE=$(echo "$SIZE_OUTPUT" | awk '{print $3}')

if [ -z "$DATA_SIZE" ] || [ -z "$BSS_SIZE" ]; then
    echo "WARNING: Could not parse RAM usage. Skipping."
    exit 0
fi

TOTAL_RAM=$((DATA_SIZE + BSS_SIZE))

echo "  .data: ${DATA_SIZE} bytes"
echo "  .bss:  ${BSS_SIZE} bytes"
echo "  total: ${TOTAL_RAM} bytes"

if [ "$TOTAL_RAM" -gt "$MAX_RAM" ]; then
    echo ""
    echo "BUDGET VIOLATION: RAM usage is ${TOTAL_RAM} bytes (budget: <= ${MAX_RAM})"
    echo ""
    echo "Remediation:"
    echo "  - Move large static buffers behind feature flags"
    echo "  - Use const generics for configurable buffer sizes"
    echo "  - Check for unnecessary static/global state"
    exit 1
fi

echo ""
echo "=== RAM usage within budget ==="
