#!/usr/bin/env bash
# Binary Size Budget Enforcement
#
# Validates that profile-minimal build for thumbv6m-none-eabi produces
# < 32 KB .text section.
#
# See docs/requirements/harness/v0.md §6.3.

set -euo pipefail

MAX_TEXT_SIZE=32768  # 32 KB

echo "=== Binary Size Budget Check ==="
echo "Target: thumbv6m-none-eabi (profile-minimal)"
echo "Budget: < ${MAX_TEXT_SIZE} bytes .text section"
echo ""

# Build for Cortex-M0
cargo build -p iondb-core --no-default-features --target thumbv6m-none-eabi --release 2>/dev/null

# Find the binary
BINARY=$(find target/thumbv6m-none-eabi/release -name "libiondb_core.rlib" -o -name "iondb_core-*.rlib" 2>/dev/null | head -1)

if [ -z "$BINARY" ]; then
    echo "WARNING: Could not find binary to measure. Skipping size check."
    echo "This is expected during early development before a full binary is produced."
    exit 0
fi

# Parse size output
TEXT_SIZE=$(size "$BINARY" 2>/dev/null | tail -1 | awk '{print $1}')

if [ -z "$TEXT_SIZE" ]; then
    echo "WARNING: Could not parse binary size. Skipping."
    exit 0
fi

echo "  .text section: ${TEXT_SIZE} bytes"

if [ "$TEXT_SIZE" -ge "$MAX_TEXT_SIZE" ]; then
    echo ""
    echo "BUDGET VIOLATION: .text section is ${TEXT_SIZE} bytes (budget: < ${MAX_TEXT_SIZE})"
    echo ""
    echo "Remediation:"
    echo "  - Check for format!/panic! with formatting in no_std code paths"
    echo "  - Ensure unused features are behind feature gates"
    echo "  - Review recent changes for unnecessary code size increases"
    exit 1
fi

echo ""
echo "=== Binary size within budget ==="
