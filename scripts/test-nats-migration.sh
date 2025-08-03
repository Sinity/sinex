#!/usr/bin/env bash
# Test script for NATS migration

set -euo pipefail

echo "=== Testing NATS Migration for Satellites ==="

# Check if NATS is running
if ! nc -z localhost 4222 2>/dev/null; then
    echo "❌ NATS is not running on localhost:4222"
    echo "   Please start NATS with: nats-server -js"
    exit 1
fi

echo "✅ NATS is running"

# Set environment variables
export DATABASE_URL="${DATABASE_URL:-postgresql://localhost/sinex_dev}"
export SINEX_USE_NATS=true
export SINEX_NATS_SERVERS="nats://localhost:4222"

# Test building with NATS support
echo ""
echo "=== Building fs-watcher with NATS support ==="
cargo build --bin sinex-fs-watcher

# Create a test directory
TEST_DIR="/tmp/sinex-nats-test"
mkdir -p "$TEST_DIR"

echo ""
echo "=== Testing fs-watcher with NATS (default) ==="
echo "Starting fs-watcher in scan mode with NATS (now default)..."

# Run fs-watcher in scan mode with NATS (default)
timeout 10s cargo run --bin sinex-fs-watcher -- \
    scan \
    --from none \
    --until snapshot \
    "$TEST_DIR" || true

echo ""
echo "=== Testing fs-watcher with gRPC (legacy) ==="
echo "Starting fs-watcher in scan mode with legacy gRPC..."

# Run fs-watcher in scan mode with gRPC (legacy)
timeout 10s cargo run --bin sinex-fs-watcher -- \
    --use-grpc \
    scan \
    --dry-run \
    --from none \
    --until snapshot \
    "$TEST_DIR" || true

echo ""
echo "=== NATS Migration Test Complete ==="
echo ""
echo "Next steps:"
echo "1. Monitor NATS for published events: nats sub 'events.>'"
echo "2. Check if events are being published directly to NATS"
echo "3. Verify gRPC mode still works for backwards compatibility"