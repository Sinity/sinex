#!/usr/bin/env bash
# Script to test kitty ingestor in dry-run mode

set -euo pipefail

cd "$(dirname "$0")/.."

echo "Testing Kitty ingestor in dry-run mode..."
echo "This will log events to stdout instead of database"
echo ""

# Run in dry-run mode with debug logging
RUST_LOG=info,kitty_ingestor=debug,sinex_shared=debug \
cargo run -p kitty-ingestor -- --dry-run

# Example with file output
# cargo run -p kitty-ingestor -- --output-file /tmp/kitty-events.json