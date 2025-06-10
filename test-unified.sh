#!/usr/bin/env bash
set -euo pipefail

echo "Testing unified collector in dry-run mode..."

# Create a test config
cat > test-unified.toml << 'EOF'
enabled_events = [
    "file.created",
    "file.modified",
    "command.executed",
    "window.focused"
]

[event.files]
watch_patterns = ["./test-data/**/*"]
ignore_patterns = ["*.tmp"]
debounce_ms = 50

[logging]
level = "debug"
EOF

# Create test directory
mkdir -p test-data

# Run in dry-run mode
echo "Starting unified collector..."
timeout 10s cargo run --package unified-collector -- --dry-run --config test-unified.toml || true

# Create some test files to trigger events
echo "Creating test files..."
echo "test content" > test-data/test1.txt
echo "more content" > test-data/test2.txt
sleep 1
echo "modified" >> test-data/test1.txt

# Run again to see events
echo "Running collector to capture events..."
timeout 5s cargo run --package unified-collector -- --dry-run --config test-unified.toml || true

# Cleanup
rm -rf test-data test-unified.toml

echo "Test complete!"