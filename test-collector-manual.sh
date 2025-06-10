#!/usr/bin/env bash
set -euo pipefail

echo "Testing unified collector file watching..."

# Create test config
cat > test-unified.toml << 'EOF'
enabled_events = [
    "file.created",
    "file.modified",
    "file.deleted"
]

[event.files]
watch_patterns = ["./test-watch/**/*"]
ignore_patterns = ["*.tmp", "*.log"]
debounce_ms = 100

[logging]
level = "debug"
EOF

# Create test directory
mkdir -p test-watch

echo "Starting unified collector in background..."
cargo run --package unified-collector -- --dry-run --config test-unified.toml 2>&1 | grep -E "(file\.|Event generated|Watching)" &
COLLECTOR_PID=$!

# Give it time to start
sleep 3

echo "Creating test files..."
echo "content1" > test-watch/file1.txt
echo "content2" > test-watch/file2.txt
sleep 1

echo "Modifying files..."
echo "modified" >> test-watch/file1.txt
sleep 1

echo "Deleting a file..."
rm test-watch/file2.txt
sleep 1

echo "Creating nested directory..."
mkdir -p test-watch/subdir
echo "nested" > test-watch/subdir/nested.txt

# Let collector run for a bit to capture events
sleep 3

# Stop collector
kill $COLLECTOR_PID 2>/dev/null || true

# Cleanup
rm -rf test-watch test-unified.toml

echo "Test complete!"