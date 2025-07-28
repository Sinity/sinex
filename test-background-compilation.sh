#!/usr/bin/env bash
# Test background compilation benefits

echo "=== Test 1: Cold check (no background process) ==="
pkill -f "cargo watch" 2>/dev/null || true
sleep 1
echo '// Change 1' >> crate/sinex-utils/src/lib.rs
time cargo check --workspace 2>&1 | grep Finished

echo -e "\n=== Test 2: With cargo watch running in background ==="
# Start cargo watch in background
cargo watch -x "check --workspace" &> /dev/null &
WATCH_PID=$!
sleep 3  # Let it do initial check

echo '// Change 2' >> crate/sinex-utils/src/lib.rs
sleep 2  # Let watch detect and start checking

# Now run our check - should be faster as watch already started
time cargo check --workspace 2>&1 | grep Finished

# Cleanup
kill $WATCH_PID 2>/dev/null
git checkout -- crate/sinex-utils/src/lib.rs