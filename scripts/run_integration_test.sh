#!/usr/bin/env bash
set -euo pipefail

# Integration test that runs real ingestors and captures actual data

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

echo -e "${YELLOW}=== Sinex Real Data Integration Test ===${NC}"
echo "This test will run actual ingestors and analyze the captured data"
echo

# Test duration (how long to capture data)
TEST_DURATION="${TEST_DURATION:-30}"
DB_URL="${DATABASE_URL:-postgresql://sinex:sinex@localhost:5432/sinex}"

# Ensure database is ready
echo -n "Checking database... "
if psql "$DB_URL" -c "SELECT 1" &>/dev/null; then
    echo -e "${GREEN}OK${NC}"
else
    echo -e "${RED}FAILED${NC}"
    exit 1
fi

# Reset database for clean test
echo "Resetting database for test..."
./scripts/db_reset.sh

# Create a test marker event
TEST_RUN_ID=$(uuidgen)
echo "Test run ID: $TEST_RUN_ID"

# Function to start an ingestor
start_ingestor() {
    local name=$1
    local binary=$2
    shift 2
    local args=$@
    
    echo -e "${BLUE}Starting $name...${NC}"
    $binary $args > "/tmp/sinex_test_${name}.log" 2>&1 &
    local pid=$!
    echo "$pid" > "/tmp/sinex_test_${name}.pid"
    
    # Wait a bit to ensure it starts
    sleep 2
    
    if kill -0 $pid 2>/dev/null; then
        echo -e "  ${GREEN}✓ Started (PID: $pid)${NC}"
        return 0
    else
        echo -e "  ${RED}✗ Failed to start${NC}"
        cat "/tmp/sinex_test_${name}.log"
        return 1
    fi
}

# Function to stop an ingestor
stop_ingestor() {
    local name=$1
    local pidfile="/tmp/sinex_test_${name}.pid"
    
    if [ -f "$pidfile" ]; then
        local pid=$(cat "$pidfile")
        if kill -0 "$pid" 2>/dev/null; then
            echo -e "Stopping $name (PID: $pid)..."
            kill -TERM "$pid"
            sleep 1
            if kill -0 "$pid" 2>/dev/null; then
                kill -KILL "$pid"
            fi
        fi
        rm -f "$pidfile"
    fi
}

# Cleanup function
cleanup() {
    echo
    echo "Cleaning up..."
    stop_ingestor "filesystem"
    stop_ingestor "hyprland"
    stop_ingestor "kitty"
}

trap cleanup EXIT

# Start filesystem ingestor (watches /tmp/sinex_test directory)
TEST_DIR="/tmp/sinex_test_$$"
mkdir -p "$TEST_DIR"
echo "Test directory: $TEST_DIR"

# Check if we're in nix develop
if command -v cargo &>/dev/null; then
    BUILD_CMD="cargo run --package"
else
    BUILD_CMD="nix develop --command cargo run --package"
fi

# Start filesystem ingestor
start_ingestor "filesystem" \
    "$BUILD_CMD filesystem-ingestor --" \
    --watch-dir "$TEST_DIR" \
    --database-url "$DB_URL" || true

# Start hyprland ingestor if Hyprland is running
if [ -n "${HYPRLAND_INSTANCE_SIGNATURE:-}" ]; then
    start_ingestor "hyprland" \
        "$BUILD_CMD hyprland-ingestor --" \
        --database-url "$DB_URL" || true
else
    echo -e "${YELLOW}Skipping Hyprland ingestor (Hyprland not running)${NC}"
fi

# Start kitty ingestor if kitty is available
if command -v kitty &>/dev/null; then
    start_ingestor "kitty" \
        "$BUILD_CMD kitty-ingestor --" \
        --database-url "$DB_URL" || true
else
    echo -e "${YELLOW}Skipping Kitty ingestor (Kitty not installed)${NC}"
fi

echo
echo -e "${GREEN}Ingestors started. Generating test activity for $TEST_DURATION seconds...${NC}"
echo

# Generate filesystem events
echo "Generating filesystem events..."
for i in {1..5}; do
    # Create file
    echo "Test content $i" > "$TEST_DIR/test_file_$i.txt"
    sleep 1
    
    # Modify file
    echo "Modified content $i" >> "$TEST_DIR/test_file_$i.txt"
    sleep 1
    
    # Create directory
    mkdir -p "$TEST_DIR/test_dir_$i"
    
    # Rename file
    mv "$TEST_DIR/test_file_$i.txt" "$TEST_DIR/test_file_${i}_renamed.txt" 2>/dev/null || true
    sleep 1
done

# If Hyprland is running, generate some window events
if [ -n "${HYPRLAND_INSTANCE_SIGNATURE:-}" ]; then
    echo "Generating Hyprland events..."
    # Switch workspaces
    for i in {1..3}; do
        hyprctl dispatch workspace $i 2>/dev/null || true
        sleep 2
    done
fi

# Generate terminal commands (if in a supported terminal)
if [ -n "${KITTY_PID:-}" ]; then
    echo "Generating terminal events..."
    # These commands will be captured by kitty ingestor
    echo "Test command 1"
    ls -la >/dev/null
    echo "Test command 2"
    pwd >/dev/null
fi

echo
echo "Waiting for events to be processed..."
sleep 5

# Stop ingestors
cleanup
trap - EXIT

echo
echo -e "${BLUE}=== Analyzing Captured Data ===${NC}"
echo

# Count events by source
echo "1. Events captured by source:"
psql "$DB_URL" -t <<EOF
SELECT 
    source,
    COUNT(*) as event_count,
    COUNT(DISTINCT event_type) as event_types
FROM raw.events
GROUP BY source
ORDER BY event_count DESC;
EOF

echo
echo "2. Event type distribution:"
psql "$DB_URL" -t <<EOF
SELECT 
    source || ' / ' || event_type as event_type,
    COUNT(*) as count
FROM raw.events
GROUP BY source, event_type
ORDER BY source, event_type;
EOF

echo
echo "3. Running assumption diagnostics..."
./scripts/diagnose_assumptions.sh | grep -A 20 "Field Usage Patterns" || true

echo
echo "4. Testing validation against real data..."

# Export some events and test validation
cat > /tmp/test_real_events.sql <<'EOF'
\set QUIET on
\pset format unaligned
\pset tuples_only on
\pset fieldsep ','

SELECT 
    source,
    event_type,
    payload::text
FROM raw.events
LIMIT 10;
EOF

psql "$DB_URL" -f /tmp/test_real_events.sql | while IFS=',' read -r source event_type payload; do
    # Create a small Rust program to test validation
    cat > /tmp/test_validation.rs <<EOF
use sinex_shared::{EventValidator, ValidationError};
use serde_json::json;

fn main() {
    let validator = EventValidator::new();
    let payload = serde_json::from_str::<serde_json::Value>(r#"$payload"#).unwrap();
    
    match validator.validate("$source", "$event_type", &payload) {
        Ok(()) => println!("✓ Valid: $source / $event_type"),
        Err(e) => println!("✗ Invalid: $source / $event_type - {:?}", e),
    }
}
EOF
done

echo
echo "5. Checking for assumption mismatches..."

# Run the assumption mismatch detection
cat > /tmp/check_assumptions.sql <<'EOF'
WITH field_patterns AS (
    SELECT 
        source,
        event_type,
        jsonb_object_keys(payload) as field,
        COUNT(*) as occurrences
    FROM raw.events
    GROUP BY source, event_type, jsonb_object_keys(payload)
),
anomalies AS (
    SELECT 
        e.id::text as event_id,
        e.source,
        e.event_type,
        ARRAY(SELECT jsonb_object_keys(e.payload)) as fields,
        CASE 
            WHEN e.source = 'filesystem' AND e.payload ? 'window' THEN 'Has window field (Hyprland?)'
            WHEN e.source = 'hyprland' AND e.payload ? 'path' THEN 'Has path field (Filesystem?)'
            WHEN e.source = 'terminal.kitty' AND e.payload ? 'size' THEN 'Has size field (Filesystem?)'
            ELSE NULL
        END as potential_issue
    FROM raw.events e
)
SELECT * FROM anomalies WHERE potential_issue IS NOT NULL;
EOF

ANOMALIES=$(psql "$DB_URL" -t -f /tmp/check_assumptions.sql | wc -l)

if [ "$ANOMALIES" -gt 0 ]; then
    echo -e "${RED}Found $ANOMALIES potential assumption mismatches!${NC}"
    psql "$DB_URL" -f /tmp/check_assumptions.sql
else
    echo -e "${GREEN}No assumption mismatches detected${NC}"
fi

echo
echo -e "${GREEN}=== Test Complete ===${NC}"
echo
echo "Summary:"
echo "- Test duration: $TEST_DURATION seconds"
echo "- Events captured: $(psql -t -c 'SELECT COUNT(*) FROM raw.events' $DB_URL)"
echo "- Anomalies found: $ANOMALIES"
echo
echo "Ingestor logs saved in: /tmp/sinex_test_*.log"

# Cleanup
rm -rf "$TEST_DIR"
rm -f /tmp/test_*.sql /tmp/test_validation.rs