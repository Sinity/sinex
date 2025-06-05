#!/usr/bin/env bash
set -euo pipefail

# Ephemeral test environment - complete system setup with real-time monitoring

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m'

TEST_ID="sinex_test_$(date +%s)"
TEST_DIR="/tmp/${TEST_ID}"
DB_PORT=$((5433 + RANDOM % 1000))
TEST_DB_URL="postgresql://sinex_test:testpass@localhost:${DB_PORT}/sinex_test"

echo -e "${CYAN}=== Sinex Ephemeral Test Environment ===${NC}"
echo "Test ID: $TEST_ID"
echo "Database Port: $DB_PORT"
echo "Test Directory: $TEST_DIR"
echo

cleanup() {
    echo -e "\n${YELLOW}🧹 Cleaning up...${NC}"
    
    # Stop all background processes
    jobs -p | xargs -r kill 2>/dev/null || true
    
    # Stop test database
    if [[ -n "${POSTGRES_PID:-}" ]]; then
        kill $POSTGRES_PID 2>/dev/null || true
        wait $POSTGRES_PID 2>/dev/null || true
    fi
    
    # Cleanup directories
    rm -rf "$TEST_DIR" 2>/dev/null || true
    
    echo "Cleanup complete"
}

trap cleanup EXIT INT TERM

# Setup test environment
setup_environment() {
    echo -e "${BLUE}🏗️  Setting up ephemeral environment...${NC}"
    
    mkdir -p "$TEST_DIR"/{data,logs,watch}
    
    # Start ephemeral PostgreSQL instance
    echo "Starting test database on port $DB_PORT..."
    
    if command -v pg_ctl >/dev/null; then
        initdb -D "$TEST_DIR/data" --auth-local=trust --auth-host=md5 -U sinex_test
        
        # Configure test database
        cat >> "$TEST_DIR/data/postgresql.conf" <<EOF
port = $DB_PORT
unix_socket_directories = '$TEST_DIR'
logging_collector = on
log_directory = '$TEST_DIR/logs'
log_filename = 'postgresql.log'
log_statement = 'all'
shared_preload_libraries = 'timescaledb'
EOF
        
        # Start PostgreSQL
        pg_ctl -D "$TEST_DIR/data" -l "$TEST_DIR/logs/postgres.log" start
        POSTGRES_PID=$(cat "$TEST_DIR/data/postmaster.pid" | head -1)
        
        # Wait for startup
        sleep 3
        
        # Create database and user
        createdb -h localhost -p $DB_PORT -U sinex_test sinex_test
        
        echo "✅ Database started (PID: $POSTGRES_PID)"
    else
        echo "❌ PostgreSQL not available"
        exit 1
    fi
}

# Run migrations
setup_schema() {
    echo -e "${BLUE}📋 Setting up schema...${NC}"
    
    export DATABASE_URL="$TEST_DB_URL"
    
    if command -v sqlx >/dev/null; then
        sqlx migrate run
        echo "✅ Migrations applied"
    else
        echo "❌ sqlx not available"
        exit 1
    fi
}

# Start real-time monitoring
start_monitoring() {
    echo -e "${BLUE}📊 Starting real-time monitoring...${NC}"
    
    # Event counter in background
    (
        while true; do
            if psql "$TEST_DB_URL" -c "\q" 2>/dev/null; then
                COUNT=$(psql "$TEST_DB_URL" -t -c "SELECT COUNT(*) FROM raw.events" 2>/dev/null | tr -d ' ')
                printf "\r${CYAN}📈 Events captured: ${COUNT:-0}${NC}"
            fi
            sleep 1
        done
    ) &
    
    # Detailed event stream in background
    (
        sleep 2
        echo -e "\n${YELLOW}📡 Live Event Stream:${NC}"
        echo "────────────────────────────────────────"
        
        while true; do
            if psql "$TEST_DB_URL" -c "\q" 2>/dev/null; then
                # Get latest events
                psql "$TEST_DB_URL" -t -c "
                SELECT 
                    '🔵 ' || source || ' → ' || event_type || 
                    ' (' || extract(epoch from (now() - ts_ingest))::int || 's ago)' ||
                    CASE 
                        WHEN payload ? 'path' THEN ' path: ' || (payload->>'path')
                        WHEN payload ? 'command' THEN ' cmd: ' || (payload->>'command') 
                        WHEN payload ? 'window' THEN ' window: ' || (payload->>'window')
                        ELSE ''
                    END
                FROM raw.events 
                WHERE ts_ingest > now() - interval '5 seconds'
                ORDER BY ts_ingest DESC
                LIMIT 3
                " 2>/dev/null | grep -v '^$' || true
            fi
            sleep 2
        done
    ) &
}

# Start filesystem ingestor
start_filesystem_ingestor() {
    echo -e "${BLUE}📁 Starting filesystem ingestor...${NC}"
    
    RUST_LOG=info cargo run --package filesystem-ingestor -- \
        --watch-dir "$TEST_DIR/watch" \
        --database-url "$TEST_DB_URL" \
        > "$TEST_DIR/logs/filesystem.log" 2>&1 &
    
    local pid=$!
    echo "✅ Filesystem ingestor started (PID: $pid)"
    
    # Give it time to start
    sleep 2
}

# Generate filesystem activity
generate_filesystem_activity() {
    echo -e "\n${YELLOW}🎬 Generating filesystem activity...${NC}"
    
    local watch_dir="$TEST_DIR/watch"
    
    # Create files with realistic content
    echo "Creating files..."
    for i in {1..5}; do
        echo "Test document $i - $(date)" > "$watch_dir/document_$i.txt"
        sleep 0.5
        
        # Create and modify
        echo "Additional content" >> "$watch_dir/document_$i.txt"
        sleep 0.5
        
        # Create subdirectory
        mkdir -p "$watch_dir/folder_$i"
        echo "Nested file" > "$watch_dir/folder_$i/nested.txt"
        sleep 0.5
    done
    
    # Simulate real work patterns
    echo "Simulating real work patterns..."
    cp "$watch_dir/document_1.txt" "$watch_dir/backup_1.txt"
    mv "$watch_dir/document_2.txt" "$watch_dir/renamed_2.txt"
    rm "$watch_dir/document_3.txt"
    
    # Binary file
    dd if=/dev/urandom of="$watch_dir/binary_file.dat" bs=1024 count=10 2>/dev/null
    
    echo "✅ Filesystem activity generated"
}

# Analyze captured events
analyze_events() {
    echo -e "\n${BLUE}🔍 Analyzing captured events...${NC}"
    
    # Wait for events to be processed
    sleep 2
    
    echo "Event Summary:"
    echo "─────────────"
    psql "$TEST_DB_URL" -c "
    SELECT 
        source,
        event_type,
        COUNT(*) as count,
        MIN(ts_ingest) as first_event,
        MAX(ts_ingest) as last_event
    FROM raw.events
    GROUP BY source, event_type
    ORDER BY source, event_type;
    "
    
    echo -e "\nField Analysis:"
    echo "──────────────"
    psql "$TEST_DB_URL" -c "
    WITH field_usage AS (
        SELECT 
            source,
            event_type,
            jsonb_object_keys(payload) as field,
            COUNT(*) as usage
        FROM raw.events
        GROUP BY source, event_type, jsonb_object_keys(payload)
    )
    SELECT 
        source || ' / ' || event_type as event_type,
        string_agg(field || '(' || usage || ')', ', ' ORDER BY usage DESC) as fields
    FROM field_usage
    GROUP BY source, event_type
    ORDER BY source, event_type;
    "
    
    echo -e "\nValidation Check:"
    echo "────────────────"
    
    # Test validation against real events
    INVALID_EVENTS=$(psql "$TEST_DB_URL" -t -c "
    SELECT COUNT(*) FROM raw.events WHERE 
        (source = 'filesystem' AND NOT (payload ? 'path'))
        OR (source = 'filesystem' AND jsonb_typeof(payload->'size') != 'number')
    " | tr -d ' ')
    
    if [[ "$INVALID_EVENTS" -eq 0 ]]; then
        echo "✅ All events pass basic validation"
    else
        echo "❌ Found $INVALID_EVENTS invalid events"
    fi
    
    # Check for expected filesystem fields
    echo -e "\nField Validation:"
    psql "$TEST_DB_URL" -c "
    SELECT 
        COUNT(*) FILTER (WHERE payload ? 'path') as events_with_path,
        COUNT(*) FILTER (WHERE payload ? 'size') as events_with_size,
        COUNT(*) FILTER (WHERE payload ? 'permissions') as events_with_permissions,
        COUNT(*) as total_events
    FROM raw.events
    WHERE source = 'filesystem';
    "
}

# Performance analysis
analyze_performance() {
    echo -e "\n${BLUE}⚡ Performance Analysis:${NC}"
    
    local total_events=$(psql "$TEST_DB_URL" -t -c "SELECT COUNT(*) FROM raw.events" | tr -d ' ')
    local duration=$(psql "$TEST_DB_URL" -t -c "
        SELECT EXTRACT(EPOCH FROM (MAX(ts_ingest) - MIN(ts_ingest)))::int 
        FROM raw.events
    " | tr -d ' ')
    
    if [[ "$duration" -gt 0 ]]; then
        local events_per_sec=$((total_events / duration))
        echo "📊 Captured $total_events events in ${duration}s (${events_per_sec} events/sec)"
    else
        echo "📊 Captured $total_events events"
    fi
    
    # Check for any processing delays
    psql "$TEST_DB_URL" -c "
    SELECT 
        source,
        event_type,
        AVG(EXTRACT(EPOCH FROM (ts_ingest - ts_orig)))::numeric(5,2) as avg_delay_seconds
    FROM raw.events 
    WHERE ts_orig IS NOT NULL
    GROUP BY source, event_type;
    "
}

# Main execution
main() {
    setup_environment
    setup_schema
    start_monitoring
    start_filesystem_ingestor
    
    echo -e "\n${GREEN}🚀 System is running! Watch the live feed above...${NC}"
    echo "Press Ctrl+C to stop and analyze results"
    echo
    
    # Generate activity
    generate_filesystem_activity
    
    # Let it run for a bit to capture events
    echo -e "\n${YELLOW}⏱️  Letting system run for 10 seconds to capture events...${NC}"
    sleep 10
    
    # Analysis
    analyze_events
    analyze_performance
    
    echo -e "\n${GREEN}✅ Test completed successfully!${NC}"
    echo "Logs available in: $TEST_DIR/logs/"
    echo "Database was: $TEST_DB_URL"
}

main "$@"