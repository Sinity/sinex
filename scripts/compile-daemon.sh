#!/usr/bin/env bash
# Lightweight compilation daemon that runs cargo check continuously
# Logs all results in machine-readable format

set -euo pipefail

COMPILE_LOG_DIR="${COMPILE_LOG_DIR:-$HOME/.sinex-compile-logs}"
STATE_FILE="$COMPILE_LOG_DIR/daemon.state"
LOG_FILE="$COMPILE_LOG_DIR/daemon.log"
LAST_RESULT="$COMPILE_LOG_DIR/last-result.json"

mkdir -p "$COMPILE_LOG_DIR"

# Write daemon state
write_state() {
    echo "{\"pid\":$$,\"status\":\"$1\",\"timestamp\":\"$(date -u +%Y-%m-%dT%H:%M:%SZ)\"}" > "$STATE_FILE"
}

# Run compilation and save results
compile_once() {
    local start_time=$(date +%s)
    local errors=0
    local warnings=0
    local status="success"
    
    # Run cargo check with JSON output
    local result_file="$COMPILE_LOG_DIR/compile-$(date +%Y%m%d_%H%M%S).json"
    
    if ! cargo check --workspace --message-format json > "$result_file" 2>&1; then
        status="failed"
    fi
    
    # Count errors and warnings
    errors=$(grep '"level":"error"' "$result_file" 2>/dev/null | wc -l || echo 0)
    warnings=$(grep '"level":"warning"' "$result_file" 2>/dev/null | wc -l || echo 0)
    
    local end_time=$(date +%s)
    local duration=$((end_time - start_time))
    
    # Write summary
    cat > "$LAST_RESULT" << EOF
{
    "timestamp": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
    "status": "$status",
    "duration": $duration,
    "errors": $errors,
    "warnings": $warnings,
    "log": "$result_file"
}
EOF
    
    # Log event
    echo "{\"timestamp\":\"$(date -u +%Y-%m-%dT%H:%M:%SZ)\",\"event\":\"compile\",\"status\":\"$status\",\"duration\":$duration,\"errors\":$errors,\"warnings\":$warnings}" >> "$LOG_FILE"
}

# Main daemon loop
daemon_loop() {
    write_state "running"
    echo "Compilation daemon started (PID: $$)" >&2
    echo "Logs: $COMPILE_LOG_DIR" >&2
    
    # Track if we need to recompile
    local needs_compile=true
    local last_change=0
    
    # Initial compilation
    compile_once
    needs_compile=false
    
    # Watch for file changes and recompile
    while true; do
        # Use inotifywait if available, otherwise poll
        if command -v inotifywait >/dev/null 2>&1; then
            # Wait for Rust file changes (with timeout to check for pending compiles)
            if inotifywait -q -r -e modify,create,delete \
                --include '.*\.(rs|toml)$' \
                --exclude '(target/|\.git/)' \
                --timeout 2 \
                . 2>/dev/null; then
                # File changed
                needs_compile=true
                last_change=$(date +%s)
            fi
        else
            # Simple polling fallback
            sleep 2
            # Check if any files changed (simple approach)
            if find . -name "*.rs" -o -name "Cargo.toml" -newer "$LAST_RESULT" 2>/dev/null | grep -q .; then
                needs_compile=true
                last_change=$(date +%s)
            fi
        fi
        
        # If we need to compile and enough time has passed since last change (debounce)
        if [ "$needs_compile" = true ]; then
            local now=$(date +%s)
            local elapsed=$((now - last_change))
            
            # Wait at least 1 second after last change before compiling
            if [ $elapsed -ge 1 ]; then
                # Mark that we're compiling
                echo "{\"timestamp\":\"$(date -u +%Y-%m-%dT%H:%M:%SZ)\",\"status\":\"compiling\",\"message\":\"Compilation started...\"}" > "$LAST_RESULT"
                
                # Compile
                compile_once
                needs_compile=false
                
                # Note: If files changed during compilation, inotifywait will catch them
                # and set needs_compile=true again
            fi
        fi
    done
}

# Start daemon (idempotent)
start() {
    # Check if already running
    if [ -f "$STATE_FILE" ]; then
        local pid=$(jq -r '.pid' "$STATE_FILE" 2>/dev/null || echo "")
        if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
            echo "Daemon already running (PID: $pid)"
            return 0  # Success - daemon is running (idempotent)
        else
            # Stale state file, clean it up
            rm -f "$STATE_FILE"
        fi
    fi
    
    # Ensure we're in the project directory
    if [ ! -f "Cargo.toml" ]; then
        echo "Error: Not in a Rust project directory"
        return 1
    fi
    
    # Start in background
    nohup "$0" daemon > /dev/null 2>&1 &
    local new_pid=$!
    
    # Wait a moment to ensure it started
    sleep 1
    if kill -0 "$new_pid" 2>/dev/null; then
        echo "Started compilation daemon (PID: $new_pid)"
        return 0
    else
        echo "Failed to start daemon"
        return 1
    fi
}

# Stop daemon
stop() {
    if [ -f "$STATE_FILE" ]; then
        local pid=$(jq -r '.pid' "$STATE_FILE" 2>/dev/null || echo "")
        if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
            kill "$pid"
            echo "Stopped daemon (PID: $pid)"
            rm -f "$STATE_FILE"
        else
            echo "Daemon not running"
        fi
    else
        echo "No daemon state found"
    fi
}

# Get daemon status
status() {
    if [ -f "$STATE_FILE" ]; then
        local state=$(cat "$STATE_FILE")
        local pid=$(echo "$state" | jq -r '.pid')
        
        if kill -0 "$pid" 2>/dev/null; then
            # Just return simple running status
            echo '{"status":"running","pid":'$pid'}'
        else
            echo '{"status":"dead","state_file":"stale"}'
        fi
    else
        echo '{"status":"not_running"}'
    fi
}

# Get last compilation result
last() {
    if [ -f "$LAST_RESULT" ]; then
        cat "$LAST_RESULT"
    else
        echo '{"error":"No compilation results found"}'
    fi
}

# Main command
case "${1:-help}" in
    start)   start ;;
    stop)    stop ;;
    status)  status ;;
    last)    last ;;
    daemon)  daemon_loop ;;
    *)
        cat << EOF
compile-daemon.sh - Background compilation with logging

Commands:
  start   - Start compilation daemon
  stop    - Stop compilation daemon  
  status  - Show daemon status
  last    - Get last compilation result

The daemon:
- Runs cargo check continuously in background
- Watches for file changes (or polls every 5s)
- Logs all results to $COMPILE_LOG_DIR
- Keeps last result in JSON format

Example:
  ./compile-daemon.sh start
  # ... edit files ...
  ./compile-daemon.sh last
  # {"status":"failed","errors":3,"warnings":1,...}
EOF
        ;;
esac