#!/usr/bin/env bash
# Lightweight compilation daemon - focuses on real-time status
# Analytics are handled by compile-analytics.sh via cargo wrapper

set -euo pipefail

COMPILE_STATE_DIR="${SINEX_COMPILE_STATE:-$HOME/.sinex-compile-state}"
STATE_FILE="$COMPILE_STATE_DIR/daemon.state"
CURRENT_BUILD="$COMPILE_STATE_DIR/current-build.json"
LAST_COMPLETE="$COMPILE_STATE_DIR/last-complete.json"
GIT_SNAPSHOT="$COMPILE_STATE_DIR/last-build-snapshot.txt"

mkdir -p "$COMPILE_STATE_DIR"

# Run compilation
compile_once() {
    local start_time=$(date +%s%N)
    
    # Capture what files exist at build start
    git status --porcelain > "$GIT_SNAPSHOT" 2>/dev/null || true
    
    # Mark as compiling
    echo "{
        \"status\": \"compiling\",
        \"started\": \"$(date -u +%Y-%m-%dT%H:%M:%SZ)\",
        \"git_state_hash\": \"$(git status --porcelain 2>/dev/null | sha256sum | cut -d' ' -f1)\"
    }" > "$CURRENT_BUILD"
    
    # Run compilation (analytics handled by wrapper)
    cargo check --workspace --all-targets --message-format=json \
        2>&1 | tee "$COMPILE_STATE_DIR/live-output.jsonl" > /dev/null
    
    local exit_code=$?
    local end_time=$(date +%s%N)
    local duration_ms=$(( (end_time - start_time) / 1000000 ))
    
    # Count errors/warnings from output
    local errors=$(grep -c '"level":"error"' "$COMPILE_STATE_DIR/live-output.jsonl" 2>/dev/null || echo 0)
    local warnings=$(grep -c '"level":"warning"' "$COMPILE_STATE_DIR/live-output.jsonl" 2>/dev/null || echo 0)
    
    # Create result
    echo "{
        \"status\": \"$([ $exit_code -eq 0 ] && echo "success" || echo "failed")\",
        \"completed\": \"$(date -u +%Y-%m-%dT%H:%M:%SZ)\",
        \"duration_ms\": $duration_ms,
        \"exit_code\": $exit_code,
        \"errors\": $errors,
        \"warnings\": $warnings,
        \"git_state_hash\": \"$(git status --porcelain 2>/dev/null | sha256sum | cut -d' ' -f1)\",
        \"git_snapshot\": \"$GIT_SNAPSHOT\"
    }" > "$LAST_COMPLETE"
}

# Daemon loop
daemon_loop() {
    echo "{\"pid\": $$, \"status\": \"running\", \"started\": \"$(date -u +%Y-%m-%dT%H:%M:%SZ)\"}" > "$STATE_FILE"
    
    # Initial compilation
    compile_once
    
    # Watch for changes
    while true; do
        if command -v inotifywait >/dev/null 2>&1; then
            inotifywait -qr -e modify,create,delete \
                --include '.*\.(rs|toml)$' \
                --exclude '(target/|\.git/)' \
                --timeout 2 \
                . 2>/dev/null || true
        else
            sleep 2
        fi
        
        # Check if files actually changed
        if [ -f "$LAST_COMPLETE" ]; then
            last_hash=$(jq -r '.git_state_hash // ""' "$LAST_COMPLETE" 2>/dev/null)
            current_hash=$(git status --porcelain 2>/dev/null | sha256sum | cut -d' ' -f1)
            
            if [ "$last_hash" != "$current_hash" ]; then
                compile_once
            fi
        else
            compile_once
        fi
    done
}

# Get current status
status() {
    if [ -f "$CURRENT_BUILD" ] && [ -f "$LAST_COMPLETE" ]; then
        current=$(cat "$CURRENT_BUILD")
        last=$(cat "$LAST_COMPLETE")
        
        if echo "$current" | jq -e '.status == "compiling"' >/dev/null 2>&1; then
            echo "$current"
        else
            echo "$last"
        fi
    elif [ -f "$LAST_COMPLETE" ]; then
        cat "$LAST_COMPLETE"
    else
        echo '{"status": "no_data"}'
    fi
}

# Main commands
case "${1:-help}" in
    start)
        if [ -f "$STATE_FILE" ]; then
            pid=$(jq -r '.pid' "$STATE_FILE" 2>/dev/null || echo "")
            if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
                echo "Daemon already running (PID: $pid)"
                exit 0
            fi
        fi
        
        # Use setsid to detach from terminal properly
        setsid "$0" daemon > /dev/null 2>&1 < /dev/null &
        sleep 0.5  # Give it a moment to start
        echo "Started compilation daemon"
        ;;
        
    stop)
        if [ -f "$STATE_FILE" ]; then
            pid=$(jq -r '.pid' "$STATE_FILE" 2>/dev/null || echo "")
            if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
                kill "$pid"
                echo "Stopped daemon"
                rm -f "$STATE_FILE"
            fi
        fi
        ;;
        
    status)
        status
        ;;
        
    await-current)
        # Wait for compilation to complete for current source state
        current_hash=$(git status --porcelain 2>/dev/null | sha256sum | cut -d' ' -f1)
        max_wait=30
        waited=0
        
        while [ $waited -lt $max_wait ]; do
            if [ -f "$LAST_COMPLETE" ]; then
                last_hash=$(jq -r '.git_state_hash // ""' "$LAST_COMPLETE" 2>/dev/null)
                if [ "$last_hash" = "$current_hash" ]; then
                    status
                    exit 0
                fi
            fi
            sleep 1
            waited=$((waited + 1))
        done
        
        echo '{"status": "timeout", "message": "Compilation did not complete within 30 seconds"}'
        exit 1
        ;;
        
    daemon)
        daemon_loop
        ;;
        
    *)
        echo "Usage: $0 {start|stop|status|await-current}"
        echo "  start         - Start the compilation daemon"
        echo "  stop          - Stop the compilation daemon"
        echo "  status        - Get current compilation status (JSON)"
        echo "  await-current - Wait for compilation of current source state"
        ;;
esac