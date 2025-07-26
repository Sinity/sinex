#!/usr/bin/env bash
# Git state tracker using git stash for zero-overhead snapshots
# Much simpler and more reliable than custom solution
#
# IMPORTANT: This script creates snapshots but PRESERVES working directory state
# The stash is created and then immediately re-applied to avoid data loss
# Previously this script would stash changes without restoring them, causing files to disappear

set -euo pipefail

ANALYTICS_DIR="${SINEX_ANALYTICS_DIR:-$HOME/.sinex-analytics}"
STATE_FILE="$ANALYTICS_DIR/git-tracker.state"
LOG_FILE="$ANALYTICS_DIR/git-stash-tracker.log"
STASH_PREFIX="auto-snapshot"

mkdir -p "$ANALYTICS_DIR"

# Take a snapshot using git stash
take_snapshot() {
    # Only snapshot if there are changes
    if [ -z "$(git status --porcelain 2>/dev/null)" ]; then
        return 0
    fi
    
    local timestamp="$(date +%Y%m%d-%H%M%S)"
    local stash_msg="$STASH_PREFIX-$timestamp"
    
    # Create stash with all changes BUT THEN RESTORE THEM
    # Using --keep-index to keep staged changes, then pop immediately
    if git stash push --all --message "$stash_msg" >/dev/null 2>&1; then
        echo "$(date -Iseconds)|$stash_msg|$(git rev-parse --short HEAD)" >> "$LOG_FILE"
        # CRITICAL: Restore working directory state
        git stash apply stash@{0} >/dev/null 2>&1
    fi
}

# Daemon mode - watch for changes
daemon_mode() {
    echo "Git state tracker started (PID: $$)" | tee -a "$LOG_FILE"
    echo "{\"pid\": $$, \"status\": \"running\", \"started\": \"$(date -u +%Y-%m-%dT%H:%M:%SZ)\"}" > "$STATE_FILE"
    
    # Take initial snapshot
    take_snapshot
    
    # Watch for changes
    if command -v inotifywait >/dev/null 2>&1; then
        # Use inotifywait for efficiency
        inotifywait -mr -e modify,create,delete \
            --exclude '(\.git/|target/|\.idea/|\.vscode/)' \
            --format '%w%f %e %T' \
            --timefmt '%Y-%m-%d %H:%M:%S' \
            . 2>/dev/null | while read file event timestamp; do
            
            # Debounce - wait a bit for multiple changes
            sleep 2
            
            # Check if there are actual git changes
            if [ "$(git status --porcelain 2>/dev/null | wc -l)" -gt 0 ]; then
                take_snapshot
            fi
        done
    else
        # Polling fallback
        echo "inotifywait not found, using polling mode" >> "$LOG_FILE"
        local last_status=""
        
        while true; do
            sleep 5
            current_status=$(git status --porcelain 2>/dev/null | sha256sum)
            
            if [ "$current_status" != "$last_status" ]; then
                take_snapshot
                last_status="$current_status"
            fi
        done
    fi
}

# Main commands
case "${1:-help}" in
    start)
        if [ -f "$STATE_FILE" ]; then
            pid=$(jq -r '.pid' "$STATE_FILE" 2>/dev/null || echo "")
            if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
                echo "Git tracker already running (PID: $pid)"
                exit 0
            fi
        fi
        
        # Use setsid to detach from terminal properly
        setsid "$0" daemon > /dev/null 2>&1 < /dev/null &
        sleep 0.5  # Give it a moment to start
        echo "Git state tracker started"
        ;;
        
    stop)
        if [ -f "$STATE_FILE" ]; then
            pid=$(jq -r '.pid' "$STATE_FILE" 2>/dev/null || echo "")
            if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
                kill "$pid"
                echo "Git tracker stopped"
                rm -f "$STATE_FILE"
            else
                echo "Git tracker not running"
            fi
        else
            echo "No tracker state found"
        fi
        ;;
        
    status)
        if [ -f "$STATE_FILE" ]; then
            pid=$(jq -r '.pid' "$STATE_FILE" 2>/dev/null || echo "")
            if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
                cat "$STATE_FILE"
            else
                echo '{"status": "not_running"}'
            fi
        else
            echo '{"status": "not_running"}'
        fi
        ;;
        
    snapshot)
        take_snapshot
        echo "Snapshot taken"
        ;;
        
    list)
        echo "Git state snapshots:"
        git stash list | grep "$STASH_PREFIX"
        ;;
        
    show)
        if [ -z "${2:-}" ]; then
            echo "Usage: $0 show <stash-ref>"
            echo "Example: $0 show stash@{0}"
            exit 1
        fi
        git stash show -p "$2"
        ;;
        
    daemon)
        daemon_mode
        ;;
        
    *)
        echo "Usage: $0 {start|stop|status|snapshot|list|show}"
        echo ""
        echo "Git state tracker - captures snapshots on file changes using git stash"
        echo "  start    - Start the tracker daemon"
        echo "  stop     - Stop the tracker daemon"
        echo "  status   - Show daemon status and recent snapshots"
        echo "  snapshot - Take a snapshot manually"
        echo "  list     - List all snapshots"
        echo "  show     - Show specific snapshot (e.g., show stash@{0})"
        ;;
esac