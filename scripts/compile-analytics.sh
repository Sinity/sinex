#!/usr/bin/env bash
# Lightweight compilation analytics - runs with every cargo invocation
# Stores data in ~/.sinex-analytics/compilations/
#
# Behavior:
# - Interactive terminal: Shows cargo's progress bar, captures timing report
# - Non-interactive/CI: Uses JSON output for detailed analytics
# - Always collects timing data via --timings flag

set -euo pipefail

ANALYTICS_DIR="${SINEX_ANALYTICS_DIR:-$HOME/.sinex-analytics}"
COMPILE_LOG_DIR="$ANALYTICS_DIR/compilations"
mkdir -p "$COMPILE_LOG_DIR"

# Generate unique run ID
RUN_ID="$(date +%Y%m%d_%H%M%S)_$$"
RUN_DIR="$COMPILE_LOG_DIR/$RUN_ID"
mkdir -p "$RUN_DIR"

# Capture pre-compilation state
cat > "$RUN_DIR/start.json" << EOF
{
  "run_id": "$RUN_ID",
  "timestamp": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
  "command": "$*",
  "pwd": "$(pwd)",
  "cargo_target_dir": "${CARGO_TARGET_DIR:-target}",
  "rustc_wrapper": "${RUSTC_WRAPPER:-none}",
  "rustflags": "${RUSTFLAGS:-}",
  "profile": "${PROFILE:-dev}",
  "cpu_count": $(nproc),
  "load_avg": "$(uptime | awk -F'load average:' '{print $2}' | xargs)",
  "mem_available_mb": $(free -m | awk '/^Mem:/ {print $7}')
}
EOF

# Run the actual cargo command with timing
START_TIME=$(date +%s%N)

# Execute cargo with --timings and message format
CARGO_CMD="$1"
shift

# Detect if we're in an interactive terminal
if [ -t 1 ] && [ -t 2 ] && [ "${CI:-}" != "true" ] && [ "${CARGO_TERM_PROGRESS_WHEN:-auto}" != "never" ]; then
    # Interactive mode: show progress bar, capture timing
    # Force color and progress even if cargo might not auto-detect it
    CARGO_TERM_COLOR=always CARGO_TERM_PROGRESS_WHEN=always CARGO_TERM_PROGRESS_WIDTH="${COLUMNS:-80}" \
        "$CARGO_CMD" --timings "$@"
    EXIT_CODE=$?
    
    # Still capture some analytics in background
    if ls target/cargo-timings/cargo-timing-*.html >/dev/null 2>&1; then
        cp target/cargo-timings/cargo-timing-*.html "$RUN_DIR/" 2>/dev/null || true
    fi
else
    # Non-interactive mode: use JSON output for full analytics
    "$CARGO_CMD" --timings --message-format=json "$@" 2>&1 | tee "$RUN_DIR/output.jsonl" | \
        grep -E '^{' | jq -c 'select(.reason == "compiler-artifact" or .reason == "build-finished")' > "$RUN_DIR/artifacts.jsonl"
    EXIT_CODE=$?
fi
END_TIME=$(date +%s%N)
DURATION_MS=$(( (END_TIME - START_TIME) / 1000000 ))

# Capture post-compilation state
cat > "$RUN_DIR/end.json" << EOF
{
  "run_id": "$RUN_ID",
  "timestamp": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
  "duration_ms": $DURATION_MS,
  "exit_code": $EXIT_CODE,
  "success": $([ $EXIT_CODE -eq 0 ] && echo true || echo false)
}
EOF

# Copy timing report if generated
if ls target/cargo-timings/cargo-timing-*.html >/dev/null 2>&1; then
    cp target/cargo-timings/cargo-timing-*.html "$RUN_DIR/"
fi

# Get sccache stats if available
if command -v sccache >/dev/null 2>&1; then
    sccache --show-stats --stats-format json > "$RUN_DIR/sccache-stats.json" 2>/dev/null || true
fi

# Create index entry for fast queries
echo "$RUN_ID|$(date +%s)|$DURATION_MS|$EXIT_CODE|$CARGO_CMD $*" >> "$COMPILE_LOG_DIR/index.csv"

# Exit with the same code as cargo
exit $EXIT_CODE