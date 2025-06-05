#!/usr/bin/env bash
# Tail logs for a specific Sinex agent
set -euo pipefail

AGENT_NAME=${1:-}
LOG_DIR="/tmp/sinex-logs"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

if [ -z "$AGENT_NAME" ]; then
    echo -e "${RED}❌ Usage: $0 <agent_name>${NC}"
    echo ""
    echo -e "${BLUE}Available agents:${NC}"
    for log_file in "$LOG_DIR"/*.log; do
        if [ -f "$log_file" ]; then
            basename "$log_file" .log | sed 's/^/   /'
        fi
    done
    echo ""
    echo -e "${BLUE}Example:${NC}"
    echo "   $0 hyprland"
    echo "   $0 kitty" 
    echo "   $0 filesystem"
    exit 1
fi

LOG_FILE="$LOG_DIR/$AGENT_NAME.log"

if [ ! -f "$LOG_FILE" ]; then
    echo -e "${RED}❌ Log file not found: $LOG_FILE${NC}"
    echo ""
    echo -e "${BLUE}Available log files:${NC}"
    ls -la "$LOG_DIR"/*.log 2>/dev/null | sed 's/^/   /' || echo "   No log files found"
    exit 1
fi

echo -e "${GREEN}📋 Tailing logs for $AGENT_NAME agent${NC}"
echo -e "${BLUE}   Log file: $LOG_FILE${NC}"
echo -e "${YELLOW}   Press Ctrl+C to stop${NC}"
echo ""

# Use multitail if available for better formatting
if command -v multitail > /dev/null 2>&1; then
    multitail -s 2 -cT ansi "$LOG_FILE"
else
    tail -f "$LOG_FILE"
fi