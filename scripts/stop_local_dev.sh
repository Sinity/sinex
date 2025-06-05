#!/usr/bin/env bash
# Stop all Sinex local development ingestors
set -euo pipefail

LOG_DIR="/tmp/sinex-logs"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}🛑 Stopping Sinex local development ingestors...${NC}"

# Function to stop an ingestor
stop_ingestor() {
    local name=$1
    local pid_file="$LOG_DIR/$name.pid"
    
    if [ -f "$pid_file" ]; then
        local pid=$(cat "$pid_file")
        if kill -0 "$pid" 2>/dev/null; then
            echo -e "${BLUE}   Stopping $name (PID: $pid)...${NC}"
            kill "$pid"
            
            # Wait up to 10 seconds for graceful shutdown
            for i in {1..10}; do
                if ! kill -0 "$pid" 2>/dev/null; then
                    echo -e "${GREEN}   ✅ $name stopped gracefully${NC}"
                    break
                fi
                sleep 1
            done
            
            # Force kill if still running
            if kill -0 "$pid" 2>/dev/null; then
                echo -e "${YELLOW}   ⚠️  Force killing $name...${NC}"
                kill -9 "$pid" 2>/dev/null || true
            fi
        else
            echo -e "${YELLOW}   ⚠️  $name was not running (stale PID file)${NC}"
        fi
        rm -f "$pid_file"
    else
        echo -e "${YELLOW}   ⚠️  No PID file for $name${NC}"
    fi
}

# Stop all known ingestors
stop_ingestor "hyprland"
stop_ingestor "kitty"
stop_ingestor "filesystem"

# Also kill any remaining processes by name
echo -e "${BLUE}   Checking for remaining processes...${NC}"
for proc in hyprland-ingestor kitty-ingestor filesystem-ingestor; do
    pkill -f "$proc" 2>/dev/null && echo -e "${GREEN}   ✅ Killed remaining $proc processes${NC}" || true
done

echo ""
echo -e "${GREEN}🎉 All ingestors stopped!${NC}"
echo ""
echo -e "${BLUE}📋 Clean up:${NC}"
echo "   Log files remain in: $LOG_DIR"
echo "   To view final logs:  tail $LOG_DIR/*.log"
echo "   To clean logs:       rm -rf $LOG_DIR"