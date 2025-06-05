#!/usr/bin/env bash
# Local development runner for Sinex Phase 2 ingestors
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
LOG_DIR="/tmp/sinex-logs"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${GREEN}🚀 Starting Sinex Phase 2 Local Development Environment${NC}"

# Create log directory
mkdir -p "$LOG_DIR"

# Check if database is accessible
echo -e "${BLUE}📊 Checking database connection...${NC}"
if ! psql -U sinex -d sinex -c "SELECT 1;" > /dev/null 2>&1; then
    echo -e "${RED}❌ Database not accessible. Run ./scripts/db_reset.sh first${NC}"
    exit 1
fi

echo -e "${GREEN}✅ Database connection OK${NC}"

# Function to start an ingestor
start_ingestor() {
    local name=$1
    local binary_path=$2
    local config_path=$3
    
    echo -e "${BLUE}🔧 Starting $name ingestor...${NC}"
    
    if [ ! -f "$binary_path" ]; then
        echo -e "${YELLOW}⚠️  Building $name ingestor first...${NC}"
        (cd "$(dirname "$binary_path")" && cargo build --release)
    fi
    
    # Start ingestor in background
    RUST_LOG=info "$binary_path" --config "$config_path" run > "$LOG_DIR/$name.log" 2>&1 &
    local pid=$!
    echo "$pid" > "$LOG_DIR/$name.pid"
    
    echo -e "${GREEN}✅ $name ingestor started (PID: $pid)${NC}"
}

# Build all ingestors first
echo -e "${BLUE}🔨 Building all ingestors...${NC}"
cd "$PROJECT_ROOT/ingestors"
cargo build --release

# Start Hyprland ingestor
if command -v hyprctl > /dev/null 2>&1; then
    start_ingestor "hyprland" \
        "$PROJECT_ROOT/ingestors/target/release/hyprland-ingestor" \
        "$PROJECT_ROOT/ingestors/hyprland/config/development.toml"
else
    echo -e "${YELLOW}⚠️  Hyprland not detected, skipping hyprland-ingestor${NC}"
fi

# Start Kitty ingestor
if command -v kitty > /dev/null 2>&1; then
    start_ingestor "kitty" \
        "$PROJECT_ROOT/ingestors/target/release/kitty-ingestor" \
        "$PROJECT_ROOT/ingestors/kitty/config/development.toml"
else
    echo -e "${YELLOW}⚠️  Kitty not detected, skipping kitty-ingestor${NC}"
fi

# Start Filesystem ingestor
start_ingestor "filesystem" \
    "$PROJECT_ROOT/ingestors/target/release/filesystem-ingestor" \
    "$PROJECT_ROOT/ingestors/filesystem/config/development.toml"

echo ""
echo -e "${GREEN}🎉 All ingestors started!${NC}"
echo ""
echo -e "${BLUE}📋 Monitoring commands:${NC}"
echo "   View all logs:       tail -f $LOG_DIR/*.log"
echo "   View hyprland logs:  tail -f $LOG_DIR/hyprland.log"
echo "   View kitty logs:     tail -f $LOG_DIR/kitty.log"
echo "   View filesystem logs: tail -f $LOG_DIR/filesystem.log"
echo ""
echo -e "${BLUE}🔍 Query commands:${NC}"
echo "   Recent events:       $PROJECT_ROOT/cli/exo.py query --last 1h"
echo "   Event sources:       $PROJECT_ROOT/cli/exo.py sources"
echo "   Agent status:        $PROJECT_ROOT/cli/exo.py agent list"
echo "   Schema list:         $PROJECT_ROOT/cli/exo.py schema list"
echo ""
echo -e "${BLUE}🛑 Stop all ingestors:${NC}"
echo "   ./scripts/stop_local_dev.sh"
echo ""
echo -e "${YELLOW}Press Ctrl+C to stop monitoring (ingestors will continue running)${NC}"

# Monitor logs
trap 'echo -e "\n${BLUE}📝 Logs saved in: $LOG_DIR${NC}"; exit 0' INT

tail -f "$LOG_DIR"/*.log 2>/dev/null || {
    echo -e "${YELLOW}⚠️  No logs yet, waiting for ingestors to start...${NC}"
    sleep 5
    tail -f "$LOG_DIR"/*.log 2>/dev/null || echo -e "${RED}❌ No log files found${NC}"
}