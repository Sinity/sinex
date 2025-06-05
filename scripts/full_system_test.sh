#!/usr/bin/env bash
set -euo pipefail

# Complete system test with ephemeral setup and real-time monitoring

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m'

TEST_MODE="${1:-auto}"  # auto, interactive, or monitor
TEST_DURATION="${2:-30}"

echo -e "${CYAN}╔══════════════════════════════════════════════════════════════════╗${NC}"
echo -e "${CYAN}║                    🧪 SINEX FULL SYSTEM TEST                    ║${NC}"
echo -e "${CYAN}╚══════════════════════════════════════════════════════════════════╝${NC}"
echo
echo -e "${BLUE}Mode: $TEST_MODE${NC}"
echo -e "${BLUE}Duration: ${TEST_DURATION}s${NC}"
echo

case "$TEST_MODE" in
    "auto")
        echo "🤖 Running automated full system test..."
        ./scripts/ephemeral_test.sh
        ;;
    "interactive")
        echo "🎮 Starting interactive test environment..."
        echo "This will:"
        echo "  1. Start ephemeral database"
        echo "  2. Launch ingestors"
        echo "  3. Generate test activity"
        echo "  4. Show live monitoring"
        echo
        read -p "Press Enter to continue or Ctrl+C to cancel..."
        
        # Start ephemeral environment in background
        ./scripts/ephemeral_test.sh &
        EPHEMERAL_PID=$!
        
        # Give it time to start
        sleep 10
        
        # Start live monitor
        echo -e "\n${GREEN}🔍 Starting live monitor (Ctrl+C to exit)...${NC}"
        sleep 2
        ./scripts/live_monitor.sh
        
        # Cleanup
        kill $EPHEMERAL_PID 2>/dev/null || true
        ;;
    "monitor")
        echo "📊 Starting live monitor for existing system..."
        if [[ -z "${DATABASE_URL:-}" ]]; then
            echo -e "${RED}❌ DATABASE_URL not set${NC}"
            echo "Set DATABASE_URL to point to running Sinex database"
            exit 1
        fi
        ./scripts/live_monitor.sh
        ;;
    *)
        echo "Usage: $0 [auto|interactive|monitor] [duration_seconds]"
        echo
        echo "Modes:"
        echo "  auto       - Automated test with ephemeral setup (default)"
        echo "  interactive - Start test environment and show live monitor"
        echo "  monitor    - Monitor existing system (requires DATABASE_URL)"
        echo
        echo "Examples:"
        echo "  $0                          # Auto test"
        echo "  $0 interactive              # Interactive mode"
        echo "  $0 monitor                  # Monitor existing system"
        echo "  DATABASE_URL=... $0 monitor # Monitor specific database"
        exit 1
        ;;
esac