#!/usr/bin/env bash
set -euo pipefail

# Colors
BLUE='\033[0;34m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log() { echo -e "${BLUE}🚀${NC} $*"; }
success() { echo -e "${GREEN}✅${NC} $*"; }
warning() { echo -e "${YELLOW}⚠️${NC} $*"; }

# Ensure database is setup
./script/db.sh setup dev

# Create mprocs config
cat > .mprocs.yaml << EOF
procs:
  filesystem:
    cmd: ["cargo", "run", "--bin", "filesystem-ingestor"]
    env:
      RUST_LOG: info
    autostart: false
  
  kitty:
    cmd: ["cargo", "run", "--bin", "kitty-ingestor"]
    env:
      RUST_LOG: info
    autostart: false
  
  hyprland:
    cmd: ["cargo", "run", "--bin", "hyprland-ingestor"]
    env:
      RUST_LOG: info
    autostart: false
  
  monitor:
    cmd: ["./script/monitor.sh", "dashboard"]
    autostart: false
EOF

log "Starting development environment..."
warning "Use keys: [f] Filesystem  [k] Kitty  [h] Hyprland  [m] Monitor"
warning "Press [Ctrl+A] then [q] to quit"

mprocs --config .mprocs.yaml