# Sinex Development Workflows
# Run `just` to see all available commands

# Default: show all recipes
default:
    @just --list --unsorted

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 🚀 Quick Start
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

# Switch to dev database (idempotent setup)
dev:
    ./script/db.sh dev

# Show current database status
status:
    ./script/db.sh

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 🧪 Testing
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

# Run all tests
test:
    cargo test

# Run tests in isolated ephemeral environment
test-isolated:
    #!/usr/bin/env bash
    ./script/db.sh tmp_0
    cargo test

# Create ephemeral database for testing  
ephemeral NUMBER="0":
    ./script/db.sh tmp_{{NUMBER}}

# Switch to ephemeral database
tmp NUMBER="0":
    ./script/db.sh tmp_{{NUMBER}}

# Destroy ephemeral database
destroy:
    ./script/db.sh destroy

# Watch tests
watch:
    cargo watch -x test

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 🔨 Build & Check
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

# Check code (fast)
check:
    cargo check --all-features

# Full check with clippy
check-all:
    cargo check --all-features
    cargo clippy --all-features -- -D warnings

# Build everything
build:
    cargo build --all-features

# Build release
release:
    cargo build --release --all-features

# Format code
fmt:
    cargo fmt --all

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 🗄️  Database Operations
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

# Setup development database
db-setup:
    ./script/db.sh dev

# Switch to development database
db-dev:
    ./script/db.sh dev

# Switch to production database
db-prod:
    ./script/db.sh prod

# Reset database (WARNING: destructive)
db-reset:
    ./script/db.sh reset

# Connect to database
psql:
    ./script/db.sh shell

# Run migrations
migrate:
    sqlx migrate run

# Create a new migration
migrate-create NAME:
    sqlx migrate add {{NAME}}

# Update SQLX offline cache
sqlx-prepare:
    ./script/sqlx-prepare.sh

# Check if SQLX cache is up to date
sqlx-check:
    cargo sqlx prepare --workspace --check -- --all-targets --all-features

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 🏃 Running Ingestors (using nix packages)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

# Run filesystem ingestor (nix package)
filesystem:
    nix run .#filesystemIngestor

# Run kitty ingestor (nix package)
kitty:
    nix run .#kittyIngestor

# Run hyprland ingestor (nix package)
hyprland:
    nix run .#hyprlandIngestor

# Run promo worker (nix package)
worker:
    nix run .#sinexPromoWorker

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 📊 Monitoring & Queries
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

# Live monitoring dashboard
monitor:
    ./script/monitor.sh

# Query recent events
query LIMIT="10":
    @python3 ./cli/exo.py query --limit {{LIMIT}}

# Show event flow demo
demo:
    ./script/demo_event_flow.sh

# Diagnose event assumptions
diagnose:
    ./script/diagnose_assumptions.sh

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 🎛️  Complex Orchestration
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

# Kill all ingestors
kill-ingestors:
    pkill -f "ingestor" || true

# Full system test with real data
system-test:
    #!/usr/bin/env bash
    set -e
    echo "🧪 Starting full system test..."
    
    # Ensure clean state
    just db-reset
    
    # Run ingestors in background for 10 seconds
    echo "📡 Starting ingestors..."
    just filesystem &
    FILESYSTEM_PID=$!
    
    sleep 10
    
    # Stop ingestors
    kill $FILESYSTEM_PID 2>/dev/null || true
    
    # Check results
    EVENT_COUNT=$(psql $DATABASE_URL -t -c "SELECT COUNT(*) FROM raw.events" | xargs)
    echo "✅ Captured $EVENT_COUNT events"
    
    # Run diagnostics
    just diagnose

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 🧹 Maintenance
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

# Clean build artifacts
clean:
    cargo clean

# Update dependencies
update:
    cargo update

# Show disk usage
disk-usage:
    @echo "📁 Project disk usage:"
    @du -sh target/ 2>/dev/null || echo "  target/: not built yet"
    @du -sh ~/.cargo/registry/ 2>/dev/null || echo "  cargo registry: N/A"

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 🎯 Shortcuts (even shorter aliases)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

alias c := check
alias ca := check-all
alias b := build
alias t := test
alias m := monitor
alias d := dev
alias fs := filesystem
alias kt := kitty
alias hy := hyprland