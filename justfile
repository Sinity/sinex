# Sinex Development Workflows
# Run `just` to see all available commands

# Default: show all recipes
default:
    @just --list --unsorted

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 🚀 Quick Start
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

# Start full development environment with process management
dev:
    nix run .#dev

# Start database only
db:
    nix run .#db-setup dev

# Quick status check
status:
    @echo "🗄️  Database Status:"
    @nix run .#db-setup check 2>/dev/null && echo "✅ Connected" || echo "❌ Not running"
    @echo ""
    @echo "📊 Event Count:"
    @psql $DATABASE_URL -t -c "SELECT COUNT(*) FROM raw.events" 2>/dev/null || echo "N/A"

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 🧪 Testing
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

# Run tests (unit, integration, or all)
test TYPE="unit":
    nix run .#test {{TYPE}}

# Run tests in isolated ephemeral environment
test-isolated:
    nix run .#ephemeral test

# Interactive ephemeral environment
ephemeral:
    nix run .#ephemeral interactive

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
    nix run .#db-setup dev

# Reset database (WARNING: destructive)
db-reset:
    nix run .#db-setup reset

# Connect to database
psql:
    psql $DATABASE_URL

# Run migrations
migrate:
    sqlx migrate run

# Create a new migration
migrate-create NAME:
    sqlx migrate add {{NAME}}

# Update SQLX offline cache
sqlx-prepare:
    nix run .#sqlx-prepare

# Update SQLX cache (alternative)
sqlx-update:
    ./scripts/update-sqlx-cache.sh

# Check if SQLX cache is up to date
sqlx-check:
    cargo sqlx prepare --workspace --check -- --all-targets --all-features

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 📊 Monitoring & Queries
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

# Live monitoring dashboard
monitor:
    nix run .#monitor

# Query recent events
query LIMIT="10":
    @python3 ./cli/exo.py query --limit {{LIMIT}}

# Show event flow demo
demo:
    ./scripts/demo_event_flow.sh

# Diagnose event assumptions
diagnose:
    ./scripts/diagnose_assumptions.sh

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 🎛️  Complex Orchestration
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

# Start ingestors individually
ingestor NAME:
    #!/usr/bin/env bash
    case {{NAME}} in
        filesystem)
            RUST_LOG=info DATABASE_URL=$DATABASE_URL filesystem-ingestor run
            ;;
        kitty)
            RUST_LOG=info DATABASE_URL=$DATABASE_URL kitty-ingestor run
            ;;
        hyprland)
            RUST_LOG=info DATABASE_URL=$DATABASE_URL hyprland-ingestor run
            ;;
        *)
            echo "Unknown ingestor: {{NAME}}"
            echo "Available: filesystem, kitty, hyprland"
            exit 1
            ;;
    esac

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
    
    # Start database
    just db-setup
    
    # Run ingestors in background for 10 seconds
    echo "📡 Starting ingestors..."
    just ingestor filesystem &
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