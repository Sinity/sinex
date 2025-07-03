# Show available commands
default:
    @just --list --unsorted

# === Testing ===

# Run all tests
test *ARGS:
    cargo nextest run {{ARGS}}

# Run fast tests (unit + property)
test-fast *ARGS:
    cargo nextest run -E "test(unit::) or test(property::)" {{ARGS}}

# Watch tests (re-run on file changes)
watch *ARGS:
    cargo watch -x "nextest run {{ARGS}}"

# Watch only fast tests (unit + property)
watch-fast *ARGS:
    cargo watch -x "nextest run -E 'test(unit::) or test(property::)' {{ARGS}}"

# === VM Tests ===

# Run VM tests
test-vm:
    ./test/nixos-vm/run-vm-tests.sh -c smoke

# Run all VM tests
test-vm-all:
    ./test/nixos-vm/run-vm-tests.sh -c all

# Debug specific VM test
test-vm-debug TEST="basic-flow":
    ./test/nixos-vm/run-vm-tests.sh -d {{TEST}}

# === Database ===

# Run migrations
migrate:
    sqlx migrate run

# Create new migration
migrate-create NAME:
    sqlx migrate add {{NAME}}

# Connect to database
psql *ARGS:
    psql "${DATABASE_URL:-postgresql:///sinex_dev?host=/run/postgresql}" {{ARGS}}

# Update SQLX offline cache
sqlx-prepare:
    @echo "🗄️  Updating SQLX offline cache..."
    sqlx migrate run
    cargo sqlx prepare --workspace -- --all-targets --all-features
    @echo "✅ SQLX cache updated"
    @echo "⚠️  Don't forget to commit .sqlx/"

# Check SQLX cache is up to date
sqlx-check:
    cargo sqlx prepare --workspace --check -- --all-targets --all-features

# === Running Services ===

# Run unified collector
collector *ARGS:
    cargo run --bin sinex-collector {{ARGS}}

# Run promotion worker
worker *ARGS:
    cargo run --bin sinex-promo-worker {{ARGS}}

# Query recent events
query LIMIT="10" *ARGS:
    python3 ./cli/exo.py query --limit {{LIMIT}} {{ARGS}}

# === Building ===

# Check code compiles
check:
    cargo check --workspace --all-features

# Build debug version
build:
    cargo build --workspace --all-features

# Build release version
release:
    cargo build --release --workspace --all-features

# Format code
fmt:
    cargo fmt --all

# Lint code
lint:
    cargo clippy --workspace --all-features -- -D warnings

# === Coverage ===

# Generate test coverage report
coverage *ARGS:
    cargo llvm-cov --workspace --all-features {{ARGS}}

# Generate HTML coverage report
coverage-html:
    cargo llvm-cov --workspace --all-features --html
    @echo "📊 Coverage report: target/llvm-cov/html/index.html"

# === Utilities ===

# Clean build artifacts
clean:
    cargo clean

# Update dependencies
update:
    cargo update

# === Common Workflows ===

# Quick check before commit
pre-commit: fmt lint check test-fast

# Full validation
validate: fmt lint check test

# === Aliases ===
alias t := test
alias c := check
alias b := build
alias w := watch