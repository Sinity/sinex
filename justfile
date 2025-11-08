# Essential aliases
alias m := migrate
alias q := query

# Show available commands
default:
    @echo -e "\033[1m⚡ Sinex Commands\033[0m\n"
    @echo -e "\033[90mDevelopment:\033[0m"
    @echo -e "  \033[1mcheck\033[0m        Fast compilation check"
    @echo -e "  \033[1mtest\033[0m         Run unit + property tests"
    @echo -e "  \033[1mtest-all\033[0m     Run complete test suite"
    @echo -e "  \033[1mpre-commit\033[0m   Format + lint + check + test\n"
    @echo -e "\033[90mDatabase:\033[0m"
    @echo -e "  \033[1mmigrate\033[0m      Apply migrations (alias: m)"
    @echo -e "  \033[1mpsql\033[0m         Connect to database"
    @echo -e "  \033[1msqlx-prepare\033[0m Update SQLX cache (commit .sqlx/!)\n"
    @echo -e "\033[90mServices:\033[0m"
    @echo -e "  \033[1mingestd\033[0m      Start central coordinator"
    @echo -e "  \033[1mgateway\033[0m      Start API gateway"
    @echo -e "  \033[1mquery\033[0m        Query events (alias: q)"
    @echo -e "\n"
    @echo -e "\033[90mSatellites:\033[0m"
    @echo -e "  \033[1mfs-watcher\033[0m   File system events"
    @echo -e "  \033[1mterminal\033[0m     Terminal events"
    @echo -e "  \033[1mdesktop\033[0m      Desktop events"
    @echo -e "  \033[1msystem\033[0m       System events\n"
    @just --list --unsorted

# === Development ===

# Format code
fmt:
    cargo fmt --all

# Lint with clippy
lint:
    cargo clippy --workspace --all-targets --all-features -- -D warnings

# Fast compilation check
check:
    cargo check --workspace --all-features

# Build all binaries
build:
    cargo build --workspace

# Pre-commit: format + lint + check + test
pre-commit: fmt lint check test

# === Testing ===

# Run fast tests (unit + property) with nextest
test:
    SINEX_ALLOW_NATIVE_TESTS=1 cargo nextest run --workspace --lib --profile reliable
    SINEX_ALLOW_NATIVE_TESTS=1 PROPTEST_CASES=${PROPTEST_CASES:-64} cargo nextest run --workspace --test property_tests --profile reliable

# Run all tests with nextest
test-all:
    just db-setup
    LD_LIBRARY_PATH="$(pkg-config --variable=libdir dbus-1)${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}" \
    RUST_LOG=${RUST_LOG:-} SINEX_ALLOW_NATIVE_TESTS=1 cargo nextest run --workspace --profile reliable

# (integration target removed to keep surface minimal; use `just test-all` with filters if needed)

# Run VM tests
test-vm:
    ./tests/e2e/nixos-vm/run-vm-tests.sh -c smoke

# === Database ===

# Apply migrations
migrate:
    cd crate/lib/sinex-schema && DATABASE_URL="${DATABASE_URL:-postgresql:///sinex_dev?host=/run/postgresql}" cargo run -- up

# Create new migration
migrate-create NAME:
    cd crate/lib/sinex-schema && cargo run -- generate {{NAME}}

# Check migration status
migrate-status:
    cd crate/lib/sinex-schema && DATABASE_URL="${DATABASE_URL:-postgresql:///sinex_dev?host=/run/postgresql}" cargo run -- status

# Reset database
db-reset:
    dropdb --if-exists --force sinex_dev
    createdb sinex_dev
    just migrate

# Setup test database
db-setup:
    createdb sinex_dev 2>/dev/null || true
    just migrate

# Connect to database
psql *ARGS:
    psql "${DATABASE_URL:-postgresql:///sinex_dev?host=/run/postgresql}" {{ARGS}}

# Update SQLX offline cache (for Nix builds)
sqlx-prepare:
    just migrate
    # Prepare SQLx offline cache for workspace members (include test targets for all queries)
    cargo sqlx prepare --workspace -- --all-targets
    @echo "✅ SQLX cache updated - remember to commit .sqlx/"

# Check SQLX cache
sqlx-check:
    cargo sqlx prepare --workspace --check -- --all-targets

# === Services ===

# Start ingestd
ingestd:
    cargo run --bin sinex-ingestd

# Start gateway
gateway:
    cargo run --bin sinex-gateway

# Query events
query LIMIT='10':
    ./cli/exo.py query --limit {{LIMIT}}

# (monitor target removed; config not present)

# === Satellites ===

# File system watcher
fs-watcher:
    cargo run --bin sinex-fs-watcher

# Terminal satellite
terminal:
    cargo run --bin sinex-terminal-satellite

# Desktop satellite
desktop:
    cargo run --bin sinex-desktop-satellite

# System satellite
system:
    cargo run --bin sinex-system-satellite

# Canonicalizer
canonicalizer:
    cargo run --bin sinex-terminal-command-canonicalizer

# Health aggregator
health:
    cargo run --bin sinex-health-aggregator

# === Quick Checks ===

# Show compilation errors
errors:
    cargo check --workspace --all-features 2>&1 | grep -E "^error" || echo "No errors found"

# Show compilation warnings
warnings:
    cargo check --workspace --all-features 2>&1 | grep -E "^warning" || echo "No warnings found"

# === Utilities ===

# Watch for changes
watch:
    bacon

# Build documentation
docs:
    cargo doc --workspace --no-deps --open

# Clean build artifacts
clean:
    cargo clean

# Code statistics
stats:
    tokei

# Generate coverage report
coverage:
    cargo tarpaulin --workspace --out Html --output-dir target/coverage
    @echo "Coverage report: target/coverage/index.html"

# Update dependencies
update:
    cargo update

# Security audit
audit:
    cargo audit

# Check unused dependencies
unused:
    cargo machete
