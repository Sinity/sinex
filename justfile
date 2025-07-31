# Convenience aliases for common commands
alias m := migrate
alias ms := migrate-status
alias mc := migrate-create
alias mt := test-fast
alias d := dev
alias q := query

# Show available commands with descriptions
default:
    @echo -e "\033[1m⚡ Sinex Quick Reference\033[0m"
    @echo ""
    @echo -e "\033[90mEssential Commands:\033[0m"
    @echo -e "  \033[1mdev\033[0m         Format → Check → Test (~1min)"
    @echo -e "  \033[1mqc\033[0m          Compilation status (instant)"
    @echo -e "  \033[1merrors\033[0m      Show errors/warnings"
    @echo -e "  \033[1mtest-fast\033[0m   Unit tests only (~30s)"
    @echo ""
    @echo -e "\033[90mDatabase:\033[0m"
    @echo -e "  \033[1mmigrate\033[0m     Run database migrations (alias: m)"
    @echo -e "  \033[1mpsql\033[0m        Connect to database"
    @echo ""
    @echo -e "\033[90mServices:\033[0m"
    @echo -e "  \033[1mingestd\033[0m     Central coordinator (gRPC)"
    @echo -e "  \033[1mmonitor\033[0m     Dev dashboard (mprocs UI)"
    @echo -e "  \033[1mquery\033[0m       Query recent events"
    @echo ""
    @echo -e "\033[90mEvent Satellites:\033[0m"
    @echo -e "  \033[1mfs-watcher\033[0m  File system changes"
    @echo -e "  \033[1mterminal\033[0m    Terminal commands"
    @echo -e "  \033[1mdesktop\033[0m     Clipboard/window events"
    @echo ""
    @echo -e "\033[33mAliases: m=migrate, ms=migrate-status, mc=migrate-create, mt=test-fast, d=dev, q=query\033[0m"
    @echo ""
    @just --list --unsorted

# === Development Workflow ===

# 🏗️ Main development workflow: Format → Check → Test
dev: fmt check test-fast
    @echo "✅ Development checks passed!"

# 🚀 Pre-commit workflow
pre-commit: fmt lint check test-fast

# 🎯 Full test suite before pushing
push-ready: fmt lint check test-all

# === Code Quality ===

# 🎨 Format code with rustfmt
fmt:
    @echo "🎨 Formatting code..."
    cargo fmt --all

# 🔍 Run clippy lints
lint:
    @echo "🔍 Running clippy..."
    cargo clippy --workspace --all-targets --all-features -- -D warnings

# ✅ Check compilation
check:
    @echo "✅ Checking compilation..."
    cargo check --workspace --all-targets --all-features

# 🚀 Build all binaries
build:
    @echo "🚀 Building all binaries..."
    cargo build --workspace --all-targets

# 🏃 Build optimized release binaries
release:
    @echo "🏃 Building release binaries..."
    cargo build --workspace --release

# === Testing ===

# 🧪 Run fast tests (unit + property)
test-fast:
    @echo "🧪 Running fast tests..."
    just test-unit
    just test-property

# 🧬 Run unit tests only
test-unit:
    @echo "🧬 Running unit tests..."
    cargo test --workspace --lib

# 🔀 Run property-based tests
test-property:
    @echo "🔀 Running property tests..."
    cargo test --workspace --test property_tests

# 🔗 Run integration tests
test-integration:
    @echo "🔗 Running integration tests..."
    just db-setup
    cargo test --workspace --test '*integration*'

# 🌐 Run system tests
test-system:
    @echo "🌐 Running system tests..."
    just db-setup
    cargo test --workspace --test '*system*'

# 🛡️ Run all tests
test-all:
    @echo "🛡️ Running all tests..."
    just db-setup
    cargo test --workspace

# 🖥️ Run NixOS VM tests
test-vm:
    @echo "🖥️ Running VM tests..."
    ./test/nixos-vm/run-vm-tests.sh -c smoke

# === Database Management ===

# 📊 Apply database migrations
migrate:
    @echo "📊 Running database migrations..."
    cd crate/sinex-db/migration && DATABASE_URL="${DATABASE_URL:-postgresql:///sinex_dev?host=/run/postgresql}" cargo run -- up

# 📝 Create new database migration
migrate-create NAME:
    @echo "📝 Creating migration: {{NAME}}"
    cd crate/sinex-db/migration && cargo run -- migrate generate {{NAME}}

# 📊 Check migration status
migrate-status:
    @echo "📊 Checking migration status..."
    cd crate/sinex-db/migration && DATABASE_URL="${DATABASE_URL:-postgresql:///sinex_dev?host=/run/postgresql}" cargo run -- status

# 📋 List all available migrations
migrate-list:
    @echo "📋 Available migrations:"
    @ls -1 crate/sinex-db/migration/src/m*.rs | sed 's/.*\///' | sed 's/\.rs$//' | sort

# 📉 Rollback last migration
migrate-down:
    @echo "📉 Rolling back last migration..."
    cd crate/sinex-db/migration && DATABASE_URL="${DATABASE_URL:-postgresql:///sinex_dev?host=/run/postgresql}" cargo run -- down

# 🔄 Refresh database (drop all, recreate, migrate)
db-reset:
    @echo "🔄 Resetting test database..."
    dropdb --if-exists --force sinex_dev
    createdb sinex_dev
    just migrate

# 🧪 Setup test database
db-setup:
    @echo "🧪 Setting up test database..."
    createdb sinex_dev 2>/dev/null || true
    just migrate

# 🔌 Connect to development database
psql *ARGS:
    @echo "🔌 Connecting to database..."
    psql "${DATABASE_URL:-postgresql:///sinex_dev?host=/run/postgresql}" {{ARGS}}

# 💾 Update SQLX offline cache (for Nix builds)
sqlx-prepare:
    @echo "💾 Updating SQLX offline cache..."
    just migrate
    cargo sqlx prepare --workspace -- --all-targets
    @echo "✅ SQLX cache updated"
    @echo "⚠️  IMPORTANT: Commit .sqlx/ directory for Nix builds!"

# ✅ Verify SQLX cache is up to date
sqlx-check:
    @echo "✅ Checking SQLX cache consistency..."
    cargo sqlx prepare --workspace --check -- --all-targets

# === Service Management ===

# 🚀 Run ingestd service
ingestd:
    @echo "🚀 Starting ingestd..."
    cargo run --bin sinex-ingestd

# 🎯 Run gateway service
gateway:
    @echo "🎯 Starting gateway..."
    cargo run --bin sinex-gateway

# 📂 Run filesystem watcher
fs-watcher:
    @echo "📂 Starting filesystem watcher..."
    cargo run --bin sinex-fs-watcher

# 💻 Run terminal satellite
terminal:
    @echo "💻 Starting terminal satellite..."
    cargo run --bin sinex-terminal-satellite

# 🖥️ Run desktop satellite
desktop:
    @echo "🖥️ Starting desktop satellite..."
    cargo run --bin sinex-desktop-satellite

# 🔧 Run system satellite
system:
    @echo "🔧 Starting system satellite..."
    cargo run --bin sinex-system-satellite

# 🔍 Run canonicalizer automaton
canonicalizer:
    @echo "🔍 Starting canonicalizer..."
    cargo run --bin sinex-terminal-command-canonicalizer

# 📊 Query recent events
query LIMIT='10':
    @echo "📊 Querying {{LIMIT}} recent events..."
    ./cli/exo.py query --limit {{LIMIT}}

# 📈 Development monitor (all services)
monitor:
    @echo "📈 Starting development monitor..."
    mprocs -c config/mprocs-dev.yaml

# === Compilation Shortcuts ===

# ⚡ Quick check (full workspace)
qc:
    @cargo check --workspace --all-features

# 🎯 Quick check specific crate
qcc CRATE:
    @cargo check -p {{CRATE}} --all-features

# 🧠 Smart check (only changed crates)
qcs:
    @cargo check --workspace --all-features -- -Z unstable-options --changed-since=HEAD

# 📊 Show compilation errors
errors:
    @cargo check --workspace --all-features --message-format=json 2>&1 | jq -r 'select(.reason=="compiler-message" and .message.level=="error") | .message.rendered' | head -50

# ⚠️ Show compilation warnings
warnings:
    @cargo check --workspace --all-features --message-format=json 2>&1 | jq -r 'select(.reason=="compiler-message" and .message.level=="warning") | .message.rendered' | head -50

# === Documentation ===

# 📚 Build and open documentation
docs:
    @echo "📚 Building documentation..."
    cargo doc --workspace --no-deps --open

# 📖 Build documentation (no open)
doc:
    @echo "📖 Building documentation..."
    cargo doc --workspace --no-deps

# === Utilities ===

# 🧹 Clean build artifacts
clean:
    @echo "🧹 Cleaning build artifacts..."
    cargo clean

# 🔍 Search for TODO comments
todos:
    @echo "🔍 Searching for TODOs..."
    rg "TODO|FIXME|HACK|XXX" --type rust

# 📊 Code statistics
stats:
    @echo "📊 Code statistics:"
    @tokei

# 🌳 Show project structure
tree:
    @echo "🌳 Project structure:"
    @tree -I 'target|.git|*.pyc|__pycache__|.sqlx' -L 3

# 🏥 Health check
health:
    @echo "🏥 Running health checks..."
    @echo "Checking database connection..."
    @psql -c "SELECT 1" >/dev/null 2>&1 && echo "✅ Database: OK" || echo "❌ Database: FAILED"
    @echo "Checking Redis connection..."
    @redis-cli ping >/dev/null 2>&1 && echo "✅ Redis: OK" || echo "❌ Redis: FAILED"
    @echo "Checking compilation..."
    @cargo check --workspace >/dev/null 2>&1 && echo "✅ Compilation: OK" || echo "❌ Compilation: FAILED"

# === Continuous Development ===

# 👁️ Watch for changes and re-run checks
watch:
    @echo "👁️ Watching for changes..."
    bacon

# 👁️ Watch specific crate
watchc CRATE:
    @echo "👁️ Watching {{CRATE}} for changes..."
    bacon --path crate/{{CRATE}}

# 🔄 Watch and run tests
watch-test:
    @echo "🔄 Watching and running tests..."
    bacon test

# === Coverage ===

# 📊 Generate coverage report
coverage:
    @echo "📊 Generating coverage report..."
    cargo tarpaulin --workspace --out Html --output-dir target/coverage

# 📈 Open coverage report
coverage-open: coverage
    @echo "📈 Opening coverage report..."
    open target/coverage/index.html || xdg-open target/coverage/index.html

# === Maintenance ===

# 🔄 Update dependencies
update:
    @echo "🔄 Updating dependencies..."
    cargo update

# 🔍 Check for outdated dependencies
outdated:
    @echo "🔍 Checking for outdated dependencies..."
    cargo outdated

# 🛡️ Security audit
audit:
    @echo "🛡️ Running security audit..."
    cargo audit

# 📦 Check unused dependencies
unused:
    @echo "📦 Checking unused dependencies..."
    cargo machete

# === Performance ===

# 🏃 Run benchmarks
bench:
    @echo "🏃 Running benchmarks..."
    cargo bench --workspace

# 🔥 Profile with flamegraph
flamegraph BINARY:
    @echo "🔥 Profiling {{BINARY}} with flamegraph..."
    cargo flamegraph --bin {{BINARY}}

# === Help ===

# ❓ Show detailed help for a command
help COMMAND:
    @just --show {{COMMAND}}

