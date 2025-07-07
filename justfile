# Show available commands with descriptions
default:
    @echo "🚀 Sinex Development Commands"
    @echo "============================"
    @echo ""
    @echo "📋 Common Workflows:"
    @echo "  just dev         - Quick development cycle (fmt + check + fast tests)"
    @echo "  just pre-commit  - Pre-commit validation (fmt + lint + check + fast tests)"
    @echo "  just ci          - CI-style validation (all tests except VM)"
    @echo "  just validate    - Full validation including VM tests"
    @echo ""
    @echo "🧪 Testing (by speed/scope):"
    @echo "  just test-fast   - Fast tests only (~30s: unit + property)"
    @echo "  just test-unit   - Unit tests only"
    @echo "  just test-integration - Integration tests"
    @echo "  just test-system - System/E2E tests"
    @echo "  just test-all    - All tests including VM (~10-15min)"
    @echo ""
    @echo "🔧 Development:"
    @echo "  just check       - Fast compile check"
    @echo "  just build       - Full build"
    @echo "  just fmt         - Format code"
    @echo "  just lint        - Lint with clippy"
    @echo ""
    @echo "🗄️  Database:"
    @echo "  just migrate     - Run migrations"
    @echo "  just psql        - Connect to database"
    @echo "  just sqlx-prepare - Update SQLX cache (commit .sqlx/)"
    @echo ""
    @echo "▶️  Services:"
    @echo "  just collector   - Run unified collector"
    @echo "  just worker      - Run promotion worker"
    @echo "  just query       - Query recent events"
    @echo ""
    @echo "📊 Coverage:"
    @echo "  just coverage-html - Generate HTML coverage report"
    @echo ""
    @echo "Use 'just --list' for complete command list"

# === Testing ===

# 🧪 Run complete test suite (unit → integration → system → stress → property → adversarial → VM)
test-all:
    @echo "🧪 Running complete Sinex test suite..."
    @echo "This includes: unit → integration → system → stress → property → adversarial → VM"
    @echo "Expected duration: 10-15 minutes"
    @echo ""
    just test-unit
    just test-integration  
    just test-system
    just test-stress
    just test-property
    just test-adversarial
    just test-vm

# 📦 Unit tests - Fast isolated component tests (~5s)
test-unit *ARGS:
    @echo "📦 Running unit tests (fast, isolated components)..."
    cargo nextest run -E "test(unit::)" {{ARGS}}

# 📦 Unit tests with limited parallelism - Reliable test execution
test-unit-reliable:
    @echo "📦 Running unit tests with limited parallelism for reliability..."
    cargo nextest run -E "test(unit::)" -j 2

# 🔗 Integration tests - Component interaction tests (~30s)
test-integration *ARGS:
    @echo "🔗 Running integration tests (component interactions)..."
    cargo nextest run -E "test(integration::)" {{ARGS}}

# 🌐 System tests - Full pipeline E2E tests (~2min)
test-system *ARGS:
    @echo "🌐 Running system tests (full pipeline E2E)..."
    cargo nextest run -E "test(system::)" {{ARGS}}

# 💪 Stress tests - Load and performance tests (~1min)
test-stress *ARGS:
    @echo "💪 Running stress tests (load and performance)..."
    cargo nextest run -E "test(stress_test)" {{ARGS}}

# 🎲 Property-based tests - Randomized edge case testing (~1min)
test-property *ARGS:
    @echo "🎲 Running property-based tests (randomized edge cases)..."
    cargo nextest run -E "test(property::)" {{ARGS}}

# ⚔️  Adversarial tests - Security and chaos testing (~3min)
test-adversarial *ARGS:
    @echo "⚔️ Running adversarial tests (security and chaos scenarios)..."
    cargo nextest run -E "test(adversarial::)" {{ARGS}}

# ⚡ Fast tests only - Unit + property tests for quick feedback (~30s)
test-fast *ARGS:
    @echo "⚡ Running fast tests only (unit + property)..."
    cargo nextest run -E "test(unit::) or test(property::)" {{ARGS}}

# 🎯 Run specific tests with custom filter
test *ARGS:
    @echo "🎯 Running tests with filter: {{ARGS}}"
    cargo nextest run {{ARGS}}

# 👀 Watch tests - Re-run tests on file changes
watch *ARGS:
    @echo "👀 Watching for changes, running tests with filter: {{ARGS}}"
    cargo watch -x "nextest run {{ARGS}}"

# ⚡👀 Watch fast tests only - Re-run unit + property tests on changes
watch-fast *ARGS:
    @echo "⚡👀 Watching for changes, running fast tests only..."
    cargo watch -x "nextest run -E 'test(unit::) or test(property::)' {{ARGS}}"

# === VM Tests ===

# 🖥️  VM smoke tests - Basic VM functionality (~5min)
test-vm:
    @echo "🖥️ Running VM smoke tests (basic functionality)..."
    ./test/nixos-vm/run-vm-tests.sh -c smoke

# 🖥️  All VM tests - Complete VM test suite (~15min)
test-vm-all:
    @echo "🖥️ Running complete VM test suite..."
    ./test/nixos-vm/run-vm-tests.sh -c all

# 🐛 Debug specific VM test
test-vm-debug TEST="basic-flow":
    @echo "🐛 Debugging VM test: {{TEST}}"
    ./test/nixos-vm/run-vm-tests.sh -d {{TEST}}

# === Database ===

# 📊 Apply database migrations
migrate:
    @echo "📊 Running database migrations..."
    sqlx migrate run

# 📝 Create new database migration
migrate-create NAME:
    @echo "📝 Creating migration: {{NAME}}"
    sqlx migrate add {{NAME}}

# 🔌 Connect to development database
psql *ARGS:
    @echo "🔌 Connecting to database..."
    psql "${DATABASE_URL:-postgresql:///sinex_dev?host=/run/postgresql}" {{ARGS}}

# 💾 Update SQLX offline cache (required for Nix builds)
sqlx-prepare:
    @echo "💾 Updating SQLX offline cache..."
    sqlx migrate run
    cargo sqlx prepare --workspace -- --all-targets --all-features
    @echo "✅ SQLX cache updated"
    @echo "⚠️  IMPORTANT: Commit .sqlx/ directory for Nix builds!"

# ✅ Verify SQLX cache is up to date
sqlx-check:
    @echo "✅ Checking SQLX cache consistency..."
    cargo sqlx prepare --workspace --check -- --all-targets --all-features

# === Running Services ===

# 🚀 Run unified event collector
collector *ARGS:
    @echo "🚀 Starting unified event collector..."
    cargo run --bin sinex-collector {{ARGS}}

# ⚙️  Run promotion worker
worker *ARGS:
    @echo "⚙️ Starting promotion worker..."
    cargo run --bin sinex-promo-worker {{ARGS}}

# 🔍 Query recent events from database
query LIMIT="10" *ARGS:
    @echo "🔍 Querying {{LIMIT}} recent events..."
    python3 ./cli/exo.py query --limit {{LIMIT}} {{ARGS}}

# === Building ===

# ⚡ Fast compilation check (no binary output)
check:
    @echo "⚡ Running fast compilation check..."
    cargo check --workspace --all-features

# 🔨 Build debug binaries
build:
    @echo "🔨 Building debug binaries..."
    cargo build --workspace --all-features

# 🚀 Build optimized release binaries
release:
    @echo "🚀 Building release binaries (optimized)..."
    cargo build --release --workspace --all-features

# 🎨 Format all code with rustfmt
fmt:
    @echo "🎨 Formatting code..."
    cargo fmt --all

# 📋 Lint code with clippy (enforce warnings as errors)
lint:
    @echo "📋 Linting with clippy..."
    cargo clippy --workspace --all-features -- -D warnings

# === Coverage ===

# 📊 Generate test coverage report (terminal output)
coverage *ARGS:
    @echo "📊 Generating coverage report..."
    cargo llvm-cov --workspace --all-features {{ARGS}}

# 📊 Generate HTML coverage report with browser view
coverage-html:
    @echo "📊 Generating HTML coverage report..."
    cargo llvm-cov --workspace --all-features --html
    @echo "📊 Coverage report: target/llvm-cov/html/index.html"

# 📦 Coverage for unit tests only
coverage-unit:
    @echo "📦 Generating coverage for unit tests..."
    cargo llvm-cov --workspace --all-features -E "test(unit::)" --html

# 🔗 Coverage for integration tests only
coverage-integration:
    @echo "🔗 Generating coverage for integration tests..."
    cargo llvm-cov --workspace --all-features -E "test(integration::)" --html

# === Utilities ===

# 🧹 Clean all build artifacts and caches
clean:
    @echo "🧹 Cleaning build artifacts..."
    cargo clean

# 📦 Update all dependencies to latest compatible versions
update:
    @echo "📦 Updating dependencies..."
    cargo update

# === Common Workflows ===

# ⚡ Quick development cycle - Format, check, and run fast tests
dev: fmt check test-fast

# 🚀 Pre-commit validation - Essential checks before committing
pre-commit: fmt lint check test-fast

# 🔄 CI-style validation - All tests except VM (for automation)
ci: fmt lint check test-unit test-integration test-system test-stress test-property test-adversarial

# ✅ Full validation - Complete test suite including VM tests
validate: fmt lint check test-all

# === Aliases ===
alias t := test
alias c := check
alias b := build
alias w := watch