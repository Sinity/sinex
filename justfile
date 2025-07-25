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
    @echo "  just test-performance - Performance/stress tests"
    @echo "  just test-services - Test new service layer functionality"
    @echo "  just test-core   - Core functionality tests (db, ulid, events)"
    @echo "  just test-individual FILE - Run specific test file"
    @echo "  just test-timeout SECONDS - Run tests with custom timeout"
    @echo "  just test-strict - Run tests with strict 2-minute timeout"
    @echo "  just test-dev    - Quick development cycle (under 2 minutes)"
    @echo "  just test-clean  - Clean test artifacts and reset environment"
    @echo "  just test-results - Show test results summary"
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
    @echo "  just db-setup    - Setup test database"
    @echo "  just db-reset    - Reset test database"
    @echo "  just db-clean    - Clean test database"
    @echo ""
    @echo "📋 Schema Management:"
    @echo "  just schema-generate - Generate JSON schemas from Rust code"
    @echo "  just schema-validate - Validate all schemas"
    @echo "  just schema-deploy   - Deploy schemas to database"
    @echo "  just schema-check    - Check backward compatibility"
    @echo ""
    @echo "▶️  Services:"
    @echo "  just collector   - Run unified collector"
    @echo "  just host        - Run sinex-gateway RPC server"
    @echo "  just canonicalizer - Run terminal command canonicalizer"
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
    cargo nextest run -E "test(unit::)" -- {{ARGS}}

# 📦 Unit tests with limited parallelism - Reliable test execution
test-unit-reliable *ARGS:
    @echo "📦 Running unit tests with limited parallelism for reliability..."
    cargo nextest run -E "test(unit::)" -j 2 -- {{ARGS}}

# 🔗 Integration tests - Component interaction tests (~30s)
test-integration *ARGS:
    @echo "🔗 Running integration tests (component interactions)..."
    cargo nextest run -E "test(integration::)" -- {{ARGS}}

# 🌐 System tests - Full pipeline E2E tests (~2min)
test-system *ARGS:
    @echo "🌐 Running system tests (full pipeline E2E)..."
    cargo nextest run -E "test(system::)" -- {{ARGS}}

# 💪 Stress tests - Load and performance tests (~1min)
test-stress *ARGS:
    @echo "💪 Running stress tests (load and performance)..."
    cargo nextest run -E "test(stress_test)" -- {{ARGS}}

# 🎲 Property-based tests - Randomized edge case testing (~1min)
test-property *ARGS:
    @echo "🎲 Running property-based tests (randomized edge cases)..."
    cargo nextest run -E "test(property::)" -- {{ARGS}}

# 🚀 Performance tests - Stress, load, and performance testing (~2min)
test-performance *ARGS:
    @echo "🚀 Running performance tests (stress, load, performance)..."
    cargo nextest run -E "test(performance::)" -- {{ARGS}}

# ⚔️  Adversarial tests - Security and chaos testing (~3min)
test-adversarial *ARGS:
    @echo "⚔️ Running adversarial tests (security and chaos scenarios)..."
    cargo nextest run -E "test(adversarial::)" -- {{ARGS}}

# ⚡ Fast tests only - Unit + property tests for quick feedback (~30s)
test-fast *ARGS:
    @echo "⚡ Running fast tests only (unit + property)..."
    cargo nextest run -E "test(unit::) or test(property::)" -- {{ARGS}}

# 🧪 Test new service layer functionality
test-services *ARGS:
    @echo "🧪 Testing service layer functionality..."
    cargo test -p sinex-services -- {{ARGS}}
    cargo test -p sinex-gateway -- {{ARGS}}

# 🔧 Test core functionality (db, ulid, events)
test-core *ARGS:
    @echo "🔧 Testing core functionality..."
    cargo nextest run -E "test(integration::database_test) or test(integration::event_sources_test)" -- {{ARGS}}

# 🎯 Run specific tests with custom filter
test *ARGS:
    @echo "🎯 Running tests with filter: {{ARGS}}"
    cargo nextest run -- {{ARGS}}

# 📁 Run specific test file or module
test-individual FILE *ARGS:
    @echo "📁 Running test file: {{FILE}}"
    cargo nextest run -E "test({{FILE}})" -- {{ARGS}}

# ⏱️ Run tests with custom timeout (in seconds)
test-timeout SECONDS *ARGS:
    @echo "⏱️ Running tests with {{SECONDS}}s timeout..."
    NEXTEST_PROFILE=default cargo nextest run --config 'profile.default.slow-timeout.period="{{SECONDS}}s"' -- {{ARGS}}

# 👀 Watch tests - Re-run tests on file changes
watch *ARGS:
    @echo "👀 Watching for changes, running tests with filter: {{ARGS}}"
    cargo watch -x "nextest run -- {{ARGS}}"

# ⚡👀 Watch fast tests only - Re-run unit + property tests on changes
watch-fast *ARGS:
    @echo "⚡👀 Watching for changes, running fast tests only..."
    cargo watch -x "nextest run -E 'test(unit::) or test(property::)' -- {{ARGS}}"

# 🥓 Run bacon for background compilation (for human users with TUI)
bacon *ARGS:
    @echo "🥓 Starting bacon (background rust checker)..."
    @echo "Press 'h' for help, 'q' to quit"
    @echo "Note: This is a TUI tool - better for human users than AI assistants"
    bacon {{ARGS}}

# 🚦 Pre-edit status check (call before making changes)
pre-edit:
    @echo "📸 Capturing pre-edit state..."
    @echo ""
    @echo "📝 Recent changes:"
    @git status --short | head -10 || true

# 🎯 Post-edit check (call after changes to see impact)
post-edit:
    @echo "🔍 Checking impact of changes..."
    @just check

# 📋 Check continuously and save to log (better for AI/scripts)
check-continuous:
    @echo "📋 Starting continuous compilation check..."
    @echo "Output will be saved to compilation-watch.log"
    cargo watch -x "check --workspace --all-targets --message-format short" 2>&1 | tee compilation-watch.log

# 🔄 Watch and report errors/warnings
watch-errors:
    @echo "🔄 Watching for compilation errors..."
    cargo watch -s 'just check-all && just errors || just errors'

# 👀 Watch and compile only (no tests)
watch-check:
    @echo "👀 Watching for compilation..."
    cargo watch -x check

# 👀 Watch and run specific command
watch-cmd CMD:
    @echo "👀 Watching and running: {{CMD}}"
    cargo watch -x "{{CMD}}"

# 🔧 Run tests with limited parallelism (for flaky tests)
test-reliable *ARGS:
    @echo "🔧 Running tests with limited parallelism for reliability..."
    cargo nextest run -j 2 -- {{ARGS}}

# 🔍 Run tests with verbose output
test-verbose *ARGS:
    @echo "🔍 Running tests with verbose output..."
    cargo nextest run --success-output immediate-final -- {{ARGS}}

# 🎯 Run tests matching a pattern
test-pattern PATTERN *ARGS:
    @echo "🎯 Running tests matching pattern: {{PATTERN}}"
    cargo nextest run -E "test(~{{PATTERN}})" -- {{ARGS}}

# 🏃 Run tests with fast profile (60s timeout, 4 threads)
test-fast-profile *ARGS:
    @echo "🏃 Running tests with fast profile (60s timeout, 4 threads)..."
    cargo nextest run -P fast -- {{ARGS}}

# 🚀 Run tests with maximum parallelism for speed
test-parallel *ARGS:
    @echo "🚀 Running tests with maximum parallelism ($(nproc) cores)..."
    cargo nextest run --profile parallel -- {{ARGS}}

# 🏃 Run all tests in parallel with optimized settings
test-all-parallel:
    @echo "🏃 Running all tests in parallel for maximum speed..."
    @echo "Using $(nproc) CPU cores for test execution"
    cargo nextest run --profile parallel

# 📊 Run tests and show parallelism statistics
test-parallel-stats *ARGS:
    @echo "📊 Running tests with parallelism statistics..."
    NEXTEST_EXPERIMENTAL_LIBTEST_JSON=1 cargo nextest run --profile parallel --reporter libtest-json -- {{ARGS}} | jq -r 'select(.type == "suite") | "\(.event): \(.exec_time // 0)s' || cargo nextest run --profile parallel -- {{ARGS}}

# 🛡️ Run tests with reliable profile (180s timeout, 2 threads, 3 retries)
test-reliable-profile *ARGS:
    @echo "🛡️ Running tests with reliable profile (180s timeout, 2 threads, 3 retries)..."
    cargo nextest run -P reliable -- {{ARGS}}

# 🐛 Run tests with debug profile (300s timeout, 1 thread, full output)
test-debug-profile *ARGS:
    @echo "🐛 Running tests with debug profile (300s timeout, 1 thread, full output)..."
    cargo nextest run -P debug -- {{ARGS}}

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

# 🔄 Reset test database (for integration tests)
db-reset:
    @echo "🔄 Resetting test database..."
    dropdb --if-exists sinex_test
    createdb sinex_test
    DATABASE_URL="postgresql:///sinex_test?host=/run/postgresql" sqlx migrate run

# 🧪 Setup test database (create if not exists)
db-setup:
    @echo "🧪 Setting up test database..."
    createdb sinex_test 2>/dev/null || true
    DATABASE_URL="postgresql:///sinex_test?host=/run/postgresql" sqlx migrate run

# 🧹 Clean test database
db-clean:
    @echo "🧹 Cleaning test database..."
    dropdb --if-exists sinex_test

# === Schema Management ===

# 🔨 Generate JSON schemas from Rust structs
schema-generate:
    @echo "🔨 Generating JSON schemas from Rust code..."
    cargo run --package sinex-events --bin generate-schemas
    @echo "✅ Schemas generated. Run 'just schema-diff' to see changes"

# 🔍 Validate all JSON schemas
schema-validate:
    @echo "🔍 Validating JSON schemas..."
    ./scripts/schema-dev.sh validate

# 🚀 Deploy schemas to local database
schema-deploy:
    @echo "🚀 Deploying schemas to database..."
    ./scripts/deploy-schemas.sh

# 🔄 Check backward compatibility against master
schema-check BRANCH="master":
    @echo "🔄 Checking schema compatibility against {{BRANCH}}..."
    ./scripts/check-schema-compatibility.sh {{BRANCH}}

# 📊 Show uncommitted schema changes
schema-diff:
    @echo "📊 Schema changes:"
    ./scripts/schema-dev.sh diff

# 📈 Show schema statistics
schema-stats:
    @echo "📈 Schema statistics:"
    ./scripts/schema-dev.sh stats

# 🔄 Full schema workflow: generate, validate, check compatibility
schema-workflow: schema-generate schema-validate schema-check
    @echo "✅ Schema workflow complete"

# === Running Services ===

# 🖥️  Run sinex-gateway RPC server for CLI/browser integration
host *ARGS:
    @echo "🖥️ Starting sinex-gateway RPC server..."
    cargo run --bin sinex-gateway rpc-server {{ARGS}}

# 📥 Run sinex-ingestd gRPC server (satellite coordinator)
ingestd *ARGS:
    @echo "📥 Starting sinex-ingestd gRPC server..."
    cargo run --bin sinex-ingestd {{ARGS}}

# 🗂️  Run filesystem watcher satellite
fs-watcher *ARGS:
    @echo "🗂️ Starting filesystem watcher satellite..."
    cargo run --bin sinex-fs-watcher {{ARGS}}

# 🖥️  Run desktop events satellite
desktop *ARGS:
    @echo "🖥️ Starting desktop events satellite..."
    cargo run --bin sinex-desktop-satellite {{ARGS}}

# 💻 Run terminal events satellite
terminal *ARGS:
    @echo "💻 Starting terminal events satellite..."
    cargo run --bin sinex-terminal-satellite {{ARGS}}

# ⚙️  Run system events satellite  
system *ARGS:
    @echo "⚙️ Starting system events satellite..."
    cargo run --bin sinex-system-satellite {{ARGS}}

# 🔧 Run terminal command canonicalizer
canonicalizer *ARGS:
    @echo "🔧 Starting terminal command canonicalizer..."
    cargo run --bin sinex-terminal-command-canonicalizer {{ARGS}}

# 📊 Run health aggregator
health *ARGS:
    @echo "📊 Starting health aggregator..."
    cargo run --bin sinex-health-aggregator {{ARGS}}

# ✅ Run preflight verification
preflight *ARGS:
    @echo "✅ Running preflight verification..."
    cargo run --bin sinex-preflight {{ARGS}}

# 🔍 Query recent events from database
query LIMIT="10" *ARGS:
    @echo "🔍 Querying {{LIMIT}} recent events..."
    python3 ./cli/exo.py query --limit {{LIMIT}} {{ARGS}}

# === Building ===

# ⚡ Fast compilation check (no binary output)
check:
    @echo "⚡ Running fast compilation check..."
    cargo check --workspace --all-features 2>&1 | tee compilation.log
    @echo "✅ Output saved to compilation.log"

# 🔍 Check with all targets (includes tests)
check-all:
    @echo "🔍 Checking all targets including tests..."
    cargo check --workspace --all-targets --all-features 2>&1 | tee compilation-all.log
    @echo "✅ Output saved to compilation-all.log"

# 📋 Check specific package
check-pkg PKG:
    @echo "📋 Checking package: {{PKG}}"
    cargo check -p {{PKG}} --all-targets 2>&1 | tee compilation-{{PKG}}.log
    @echo "✅ Output saved to compilation-{{PKG}}.log"

# 🔨 Build debug binaries
build:
    @echo "🔨 Building debug binaries..."
    cargo build --workspace --all-features 2>&1 | tee build.log
    @echo "✅ Output saved to build.log"

# 🚀 Build optimized release binaries
release:
    @echo "🚀 Building release binaries (optimized)..."
    cargo build --release --workspace --all-features 2>&1 | tee build-release.log
    @echo "✅ Output saved to build-release.log"

# 📊 Analyze compilation errors from bacon output
errors:
    @echo "📊 Analyzing compilation errors from bacon..."
    @# Simple wait for bacon to finish
    @if [ -f .claude-outputs/bacon.log ]; then \
        count=0; \
        while find .claude-outputs/bacon.log -newermt '-2 seconds' 2>/dev/null | grep -q . && [ $$count -lt 10 ]; do \
            echo "⏳ Waiting for bacon to finish..."; \
            sleep 1; \
            count=$$\(\(count + 1\)\); \
        done; \
        errors=$$(rg -c 'error\[E[0-9]+\]:' .claude-outputs/bacon.log 2>/dev/null || echo 0); \
        echo "Found $$errors errors"; \
        echo ""; \
        echo "Error summary:"; \
        rg 'error\[E[0-9]+\]:' .claude-outputs/bacon.log | tail -20 | sort | uniq -c || echo "No errors found"; \
    else \
        echo "⚠️  No bacon.log found. Is sinex-devtools running? Try 'sinex-attach'"; \
    fi

# 🔍 Show compilation warnings from bacon
warnings:
    @echo "🔍 Showing compilation warnings from bacon..."
    @# Simple wait for bacon to finish
    @if [ -f .claude-outputs/bacon.log ]; then \
        count=0; \
        while find .claude-outputs/bacon.log -newermt '-2 seconds' 2>/dev/null | grep -q . && [ $$count -lt 10 ]; do \
            echo "⏳ Waiting for bacon to finish..."; \
            sleep 1; \
            count=$$\(\(count + 1\)\); \
        done; \
        warnings=$$(rg -c 'warning:' .claude-outputs/bacon.log 2>/dev/null || echo 0); \
        echo "Found $$warnings warnings"; \
        echo ""; \
        rg 'warning:' .claude-outputs/bacon.log | tail -20 || echo "No warnings found"; \
    else \
        echo "⚠️  No bacon.log found. Is sinex-devtools running? Try 'sinex-attach'"; \
    fi

# 📊 Analyze warnings by category from bacon
warnings-summary:
    @echo "📊 Warning summary from bacon:"
    @# Wait for bacon to finish
    @while [ -f .claude-outputs/bacon.log ] && find .claude-outputs/bacon.log -newermt '-2 seconds' 2>/dev/null | grep -q .; do \
        echo "⏳ Waiting for bacon..."; \
        sleep 1; \
    done
    @if [ -f .claude-outputs/bacon.log ]; then \
        echo "Total warnings: $(rg -c 'warning:' .claude-outputs/bacon.log || echo 0)"; \
        echo ""; \
        echo "By type:"; \
        rg 'warning:' .claude-outputs/bacon.log | sed 's/warning: //' | cut -d' ' -f1-3 | sort | uniq -c | sort -rn | head -20; \
    else \
        echo "⚠️  No bacon.log found. Is sinex-devtools running? Try 'sinex-attach'"; \
    fi

# 🧹 Fix auto-fixable warnings
fix-warnings:
    @echo "🧹 Fixing auto-fixable warnings..."
    cargo fix --workspace --allow-dirty --allow-staged 2>&1 | tee fix.log
    @echo "✅ Fixed warnings saved to fix.log"

# 🚫 Show unused code
unused:
    @echo "🚫 Finding unused code..."
    @if [ -f compilation.log ]; then \
        rg 'warning:.*never used' compilation.log | head -30 || echo "No unused code warnings"; \
    else \
        echo "⚠️  No compilation.log found. Run 'just check' first."; \
    fi

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

# 🚀 Coverage for performance tests only
coverage-performance:
    @echo "🚀 Generating coverage for performance tests..."
    cargo llvm-cov --workspace --all-features -E "test(performance::)" --html

# 📊 Coverage for fast tests only
coverage-fast:
    @echo "📊 Generating coverage for fast tests..."
    cargo llvm-cov --workspace --all-features -E "test(unit::) or test(property::)" --html

# 📈 Coverage report with timeout
coverage-timeout SECONDS:
    @echo "📈 Generating coverage with {{SECONDS}}s timeout..."
    NEXTEST_PROFILE=default cargo llvm-cov --workspace --all-features --html --config 'profile.default.slow-timeout.period="{{SECONDS}}s"'

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

# 🚀 Test workflow with database setup
test-with-db: db-setup test-fast

# 🔄 Full test cycle with database reset
test-full-cycle: db-reset test-unit test-integration

# 🎯 Quick test for specific functionality
test-quick PATTERN: 
    @echo "🎯 Quick test for: {{PATTERN}}"
    just test-fast-profile {{PATTERN}}

# 🧹 Clean test artifacts and reset environment
test-clean:
    @echo "🧹 Cleaning test artifacts..."
    rm -rf target/nextest/
    rm -rf target/llvm-cov/
    just db-clean

# 📊 Show test results summary (if exists)
test-results:
    @echo "📊 Test results summary:"
    @if [ -f target/nextest/default/junit.xml ]; then echo "📄 JUnit results: target/nextest/default/junit.xml"; else echo "⚠️  No JUnit results found"; fi
    @if [ -d target/llvm-cov/html ]; then echo "📊 Coverage report: target/llvm-cov/html/index.html"; else echo "⚠️  No coverage report found"; fi

# 🧪 Run tests with strict 2-minute timeout (for user constraint)
test-strict:
    @echo "🧪 Running tests with strict 2-minute timeout..."
    timeout 120 just test-fast-profile || echo "⚠️  Tests exceeded 2-minute limit"

# 🔄 Quick development test cycle (under 2 minutes)
test-dev: 
    @echo "🔄 Quick development test cycle..."
    just db-setup
    just test-fast

# === Development Helpers ===

# 🔄 Quick compile and test cycle for a package
dev-pkg PKG:
    @echo "🔄 Development cycle for {{PKG}}..."
    just check-pkg {{PKG}}
    just test-fast -p {{PKG}}


# 🏃 Quick check - check bacon compilation status
qc:
    @echo "🏃 Checking compilation status from bacon..."
    @# Simple wait for bacon to settle
    @if [ -f .claude-outputs/bacon.log ]; then \
        count=0; \
        while find .claude-outputs/bacon.log -newermt '-2 seconds' 2>/dev/null | grep -q . && [ $$count -lt 5 ]; do \
            echo "⏳ Bacon is compiling..."; \
            sleep 1; \
            count=$$\(\(count + 1\)\); \
        done; \
        if tail -50 .claude-outputs/bacon.log | rg -q "error\[E[0-9]+\]:"; then \
            echo "❌ Compilation failed with errors:"; \
            tail -50 .claude-outputs/bacon.log | rg "error\[E[0-9]+\]:" | head -5; \
            echo ""; \
            echo "Run 'just errors' for full error details"; \
        elif tail -50 .claude-outputs/bacon.log | rg -q "warning:"; then \
            echo "⚠️  Compilation succeeded with warnings"; \
            echo "Run 'just warnings' for details"; \
        elif tail -20 .claude-outputs/bacon.log | rg -q "(✓|success|compiled|Finished)"; then \
            echo "✅ Compilation successful"; \
        else \
            echo "🔍 Compilation status unclear. Showing recent output:"; \
            tail -10 .claude-outputs/bacon.log; \
        fi; \
    else \
        echo "⚠️  No bacon.log found. Is sinex-devtools running? Try 'sinex-attach'"; \
    fi

# 🔍 Check and show errors immediately from bacon
ce:
    @echo "🔍 Checking for errors from bacon..."
    @just qc > /dev/null 2>&1 || true
    @just errors

# 🧪 Test with timeout safety (prevents hanging tests)
test-safe PKG="" TIMEOUT="60":
    @echo "🧪 Running tests with {{TIMEOUT}}s safety timeout..."
    @if [ -z "{{PKG}}" ]; then \
        timeout {{TIMEOUT}} cargo nextest run || echo "⚠️  Tests exceeded {{TIMEOUT}}s timeout"; \
    else \
        timeout {{TIMEOUT}} cargo nextest run -p {{PKG}} || echo "⚠️  Tests exceeded {{TIMEOUT}}s timeout"; \
    fi

# 📝 Show recent changes to help understand context
recent-changes:
    @echo "📝 Recent git changes:"
    git log --oneline -10
    @echo ""
    @echo "📝 Modified files:"
    git status --short

# 🔍 Find specific error type
find-error PATTERN:
    @echo "🔍 Finding error: {{PATTERN}}"
    @if [ -f compilation.log ]; then \
        rg -A5 -B2 "{{PATTERN}}" compilation.log || echo "Pattern not found"; \
    else \
        echo "⚠️  No compilation.log found. Run 'just check' first."; \
    fi

# 🎯 Check specific file
check-file FILE:
    @echo "🎯 Checking file: {{FILE}}"
    rustc --edition 2021 --crate-type lib --emit metadata {{FILE}} 2>&1 | tee check-file.log

# 💡 Show hints for common errors
hints:
    @echo "💡 Common error hints:"
    @echo "  - 'trait bound not satisfied': Check imports and trait implementations"
    @echo "  - 'cannot move out of': Use .clone() or references"
    @echo "  - 'ambiguous name': Use :: prefix or explicit imports"
    @echo "  - 'not a future': Add 'async' or use block_on"
    @echo "  - 'unused': Prefix with _ or add #[allow(unused)]"

# 📦 Test specific package with nextest
test-pkg PKG *ARGS:
    @echo "📦 Testing package: {{PKG}}"
    cargo nextest run -p {{PKG}} {{ARGS}}

# 🔍 Find TODOs and FIXMEs in the code
todos:
    @echo "🔍 Finding TODOs and FIXMEs..."
    rg -n "TODO|FIXME|HACK|XXX" --type rust | head -20

# 📊 Show crate dependencies
deps PKG="":
    @if [ -z "{{PKG}}" ]; then \
        echo "📊 Workspace dependencies:"; \
        cargo tree --workspace; \
    else \
        echo "📊 Dependencies for {{PKG}}:"; \
        cargo tree -p {{PKG}}; \
    fi

# 🔍 Search for a pattern in Rust files
search PATTERN:
    @echo "🔍 Searching for: {{PATTERN}}"
    rg "{{PATTERN}}" --type rust

# 📋 List all test functions
list-tests:
    @echo "📋 Listing all test functions..."
    rg "^\s*(#\[test\]|#\[sinex_test\])" --type rust -A 1 | grep -E "^.*\.rs-\s*(async )?fn" | sed 's/.*fn //' | sed 's/(.*//' | sort | uniq

# === Development Workflow Commands ===

# 🚀 Start development session - check environment
dev-start:
    @echo "🚀 Starting development session..."
    @echo "✓ Checking database connection..."
    @psql "${DATABASE_URL:-postgresql:///sinex_dev?host=/run/postgresql}" -c "SELECT 1" > /dev/null 2>&1 && echo "✓ Database OK" || echo "✗ Database not available"
    @echo "✓ Checking Redis..."
    @redis-cli ping > /dev/null 2>&1 && echo "✓ Redis OK" || echo "✗ Redis not available"
    @echo ""
    @echo "🤖 Compilation handled by sinex-devtools user service"
    @echo ""
    @echo "📋 Quick commands:"
    @echo "  just tf        - Run fast tests"
    @echo "  just tp PKG    - Test package"
    @echo "  just check     - Quick compilation check"
    @echo "  just errors    - Show current errors"
    @echo ""

# 📊 Show current nextest status/results
test-status:
    @echo "📊 Latest test results:"
    @if [ -f target/nextest/default/junit.xml ]; then \
        echo "Found test results from $(stat -c %y target/nextest/default/junit.xml 2>/dev/null || stat -f "%Sm" target/nextest/default/junit.xml)"; \
        echo ""; \
        xmllint --xpath "//testsuite/@tests | //testsuite/@failures | //testsuite/@errors" target/nextest/default/junit.xml 2>/dev/null | sed 's/tests=/Total: /; s/failures=/Failed: /; s/errors=/Errors: /' || echo "Unable to parse results"; \
    else \
        echo "No test results found. Run tests first."; \
    fi

# 🏃 Run tests in background and notify when done
test-bg PKG="" NOTIFY="echo":
    @echo "🏃 Starting background tests..."
    @if [ -z "{{PKG}}" ]; then \
        (cargo nextest run && {{NOTIFY}} "✅ Tests passed!" || {{NOTIFY}} "❌ Tests failed!") & \
        echo "Tests running in background (PID: $!)"; \
    else \
        (cargo nextest run -p {{PKG}} && {{NOTIFY}} "✅ Tests passed!" || {{NOTIFY}} "❌ Tests failed!") & \
        echo "Tests for {{PKG}} running in background (PID: $!)"; \
    fi

# 🔄 Full development cycle - format, check, test
cycle PKG="":
    @if [ -z "{{PKG}}" ]; then \
        echo "🔄 Full development cycle..."; \
        just fmt; \
        just check-all; \
        just test-fast; \
    else \
        echo "🔄 Development cycle for {{PKG}}..."; \
        cargo fmt -p {{PKG}}; \
        just check-pkg {{PKG}}; \
        just test-pkg {{PKG}}; \
    fi


# === Aliases ===
alias t := test
alias c := check
alias ca := check-all
alias cp := check-pkg
alias b := build
alias w := watch
alias tf := test-fast
alias tu := test-unit
alias ti := test-integration
alias tp := test-pkg
alias e := errors
alias warn := warnings
alias ws := warnings-summary
alias fw := fix-warnings
alias ts := test-safe
alias rc := recent-changes
