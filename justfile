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
    @echo -e "\033[90mServices:\033[0m"
    @echo -e "  \033[1mingestd\033[0m     Central coordinator (gRPC)"
    @echo -e "  \033[1mmonitor\033[0m     Dev dashboard (mprocs UI)"
    @echo -e "  \033[1mquery\033[0m       Query recent events"
    @echo ""
    @echo -e "\033[90mEvent Satellites:\033[0m"
    @echo -e "  \033[1mfs-watcher\033[0m  File system changes"
    @echo -e "  \033[1mterminal\033[0m    Terminal commands"
    @echo -e "  \033[1mdesktop\033[0m     Clipboard/window events"
    @echo -e "  \033[1msystem\033[0m      D-Bus/systemd events"
    @echo ""
    @echo -e "\033[90mRun\033[0m \033[1mjust --list\033[0m \033[90mfor all $(( $(just --list 2>/dev/null | wc -l) - 1 )) commands\033[0m"

# === Testing ===

# 🧪 Run all tests (inline tests in library crates)
test *ARGS:
    @echo "🧪 Running all tests..."
    cargo test --workspace --lib -- {{ARGS}}

# 🎯 Run specific test with pattern matching
test-filter PATTERN *ARGS:
    @echo "🎯 Running tests matching: {{PATTERN}}"
    cargo test --workspace --lib {{PATTERN}} -- {{ARGS}}

# 👀 Watch and run tests continuously
test-watch:
    @echo "👀 Watching and running tests..."
    cargo watch -x "test --workspace --lib"

# 🛡️ Run tests with limited parallelism (for flaky tests)
test-serial *ARGS:
    @echo "🛡️ Running tests with limited parallelism..."
    cargo test --workspace --lib -- --test-threads=1 {{ARGS}}

# === VM Tests (separate test suite in test/*) ===

# 🖥️  VM tests - Run NixOS VM test suite
test-vm:
    @echo "🖥️ Running VM tests (separate test suite)..."
    @echo "Note: VM tests are in test/* and excluded from workspace"
    ./test/nixos-vm/run-vm-tests.sh -c smoke

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
    dropdb --if-exists --force sinex_dev
    createdb sinex_dev
    DATABASE_URL="postgresql:///sinex_dev?host=/run/postgresql" sqlx migrate run

# 🧪 Setup test database (create if not exists)
db-setup:
    @echo "🧪 Setting up test database..."
    createdb sinex_dev 2>/dev/null || true
    DATABASE_URL="postgresql:///sinex_dev?host=/run/postgresql" sqlx migrate run

# 🧹 Clean test database
db-clean:
    @echo "🧹 Cleaning test database..."
    dropdb --if-exists --force sinex_dev

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


# 📊 Show recent compilation errors
errors:
    @cargo check --workspace --all-targets 2>&1 | grep -E "^error\[E[0-9]+\]:|^error:" -A 3 | head -20 || echo "✅ No errors"

# 🔍 Show compilation warnings
warnings:
    @cargo check --workspace --all-targets 2>&1 | grep "^warning:" -A 2 | head -20 || echo "✅ No warnings"



# 🎨 Format all code with rustfmt
fmt:
    cargo fmt --all

# 📋 Lint code with clippy (enforce warnings as errors)
lint:
    @echo "📋 Linting with clippy..."
    cargo clippy --workspace --all-features -- -D warnings

# === Coverage ===


# 📊 Generate HTML coverage report with browser view
coverage-html:
    @echo "📊 Generating HTML coverage report..."
    cargo llvm-cov --workspace --all-features --html
    @echo "📊 Coverage report: target/llvm-cov/html/index.html"






# === Utilities ===

# 🔗 Install git hooks
install-hooks:
    @echo "🔗 Installing git hooks..."
    @git config core.hooksPath .githooks
    @chmod +x .githooks/pre-commit
    @echo "✅ Git hooks installed! Pre-commit will check formatting."

# 📊 Monitor development dashboard (attach to sinex-devtools)
monitor:
    @echo "📊 Attaching to development dashboard..."
    @echo "Press 'q' to detach, Ctrl+q to quit mprocs"
    @tmux attach-session -t sinex-mprocs || echo "⚠️  No session found. Is sinex-devtools running?"


# 🧹 Clean all build artifacts, caches, and logs
clean:
    @echo "🧹 Cleaning build artifacts and logs..."
    cargo clean
    rm -rf .claude-outputs/*.log
    rm -f compilation*.log build*.log fix.log check-file.log
    rm -rf target/nextest/
    rm -rf target/llvm-cov/
    @echo "✅ Cleaned build artifacts and logs"

# 📚 Generate and open documentation
docs:
    @echo "📚 Building documentation..."
    cargo doc --workspace --all-features --no-deps
    @echo "📚 Opening documentation in browser..."
    @open target/doc/sinex/index.html || xdg-open target/doc/sinex/index.html || echo "📚 Documentation at: target/doc/sinex/index.html"





# === Common Workflows ===


# ⚡ Quick development cycle - Format, check, and run tests
dev: fmt qc test

# 🚀 Pre-commit validation - Essential checks before committing
pre-commit: fmt lint qc test

# 🔄 CI-style validation - All tests except VM (for automation)
ci: fmt lint test

# 🚀 PR validation - Run same checks as CI would
pr-check:
    @echo "🚀 Running PR validation checks..."
    @echo "This runs the same checks that CI will run on your PR"
    just fmt
    just lint
    just qc
    just test-unit
    just test-integration
    @echo "✅ PR validation passed! Safe to push."


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


# 🔄 Quick development test cycle (under 2 minutes)
test-dev: 
    @echo "🔄 Quick development test cycle..."
    just db-setup
    just test-fast

# === Development Helpers ===

# 🏃 Quick compilation check
qc:
    @cargo check --workspace --all-targets

# 🎯 Quick check specific crate (much faster for single-crate work)
qcc CRATE:
    @cargo check -p {{CRATE}}

# 🧠 Smart check - only checks crates with changes
qcs:
    @./scripts/smart-check.sh

# 🥓 Run bacon for continuous checking
bacon:
    bacon


# 🤖 Get compilation status as JSON (for AI agents)
status-json:
    @cargo check --workspace --all-targets --message-format json 2>&1 | jq -s '{status: "completed", errors: [.[] | select(.reason == "compiler-message" and .message.level == "error")], warnings: [.[] | select(.reason == "compiler-message" and .message.level == "warning")]}'

# 🤖 Get errors and warnings as JSON (for AI agents)
errors-json:
    @cargo check --workspace --all-targets --message-format json 2>&1 | \
        jq -s '{errors: [.[] | select(.reason == "compiler-message" and .message.level == "error") | {file: .message.spans[0].file_name, line: .message.spans[0].line_start, message: .message.message}], warnings: [.[] | select(.reason == "compiler-message" and .message.level == "warning") | {file: .message.spans[0].file_name, line: .message.spans[0].line_start, message: .message.message}]}'

# 🔍 Check and show errors immediately
ce: qc errors







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
    @echo "📊 Dependencies{{PKG}}:"
    @if [ -z "{{PKG}}" ]; then \
        cargo tree --workspace; \
    else \
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






# === Aliases ===
alias t := test
alias b := build
# alias tf := test-fast  # TODO: Add test-fast target
# alias tu := test-unit  # TODO: Add test-unit target
# alias ti := test-integration  # TODO: Add test-integration target
alias tp := test-pkg
alias e := errors
alias w := warnings



