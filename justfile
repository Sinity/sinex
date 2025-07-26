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

# 🧪 Run complete test suite (unit → integration → system → stress → property → adversarial → VM)
test-all:
    @echo "🧪 Running complete Sinex test suite..."
    @echo "This includes: unit → integration → system → stress → property → adversarial → VM"
    @echo "Expected duration: 10-15 minutes"
    @echo ""
    just test-unit
    just test-integration  
    just test-system
    just test-property
    just test-performance
    just test-adversarial
    just test-vm

# 📦 Unit tests - Fast isolated component tests (~5s)
test-unit *ARGS:
    @echo "📦 Running unit tests (fast, isolated components)..."
    cargo nextest run -E "test(unit::)" -- {{ARGS}}


# 🔗 Integration tests - Component interaction tests (~30s)
test-integration *ARGS:
    @echo "🔗 Running integration tests (component interactions)..."
    cargo nextest run -E "test(integration::)" -- {{ARGS}}

# 🌐 System tests - Full pipeline E2E tests (~2min)
test-system *ARGS:
    @echo "🌐 Running system tests (full pipeline E2E)..."
    cargo nextest run -E "test(system::)" -- {{ARGS}}


# 🎲 Property-based tests - Randomized edge case testing (~1min)
test-property *ARGS:
    @echo "🎲 Running property-based tests (randomized edge cases)..."
    cargo nextest run -E "test(property::)" -- {{ARGS}}

# 🚀 Performance tests - Load and performance testing (~2min)
test-performance *ARGS:
    @echo "🚀 Running performance tests..."
    cargo nextest run -E "test(performance::) or test(stress_test)" -- {{ARGS}}

# ⚔️  Adversarial tests - Security and chaos testing (~3min)
test-adversarial *ARGS:
    @echo "⚔️ Running adversarial tests (security and chaos scenarios)..."
    cargo nextest run -E "test(adversarial::)" -- {{ARGS}}

# ⚡ Fast tests only - Unit + property tests for quick feedback (~30s)
test-fast *ARGS:
    @echo "⚡ Running fast tests only (unit + property)..."
    @./scripts/test-analytics.sh -E "test(unit::) or test(property::)" -- {{ARGS}}



# 🎯 Run specific test file or pattern
test FILE="" *ARGS:
    @echo "🎯 Running tests: {{FILE}} {{ARGS}}"
    @if [ -n "{{FILE}}" ]; then \
        cargo nextest run -E "test({{FILE}})" -- {{ARGS}}; \
    else \
        cargo nextest run -- {{ARGS}}; \
    fi

# 📁 Test a specific file (finds tests in the file)
test-file FILE *ARGS:
    @echo "📁 Running tests in file: {{FILE}}"
    @# Convert file path to test pattern
    @if [[ "{{FILE}}" == *".rs" ]]; then \
        # Extract crate name and module path \
        if [[ "{{FILE}}" == crate/* ]]; then \
            crate=$$(echo "{{FILE}}" | cut -d/ -f2); \
            echo "Testing in crate: $$crate"; \
            cd "crate/$$crate" && cargo nextest run -- {{ARGS}}; \
        else \
            echo "Testing file: {{FILE}}"; \
            cargo nextest run -- {{ARGS}}; \
        fi; \
    else \
        echo "⚠️  Not a Rust file: {{FILE}}"; \
    fi





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

# Alias for host command
gateway *ARGS: (host ARGS)

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
    @just ai-errors

# 🔍 Show compilation warnings
warnings:
    @echo "🔍 Showing compilation warnings..."
    @if [ -f "$HOME/.sinex-compile-logs/last-result.json" ]; then \
        log=$(jq -r '.log' "$HOME/.sinex-compile-logs/last-result.json" 2>/dev/null); \
        if [ -n "$log" ] && [ -f "$log" ]; then \
            echo "Warnings: $(jq -r '.warnings // 0' "$HOME/.sinex-compile-logs/last-result.json")"; \
            jq -r 'select(.message.level == "warning") | "\(.message.spans[0].file_name // "unknown"):\(.message.spans[0].line_start // 0): \(.message.message)"' "$log" 2>/dev/null | head -10 || echo "No warnings"; \
        fi; \
    else \
        echo "No compilation results. Run 'just compile-start' first"; \
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

# 🖥️ Alias for monitor
mprocs: monitor

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



# 📦 Update all dependencies to latest compatible versions
update:
    @echo "📦 Updating dependencies..."
    cargo update

# 🚀 Show sccache statistics
cache-stats:
    @echo "🚀 sccache statistics:"
    @sccache --show-stats || echo "sccache not available"


# === Common Workflows ===

# 🎯 Run command in specific crate directory
in CRATE CMD *ARGS:
    @echo "🎯 Running '{{CMD}} {{ARGS}}' in {{CRATE}}..."
    @cd crate/{{CRATE}} && {{CMD}} {{ARGS}}

# ⚡ Quick development cycle - Format, check, and run fast tests
dev: fmt qc test-fast

# 🚀 Pre-commit validation - Essential checks before committing
pre-commit: fmt lint qc test-fast

# 🔄 CI-style validation - All tests except VM (for automation)
ci: fmt lint test-unit test-integration test-system test-property test-performance test-adversarial

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

# 🏃 Quick compilation check - show daemon status
qc:
    @./scripts/compile-daemon.sh status

# 🤖 AI Agent: Get compilation status as JSON (blocks until current sources compiled)
ai-status:
    @./scripts/compile-daemon.sh await-current

# 🤖 AI Agent: Get errors and warnings as JSON
ai-errors-json:
    @if [ -f ~/.sinex-compile-state/live-output.jsonl ]; then \
        echo '{"errors": ['; \
        jq -c 'select(.message.level == "error") | {file: .message.spans[0].file_name, line: .message.spans[0].line_start, message: .message.message}' \
            ~/.sinex-compile-state/live-output.jsonl 2>/dev/null | head -10 | sed '$ ! s/$/,/' || echo ''; \
        echo '], "warnings": ['; \
        jq -c 'select(.message.level == "warning") | {file: .message.spans[0].file_name, line: .message.spans[0].line_start, message: .message.message}' \
            ~/.sinex-compile-state/live-output.jsonl 2>/dev/null | head -10 | sed '$ ! s/$/,/' || echo ''; \
        echo ']}'; \
    else \
        echo '{"error": "No compilation data available"}'; \
    fi

# 🔍 Show compilation errors from last build (human readable)
ai-errors:
    @if [ -f ~/.sinex-compile-state/live-output.jsonl ]; then \
        jq -r 'select(.message.level == "error") | "\(.message.spans[0].file_name // "unknown"):\(.message.spans[0].line_start // 0): \(.message.message)"' \
            ~/.sinex-compile-state/live-output.jsonl 2>/dev/null | head -10 || echo "✅ No errors"; \
    else \
        echo "No compilation data. Background compilation daemon should be running."; \
    fi

# 🔍 Check and show errors immediately
ce: qc ai-errors






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






# === Environment Management ===

# 🧹 Clean sccache
cache-clean:
    @echo "🧹 Cleaning sccache..."
    @sccache --stop-server 2>/dev/null || true
    @rm -rf ~/.cache/sccache
    @echo "✅ sccache cleaned"

# === Aliases ===
alias t := test
alias b := build
alias tf := test-fast
alias tu := test-unit
alias ti := test-integration
alias tp := test-pkg
alias e := errors
alias w := warnings

# === Analytics Commands ===

# 📊 What changed since last build?
changes-since-build:
    @echo "📊 Changes since last successful build:"
    @if [ -f ~/.sinex-compile-state/last-build-snapshot.txt ]; then \
        echo "Files from last build vs now:"; \
        echo ""; \
        diff -u ~/.sinex-compile-state/last-build-snapshot.txt <(git status --porcelain) | grep -E "^[+-]" | head -20 || echo "No changes"; \
    else \
        echo "No build snapshot yet"; \
    fi

# 📊 Show recent compilations
analytics-recent:
    @echo "📊 Recent compilations:"
    @tail -20 ~/.sinex-analytics/compilations/index.csv 2>/dev/null | column -t -s'|' || echo "No compilation data yet"

# 📈 Show compilation statistics
analytics-stats:
    @echo "📈 Compilation statistics:"
    @if [ -f ~/.sinex-analytics/compilations/index.csv ]; then \
        total=$(wc -l < ~/.sinex-analytics/compilations/index.csv); \
        success=$(awk -F'|' '$4==0' ~/.sinex-analytics/compilations/index.csv | wc -l); \
        avg_time=$(awk -F'|' '{sum+=$3; count++} END {if(count>0) print int(sum/count) "ms"}' ~/.sinex-analytics/compilations/index.csv); \
        echo "  Total builds: $total"; \
        echo "  Successful: $success ($(( success * 100 / total ))%)"; \
        echo "  Average time: $avg_time"; \
    else \
        echo "No compilation data yet"; \
    fi

# 📸 Show git state snapshots
git-snapshots:
    @echo "📸 Recent git snapshots:"
    @git stash list | grep "auto-snapshot" | head -10 || echo "No snapshots yet"

# 🛑 Stop git state tracker
git-tracker-stop:
    @./scripts/git-state-tracker.sh stop

# 📊 Git tracker status
git-tracker-status:
    @./scripts/git-state-tracker.sh status
