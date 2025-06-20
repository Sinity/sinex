default:
    @just --list --unsorted

# Testing  
test:
    cargo test

test-unit:
    cargo test --test integration unit::

test-integration:
    cargo test --test integration integration::

test-system:
    cargo test --test integration system::

test-dlq:
    cargo test --test integration ingestor::dlq_tests

# NixOS VM tests with enhanced runner
test-vm:
    ./test/nixos-vm/run-vm-tests.sh -c smoke

test-vm-interactive:
    ./test/nixos-vm/run-vm-tests.sh -d basic-flow

test-vm-quick:
    ./test/nixos-vm/run-vm-tests.sh basic-flow

# Advanced VM tests
test-vm-chaos:
    nix build .#checks.x86_64-linux.sinex-vm-chaos -L

test-vm-production:
    nix build .#checks.x86_64-linux.sinex-vm-production -L

test-vm-advanced:
    echo "🧪 Running advanced VM tests..."
    just test-vm-chaos
    echo "✅ Chaos engineering tests completed"
    just test-vm-production  
    echo "✅ Production scale tests completed"
    echo "🎉 Advanced VM tests passed!"

test-vm-all:
    ./test/nixos-vm/run-vm-tests.sh -c all

test-vm-parallel:
    ./test/nixos-vm/run-vm-tests.sh -c all -p

test-vm-debug TEST="basic-flow":
    ./test/nixos-vm/run-vm-tests.sh -d {{TEST}}

test-e2e:
    cargo test --test integration system::end_to_end:: -- --nocapture

test-e2e-full:
    cargo test --test integration system::end_to_end::full_pipeline_tests::test_full_system -- --ignored --nocapture

test-e2e-dry-run:
    cargo test --test integration system::end_to_end::full_pipeline_tests::test_full_system_dry_run -- --nocapture

test-cli:
    python3 -m pytest test/cli/test_exo_cli.py -v

test-cli-integration:
    python3 -m pytest test/cli/test_exo_cli_integration.py -v

test-cli-all:
    python3 -m pytest test/cli/ -v

# Focused test categories
test-core:
    cargo test --lib --workspace

test-database:
    cargo test --test integration integration::database::

test-adversarial:
    cargo test --test integration adversarial::

test-worker:
    cargo test --test integration integration::worker::

test-regression:
    cargo test --test integration system::regression::

# System integration tests
test-system-startup:
    cargo test --test integration integration::full_system_startup_test:: -- --nocapture

test-failure-recovery:
    cargo test --test integration integration::failure_recovery_integration_test:: -- --nocapture

test-health-monitoring:
    cargo test --test integration integration::health_monitoring_integration_test:: -- --nocapture

test-git-annex-integration:
    cargo test --test integration integration::git_annex_full_integration_test:: -- --nocapture

test-config-validation:
    cargo test --test integration integration::configuration_validation_integration_test:: -- --nocapture

# Comprehensive integration test suite
test-integration-comprehensive:
    echo "🧪 Running comprehensive integration test suite..."
    just test-system-startup
    echo "✅ System startup tests completed"
    just test-failure-recovery  
    echo "✅ Failure recovery tests completed"
    just test-health-monitoring
    echo "✅ Health monitoring tests completed"
    just test-config-validation
    echo "✅ Configuration validation tests completed"
    just test-git-annex-integration
    echo "✅ Git-annex integration tests completed"
    echo "🎉 All comprehensive integration tests passed!"

test-all:
    echo "🧪 Running comprehensive test suite..."
    just test
    echo "✅ Rust tests completed"
    just test-cli
    echo "✅ CLI tests completed"
    just test-e2e-dry-run
    echo "✅ E2E tests completed"
    echo "🎉 All core tests passed!"

test-full:
    echo "🧪 Running complete test suite..."
    just test-all
    echo "✅ Core test suite completed"
    just test-integration-comprehensive
    echo "✅ System integration tests completed"
    just test-vm-all
    echo "✅ VM tests completed"
    echo "🎉 All tests passed - system fully validated!"

watch:
    cargo watch -x test

# Build
check:
    cargo check --all-features

check-all:
    cargo check --all-features
    cargo clippy --all-features -- -D warnings

build:
    cargo build --all-features

release:
    cargo build --release --all-features

fmt:
    cargo fmt --all

# Migrations
migrate:
    sqlx migrate run

migrate-create NAME:
    sqlx migrate add {{NAME}}

sqlx-prepare:
    #!/usr/bin/env bash
    set -euo pipefail
    echo "🗄️  Updating SQLX offline cache..."
    # Ensure migrations are up to date
    sqlx migrate run
    # Update the cache
    cargo sqlx prepare --workspace -- --all-targets --all-features
    echo "✅ SQLX cache updated successfully"
    echo "⚠️  Don't forget to commit the changes in .sqlx/"

sqlx-check:
    cargo sqlx prepare --workspace --check -- --all-targets --all-features

# Ingestors
unified *ARGS:
    nix run .#unifiedCollector -- {{ARGS}}

worker *ARGS:
    nix run .#sinexPromoWorker -- {{ARGS}}

# Run all ingestors in background
ingestors-start *ARGS:
    #!/usr/bin/env bash
    echo "Starting all ingestors in background..."
    nix run .#unifiedCollector -- {{ARGS}} &
    nix run .#sinexPromoWorker -- {{ARGS}} &
    echo "All ingestors started. Use 'just ingestors-stop' to stop them."

# Stop all running ingestors
ingestors-stop:
    pkill -f "unified-collector" || true
    pkill -f "sinex-promo-worker" || true
    @echo "All ingestors stopped."

# Query
query LIMIT="10":
    @python3 ./cli/exo.py query --limit {{LIMIT}}


clean:
    cargo clean

update:
    cargo update

# Database utilities
psql:
    psql "$DATABASE_URL"

# Coverage
coverage:
    #!/usr/bin/env bash
    echo "🧪 Running tests with coverage..."
    export PATH="$(find /nix/store -name "cargo-llvm-cov" -type f 2>/dev/null | head -1 | xargs dirname):$PATH"
    export LLVM_COV="$(find /nix/store -name "llvm-cov" -type f 2>/dev/null | grep llvm-tools | head -1)"
    export LLVM_PROFDATA="$(find /nix/store -name "llvm-profdata" -type f 2>/dev/null | grep llvm-tools | head -1)"
    cargo llvm-cov --all-features --workspace --exclude-from-report="test/*" --exclude-from-report="**/tests/*"

coverage-html:
    #!/usr/bin/env bash
    echo "🧪 Generating HTML coverage report..."
    export PATH="$(find /nix/store -name "cargo-llvm-cov" -type f 2>/dev/null | head -1 | xargs dirname):$PATH"
    export LLVM_COV="$(find /nix/store -name "llvm-cov" -type f 2>/dev/null | grep llvm-tools | head -1)"
    export LLVM_PROFDATA="$(find /nix/store -name "llvm-profdata" -type f 2>/dev/null | grep llvm-tools | head -1)"
    cargo llvm-cov --all-features --workspace --exclude-from-report="test/*" --exclude-from-report="**/tests/*" --html
    echo "📊 Coverage report generated in target/llvm-cov/html/index.html"

coverage-lcov:
    #!/usr/bin/env bash
    echo "🧪 Generating LCOV coverage report..."
    export PATH="$(find /nix/store -name "cargo-llvm-cov" -type f 2>/dev/null | head -1 | xargs dirname):$PATH"
    export LLVM_COV="$(find /nix/store -name "llvm-cov" -type f 2>/dev/null | grep llvm-tools | head -1)"
    export LLVM_PROFDATA="$(find /nix/store -name "llvm-profdata" -type f 2>/dev/null | grep llvm-tools | head -1)"
    cargo llvm-cov --all-features --workspace --exclude-from-report="test/*" --exclude-from-report="**/tests/*" --lcov --output-path target/llvm-cov/coverage.lcov
    echo "📊 LCOV report generated in target/llvm-cov/coverage.lcov"

coverage-report: coverage-html
    @echo "📊 Opening coverage report..."
    xdg-open target/llvm-cov/html/index.html 2>/dev/null || echo "💡 Open target/llvm-cov/html/index.html in your browser"

# Fun / Analysis
fun:
    echo "🎯 Running entropy analysis and other fun tests..."
    cargo test ulid::analysis::entropy_analysis -- --ignored --nocapture
    echo "🎉 Analysis complete! Check the output above for mathematical insights."

# Aliases
alias c := check
alias t := test
alias cov := coverage