default:
    @just --list --unsorted

# Testing  
test:
    cargo test

test-unit:
    cargo test --lib

test-integration:
    cargo test --test integration integration::

test-system:
    cargo test --test integration system::

test-dlq:
    cargo test --test integration ingestor::dlq_tests

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

# New test categories from reorganization
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

test-all:
    echo "🧪 Running comprehensive test suite..."
    just test
    echo "✅ Rust tests completed"
    nix develop --command python3 -m pytest test/cli/test_exo_cli.py -v
    echo "✅ CLI unit tests completed"
    just test-e2e-dry-run
    echo "✅ E2E dry-run tests completed"
    echo "🎉 All core tests passed!"

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

# Aliases
alias c := check
alias t := test
alias cov := coverage