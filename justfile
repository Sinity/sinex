default:
    @just --list --unsorted

# Testing  
test:
    cargo test

test-unit:
    cargo test --lib

test-integration:
    cargo test --test integration

test-dlq:
    cargo test --test integration ingestor::dlq_tests

test-e2e:
    cargo test --test integration e2e:: -- --nocapture

test-e2e-full:
    cargo test --test integration e2e::full_system_test -- --ignored --nocapture

test-e2e-dry-run:
    cargo test --test integration e2e::test_full_system_dry_run -- --nocapture

test-cli:
    python3 -m pytest test/cli/test_exo_cli.py -v

test-cli-integration:
    python3 -m pytest test/cli/test_exo_cli_integration.py -v

test-cli-all:
    python3 -m pytest test/cli/ -v

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
    sqlx migrate run --source migration
    # Update the cache
    cargo sqlx prepare --workspace -- --all-targets --all-features
    echo "✅ SQLX cache updated successfully"
    echo "⚠️  Don't forget to commit the changes in .sqlx/"

sqlx-check:
    cargo sqlx prepare --workspace --check -- --all-targets --all-features

# Ingestors
filesystem *ARGS:
    nix run .#filesystemIngestor -- {{ARGS}}

kitty *ARGS:
    nix run .#kittyIngestor -- {{ARGS}}

hyprland *ARGS:
    nix run .#hyprlandIngestor -- {{ARGS}}

worker *ARGS:
    nix run .#sinexPromoWorker -- {{ARGS}}

# Run all ingestors in background
ingestors-start *ARGS:
    #!/usr/bin/env bash
    echo "Starting all ingestors in background..."
    nix run .#filesystemIngestor -- {{ARGS}} &
    nix run .#kittyIngestor -- {{ARGS}} &
    nix run .#hyprlandIngestor -- {{ARGS}} &
    nix run .#sinexPromoWorker -- {{ARGS}} &
    echo "All ingestors started. Use 'just ingestors-stop' to stop them."

# Stop all running ingestors
ingestors-stop:
    pkill -f "filesystem-ingestor" || true
    pkill -f "kitty-ingestor" || true
    pkill -f "hyprland-ingestor" || true
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

# Aliases
alias c := check
alias t := test