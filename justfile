default:
    @just --list --unsorted

# Testing  
test:
    cargo test

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
    ./script/sqlx-prepare.sh

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