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
filesystem:
    nix run .#filesystemIngestor

kitty:
    nix run .#kittyIngestor

hyprland:
    nix run .#hyprlandIngestor

worker:
    nix run .#sinexPromoWorker

# Query
query LIMIT="10":
    @python3 ./cli/exo.py query --limit {{LIMIT}}

# Utilities
kill-ingestors:
    pkill -f "ingestor" || true

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