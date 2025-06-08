default:
    @just --list --unsorted

# Database
dev:
    ./script/db.sh dev

status:
    ./script/db.sh

prod:
    ./script/db.sh prod

reset:
    ./script/db.sh reset

psql:
    ./script/db.sh shell

# Testing  
test:
    DATABASE_URL="$(./script/db.sh get-url)" cargo test

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
    DATABASE_URL="$(./script/db.sh get-url)" sqlx migrate run

migrate-create NAME:
    DATABASE_URL="$(./script/db.sh get-url)" sqlx migrate add {{NAME}}

sqlx-prepare:
    DATABASE_URL="$(./script/db.sh get-url)" ./script/sqlx-prepare.sh

sqlx-check:
    DATABASE_URL="$(./script/db.sh get-url)" cargo sqlx prepare --workspace --check -- --all-targets --all-features

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

# Aliases
alias c := check
alias t := test
alias d := dev