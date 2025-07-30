#!/usr/bin/env bash

# Script to check latest versions of dependencies

echo "Checking latest versions of dependencies..."

# Function to get latest version from crates.io
get_latest_version() {
    local crate=$1
    curl -s "https://crates.io/api/v1/crates/$crate" | jq -r '.crate.max_stable_version' 2>/dev/null || echo "unknown"
}

# Key dependencies to check
deps=(
    "tokio"
    "async-trait"
    "sqlx"
    "serde"
    "serde_json"
    "anyhow"
    "thiserror"
    "color-eyre"
    "serde_path_to_error"
    "tracing"
    "tracing-subscriber"
    "ulid"
    "uuid"
    "chrono"
    "axum"
    "rand"
    "once_cell"
    "blake3"
    "bon"
    "tonic"
    "prost"
    "redis"
    "notify"
    "clap"
    "regex"
    "validator"
    "figment"
    "rusqlite"
    "jsonschema"
    "gethostname"
    "which"
)

for dep in "${deps[@]}"; do
    version=$(get_latest_version "$dep")
    echo "$dep: $version"
done