#!/usr/bin/env bash
set -euo pipefail

# Simple test runner with optional arguments
cargo test --all-features "$@"