#!/usr/bin/env bash
# Run heavy / ignored tests (uses xtask wrapper)
set -euo pipefail

# Run from repo root. Uses direnv if available in developer environment.
ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

# Use direnv exec if available to load the developer environment; fall back to plain xtask.
if command -v direnv >/dev/null 2>&1; then
  direnv exec "$ROOT_DIR" xtask test:heavy --prime "$@"
else
  xtask test --include-ignored --prime "$@"
fi
