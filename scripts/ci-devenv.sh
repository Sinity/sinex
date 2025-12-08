#!/usr/bin/env bash
set -euo pipefail

echo "📦 entering dev shell via nix develop..."
exec nix develop --accept-flake-config --no-pure-eval --command "$@"
