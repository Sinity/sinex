#!/usr/bin/env bash
set -euo pipefail

echo "📦 entering dev shell via nix develop..."
nix develop --accept-flake-config --no-pure-eval --command "$@" \
  || { echo "❌ nix develop failed"; exit 1; }
echo "✅ dev shell ready"
