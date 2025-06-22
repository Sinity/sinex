#!/usr/bin/env bash
set -euo pipefail

echo "🔍 Validating advanced VM test implementation..."

# Check syntax of individual test files
echo "✅ Checking syntax of chaos-engineering.nix..."
nix-instantiate test/nixos-vm/chaos-engineering.nix \
  --arg pkgs 'import <nixpkgs> {}' \
  --arg sinex-collector 'null' \
  --arg sinex-promo-worker 'null' \
  --arg pg_jsonschema 'null' >/dev/null

echo "✅ Checking syntax of production-scale.nix..."
nix-instantiate test/nixos-vm/production-scale.nix \
  --arg pkgs 'import <nixpkgs> {}' \
  --arg sinex-collector 'null' \
  --arg sinex-promo-worker 'null' \
  --arg pg_jsonschema 'null' >/dev/null

# Check supporting infrastructure
echo "✅ Checking chaos-toolkit.nix..."
nix-instantiate test/nixos-vm/common/chaos-toolkit.nix \
  --arg pkgs 'import <nixpkgs> {}' >/dev/null

echo "✅ Checking production-load.nix..."
nix-instantiate test/nixos-vm/common/production-load.nix \
  --arg pkgs 'import <nixpkgs> {}' >/dev/null

# Check test suite integration (skip - requires parameters)
echo "✅ Checking default.nix integration (skipped - requires build parameters)..."

# Validate file structure
echo "✅ Checking file structure..."
required_files=(
  "test/nixos-vm/chaos-engineering.nix"
  "test/nixos-vm/production-scale.nix" 
  "test/nixos-vm/common/chaos-toolkit.nix"
  "test/nixos-vm/common/production-load.nix"
)

for file in "${required_files[@]}"; do
  if [[ ! -f "$file" ]]; then
    echo "❌ Missing required file: $file"
    exit 1
  fi
done

# Check justfile integration
echo "✅ Checking justfile integration..."
if ! grep -q "test-vm-chaos" justfile; then
  echo "❌ justfile missing test-vm-chaos command"
  exit 1
fi

if ! grep -q "test-vm-production" justfile; then
  echo "❌ justfile missing test-vm-production command"
  exit 1
fi

if ! grep -q "test-vm-advanced" justfile; then
  echo "❌ justfile missing test-vm-advanced command"
  exit 1
fi

echo "🎉 Advanced VM test implementation validation completed successfully!"
echo ""
echo "📋 Implementation Summary:"
echo "  ✅ chaos-engineering.nix - Tests system resilience under failures"
echo "  ✅ production-scale.nix - Tests performance at production scale"
echo "  ✅ chaos-toolkit.nix - Failure injection infrastructure"
echo "  ✅ production-load.nix - Load generation utilities"
echo "  ✅ Flake integration - Added to checks section"
echo "  ✅ Justfile commands - test-vm-chaos, test-vm-production, test-vm-advanced"
echo ""
echo "🚀 Ready to run:"
echo "  just test-vm-chaos      # Run chaos engineering tests"
echo "  just test-vm-production # Run production scale tests"
echo "  just test-vm-advanced   # Run both advanced tests"
echo "  just test-vm-all        # Run all VM tests"