#!/usr/bin/env bash
set -euo pipefail

nix develop --accept-flake-config --no-pure-eval --command "$@"
