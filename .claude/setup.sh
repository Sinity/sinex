#!/usr/bin/env bash
# Cloud-agent sandbox setup for sinex — thin wrapper.
#
# All real logic lives in xtask/cloud/bootstrap.sh, the single profile-aware
# bootstrap shared by Claude Code Web (this file + the SessionStart hook it
# installs) and Codex Cloud (environment setup/maintenance scripts).
#
# Profile selection: SINEX_CLOUD_PROFILE=static|db (default db — Rust work
# needs the database; see bootstrap.sh header for why even xtask does).
set -euo pipefail
exec "$(dirname "${BASH_SOURCE[0]}")/../xtask/cloud/bootstrap.sh" \
  "${SINEX_CLOUD_PROFILE:-db}"
