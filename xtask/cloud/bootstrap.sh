#!/usr/bin/env bash
# Profile-aware cloud-sandbox bootstrap for sinex
# ================================================
#
# ONE idempotent script for every hosted-agent surface:
#
#   Claude Code Web   .claude/setup.sh (thin wrapper) -> bootstrap.sh <profile>
#   Claude cached     SessionStart hook               -> bootstrap.sh <profile> --maintenance
#   Codex Cloud       env "setup script"              -> bootstrap.sh <profile>
#   Codex Cloud       env "maintenance script"        -> bootstrap.sh <profile> --maintenance
#
# Profiles:
#   static an environment for NO-COMPILE work only: docs, config, prompts,
#          shell tooling. Persists env, installs nothing heavy, starts no
#          sidecars, builds nothing. Any `xtask`/cargo invocation is out of
#          scope for this profile by definition.
#   db     the ONLY Rust profile. xtask itself depends on sinex-db, whose
#          sqlx::query! macros compile against a live, schema-populated
#          PostgreSQL (this repo keeps no offline cache by doctrine) — so
#          even "DB-less" crates like sinex-primitives are checked/tested
#          through an xtask binary that needed the DB to build. Order is
#          load-bearing: database up+healthy -> schema bootstrap -> xtask.
#
# Database strategy for `db` is PROBED, not assumed (verified 2026-07-10:
# Codex Cloud runs inside codex-universal with NO Docker daemon; the ghcr
# sidecar images are additionally unpublished):
#   external  DATABASE_URL already answers -> use it, provision nothing.
#   docker    a working Docker daemon exists (Claude Code Web) -> build the
#             in-repo sidecar images and compose them up.
#   (absent)  fail with an explicit blocker. A native-PG18 apt path
#             (timescaledb + pgvector + pg_jsonschema inside the sandbox)
#             is NOT implemented; do not pretend this profile works on a
#             daemon-less provider until a capability probe passes.
#
# --maintenance: re-convergence after a cached-session resume or a Codex
# task-branch checkout. Codex runs it AFTER checking out the task branch, so
# it must also refresh the xtask binary: a cached default-branch xtask must
# never verify branch-modified code. `cargo build -p xtask` is the freshness
# check itself — cargo's fingerprinting makes it a fast no-op when nothing
# in the xtask dependency cone changed.
#
# Env persistence: an idempotent block in ~/.bashrc (both vendors; Codex
# setup-phase exports do NOT survive into the agent phase) plus
# .claude/settings.local.json (Claude reads/merges it; gitignored).
#
# Idempotent — re-running any mode is safe.
set -euo pipefail

log() { printf '[bootstrap] %s\n' "$*" >&2; }
die() { printf '[bootstrap] ERROR: %s\n' "$*" >&2; exit 1; }

PROFILE="${1:-}"
MODE="setup"
[[ "${2:-}" == "--maintenance" ]] && MODE="maintenance"
[[ "$PROFILE" == "static" || "$PROFILE" == "db" ]] || \
  die "usage: bootstrap.sh <static|db> [--maintenance]"

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$repo_root"

# ---------------------------------------------------------------------------
# Identity preflight. The first cloud wave silently ran against an archived
# fork, so identity must be PROVEN — but proven from history, not remotes:
# Codex task checkouts at /workspace/<repo> have NO `origin` remote at all
# (verified on the corrected live canary), so remote inspection can only
# corroborate, never gate, cross-provider.
#
# Required gate: a live-repo lineage anchor — a commit that exists on the
# live repository's master strictly after the archived fork diverged
# (archive last push 2026-04-03). If the checkout doesn't contain it, the
# base is stale or mis-bound. Override per-environment with
# SINEX_LINEAGE_ANCHOR; per-packet exactness rides SINEX_EXPECTED_BASE_SHA.
# ---------------------------------------------------------------------------
# Default anchor: master commit c425316f9 (2026-07-10, test-consolidation
# merge) — present on live master, absent from the archived fork.
DEFAULT_LINEAGE_ANCHOR="c425316f9"

identity_preflight() {
  local remotes head anchor
  remotes="$(git remote -v 2>/dev/null || true)"
  head="$(git rev-parse HEAD 2>/dev/null || echo unknown)"
  log "remote(s): $(echo "${remotes:-<none — normal on Codex task checkouts>}" | head -1)"
  log "HEAD: ${head}"

  # Optional corroboration only — many hosted checkouts carry no remote.
  if [[ -n "$remotes" ]]; then
    if [[ -n "${SINEX_EXPECTED_REPO:-}" ]] && ! echo "$remotes" | grep -q "$SINEX_EXPECTED_REPO"; then
      die "remote does not match SINEX_EXPECTED_REPO=${SINEX_EXPECTED_REPO} — mis-bound environment"
    fi
    if echo "$remotes" | grep -qi 'archive'; then
      die "remote looks like an archived fork — rebind the environment to the live repo"
    fi
  fi

  # Required lineage gate: works with or without remotes.
  anchor="${SINEX_LINEAGE_ANCHOR:-${DEFAULT_LINEAGE_ANCHOR}}"
  git cat-file -e "${anchor}^{commit}" 2>/dev/null || \
    die "lineage anchor ${anchor} is absent from this checkout — stale or mis-bound base (archived fork?)"
  git merge-base --is-ancestor "$anchor" HEAD 2>/dev/null || \
    die "HEAD does not descend from lineage anchor ${anchor} — stale or mis-bound base"

  # Task packets carry an exact base_sha; when the coordinator exports it,
  # assert the checkout actually contains it (env display labels have lied).
  if [[ -n "${SINEX_EXPECTED_BASE_SHA:-}" ]]; then
    git merge-base --is-ancestor "$SINEX_EXPECTED_BASE_SHA" HEAD 2>/dev/null || \
      die "HEAD does not contain SINEX_EXPECTED_BASE_SHA=${SINEX_EXPECTED_BASE_SHA} — stale or mis-bound base"
  fi
}

# ---------------------------------------------------------------------------
# Resource caps for a ~4 vCPU / 16 GB / 30 GB sandbox.
# ---------------------------------------------------------------------------
export CARGO_HOME="${CARGO_HOME:-${repo_root}/.cargo-home}"
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-${repo_root}/.target}"
export CARGO_BUILD_JOBS="${CARGO_BUILD_JOBS:-1}"
export CARGO_INCREMENTAL=0
export CARGO_PROFILE_DEV_DEBUG=0
export CARGO_PROFILE_TEST_DEBUG=0
export DATABASE_URL="${DATABASE_URL:-postgres://sinex:dev@localhost:5432/sinex_dev}"
# xtask reads SINEX_NATS_URL; plain NATS_URL is kept for compose/tools parity.
export SINEX_NATS_URL="${SINEX_NATS_URL:-nats://localhost:4222}"
export NATS_URL="${NATS_URL:-${SINEX_NATS_URL}}"
export SINEX_AUTO_INFRA=0
export SINEX_AUTO_STATUS=0
export SINEX_CLOUD_PROFILE="$PROFILE"

# PATH: prepend once, idempotently (a naive prefix grows on every run).
path_prepend() {
  case ":${PATH}:" in *":$1:"*) ;; *) PATH="$1:${PATH}" ;; esac
}
path_prepend "${HOME}/.cargo/bin"
path_prepend "${CARGO_HOME}/bin"
path_prepend "${CARGO_TARGET_DIR}/debug"
export PATH

persist_env() {
  local marker="# >>> sinex cloud bootstrap >>>"
  local endmarker="# <<< sinex cloud bootstrap <<<"
  local bashrc="${HOME}/.bashrc"
  touch "$bashrc"
  local tmp
  tmp="$(mktemp)"
  awk -v m="$marker" -v e="$endmarker" '
    $0 == m {skip=1} skip && $0 == e {skip=0; next} !skip {print}
  ' "$bashrc" > "$tmp"
  {
    cat "$tmp"
    echo "$marker"
    echo "export CARGO_HOME=\"${CARGO_HOME}\""
    echo "export CARGO_TARGET_DIR=\"${CARGO_TARGET_DIR}\""
    echo "export CARGO_BUILD_JOBS=\"${CARGO_BUILD_JOBS}\""
    echo "export CARGO_INCREMENTAL=0"
    echo "export CARGO_PROFILE_DEV_DEBUG=0"
    echo "export CARGO_PROFILE_TEST_DEBUG=0"
    echo "export DATABASE_URL=\"${DATABASE_URL}\""
    echo "export SINEX_NATS_URL=\"${SINEX_NATS_URL}\""
    echo "export NATS_URL=\"${NATS_URL}\""
    echo "export SINEX_AUTO_INFRA=0"
    echo "export SINEX_AUTO_STATUS=0"
    echo "export SINEX_CLOUD_PROFILE=\"${PROFILE}\""
    # Idempotent PATH prepends (guarded, so re-sourcing never grows PATH).
    echo 'case ":${PATH}:" in *":'"${HOME}"'/.cargo/bin:"*) ;; *) PATH="'"${HOME}"'/.cargo/bin:${PATH}" ;; esac'
    echo 'case ":${PATH}:" in *":'"${CARGO_HOME}"'/bin:"*) ;; *) PATH="'"${CARGO_HOME}"'/bin:${PATH}" ;; esac'
    echo 'case ":${PATH}:" in *":'"${CARGO_TARGET_DIR}"'/debug:"*) ;; *) PATH="'"${CARGO_TARGET_DIR}"'/debug:${PATH}" ;; esac'
    echo 'export PATH'
    echo "$endmarker"
  } > "$bashrc"
  rm -f "$tmp"
  log "persisted env block in ~/.bashrc (profile=${PROFILE})"

  cat > "${repo_root}/.claude/settings.local.json" <<JSON
{
  "env": {
    "CARGO_HOME": "${CARGO_HOME}",
    "CARGO_TARGET_DIR": "${CARGO_TARGET_DIR}",
    "CARGO_BUILD_JOBS": "${CARGO_BUILD_JOBS}",
    "CARGO_INCREMENTAL": "0",
    "CARGO_PROFILE_DEV_DEBUG": "0",
    "CARGO_PROFILE_TEST_DEBUG": "0",
    "DATABASE_URL": "${DATABASE_URL}",
    "SINEX_NATS_URL": "${SINEX_NATS_URL}",
    "NATS_URL": "${NATS_URL}",
    "SINEX_AUTO_INFRA": "0",
    "SINEX_AUTO_STATUS": "0",
    "SINEX_CLOUD_PROFILE": "${PROFILE}",
    "PATH": "${PATH}",
    "RUSTC_WRAPPER": ""
  },
  "permissions": {
    "allow": [
      "Bash(xtask:*)",
      "Bash(rustup:*)",
      "Bash(docker:*)"
    ]
  },
  "hooks": {
    "SessionStart": [
      {
        "hooks": [
          {
            "type": "command",
            "command": "\${CLAUDE_PROJECT_DIR}/xtask/cloud/bootstrap.sh ${PROFILE} --maintenance"
          }
        ]
      }
    ]
  }
}
JSON
  log "wrote .claude/settings.local.json (sandbox-only overrides + SessionStart hook)"
}

database_reachable() {
  # Cheap TCP/auth probe against DATABASE_URL without needing psql: use
  # pg_isready when present, else a bash /dev/tcp connect on host:port.
  if command -v pg_isready >/dev/null 2>&1; then
    pg_isready -d "$DATABASE_URL" -t 3 >/dev/null 2>&1 && return 0
  fi
  local hostport
  hostport="$(echo "$DATABASE_URL" | sed -E 's|.*@([^/]+)/.*|\1|')"
  local host="${hostport%%:*}" port="${hostport##*:}"
  [[ "$port" == "$host" ]] && port=5432
  (exec 3<>"/dev/tcp/${host}/${port}") 2>/dev/null && { exec 3>&- 3<&-; return 0; }
  return 1
}

converge_database() {
  if database_reachable; then
    log "database strategy: external (DATABASE_URL already reachable)"
    return 0
  fi
  if command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1; then
    local compose="${repo_root}/xtask/cloud/docker-compose.yml"
    [[ -f "$compose" ]] || die "missing ${compose}"
    log "database strategy: docker (building in-repo sidecar images; ghcr tags are unpublished)"
    docker compose -f "$compose" up -d --build --wait --wait-timeout 300 || \
      die "sidecars failed to converge; report this as a blocker (never SQLX_OFFLINE)"
    log "sidecars healthy"
    return 0
  fi
  die "profile db: no reachable DATABASE_URL and no Docker daemon (codex-universal has none). \
Options: point DATABASE_URL at an external ephemeral Postgres, run on a Docker-capable provider, \
or use the static profile. Do NOT weaken verification to proceed."
}

bootstrap_schema() {
  # sqlx macros need a schema-POPULATED database, and xtask (the normal
  # frontend for schema work) cannot exist yet because building it needs
  # those same macros to resolve. schema-apply-bootstrap is the repo's
  # explicit chicken-and-egg exception: it deliberately avoids sinex-db.
  log "bootstrapping database schema (pre-xtask exception path)"
  cargo run --locked -p sinex-schema --bin schema-apply-bootstrap || \
    die "schema bootstrap failed; sqlx compilation cannot proceed"
}

build_xtask() {
  # Also the freshness check: cargo fingerprinting makes this a fast no-op
  # when the xtask dependency cone is unchanged, and a real rebuild when a
  # task branch modified xtask or a crate it depends on. A stale cached
  # xtask must never verify new code.
  log "building repository xtask (fingerprint-checked)"
  cargo build --locked -p xtask
  command -v cargo-nextest >/dev/null 2>&1 || {
    log "installing cargo-nextest (required)"
    cargo install --locked cargo-nextest || die "cargo-nextest is required for xtask test"
  }
}

# ---------------------------------------------------------------------------
# Maintenance: converge a warm sandbox (cached Claude session / Codex task
# branch). Cheap when nothing changed; correct when something did.
# ---------------------------------------------------------------------------
if [[ "$MODE" == "maintenance" ]]; then
  identity_preflight
  persist_env
  if [[ "$PROFILE" == "db" ]]; then
    converge_database
    bootstrap_schema
    build_xtask
  fi
  log "maintenance complete (profile=${PROFILE})"
  exit 0
fi

# ---------------------------------------------------------------------------
# Full setup
# ---------------------------------------------------------------------------
log "setup start (profile=${PROFILE})"
identity_preflight

if [[ "$PROFILE" == "static" ]]; then
  persist_env
  log "setup complete (static profile: no compilers, no sidecars, no builds)"
  exit 0
fi

log "installing apt packages"
sudo_cmd=""
[[ $EUID -ne 0 ]] && sudo_cmd="sudo"
${sudo_cmd} apt-get update -qq
${sudo_cmd} apt-get install -y --no-install-recommends \
  pkg-config libssl-dev libdbus-1-dev libsystemd-dev protobuf-compiler \
  mold clang ca-certificates curl git

if ! command -v rustup >/dev/null 2>&1; then
  log "installing rustup"
  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | \
    sh -s -- -y --profile minimal --no-modify-path --default-toolchain none
fi
# shellcheck disable=SC1091
. "${HOME}/.cargo/env" 2>/dev/null || true

log "resolving toolchain from rust-toolchain.toml"
rustup show

# Order is load-bearing (see header): DB reachable -> schema present -> xtask builds.
converge_database
bootstrap_schema
build_xtask
persist_env

log "setup complete (profile=${PROFILE})"
