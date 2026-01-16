#!/usr/bin/env bash
set -euo pipefail

# Directory for NATS state
NATS_DIR="${SINEX_NATS_DIR:-${XDG_STATE_HOME:-$HOME/.local/state}/sinex/nats}"
NSC_STORE="$NATS_DIR/nsc"
CONF_FILE="$NATS_DIR/nats.conf"
OPERATOR="sinex-dev"
APP_ACCOUNT="sinex-dev"

log() {
    if [ -z "${SINEX_NATS_BOOTSTRAP_QUIET:-}" ]; then
        echo "$@"
    fi
}

if ! command -v nsc >/dev/null 2>&1; then
    log "nsc not found; skipping NATS bootstrap"
    exit 0
fi

# Set NSC store to local directory
export NKEYS_PATH="$NSC_STORE"
export NSC_HOME="$NSC_STORE"

# Ensure directories exist
mkdir -p "$NSC_STORE"

# Check if configuration already exists
# Look for actual files where NSC creates them
OPERATOR_DIR="$NSC_STORE/$OPERATOR"
SYS_CREDS="$NSC_STORE/creds/$OPERATOR/SYS/sys.creds"
APP_CREDS="$NSC_STORE/creds/$OPERATOR/$APP_ACCOUNT/$APP_ACCOUNT.creds"

if [ -f "$CONF_FILE" ] && [ -f "$SYS_CREDS" ] && [ -f "$APP_CREDS" ]; then
    # Already bootstrapped, exit silently
    exit 0
fi

log "Bootstrapping NATS Auth (Operator Mode)..."

# Initialize Operator (check for operator JWT file)
if [ ! -f "$OPERATOR_DIR/$OPERATOR.jwt" ]; then
    nsc init -n "$OPERATOR" --dir "$NSC_STORE" >/dev/null
    log "[ OK ] created operator $OPERATOR"
fi

nsc env -o "$OPERATOR" >/dev/null 2>&1

# Create System Account (check for account JWT)
if [ ! -f "$OPERATOR_DIR/accounts/SYS/SYS.jwt" ]; then
    nsc add account -n SYS >/dev/null
    log "[ OK ] created system_account: name:SYS"
    nsc add user -n sys -a SYS >/dev/null
    log "[ OK ] created system account user: name:sys"
    nsc generate creds -a SYS -n sys >/dev/null
    log "[ OK ] system account user creds file stored in '$SYS_CREDS'"
fi

# Create Application Account (check for account JWT)
if [ ! -f "$OPERATOR_DIR/accounts/$APP_ACCOUNT/$APP_ACCOUNT.jwt" ]; then
    nsc add account -n "$APP_ACCOUNT" >/dev/null
    log "[ OK ] created account $APP_ACCOUNT"
    nsc add user -n "$APP_ACCOUNT" -a "$APP_ACCOUNT" >/dev/null
    log "[ OK ] created user \"$APP_ACCOUNT\""
    nsc generate creds -a "$APP_ACCOUNT" -n "$APP_ACCOUNT" >/dev/null
    log "[ OK ] user creds file stored in '$APP_CREDS'"
fi

# Generate NATS Configuration with Memory Resolver (only if missing)
if [ ! -f "$CONF_FILE" ]; then
    nsc generate config --mem-resolver --sys-account SYS --config-file "$CONF_FILE" >/dev/null
    log "[ OK ] NATS config generated: $CONF_FILE"
fi
