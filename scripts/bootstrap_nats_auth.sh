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

if ! command -v nsc > /dev/null 2>&1; then
    log "nsc not found; skipping NATS bootstrap"
    exit 0
fi

# Set NSC store to local directory
export NKEYS_PATH="$NSC_STORE"
export NSC_HOME="$NSC_STORE"

# Ensure directories exist
mkdir -p "$NSC_STORE"

# Check if configuration already exists
OPERATOR_DIR="$NSC_STORE/$OPERATOR"
SYS_CREDS="$NSC_STORE/creds/$OPERATOR/SYS/sys.creds"
INGESTOR_CREDS="$NSC_STORE/creds/$OPERATOR/$APP_ACCOUNT/sinex-ingestor.creds"
AUTOMATON_CREDS="$NSC_STORE/creds/$OPERATOR/$APP_ACCOUNT/sinex-automaton.creds"
GATEWAY_CREDS="$NSC_STORE/creds/$OPERATOR/$APP_ACCOUNT/sinex-gateway.creds"
APP_CREDS="$NSC_STORE/creds/$OPERATOR/$APP_ACCOUNT/$APP_ACCOUNT.creds"

if [ -f "$CONF_FILE" ] && [ -f "$SYS_CREDS" ] && [ -f "$INGESTOR_CREDS" ] && [ -f "$AUTOMATON_CREDS" ] && [ -f "$GATEWAY_CREDS" ]; then
    # Already fully bootstrapped, exit silently
    exit 0
fi

log "Bootstrapping NATS Auth (Operator Mode with Role-Based Users)..."

# Initialize Operator (check for operator JWT file)
if [ ! -f "$OPERATOR_DIR/$OPERATOR.jwt" ]; then
    nsc init -n "$OPERATOR" --dir "$NSC_STORE" > /dev/null
    log "[ OK ] created operator $OPERATOR"
fi

nsc env -o "$OPERATOR" > /dev/null 2>&1

# Create System Account (check for account JWT)
if [ ! -f "$OPERATOR_DIR/accounts/SYS/SYS.jwt" ]; then
    nsc add account -n SYS > /dev/null
    log "[ OK ] created system_account: name:SYS"
    nsc add user -n sys -a SYS > /dev/null
    log "[ OK ] created system account user: name:sys"
    nsc generate creds -a SYS -n sys > /dev/null
    log "[ OK ] system account user creds file stored in '$SYS_CREDS'"
fi

# Create Application Account if it doesn't exist
if [ ! -f "$OPERATOR_DIR/accounts/$APP_ACCOUNT/$APP_ACCOUNT.jwt" ]; then
    nsc add account -n "$APP_ACCOUNT" > /dev/null
    log "[ OK ] created account $APP_ACCOUNT"
fi

# Create ingestor user if creds don't exist
if [ ! -f "$INGESTOR_CREDS" ]; then
    nsc add user -n "sinex-ingestor" -a "$APP_ACCOUNT" \
        --allow-pub "source_material.>" \
        --allow-pub "events.>" \
        --allow-pub "_INBOX.>" \
        --deny-sub "events.>" \
        > /dev/null
    nsc generate creds -a "$APP_ACCOUNT" -n "sinex-ingestor" > /dev/null
    log "[ OK ] created ingestor user (publish-only): $INGESTOR_CREDS"
fi

# Create automaton user if creds don't exist
if [ ! -f "$AUTOMATON_CREDS" ]; then
    nsc add user -n "sinex-automaton" -a "$APP_ACCOUNT" \
        --allow-sub "events.>" \
        --allow-pub "events.>" \
        --allow-pub "_INBOX.>" \
        --allow-sub "sinex.coordination.>" \
        --allow-pub "sinex.coordination.>" \
        > /dev/null
    nsc generate creds -a "$APP_ACCOUNT" -n "sinex-automaton" > /dev/null
    log "[ OK ] created automaton user (event pub/sub): $AUTOMATON_CREDS"
fi

# Create gateway user if creds don't exist  
if [ ! -f "$GATEWAY_CREDS" ]; then
    nsc add user -n "sinex-gateway" -a "$APP_ACCOUNT" \
        --allow-pub ">" \
        --allow-sub ">" \
        > /dev/null
    nsc generate creds -a "$APP_ACCOUNT" -n "sinex-gateway" > /dev/null
    log "[ OK ] created gateway user (full access): $GATEWAY_CREDS"
fi

# Create legacy dev user if creds don't exist
if [ ! -f "$APP_CREDS" ]; then
    nsc add user -n "$APP_ACCOUNT" -a "$APP_ACCOUNT" > /dev/null
    nsc generate creds -a "$APP_ACCOUNT" -n "$APP_ACCOUNT" > /dev/null
    log "[ WARN ] created legacy user '$APP_ACCOUNT' for backward compatibility (deprecated)"
    log "[ WARN ] use role-specific creds instead: ingestor, automaton, gateway"
fi

# Generate NATS Configuration with Memory Resolver (only if missing)
if [ ! -f "$CONF_FILE" ]; then
    nsc generate config --mem-resolver --sys-account SYS --config-file "$CONF_FILE" > /dev/null
    log "[ OK ] NATS config generated: $CONF_FILE"
fi

log ""
log "Role-specific credentials:"
log "  Ingestor: $INGESTOR_CREDS"
log "  Automaton: $AUTOMATON_CREDS"
log "  Gateway: $GATEWAY_CREDS"
