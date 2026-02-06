#!/usr/bin/env bash
# Quick test to publish an event to NATS JetStream

NATS_URL="nats://localhost:4260"
STREAM="DEV_SINEX_RAW_EVENTS"
SUBJECT="dev.raw.events.test"

# Create a test event payload
EVENT_ID=$(uuidgen | tr '[:upper:]' '[:lower:]')
TIMESTAMP=$(date -u +"%Y-%m-%dT%H:%M:%S.%3NZ")

cat << EOF | nix-shell -p nats-server --run "nats -s $NATS_URL pub $SUBJECT"
{
  "id": "$EVENT_ID",
  "event_source": "test-publisher",
  "event_type": "test.event",
  "timestamp": "$TIMESTAMP",
  "payload": {
    "message": "Hello from test publisher!",
    "test_run": true
  }
}
EOF
