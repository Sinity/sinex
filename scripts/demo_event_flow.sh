#!/usr/bin/env bash
set -euo pipefail

# Demo script showing event flow through the system

echo "=== Sinex Event Flow Demo ==="
echo
echo "This demo will:"
echo "1. Insert a test event into the database"
echo "2. Show the event in raw.events table"
echo "3. Show any promotion queue entries created"
echo

# Database URL
DB_URL="${DATABASE_URL:-postgresql://sinex:sinex@localhost:5432/sinex}"

# First, create a test agent that subscribes to events
echo "Setting up test agent..."
psql "$DB_URL" <<EOF
INSERT INTO sinex_schemas.agent_manifests 
    (agent_name, version, status, agent_type, subscribes_to_event_types)
VALUES 
    ('demo-processor', '1.0.0', 'running', 'promoter', 
     '{"raw.events_feed_all": [{"source_filter": "demo"}]}'::jsonb)
ON CONFLICT (agent_name) DO UPDATE 
SET status = 'running',
    subscribes_to_event_types = '{"raw.events_feed_all": [{"source_filter": "demo"}]}'::jsonb;
EOF

echo
echo "Inserting test event..."

# Insert an event using psql
EVENT_ID=$(psql -t -A "$DB_URL" <<EOF
INSERT INTO raw.events 
    (source, event_type, host, payload)
VALUES 
    ('demo', 'test_event', 'demo-host', 
     '{"message": "Hello Sinex!", "timestamp": "$(date -u +%Y-%m-%dT%H:%M:%SZ)", "demo": true}'::jsonb)
RETURNING id;
EOF
)

echo "Created event with ID: $EVENT_ID"
echo

# Show the event
echo "Event in raw.events:"
psql "$DB_URL" <<EOF
SELECT 
    id::text as id,
    source,
    event_type,
    ts_ingest,
    jsonb_pretty(payload) as payload
FROM raw.events
WHERE id = '$EVENT_ID'::ulid;
EOF

echo
echo "Promotion queue entries created by trigger:"
psql "$DB_URL" <<EOF
SELECT 
    queue_id::text as queue_id,
    target_agent_name,
    status,
    created_at
FROM sinex_schemas.promotion_queue
WHERE raw_event_id = '$EVENT_ID'::ulid;
EOF

echo
echo "Recent events (last 5):"
psql "$DB_URL" <<EOF
SELECT 
    id::text as id,
    source,
    event_type,
    ts_ingest,
    payload->>'message' as message
FROM raw.events
ORDER BY ts_ingest DESC
LIMIT 5;
EOF

echo
echo "Demo complete!"