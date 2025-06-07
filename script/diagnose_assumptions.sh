#!/usr/bin/env bash
set -euo pipefail

# Script to diagnose assumption mismatches in the event data
# Updated to work with flake apps

# Ensure database is available
echo "🗄️ Checking database..."
if ! nix run .#db-setup check >/dev/null 2>&1; then
    echo "❌ Database not available. Run: nix run .#db-setup dev"
    exit 1
fi

DB_URL="${DATABASE_URL:-postgresql://localhost:5432/sinex_dev}"

echo "=== Sinex Event Assumption Diagnostics ==="
echo
echo "This tool analyzes your event data to detect potential assumption mismatches"
echo

# 1. Field usage analysis
echo "1. Field Usage Patterns by Event Type:"
echo "======================================"
psql "$DB_URL" -t <<'EOF'
WITH field_usage AS (
    SELECT 
        source,
        event_type,
        jsonb_object_keys(payload) as field_name,
        COUNT(*) as usage_count
    FROM raw.events
    GROUP BY source, event_type, jsonb_object_keys(payload)
),
total_events AS (
    SELECT 
        source,
        event_type,
        COUNT(*) as total_count
    FROM raw.events
    GROUP BY source, event_type
)
SELECT 
    fu.source || ' / ' || fu.event_type as event_type,
    fu.field_name,
    fu.usage_count || '/' || te.total_count as usage,
    ROUND((fu.usage_count::float / te.total_count::float * 100)::numeric, 1) || '%' as percentage,
    CASE 
        WHEN (fu.usage_count::float / te.total_count::float) >= 0.9 THEN '✓ REQUIRED'
        WHEN (fu.usage_count::float / te.total_count::float) >= 0.5 THEN '? COMMON'
        ELSE '! RARE'
    END as status
FROM field_usage fu
JOIN total_events te ON fu.source = te.source AND fu.event_type = te.event_type
ORDER BY fu.source, fu.event_type, fu.usage_count DESC;
EOF

echo
echo "2. Cross-Source Field Confusion Detection:"
echo "=========================================="
psql "$DB_URL" -t <<'EOF'
WITH field_sources AS (
    SELECT DISTINCT
        jsonb_object_keys(payload) as field_name,
        source
    FROM raw.events
)
SELECT 
    field_name,
    string_agg(DISTINCT source, ', ' ORDER BY source) as appears_in_sources,
    COUNT(DISTINCT source) as source_count
FROM field_sources
GROUP BY field_name
HAVING COUNT(DISTINCT source) > 1
ORDER BY source_count DESC, field_name;
EOF

echo
echo "3. Outlier Events (Missing Common Fields):"
echo "=========================================="
psql "$DB_URL" -t <<'EOF'
WITH common_fields AS (
    SELECT 
        source,
        event_type,
        jsonb_object_keys(payload) as field_name,
        COUNT(*) as usage_count
    FROM raw.events
    GROUP BY source, event_type, jsonb_object_keys(payload)
    HAVING COUNT(*) >= (
        SELECT COUNT(*) * 0.8 
        FROM raw.events e2 
        WHERE e2.source = raw.events.source 
        AND e2.event_type = raw.events.event_type
    )
),
expected_fields AS (
    SELECT 
        source,
        event_type,
        string_agg(field_name, ', ' ORDER BY field_name) as common_fields
    FROM common_fields
    GROUP BY source, event_type
)
SELECT 
    e.id::text as event_id,
    e.source || ' / ' || e.event_type as type,
    e.ts_ingest,
    'Missing: ' || COALESCE(
        (SELECT string_agg(cf, ', ')
         FROM (
             SELECT unnest(string_to_array(ef.common_fields, ', ')) as cf
             EXCEPT
             SELECT jsonb_object_keys(e.payload)
         ) missing
        ), 'none'
    ) as missing_fields,
    'Has: ' || string_agg(jsonb_object_keys(e.payload), ', ') as actual_fields
FROM raw.events e
LEFT JOIN expected_fields ef ON e.source = ef.source AND e.event_type = ef.event_type
WHERE EXISTS (
    SELECT 1
    FROM (
        SELECT unnest(string_to_array(ef.common_fields, ', '))
        EXCEPT
        SELECT jsonb_object_keys(e.payload)
    ) missing
)
LIMIT 10;
EOF

echo
echo "4. Suspicious Field Combinations:"
echo "================================="
psql "$DB_URL" -t <<'EOF'
WITH filesystem_fields AS (
    SELECT DISTINCT jsonb_object_keys(payload) as field
    FROM raw.events 
    WHERE source = 'filesystem'
),
hyprland_fields AS (
    SELECT DISTINCT jsonb_object_keys(payload) as field
    FROM raw.events 
    WHERE source = 'hyprland'
),
terminal_fields AS (
    SELECT DISTINCT jsonb_object_keys(payload) as field
    FROM raw.events 
    WHERE source LIKE 'terminal%'
)
SELECT 
    e.id::text as event_id,
    e.source || ' / ' || e.event_type as declared_type,
    CASE 
        WHEN e.source = 'filesystem' AND EXISTS (
            SELECT 1 FROM hyprland_fields h 
            WHERE h.field IN (SELECT jsonb_object_keys(e.payload))
        ) THEN 'Has Hyprland fields: ' || (
            SELECT string_agg(h.field, ', ')
            FROM hyprland_fields h 
            WHERE h.field IN (SELECT jsonb_object_keys(e.payload))
        )
        WHEN e.source = 'hyprland' AND EXISTS (
            SELECT 1 FROM filesystem_fields f 
            WHERE f.field IN (SELECT jsonb_object_keys(e.payload))
        ) THEN 'Has Filesystem fields: ' || (
            SELECT string_agg(f.field, ', ')
            FROM filesystem_fields f 
            WHERE f.field IN (SELECT jsonb_object_keys(e.payload))
        )
        ELSE 'Unknown mismatch'
    END as issue
FROM raw.events e
WHERE 
    (e.source = 'filesystem' AND EXISTS (
        SELECT 1 FROM hyprland_fields h 
        WHERE h.field IN (SELECT jsonb_object_keys(e.payload))
    ))
    OR 
    (e.source = 'hyprland' AND EXISTS (
        SELECT 1 FROM filesystem_fields f 
        WHERE f.field IN (SELECT jsonb_object_keys(e.payload))
    ))
LIMIT 20;
EOF

echo
echo "5. Event Type Summary:"
echo "====================="
psql "$DB_URL" -t <<'EOF'
SELECT 
    source,
    event_type,
    COUNT(*) as event_count,
    MIN(ts_ingest) as first_seen,
    MAX(ts_ingest) as last_seen,
    COUNT(DISTINCT host) as unique_hosts
FROM raw.events
GROUP BY source, event_type
ORDER BY source, event_type;
EOF

echo
echo "Diagnostics complete!"
echo
echo "Look for:"
echo "- Fields marked as RARE that appear in only a few events (potential mistakes)"
echo "- Fields that appear in multiple sources (cross-contamination)"
echo "- Events missing common fields (outliers)"
echo "- Events with fields from wrong sources (assumption mismatches)"