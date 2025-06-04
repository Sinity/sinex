#!/usr/bin/env bash
set -euo pipefail

# Chaos testing: introduce real-world failure modes to see if detection works

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${YELLOW}=== Sinex Chaos Testing (Real Failure Simulation) ===${NC}"
echo
echo "This test simulates real-world bugs and misconfigurations to see if we can detect them"
echo

DB_URL="${DATABASE_URL:-postgresql://sinex:sinex@localhost:5432/sinex}"

# Reset database
./scripts/db_reset.sh

echo "=== CHAOS SCENARIO 1: Misconfigured Filesystem Ingestor ==="
echo "Simulating: Filesystem ingestor thinks it's watching files but is actually getting process data"
echo

# Simulate a filesystem ingestor that accidentally gets fed process information
cat <<'EOF' | psql "$DB_URL"
-- Simulate filesystem ingestor that's broken and sending wrong data
INSERT INTO raw.events (source, event_type, host, payload) VALUES
('filesystem', 'file_created', 'chaos-test', '{"pid": 1234, "command": "/usr/bin/firefox", "memory_mb": 512}'),
('filesystem', 'file_created', 'chaos-test', '{"pid": 5678, "command": "/bin/bash", "cpu_percent": 2.5}'),
('filesystem', 'file_modified', 'chaos-test', '{"process_name": "vim", "exit_code": 0, "duration_ms": 15000}');
EOF

echo "=== CHAOS SCENARIO 2: Version Mismatch ==="
echo "Simulating: New ingestor version with changed event structure running alongside old version"
echo

cat <<'EOF' | psql "$DB_URL"
-- Old version format
INSERT INTO raw.events (source, event_type, host, payload) VALUES
('hyprland', 'window_focused', 'chaos-test', '{"window": "firefox", "workspace": 1}'),
('hyprland', 'window_focused', 'chaos-test', '{"window": "terminal", "workspace": 2}');

-- New version format (incompatible change)
INSERT INTO raw.events (source, event_type, host, payload) VALUES
('hyprland', 'window_focused', 'chaos-test', '{"window_info": {"class": "firefox", "title": "Mozilla Firefox"}, "workspace_id": 1, "window_geometry": {"x": 0, "y": 0, "width": 1920, "height": 1080}}'),
('hyprland', 'window_focused', 'chaos-test', '{"window_info": {"class": "kitty", "title": "Terminal"}, "workspace_id": 2, "monitor": "DP-1"}');
EOF

echo "=== CHAOS SCENARIO 3: Copy-Paste Error ==="
echo "Simulating: Developer copied code from one ingestor to another and forgot to change event types"
echo

cat <<'EOF' | psql "$DB_URL"
-- Terminal ingestor accidentally sending filesystem events
INSERT INTO raw.events (source, event_type, host, payload) VALUES
('terminal.kitty', 'command_executed', 'chaos-test', '{"path": "/home/user/.bashrc", "size": 2048, "permissions": "644"}'),
('terminal.kitty', 'command_executed', 'chaos-test', '{"path": "/home/user/script.sh", "size": 512, "modification_time": "2024-01-01T12:00:00Z"}');
EOF

echo "=== CHAOS SCENARIO 4: Network Data Confusion ==="
echo "Simulating: Ingestor accidentally captures network monitoring data instead of intended data"
echo

cat <<'EOF' | psql "$DB_URL"
-- Filesystem ingestor that's actually monitoring network activity
INSERT INTO raw.events (source, event_type, host, payload) VALUES
('filesystem', 'file_created', 'chaos-test', '{"src_ip": "192.168.1.100", "dst_ip": "8.8.8.8", "port": 53, "protocol": "UDP", "bytes": 64}'),
('filesystem', 'file_modified', 'chaos-test', '{"connection_id": "conn_12345", "state": "ESTABLISHED", "duration_seconds": 120}');
EOF

echo "=== CHAOS SCENARIO 5: Environmental Differences ==="
echo "Simulating: Same ingestor behaving differently in different environments"
echo

cat <<'EOF' | psql "$DB_URL"
-- Production vs development environment differences
-- Dev environment (detailed debug info)
INSERT INTO raw.events (source, event_type, host, payload) VALUES
('hyprland', 'workspace_changed', 'dev-machine', '{"workspace": 3, "debug_info": {"stack_trace": "...", "memory_usage": "45MB"}, "dev_mode": true}');

-- Production environment (minimal info due to different config)
INSERT INTO raw.events (source, event_type, host, payload) VALUES
('hyprland', 'workspace_changed', 'prod-machine', '{"ws": 3}');
EOF

echo
echo "=== RUNNING DETECTION ANALYSIS ==="
echo

echo "1. Field Usage Analysis (should show anomalies):"
psql "$DB_URL" -c "
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
    fu.source,
    fu.event_type,
    fu.field_name,
    fu.usage_count || '/' || te.total_count as usage,
    ROUND((fu.usage_count::float / te.total_count::float * 100)::numeric, 1) || '%' as percentage,
    CASE 
        WHEN (fu.usage_count::float / te.total_count::float) >= 0.8 THEN 'COMMON'
        WHEN (fu.usage_count::float / te.total_count::float) >= 0.3 THEN 'SOMETIMES'
        ELSE 'RARE/ANOMALY'
    END as status
FROM field_usage fu
JOIN total_events te ON fu.source = te.source AND fu.event_type = te.event_type
ORDER BY fu.source, fu.event_type, fu.usage_count DESC;
"

echo
echo "2. Cross-Source Field Contamination:"
psql "$DB_URL" -c "
WITH source_fields AS (
    SELECT DISTINCT
        source,
        jsonb_object_keys(payload) as field_name
    FROM raw.events
),
suspicious_fields AS (
    SELECT 
        field_name,
        string_agg(DISTINCT source, ', ' ORDER BY source) as found_in_sources,
        COUNT(DISTINCT source) as source_count
    FROM source_fields
    GROUP BY field_name
    HAVING COUNT(DISTINCT source) > 1
)
SELECT 
    field_name,
    found_in_sources,
    CASE 
        WHEN field_name IN ('pid', 'command', 'exit_code') AND found_in_sources LIKE '%filesystem%' 
        THEN 'LIKELY BUG: Process fields in filesystem events'
        WHEN field_name IN ('path', 'size', 'permissions') AND found_in_sources NOT LIKE '%filesystem%'
        THEN 'LIKELY BUG: File fields in non-filesystem events'
        WHEN field_name IN ('src_ip', 'dst_ip', 'port', 'protocol') 
        THEN 'LIKELY BUG: Network fields in wrong events'
        ELSE 'Investigate: Unexpected field sharing'
    END as assessment
FROM suspicious_fields
ORDER BY source_count DESC, field_name;
"

echo
echo "3. Specific Anomaly Detection:"
psql "$DB_URL" -c "
SELECT 
    id::text,
    source,
    event_type,
    CASE 
        WHEN source = 'filesystem' AND (payload ? 'pid' OR payload ? 'command' OR payload ? 'exit_code') 
        THEN 'FILESYSTEM INGESTOR SENDING PROCESS DATA'
        WHEN source = 'filesystem' AND (payload ? 'src_ip' OR payload ? 'dst_ip' OR payload ? 'protocol')
        THEN 'FILESYSTEM INGESTOR SENDING NETWORK DATA'  
        WHEN source = 'terminal.kitty' AND (payload ? 'path' OR payload ? 'size' OR payload ? 'permissions')
        THEN 'TERMINAL INGESTOR SENDING FILE DATA'
        WHEN source = 'hyprland' AND jsonb_typeof(payload->'window') = 'object' AND payload ? 'window_info'
        THEN 'HYPRLAND SCHEMA VERSION MISMATCH'
        WHEN source = 'hyprland' AND payload ? 'ws' AND NOT payload ? 'workspace'
        THEN 'HYPRLAND FIELD NAME INCONSISTENCY'
        ELSE 'Unknown anomaly'
    END as detected_issue,
    payload
FROM raw.events
WHERE 
    (source = 'filesystem' AND (payload ? 'pid' OR payload ? 'command' OR payload ? 'exit_code' OR payload ? 'src_ip'))
    OR (source = 'terminal.kitty' AND (payload ? 'path' OR payload ? 'size'))
    OR (source = 'hyprland' AND (payload ? 'window_info' OR payload ? 'ws'))
ORDER BY detected_issue;
"

echo
echo "4. Testing Validation Against Chaos Data:"

# Create a simple validation test
cat > /tmp/test_chaos_validation.py <<'EOF'
import subprocess
import json
import sys

def test_event_validation():
    # Get some chaotic events
    result = subprocess.run([
        'psql', sys.argv[1], '-t', '-c',
        "SELECT source, event_type, payload FROM raw.events LIMIT 10"
    ], capture_output=True, text=True)
    
    chaos_events = 0
    for line in result.stdout.strip().split('\n'):
        if '|' in line:
            parts = [p.strip() for p in line.split('|')]
            if len(parts) >= 3:
                source, event_type, payload_str = parts[0], parts[1], parts[2]
                print(f"Testing: {source} / {event_type}")
                
                # Try to parse payload
                try:
                    payload = json.loads(payload_str)
                    
                    # Check for obvious mismatches
                    if source == 'filesystem':
                        if 'pid' in payload or 'command' in payload:
                            print(f"  ❌ CHAOS DETECTED: Filesystem event has process fields!")
                            chaos_events += 1
                        elif 'src_ip' in payload or 'protocol' in payload:
                            print(f"  ❌ CHAOS DETECTED: Filesystem event has network fields!")
                            chaos_events += 1
                        elif 'path' in payload and 'size' in payload:
                            print(f"  ✅ Looks like valid filesystem event")
                        else:
                            print(f"  ⚠️  Unusual filesystem event structure")
                            
                    elif source == 'hyprland':
                        if 'path' in payload or 'size' in payload:
                            print(f"  ❌ CHAOS DETECTED: Hyprland event has file fields!")
                            chaos_events += 1
                        elif 'window_info' in payload and 'window' not in payload:
                            print(f"  ⚠️  Schema version mismatch detected")
                        elif 'window' in payload:
                            print(f"  ✅ Looks like valid hyprland event")
                            
                    elif 'terminal' in source:
                        if 'path' in payload and 'size' in payload:
                            print(f"  ❌ CHAOS DETECTED: Terminal event has file fields!")
                            chaos_events += 1
                        elif 'command' in payload:
                            print(f"  ✅ Looks like valid terminal event")
                            
                except json.JSONDecodeError:
                    print(f"  ❌ Invalid JSON payload")
                    
    print(f"\nSummary: Found {chaos_events} chaos events out of sample")
    return chaos_events

if __name__ == '__main__':
    chaos_count = test_event_validation()
    if chaos_count > 0:
        print("🎯 SUCCESS: Chaos detection is working!")
        exit(0)
    else:
        print("😞 FAILURE: Should have detected chaos events")
        exit(1)
EOF

python3 /tmp/test_chaos_validation.py "$DB_URL"
test_result=$?

echo
echo "=== CHAOS TEST RESULTS ==="
echo

if [ $test_result -eq 0 ]; then
    echo -e "${GREEN}✅ SUCCESS: Chaos detection is working!${NC}"
    echo "The system successfully detected the intentional bugs and misconfigurations."
else
    echo -e "${RED}❌ FAILURE: Chaos detection failed${NC}"
    echo "The system should have detected the intentional problems."
fi

echo
echo "Key insights from chaos testing:"
echo "- Real bugs leave statistical fingerprints in the data"
echo "- Field usage patterns reveal misconfigurations"
echo "- Cross-source contamination is detectable"
echo "- Schema evolution creates detectable inconsistencies"
echo "- Environmental differences show up in field usage"

echo
echo "To run this test against your real data:"
echo "1. Run your actual ingestors for a while"
echo "2. Then run this chaos test to see baseline vs. chaos"
echo "3. The contrast will show if detection actually works"

# Cleanup
rm -f /tmp/test_chaos_validation.py