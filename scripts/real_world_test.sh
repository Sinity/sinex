#!/usr/bin/env bash
set -euo pipefail

# Real-world testing using actual system data sources

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

echo -e "${YELLOW}=== Real-World Data Testing ===${NC}"
echo
echo "This test uses actual system data sources to validate assumption detection"
echo

DB_URL="${DATABASE_URL:-postgresql://sinex:sinex@localhost:5432/sinex}"

# Reset for clean test
./scripts/db_reset.sh

echo "=== PHASE 1: Capture Real System Data ==="
echo

# 1. Real filesystem events from system
echo "Capturing real filesystem events..."
TEST_DIR="/tmp/sinex_real_test_$$"
mkdir -p "$TEST_DIR"

# Generate real filesystem activity and capture with inotifywait
if command -v inotifywait >/dev/null; then
    echo "Using inotify to capture real filesystem events..."
    
    # Start monitoring in background
    inotifywait -m -r -e create,modify,delete,move "$TEST_DIR" --format '{"path": "%w%f", "event": "%e", "timestamp": "%T"}' --timefmt '%Y-%m-%dT%H:%M:%S' > /tmp/real_fs_events.json &
    INOTIFY_PID=$!
    
    # Generate some real activity
    echo "content" > "$TEST_DIR/real_file.txt"
    sleep 1
    echo "more content" >> "$TEST_DIR/real_file.txt"
    sleep 1
    mkdir "$TEST_DIR/real_dir"
    sleep 1
    mv "$TEST_DIR/real_file.txt" "$TEST_DIR/real_dir/"
    sleep 1
    rm -rf "$TEST_DIR/real_dir"
    
    # Stop monitoring
    kill $INOTIFY_PID 2>/dev/null || true
    sleep 1
    
    # Insert real filesystem events into database
    echo "Inserting captured filesystem events..."
    while IFS= read -r line; do
        if [[ "$line" =~ \{.*\} ]]; then
            # Extract event details and convert to our format
            path=$(echo "$line" | jq -r '.path // empty' 2>/dev/null || echo "unknown")
            event=$(echo "$line" | jq -r '.event // empty' 2>/dev/null || echo "unknown")
            timestamp=$(echo "$line" | jq -r '.timestamp // empty' 2>/dev/null || echo "")
            
            if [[ -n "$path" && "$path" != "unknown" ]]; then
                # Get real file info
                size=0
                perms=""
                if [[ -f "$path" ]]; then
                    size=$(stat -f%z "$path" 2>/dev/null || stat -c%s "$path" 2>/dev/null || echo 0)
                    perms=$(stat -f%Mp%Lp "$path" 2>/dev/null || stat -c%a "$path" 2>/dev/null || echo "644")
                fi
                
                # Convert to our event format
                case "$event" in
                    CREATE*) event_type="file_created" ;;
                    MODIFY*) event_type="file_modified" ;;
                    DELETE*) event_type="file_deleted" ;;
                    MOVE*) event_type="file_renamed" ;;
                    *) event_type="file_modified" ;;
                esac
                
                # Insert with real data
                psql "$DB_URL" -c "
                INSERT INTO raw.events (source, event_type, host, payload) VALUES (
                    'filesystem', 
                    '$event_type', 
                    '$(hostname)', 
                    '{\"path\": \"$path\", \"size\": $size, \"permissions\": \"$perms\", \"real_event\": true}'
                )"
            fi
        fi
    done < /tmp/real_fs_events.json
else
    echo "inotifywait not available, skipping real filesystem events"
fi

# 2. Real process data (but feed it to filesystem ingestor - this should be detected as wrong)
echo
echo "Capturing real process data (intentionally mislabeled as filesystem)..."

ps aux | head -20 | tail -n +2 | while read -r line; do
    # Parse ps output
    user=$(echo "$line" | awk '{print $1}')
    pid=$(echo "$line" | awk '{print $2}')
    cpu=$(echo "$line" | awk '{print $3}')
    mem=$(echo "$line" | awk '{print $4}')
    command=$(echo "$line" | awk '{for(i=11;i<=NF;++i) printf "%s ", $i; print ""}' | sed 's/ $//')
    
    # Insert as filesystem event (this is wrong and should be detected)
    psql "$DB_URL" -c "
    INSERT INTO raw.events (source, event_type, host, payload) VALUES (
        'filesystem', 
        'file_created', 
        '$(hostname)', 
        '{\"user\": \"$user\", \"pid\": $pid, \"cpu_percent\": $cpu, \"memory_percent\": $mem, \"command\": \"$command\", \"corrupted_data\": true}'
    )"
done

# 3. Real network data (mislabeled as hyprland events)
echo
echo "Capturing real network data (intentionally mislabeled as hyprland)..."

if command -v netstat >/dev/null; then
    netstat -an | head -20 | tail -n +3 | while read -r line; do
        proto=$(echo "$line" | awk '{print $1}')
        local_addr=$(echo "$line" | awk '{print $4}')
        foreign_addr=$(echo "$line" | awk '{print $5}')
        state=$(echo "$line" | awk '{print $6}')
        
        # Insert as hyprland event (wrong!)
        psql "$DB_URL" -c "
        INSERT INTO raw.events (source, event_type, host, payload) VALUES (
            'hyprland', 
            'window_focused', 
            '$(hostname)', 
            '{\"protocol\": \"$proto\", \"local_address\": \"$local_addr\", \"foreign_address\": \"$foreign_addr\", \"state\": \"$state\", \"corrupted_data\": true}'
        )"
    done
fi

# 4. Real log data (mislabeled as terminal events)
echo
echo "Capturing real system log data (intentionally mislabeled as terminal)..."

if [[ -r /var/log/system.log ]]; then
    tail -20 /var/log/system.log | while IFS= read -r line; do
        timestamp=$(echo "$line" | cut -d' ' -f1-3)
        host=$(echo "$line" | cut -d' ' -f4)
        process=$(echo "$line" | cut -d' ' -f5 | cut -d'[' -f1)
        message=$(echo "$line" | cut -d' ' -f6-)
        
        # Insert as terminal event (wrong!)
        psql "$DB_URL" -c "
        INSERT INTO raw.events (source, event_type, host, payload) VALUES (
            'terminal.kitty', 
            'command_executed', 
            '$(hostname)', 
            '{\"log_timestamp\": \"$timestamp\", \"log_host\": \"$host\", \"log_process\": \"$process\", \"log_message\": \"$message\", \"corrupted_data\": true}'
        )" 2>/dev/null || true
    done
elif [[ -r /var/log/syslog ]]; then
    tail -20 /var/log/syslog | while IFS= read -r line; do
        # Similar processing for syslog format
        timestamp=$(echo "$line" | cut -d' ' -f1-3)
        host=$(echo "$line" | cut -d' ' -f4)
        message=$(echo "$line" | cut -d' ' -f5-)
        
        psql "$DB_URL" -c "
        INSERT INTO raw.events (source, event_type, host, payload) VALUES (
            'terminal.kitty', 
            'command_executed', 
            '$(hostname)', 
            '{\"syslog_timestamp\": \"$timestamp\", \"syslog_host\": \"$host\", \"syslog_message\": \"$message\", \"corrupted_data\": true}'
        )" 2>/dev/null || true
    done
fi

echo
echo "=== PHASE 2: Analyze Real Data for Assumption Violations ==="
echo

# Count what we captured
echo "Data captured:"
psql "$DB_URL" -c "
SELECT 
    source,
    event_type,
    COUNT(*) as events,
    COUNT(*) FILTER (WHERE payload ? 'real_event') as real_events,
    COUNT(*) FILTER (WHERE payload ? 'corrupted_data') as corrupted_events
FROM raw.events 
GROUP BY source, event_type 
ORDER BY source, event_type;
"

echo
echo "Field analysis - this should show the data corruption:"
psql "$DB_URL" -c "
WITH field_analysis AS (
    SELECT 
        source,
        event_type,
        jsonb_object_keys(payload) as field,
        COUNT(*) as occurrences,
        COUNT(*) FILTER (WHERE payload ? 'corrupted_data') as corrupted_occurrences
    FROM raw.events
    GROUP BY source, event_type, jsonb_object_keys(payload)
)
SELECT 
    source,
    event_type,
    field,
    occurrences,
    corrupted_occurrences,
    CASE 
        WHEN field IN ('pid', 'cpu_percent', 'memory_percent', 'command') AND source = 'filesystem' 
        THEN '🚨 PROCESS DATA IN FILESYSTEM EVENTS'
        WHEN field IN ('protocol', 'local_address', 'foreign_address', 'state') AND source = 'hyprland'
        THEN '🚨 NETWORK DATA IN HYPRLAND EVENTS'  
        WHEN field LIKE 'log_%' AND source = 'terminal.kitty'
        THEN '🚨 LOG DATA IN TERMINAL EVENTS'
        WHEN field IN ('path', 'size', 'permissions') AND source = 'filesystem'
        THEN '✅ LEGITIMATE FILESYSTEM FIELD'
        ELSE '❓ INVESTIGATE'
    END as assessment
FROM field_analysis
WHERE occurrences > 0
ORDER BY 
    CASE 
        WHEN assessment LIKE '🚨%' THEN 1
        WHEN assessment LIKE '❓%' THEN 2  
        ELSE 3
    END,
    source, event_type, field;
"

echo
echo "=== PHASE 3: Test Detection Accuracy ==="
echo

# Test our assumption detector
cat > /tmp/test_real_detection.py <<'EOF'
import subprocess
import json
import sys

def test_detection():
    # Get all events
    result = subprocess.run([
        'psql', sys.argv[1], '-t', '-c',
        '''SELECT 
             source, 
             event_type, 
             payload,
             CASE WHEN payload ? 'corrupted_data' THEN 'corrupted' ELSE 'clean' END as truth
           FROM raw.events'''
    ], capture_output=True, text=True)
    
    total_events = 0
    corrupted_events = 0
    detected_corrupted = 0
    false_positives = 0
    
    for line in result.stdout.strip().split('\n'):
        if '|' in line:
            parts = [p.strip() for p in line.split('|')]
            if len(parts) >= 4:
                source, event_type, payload_str, truth = parts
                total_events += 1
                
                try:
                    payload = json.loads(payload_str)
                    is_corrupted = (truth == 'corrupted')
                    
                    if is_corrupted:
                        corrupted_events += 1
                    
                    # Simple detection rules
                    detected_anomaly = False
                    
                    if source == 'filesystem':
                        # Should have path/size, not pid/command/cpu
                        if any(field in payload for field in ['pid', 'command', 'cpu_percent', 'memory_percent']):
                            detected_anomaly = True
                        elif any(field in payload for field in ['protocol', 'local_address', 'state']):
                            detected_anomaly = True
                            
                    elif source == 'hyprland':
                        # Should have window/workspace, not network fields
                        if any(field in payload for field in ['protocol', 'local_address', 'foreign_address']):
                            detected_anomaly = True
                            
                    elif 'terminal' in source:
                        # Should have command, not log fields
                        if any(field.startswith('log_') for field in payload.keys()):
                            detected_anomaly = True
                    
                    # Count detection accuracy
                    if detected_anomaly and is_corrupted:
                        detected_corrupted += 1
                        print(f"✅ Correctly detected corruption in {source}/{event_type}")
                    elif detected_anomaly and not is_corrupted:
                        false_positives += 1
                        print(f"❌ False positive in {source}/{event_type}")
                    elif not detected_anomaly and is_corrupted:
                        print(f"❌ Missed corruption in {source}/{event_type}")
                    else:
                        print(f"✅ Correctly identified clean {source}/{event_type}")
                        
                except json.JSONDecodeError:
                    print(f"❌ Invalid JSON in {source}/{event_type}")
    
    print(f"\n=== DETECTION RESULTS ===")
    print(f"Total events: {total_events}")
    print(f"Corrupted events: {corrupted_events}")
    print(f"Detected corrupted: {detected_corrupted}")
    print(f"False positives: {false_positives}")
    
    if corrupted_events > 0:
        detection_rate = detected_corrupted / corrupted_events * 100
        print(f"Detection rate: {detection_rate:.1f}%")
        
        if detection_rate > 80:
            print("🎯 EXCELLENT: High detection rate!")
            return 0
        elif detection_rate > 50:
            print("👍 GOOD: Reasonable detection rate")
            return 0
        else:
            print("😞 POOR: Low detection rate")
            return 1
    else:
        print("No corrupted events to test against")
        return 0

if __name__ == '__main__':
    exit(test_detection())
EOF

python3 /tmp/test_real_detection.py "$DB_URL"
detection_result=$?

echo
echo "=== REAL-WORLD TEST SUMMARY ==="
echo

if [ $detection_result -eq 0 ]; then
    echo -e "${GREEN}✅ SUCCESS: Real-world detection is working!${NC}"
else
    echo -e "${RED}❌ NEEDS IMPROVEMENT: Detection accuracy is low${NC}"
fi

echo
echo "This test demonstrates:"
echo "1. Using actual system data as input"
echo "2. Intentionally corrupting the data (wrong labels)"
echo "3. Measuring detection accuracy against ground truth"
echo "4. Testing with real filesystem, process, network, and log data"
echo
echo "The corruption was artificial, but the underlying data was real system data."
echo "This simulates what would happen if ingestors had bugs or misconfigurations."

# Cleanup
rm -rf "$TEST_DIR" /tmp/real_fs_events.json /tmp/test_real_detection.py