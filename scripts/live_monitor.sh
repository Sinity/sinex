#!/usr/bin/env bash
set -euo pipefail

# Live monitoring dashboard for running Sinex system

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
MAGENTA='\033[0;35m'
NC='\033[0m'

DB_URL="${DATABASE_URL:-postgresql://sinex:sinex@localhost:5432/sinex}"

# Clear screen and show header
show_header() {
    clear
    echo -e "${CYAN}╔══════════════════════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${CYAN}║                          🔍 SINEX LIVE MONITOR                              ║${NC}"
    echo -e "${CYAN}╚══════════════════════════════════════════════════════════════════════════════╝${NC}"
    echo -e "${BLUE}Database: ${DB_URL}${NC}"
    echo -e "${BLUE}Started: $(date)${NC}"
    echo
}

# Show real-time statistics
show_stats() {
    local stats=$(psql "$DB_URL" -t -c "
    SELECT 
        COUNT(*) as total_events,
        COUNT(DISTINCT source) as sources,
        COUNT(DISTINCT event_type) as event_types,
        COUNT(*) FILTER (WHERE ts_ingest > now() - interval '1 minute') as events_last_minute,
        COUNT(*) FILTER (WHERE ts_ingest > now() - interval '10 seconds') as events_last_10s
    FROM raw.events;
    " 2>/dev/null | tr -s ' ')
    
    if [[ -n "$stats" ]]; then
        read total sources types minute ten_sec <<< "$stats"
        echo -e "${GREEN}📊 STATISTICS${NC}"
        echo -e "   Total Events: ${YELLOW}$total${NC}"
        echo -e "   Active Sources: ${YELLOW}$sources${NC}"
        echo -e "   Event Types: ${YELLOW}$types${NC}"
        echo -e "   Last Minute: ${CYAN}$minute${NC}"
        echo -e "   Last 10s: ${MAGENTA}$ten_sec${NC}"
    else
        echo -e "${RED}❌ Database connection failed${NC}"
    fi
}

# Show event breakdown by source
show_breakdown() {
    echo -e "\n${GREEN}📈 EVENT BREAKDOWN${NC}"
    psql "$DB_URL" -t -c "
    SELECT 
        '   ' || source || ' → ' || event_type || ': ' || 
        COUNT(*) || ' events (' ||
        COUNT(*) FILTER (WHERE ts_ingest > now() - interval '1 minute') || ' recent)'
    FROM raw.events
    GROUP BY source, event_type
    ORDER BY COUNT(*) DESC
    LIMIT 10;
    " 2>/dev/null | head -10
}

# Show live event stream
show_live_stream() {
    echo -e "\n${GREEN}🔴 LIVE STREAM (last 8 events)${NC}"
    psql "$DB_URL" -t -c "
    SELECT 
        '   ' || 
        CASE 
            WHEN source = 'filesystem' THEN '📁'
            WHEN source = 'hyprland' THEN '🪟'
            WHEN source LIKE 'terminal%' THEN '💻'
            WHEN source = 'sinex' THEN '⚙️'
            ELSE '❓'
        END || ' ' ||
        source || ' → ' || event_type || ' (' ||
        EXTRACT(EPOCH FROM (now() - ts_ingest))::int || 's ago)' ||
        CASE 
            WHEN payload ? 'path' THEN ' | ' || substring(payload->>'path' from '[^/]*$')
            WHEN payload ? 'command' THEN ' | ' || substring(payload->>'command' from 1 for 20)
            WHEN payload ? 'window' THEN ' | ' || (payload->>'window')
            ELSE ''
        END
    FROM raw.events 
    ORDER BY ts_ingest DESC
    LIMIT 8;
    " 2>/dev/null | head -8
}

# Show validation status
show_validation() {
    echo -e "\n${GREEN}✅ VALIDATION STATUS${NC}"
    
    # Check for common validation issues
    local fs_issues=$(psql "$DB_URL" -t -c "
    SELECT COUNT(*) FROM raw.events 
    WHERE source = 'filesystem' 
    AND (NOT (payload ? 'path') OR jsonb_typeof(payload->'size') NOT IN ('number', 'null'))
    " 2>/dev/null | tr -d ' ')
    
    local cross_contamination=$(psql "$DB_URL" -t -c "
    SELECT COUNT(*) FROM raw.events 
    WHERE (source = 'filesystem' AND (payload ? 'window' OR payload ? 'workspace'))
       OR (source = 'hyprland' AND (payload ? 'path' OR payload ? 'size'))
       OR (source LIKE 'terminal%' AND (payload ? 'path' OR payload ? 'window'))
    " 2>/dev/null | tr -d ' ')
    
    if [[ "$fs_issues" -eq 0 ]]; then
        echo -e "   📁 Filesystem Events: ${GREEN}✓ Valid${NC}"
    else
        echo -e "   📁 Filesystem Events: ${RED}✗ $fs_issues invalid${NC}"
    fi
    
    if [[ "$cross_contamination" -eq 0 ]]; then
        echo -e "   🔄 Cross-contamination: ${GREEN}✓ None detected${NC}"
    else
        echo -e "   🔄 Cross-contamination: ${RED}✗ $cross_contamination suspicious${NC}"
    fi
}

# Show performance metrics
show_performance() {
    echo -e "\n${GREEN}⚡ PERFORMANCE${NC}"
    
    local perf=$(psql "$DB_URL" -t -c "
    SELECT 
        ROUND(COUNT(*) / GREATEST(EXTRACT(EPOCH FROM (MAX(ts_ingest) - MIN(ts_ingest))), 1), 1) as events_per_sec,
        COUNT(*) FILTER (WHERE ts_ingest > now() - interval '1 minute') as recent_rate
    FROM raw.events
    WHERE ts_ingest > now() - interval '5 minutes';
    " 2>/dev/null | tr -s ' ')
    
    if [[ -n "$perf" ]]; then
        read avg_rate recent_rate <<< "$perf"
        echo -e "   📊 Avg Rate: ${CYAN}$avg_rate events/sec${NC}"
        echo -e "   📈 Recent: ${CYAN}$recent_rate events/min${NC}"
    fi
}

# Show field usage patterns
show_field_patterns() {
    echo -e "\n${GREEN}🏷️  FIELD PATTERNS${NC}"
    psql "$DB_URL" -t -c "
    WITH field_stats AS (
        SELECT 
            source,
            jsonb_object_keys(payload) as field,
            COUNT(*) as usage
        FROM raw.events
        WHERE ts_ingest > now() - interval '5 minutes'
        GROUP BY source, jsonb_object_keys(payload)
    )
    SELECT 
        '   ' || source || ': ' || string_agg(field, ', ' ORDER BY usage DESC)
    FROM field_stats
    WHERE usage >= 2
    GROUP BY source
    ORDER BY source;
    " 2>/dev/null | head -5
}

# Show potential issues
show_issues() {
    echo -e "\n${RED}⚠️  POTENTIAL ISSUES${NC}"
    
    # Check for rare fields (possible bugs)
    local rare_fields=$(psql "$DB_URL" -t -c "
    WITH field_usage AS (
        SELECT 
            source, event_type,
            jsonb_object_keys(payload) as field,
            COUNT(*) as usage
        FROM raw.events
        GROUP BY source, event_type, jsonb_object_keys(payload)
    )
    SELECT COUNT(*) FROM field_usage 
    WHERE usage = 1 AND source IN ('filesystem', 'hyprland', 'terminal.kitty')
    " 2>/dev/null | tr -d ' ')
    
    if [[ "$rare_fields" -gt 0 ]]; then
        echo -e "   🔍 $rare_fields rare fields detected (possible typos)"
    fi
    
    # Check for missing expected fields
    local missing_paths=$(psql "$DB_URL" -t -c "
    SELECT COUNT(*) FROM raw.events 
    WHERE source = 'filesystem' AND NOT (payload ? 'path')
    AND ts_ingest > now() - interval '1 minute'
    " 2>/dev/null | tr -d ' ')
    
    if [[ "$missing_paths" -gt 0 ]]; then
        echo -e "   📁 $missing_paths filesystem events missing 'path' field"
    fi
    
    if [[ "$rare_fields" -eq 0 && "$missing_paths" -eq 0 ]]; then
        echo -e "   ${GREEN}✓ No issues detected${NC}"
    fi
}

# Main monitoring loop
monitor() {
    local refresh_rate=2
    
    while true; do
        show_header
        show_stats
        show_breakdown
        show_live_stream
        show_validation
        show_performance
        show_field_patterns
        show_issues
        
        echo -e "\n${BLUE}Press Ctrl+C to exit | Refreshing every ${refresh_rate}s${NC}"
        
        sleep $refresh_rate
    done
}

# Check if database is accessible
if ! psql "$DB_URL" -c "SELECT 1" >/dev/null 2>&1; then
    echo -e "${RED}❌ Cannot connect to database: $DB_URL${NC}"
    echo "Make sure the database is running and DATABASE_URL is correct"
    exit 1
fi

echo -e "${GREEN}🚀 Starting live monitor...${NC}"
echo "Monitoring database: $DB_URL"
echo "Press Ctrl+C to exit"
sleep 2

monitor