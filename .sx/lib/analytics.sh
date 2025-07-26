#!/usr/bin/env bash
# Consolidated analytics - only what matters

ANALYTICS_DIR="$HOME/.sinex-analytics"

# Get the one number that matters: how well is development flowing?
flow_score() {
    local score=100
    local issues=""
    
    # Check bacon completion rate
    if [ -f "$ANALYTICS_DIR/bacon-events.jsonl" ]; then
        local completion_rate=$(jq -s '
            map(select(.event == "compilation_complete" or .event == "compilation_interrupted")) |
            group_by(.event) | 
            map({event: .[0].event, count: length}) |
            (map(select(.event == "compilation_complete"))[0].count // 0) as $complete |
            (map(select(.event == "compilation_interrupted"))[0].count // 0) as $interrupted |
            if ($complete + $interrupted) > 0 then
                ($complete * 100 / ($complete + $interrupted))
            else 100 end
        ' "$ANALYTICS_DIR/bacon-events.jsonl" 2>/dev/null || echo "100")
        
        if (( $(echo "$completion_rate < 50" | bc -l) )); then
            score=$((score - 30))
            issues="${issues}frequent-interruptions,"
        fi
    fi
    
    # Check cache effectiveness
    if command -v sccache >/dev/null && sccache --show-stats >/dev/null 2>&1; then
        local hit_rate=$(sccache --show-stats | grep "Cache hit rate" | grep -o "[0-9.]\+" || echo "0")
        if (( $(echo "$hit_rate < 50" | bc -l) )); then
            score=$((score - 20))
            issues="${issues}low-cache-hits,"
        fi
    fi
    
    # Check build times trend
    if [ -f "$ANALYTICS_DIR/compilation-events.jsonl" ]; then
        local recent_avg=$(tail -10 "$ANALYTICS_DIR/compilation-events.jsonl" | \
            jq -r 'select(.compile_type=="build") | .duration_ms' 2>/dev/null | \
            awk '{sum+=$1; count++} END {if(count>0) printf "%.0f", sum/count; else print "0"}')
        
        if [ "$recent_avg" -gt 30000 ]; then  # > 30s average
            score=$((score - 20))
            issues="${issues}slow-builds,"
        fi
    fi
    
    echo "$score:$issues"
}

# Simple one-line summary
summary() {
    local result=$(flow_score)
    local score=$(echo "$result" | cut -d: -f1)
    local issues=$(echo "$result" | cut -d: -f2)
    
    if [ $score -ge 80 ]; then
        echo "✅ Development flow: Excellent ($score/100)"
    elif [ $score -ge 60 ]; then
        echo "⚡ Development flow: Good ($score/100)"
        [ -n "$issues" ] && echo "   Issues: $issues"
    else
        echo "⚠️  Development flow: Needs attention ($score/100)"
        [ -n "$issues" ] && echo "   Issues: $issues"
    fi
}

# Export functions
export -f flow_score
export -f summary

# If called directly
"$@"