#!/usr/bin/env bash
set -euo pipefail

# Monitor and clean up test databases if they exceed a threshold

THRESHOLD=${1:-100}  # Default threshold is 100 databases

while true; do
    count=$(psql $DATABASE_URL -t -c "SELECT COUNT(*) FROM pg_database WHERE datname LIKE 'sinex_test_%' AND datname NOT LIKE '%template%'")
    count=$(echo $count | tr -d ' ')
    
    if [ "$count" -gt "$THRESHOLD" ]; then
        echo "⚠️  Test database count ($count) exceeds threshold ($THRESHOLD), cleaning up..."
        
        # Get databases older than 5 minutes
        old_dbs=$(psql $DATABASE_URL -t -c "
            SELECT datname 
            FROM pg_database 
            WHERE datname LIKE 'sinex_test_%' 
              AND datname NOT LIKE '%template%'
              AND NOT EXISTS (
                  SELECT 1 FROM pg_stat_activity 
                  WHERE pg_stat_activity.datname = pg_database.datname
                  AND state != 'idle'
              )
            LIMIT $((count - THRESHOLD/2))
        ")
        
        cleaned=0
        for db in $old_dbs; do
            if [ -n "$db" ]; then
                psql $DATABASE_URL -c "DROP DATABASE IF EXISTS $db" 2>/dev/null && cleaned=$((cleaned + 1)) || true
            fi
        done
        
        echo "✅ Cleaned up $cleaned test databases"
    else
        echo "📊 Test database count: $count (threshold: $THRESHOLD)"
    fi
    
    sleep 10
done