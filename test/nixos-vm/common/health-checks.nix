# Health check utilities for VM tests
{ pkgs, ... }:

{
  environment.systemPackages = with pkgs; [
    # Sinex health check script
    (writeScriptBin "sinex-health-check" ''
      #!${pkgs.bash}/bin/bash
      set -euo pipefail
      
      FAILED=0
      
      # Check PostgreSQL
      if ! systemctl is-active --quiet postgresql; then
        echo "❌ PostgreSQL is not running"
        FAILED=1
      else
        echo "✅ PostgreSQL is running"
        
        # Check database connectivity
        if ! su - postgres -c "psql -d sinex -c 'SELECT 1;'" >/dev/null 2>&1; then
          echo "❌ Cannot connect to sinex database"
          FAILED=1
        else
          echo "✅ Database connection OK"
        fi
      fi
      
      # Check Sinex collector
      if ! systemctl is-active --quiet sinex-unified-collector; then
        echo "❌ Sinex collector is not running"
        FAILED=1
      else
        echo "✅ Sinex collector is running"
        
        # Check if collector is processing events
        EVENT_COUNT=$(su - postgres -c "psql -d sinex -t -c 'SELECT COUNT(*) FROM raw.events;'" 2>/dev/null | tr -d ' ' || echo "0")
        if [[ "$EVENT_COUNT" == "0" ]]; then
          echo "⚠️  No events captured yet"
        else
          echo "✅ Events captured: $EVENT_COUNT"
        fi
      fi
      
      # Check promo worker if enabled
      if systemctl list-unit-files | grep -q "sinex-promo-worker.service"; then
        if systemctl is-enabled --quiet sinex-promo-worker; then
          if ! systemctl is-active --quiet sinex-promo-worker; then
            echo "❌ Sinex promo worker is not running"
            FAILED=1
          else
            echo "✅ Sinex promo worker is running"
          fi
        fi
      fi
      
      # Check disk space
      DISK_USAGE=$(df -h / | tail -1 | awk '{print $5}' | tr -d '%')
      if [[ $DISK_USAGE -gt 90 ]]; then
        echo "❌ Disk usage critical: $DISK_USAGE%"
        FAILED=1
      elif [[ $DISK_USAGE -gt 80 ]]; then
        echo "⚠️  Disk usage high: $DISK_USAGE%"
      else
        echo "✅ Disk usage OK: $DISK_USAGE%"
      fi
      
      # Check memory
      MEM_AVAILABLE=$(free -m | awk 'NR==2{printf "%.0f", $7*100/$2}')
      if [[ $MEM_AVAILABLE -lt 10 ]]; then
        echo "❌ Memory critical: $MEM_AVAILABLE% available"
        FAILED=1
      elif [[ $MEM_AVAILABLE -lt 20 ]]; then
        echo "⚠️  Memory low: $MEM_AVAILABLE% available"
      else
        echo "✅ Memory OK: $MEM_AVAILABLE% available"
      fi
      
      # Check load average
      LOAD_AVG=$(uptime | awk -F'load average:' '{print $2}' | awk '{print $1}' | tr -d ',')
      LOAD_INT=$(echo "$LOAD_AVG" | cut -d. -f1)
      CPU_COUNT=$(nproc)
      
      if [[ $LOAD_INT -gt $((CPU_COUNT * 2)) ]]; then
        echo "❌ Load average critical: $LOAD_AVG (CPUs: $CPU_COUNT)"
        FAILED=1
      elif [[ $LOAD_INT -gt $CPU_COUNT ]]; then
        echo "⚠️  Load average high: $LOAD_AVG (CPUs: $CPU_COUNT)"
      else
        echo "✅ Load average OK: $LOAD_AVG (CPUs: $CPU_COUNT)"
      fi
      
      exit $FAILED
    '')
    
    # Quick event generator for testing
    (writeScriptBin "sinex-test-event" ''
      #!${pkgs.bash}/bin/bash
      set -euo pipefail
      
      TYPE="''${1:-test}"
      COUNT="''${2:-1}"
      
      for i in $(seq 1 "$COUNT"); do
        echo "$TYPE-event-$i-$(date +%s%3N)" > "/home/test/watched/$TYPE-$i.txt"
      done
      
      echo "Generated $COUNT test events of type '$TYPE'"
    '')
    
    # Service monitor
    (writeScriptBin "sinex-monitor" ''
      #!${pkgs.bash}/bin/bash
      set -euo pipefail
      
      INTERVAL="''${1:-5}"
      
      echo "Monitoring Sinex services (interval: ''${INTERVAL}s, press Ctrl+C to stop)"
      echo ""
      
      while true; do
        clear
        echo "=== Sinex System Monitor - $(date) ==="
        echo ""
        
        # Service status
        echo "Services:"
        for service in postgresql sinex-unified-collector sinex-promo-worker; do
          if systemctl list-unit-files | grep -q "$service.service"; then
            STATUS=$(systemctl is-active "$service" 2>/dev/null || echo "unknown")
            case $STATUS in
              active)
                echo "  ✅ $service"
                ;;
              inactive|failed)
                echo "  ❌ $service ($STATUS)"
                ;;
              *)
                echo "  ⚠️  $service ($STATUS)"
                ;;
            esac
          fi
        done
        
        echo ""
        echo "Events:"
        EVENT_COUNT=$(su - postgres -c "psql -d sinex -t -c 'SELECT COUNT(*) FROM raw.events;'" 2>/dev/null | tr -d ' ' || echo "0")
        RECENT_COUNT=$(su - postgres -c "psql -d sinex -t -c 'SELECT COUNT(*) FROM raw.events WHERE ts_ingest > NOW() - INTERVAL \"1 minute\";'" 2>/dev/null | tr -d ' ' || echo "0")
        echo "  Total: $EVENT_COUNT"
        echo "  Last minute: $RECENT_COUNT"
        
        echo ""
        echo "Resources:"
        echo "  CPU: $(top -bn1 | grep "Cpu(s)" | awk '{print $2}' | cut -d'%' -f1)%"
        echo "  Memory: $(free -m | awk 'NR==2{printf "%.1f%%", $3*100/$2}')"
        echo "  Load: $(uptime | awk -F'load average:' '{print $2}')"
        echo "  Disk: $(df -h / | tail -1 | awk '{print $5}') used"
        
        sleep "$INTERVAL"
      done
    '')
  ];
  
  # Systemd service for continuous health monitoring
  systemd.services.sinex-health-monitor = {
    description = "Sinex health monitoring service";
    after = [ "sinex-unified-collector.service" ];
    wantedBy = [ ];
    
    serviceConfig = {
      Type = "simple";
      ExecStart = ''${pkgs.bash}/bin/bash -c "while true; do sinex-health-check >/tmp/sinex-health.log 2>&1; sleep 30; done"'';
      Restart = "always";
      RestartSec = "10";
    };
  };
}
