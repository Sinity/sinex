{ pkgs, ... }:
{
  environment.systemPackages = with pkgs; [
    # Core chaos tools
    (writeScriptBin "chaos-inject" ''
      #!${pkgs.bash}/bin/bash
      set -euo pipefail
      
      FAILURE_TYPE="$1"
      DURATION="''${2:-30}"
      
      case "$FAILURE_TYPE" in
        "kill")
          # Random service kill
          SERVICES=("sinex-collector" "sinex-worker" "postgresql")
          SERVICE="''${SERVICES[''$RANDOM % ''${#SERVICES[@]}]}"
          echo "Killing ''$SERVICE for ''$DURATION seconds..."
          systemctl stop "''$SERVICE"
          sleep "''$DURATION"
          systemctl start "''$SERVICE"
          ;;
        "network")
          # Network partition
          echo "Injecting network failures for ''$DURATION seconds..."
          iptables -A INPUT -m statistic --mode random --probability 0.5 -j DROP
          iptables -A OUTPUT -m statistic --mode random --probability 0.5 -j DROP
          sleep "''$DURATION"
          iptables -F
          ;;
        "disk")
          # Disk space exhaustion
          echo "Exhausting disk space for ''$DURATION seconds..."
          dd if=/dev/zero of=/tmp/disk-exhaust bs=1M count=$((1024 * 10)) &
          DD_PID=$!
          sleep "''$DURATION"
          kill $DD_PID 2>/dev/null || true
          rm -f /tmp/disk-exhaust
          ;;
        "cpu")
          # CPU stress
          echo "Stressing CPU for ''$DURATION seconds..."
          stress-ng --cpu $(nproc) --timeout "''$DURATION"s &
          ;;
        "memory")
          # Memory pressure
          echo "Creating memory pressure for ''$DURATION seconds..."
          stress-ng --vm 4 --vm-bytes 80% --timeout "''$DURATION"s &
          ;;
        "fd-exhaust")
          # File descriptor exhaustion
          echo "Exhausting file descriptors for ''$DURATION seconds..."
          ulimit -n 100
          ${pkgs.python3}/bin/python3 -c "
import time
fds = []
try:
    while True:
        fds.append(open('/dev/null'))
except:
    pass
time.sleep(''$DURATION)
for fd in fds:
    fd.close()
          "
          ;;
      esac
    '')
    
    (writeScriptBin "chaos-scenario" ''
      #!${pkgs.bash}/bin/bash
      # Run predefined chaos scenarios
      
      SCENARIO="$1"
      case "$SCENARIO" in
        "cascading-failure")
          echo "Starting cascading failure scenario..."
          chaos-inject kill 10 &
          sleep 5
          chaos-inject network 20 &
          sleep 5
          chaos-inject memory 15 &
          wait
          ;;
        "resource-storm")
          echo "Starting resource storm scenario..."
          chaos-inject cpu 30 &
          chaos-inject memory 30 &
          chaos-inject disk 30 &
          wait
          ;;
        "intermittent-chaos")
          echo "Starting intermittent chaos (5 minutes)..."
          for i in {1..30}; do
            FAILURE_TYPE=$((RANDOM % 5))
            case $FAILURE_TYPE in
              0) chaos-inject kill 5 ;;
              1) chaos-inject network 10 ;;
              2) chaos-inject disk 8 ;;
              3) chaos-inject cpu 12 ;;
              4) chaos-inject memory 10 ;;
            esac
            sleep 10
          done
          ;;
      esac
    '')
    
    (writeScriptBin "sinex-health-check" ''
      #!${pkgs.bash}/bin/bash
      # Comprehensive health check
      
      set -e
      
      # Check services
      systemctl is-active sinex-collector >/dev/null
      systemctl is-active sinex-worker >/dev/null
      systemctl is-active postgresql >/dev/null
      
      # Check database connectivity
      sinex-query --source status >/dev/null 2>&1 || sinex-query --limit 1 >/dev/null
      
      # Check event ingestion
      BEFORE=$(sinex-query --format csv 2>/dev/null | wc -l || echo "0")
      sleep 2
      AFTER=$(sinex-query --format csv 2>/dev/null | wc -l || echo "0")
      
      if [ "''$AFTER" -le "''$BEFORE" ]; then
        echo "WARNING: No new events in 2 seconds (might be normal during low activity)"
        # Don't fail health check for this - system might be idle
      fi
      
      echo "Health check passed"
    '')
    
    stress-ng
  ];
}