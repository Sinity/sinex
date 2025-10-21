{ pkgs, ... }:
{
  environment.systemPackages = with pkgs; [
    # Core chaos tools
    (writeScriptBin "chaos-inject" ''
      #!${pkgs.bash}/bin/bash
      set -euo pipefail
      
      FAILURE_TYPE="''${1:-}"
      DURATION="''${2:-30}"

      if [[ -z "$FAILURE_TYPE" ]]; then
        echo "Usage: chaos-inject <kill|network|disk|cpu|memory|fd-exhaust> [duration]" >&2
        exit 1
      fi

      mapfile -t SERVICES < <(
        systemctl list-units 'sinex-*.service' --state=active --no-legend --plain 2>/dev/null \
          | awk '{print $1}' || true
      )

      # Fall back to core services if satellites are not yet active
      if [[ "''${#SERVICES[@]}" -eq 0 ]]; then
        SERVICES=("sinex-ingestd.service")
      fi
      SERVICES+=("postgresql.service")
      
      case "$FAILURE_TYPE" in
        "kill")
          # Random service kill
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

    stress-ng
  ];
}
