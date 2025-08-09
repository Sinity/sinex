# Common test helper functions and utilities for VM tests
{ pkgs, ... }:

let
  # Python test helpers module
  testHelpers = pkgs.writeText "test_helpers.py" ''
    import time
    import re
    import subprocess
    from typing import Optional, Tuple, List

    class TestHelpers:
        def __init__(self, machine):
            self.machine = machine
            
        def wait_for_sinex_ready(self, timeout: int = 60) -> None:
            """Wait for Sinex services to be fully ready."""
            self.machine.wait_for_unit("postgresql.service", timeout=timeout)
            self.machine.wait_for_unit("sinex-migrate.service", timeout=timeout)
            self.machine.wait_for_unit("sinex-ingestd.service", timeout=timeout)
            
            # Verify services are actually working
            self.machine.wait_until_succeeds(
                "systemctl is-active sinex-ingestd",
                timeout=30
            )
            
        def get_event_count(self) -> int:
            """Get current event count from database."""
            try:
                result = self.machine.succeed("sinex stats")
                match = re.search(r'Total events captured: (\d+)', result)
                if match:
                    return int(match.group(1))
            except:
                pass
            return 0
            
        def generate_events(self, count: int, prefix: str = "test", 
                          path: str = "/home/test/watched") -> int:
            """Generate filesystem events in batches."""
            batch_size = 50
            events_before = self.get_event_count()
            
            for batch_start in range(0, count, batch_size):
                batch_end = min(batch_start + batch_size, count)
                batch_count = batch_end - batch_start
                
                # Create batch of files in one command
                files = " ".join([f"{path}/{prefix}_{i}.txt" for i in range(batch_start, batch_end)])
                self.machine.succeed(f"su - test -c 'touch {files}'")
                
                # Small delay between batches
                if batch_end < count:
                    time.sleep(0.1)
                    
            # Wait for processing
            time.sleep(2)
            events_after = self.get_event_count()
            return events_after - events_before
            
        def check_service_health(self, service: str) -> bool:
            """Check if a service is healthy."""
            try:
                self.machine.succeed(f"systemctl is-active {service}")
                return True
            except:
                return False
                
        def wait_for_event_processing(self, expected_count: int, 
                                    timeout: int = 30) -> bool:
            """Wait for events to be processed."""
            start_time = time.time()
            
            while time.time() - start_time < timeout:
                current_count = self.get_event_count()
                if current_count >= expected_count:
                    return True
                time.sleep(1)
                
            return False
            
        def cleanup_test_data(self, path: str = "/home/test/watched") -> None:
            """Clean up test data files."""
            self.machine.succeed(f"su - test -c 'rm -f {path}/*.txt {path}/*.tmp'")
            
        def check_wayland_available(self) -> bool:
            """Check if Wayland is available (for optional tests)."""
            try:
                self.machine.succeed("test -e /run/user/1000/wayland-1")
                return True
            except:
                return False
                
        def measure_operation_time(self, operation: callable) -> float:
            """Measure how long an operation takes."""
            start = time.time()
            operation()
            return time.time() - start
  '';

  # Bash helper scripts
  vmTestHelpers = pkgs.writeScriptBin "vm-test-helpers" ''
    #!${pkgs.bash}/bin/bash
    set -euo pipefail
    
    # Function to wait for service with timeout
    wait_for_service() {
      local service="$1"
      local timeout="''${2:-60}"
      local elapsed=0
      
      echo "Waiting for $service (timeout: $timeout seconds)..."
      
      while ! systemctl is-active --quiet "$service"; do
        if [ $elapsed -ge $timeout ]; then
          echo "Timeout waiting for $service"
          return 1
        fi
        sleep 1
        elapsed=$((elapsed + 1))
      done
      
      echo "$service is ready"
    }
    
    # Function to check database connectivity
    check_db() {
      su - postgres -c "psql -d sinex -c 'SELECT 1;'" >/dev/null 2>&1
    }
    
    # Export functions for use
    export -f wait_for_service
    export -f check_db
  '';

  # Performance monitoring helpers
  perfHelpers = pkgs.writeScriptBin "perf-helpers" ''
    #!${pkgs.bash}/bin/bash
    
    # Monitor system resources during test
    monitor_resources() {
      local duration="''${1:-60}"
      local interval="''${2:-5}"
      
      echo "timestamp,cpu_usage,memory_used,load_avg,events_per_sec"
      
      for ((i=0; i<duration; i+=interval)); do
        timestamp=$(date +%s)
        cpu_usage=$(top -bn1 | grep "Cpu(s)" | awk '{print $2}' | cut -d'%' -f1)
        memory_used=$(free -m | awk 'NR==2{printf "%.1f", $3*100/$2}')
        load_avg=$(uptime | awk -F'load average:' '{print $2}' | awk '{print $1}' | tr -d ',')
        events_per_sec=$(sinex perf 2>/dev/null | grep "1 minute" | awk '{print $NF}' | tr -d ')')
        
        echo "$timestamp,$cpu_usage,$memory_used,$load_avg,$events_per_sec"
        sleep $interval
      done
    }
  '';
in
{
  environment.systemPackages = [ vmTestHelpers perfHelpers ];
  
  # Make test helpers available in test scripts
  environment.etc."nixos-test/test_helpers.py".source = testHelpers;
  
  # Configure tmpfs for test directories (faster file operations)
  fileSystems."/home/test/watched" = {
    device = "tmpfs";
    fsType = "tmpfs";
    options = [ "size=512M" "mode=755" "uid=1000" "gid=100" ];
  };
  
  fileSystems."/tmp/perf-test" = {
    device = "tmpfs";
    fsType = "tmpfs"; 
    options = [ "size=1G" "mode=1777" ];
  };
}
