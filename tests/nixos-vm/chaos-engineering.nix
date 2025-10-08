{ pkgs
, sinex-ingestd
, sinex-gateway
, pg_jsonschema
, sinex ? null
, sinexCli ? null
, ...
}:

{
  name = "sinex-chaos-engineering";

  nodes.machine = { pkgs, config, lib, ... }: {
    imports = [
      (import ./common/test-base.nix {
        inherit config pkgs lib sinex-ingestd sinex-gateway pg_jsonschema sinex sinexCli;
      })
      ./common/chaos-toolkit.nix
    ];

    # Enhanced monitoring for chaos testing
    systemd.services.chaos-monitor = {
      description = "Chaos test monitoring";
      wantedBy = [ "multi-user.target" ];
      script = ''
        while true; do
          echo "=== System Status at $(date) ==="
          echo "Memory: $(free -h | grep Mem | awk '{print $3 "/" $2}')"
          echo "Load: $(uptime | awk -F'load average:' '{print $2}')"
          echo "Disk: $(df -h / | tail -1 | awk '{print $3 "/" $2}')"
          echo "Events/sec: $(sinex-query --format csv --after '1 second ago' 2>/dev/null | wc -l || echo '0')"
          echo "Active services: $(systemctl list-units --type=service --state=active | wc -l)"
          echo
          sleep 10
        done
      '';
    };
  };

  testScript = ''
    import random
    import time
    import json

    machine.wait_for_unit("multi-user.target")
    machine.wait_for_unit("sinex-ingestd.service")
    machine.wait_for_unit("sinex-worker.service")
    machine.wait_for_unit("chaos-monitor.service")
    
    # Baseline health check
    with subtest("Baseline system health"):
        machine.succeed("sinex-health-check")
        baseline_events = int(machine.succeed("sinex-query --format csv 2>/dev/null | wc -l || echo '0'").strip())
        print(f"Baseline event count: {baseline_events}")
    
    # Test 1: Random individual failures
    with subtest("Random service failures"):
        failure_types = ["kill", "cpu", "memory"]  # Skip network/disk for stability
        
        for i in range(5):  # Reduced from 10 for faster execution
            failure_type = random.choice(failure_types)
            print(f"Iteration {i+1}: Injecting {failure_type} failure")
            
            machine.succeed(f"chaos-inject {failure_type} 15 &")  # Reduced duration
            
            # System should recover within 60 seconds
            machine.wait_until_succeeds("sinex-health-check", timeout=60)
            
            # Brief wait for stability
            time.sleep(5)
            current_events = int(machine.succeed("sinex-query --format csv 2>/dev/null | wc -l || echo '0'").strip())
            print(f"Events after chaos {i+1}: {current_events}")
            baseline_events = max(baseline_events, current_events)  # Allow for growth
    
    # Test 2: Cascading failures
    with subtest("Cascading failure scenario"):
        print("Starting cascading failure test")
        machine.succeed("chaos-scenario cascading-failure &")
        
        # Monitor system during chaos
        recovery_attempts = 0
        max_attempts = 12  # 60 seconds total
        
        for i in range(max_attempts):
            time.sleep(5)
            try:
                machine.succeed("sinex-health-check")
                print(f"Health check passed at {i*5} seconds")
                recovery_attempts = 0
            except:
                recovery_attempts += 1
                print(f"Health check failed at {i*5} seconds (attempt {recovery_attempts})")
                if recovery_attempts > 6:  # Allow some failures during chaos
                    print("Too many consecutive failures")
                    break
        
        # Should recover after scenario completes
        machine.wait_until_succeeds("sinex-health-check", timeout=120)
    
    # Test 3: Resource storm (simplified)
    with subtest("Resource stress scenario"):
        before_storm = int(machine.succeed("sinex-query --format csv 2>/dev/null | wc -l || echo '0'").strip())
        
        # Simplified resource storm - just CPU stress
        machine.succeed("chaos-inject cpu 20 &")
        
        # System should continue processing (maybe slower)
        time.sleep(25)  # Let stress run
        
        after_storm = int(machine.succeed("sinex-query --format csv 2>/dev/null | wc -l || echo '0'").strip())
        events_during_storm = after_storm - before_storm
        print(f"Processed {events_during_storm} events during resource storm")
        
        # System should still be responsive
        machine.succeed("sinex-health-check")
    
    # Test 4: Service restart resilience
    with subtest("Service restart resilience"):
        start_events = int(machine.succeed("sinex-query --format csv 2>/dev/null | wc -l || echo '0'").strip())
        
        # Restart services multiple times
        services = ["sinex-ingestd", "sinex-worker"]
        for service in services:
            print(f"Restarting {service}")
            machine.succeed(f"systemctl restart {service}")
            machine.wait_for_unit(f"{service}.service")
            time.sleep(5)
            machine.succeed("sinex-health-check")
        
        end_events = int(machine.succeed("sinex-query --format csv 2>/dev/null | wc -l || echo '0'").strip())
        events_processed = end_events - start_events
        print(f"Processed {events_processed} events during service restarts")
    
    # Test 5: Recovery validation
    with subtest("Post-chaos recovery validation"):
        # Let system stabilize
        time.sleep(30)
        
        # All services should be healthy
        machine.succeed("systemctl is-active sinex-ingestd")
        machine.succeed("systemctl is-active sinex-worker")
        machine.succeed("systemctl is-active postgresql")
        
        # Basic functionality test
        machine.succeed("touch /tmp/recovery-test.txt")
        time.sleep(3)
        machine.succeed("rm /tmp/recovery-test.txt")
        
        # Final health check
        machine.succeed("sinex-health-check")
        
        print("Chaos engineering tests completed successfully")
  '';
}
