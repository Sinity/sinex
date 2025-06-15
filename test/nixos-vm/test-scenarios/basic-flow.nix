# Basic E2E flow test for Sinex
{ pkgs }:

pkgs.nixosTest {
  name = "sinex-basic-flow";
  
  nodes.machine = { config, pkgs, ... }: {
    imports = [ ../vm-config.nix ];
    
    # For this basic test, we'll simulate Sinex with a simple script
    # In the real implementation, this would use the actual Sinex binaries
    environment.systemPackages = with pkgs; [
      (writeScriptBin "sinex-collector" ''
        #!${bash}/bin/bash
        echo "Sinex collector starting..."
        echo "Monitoring /home/test/watched"
        
        # Simple file watcher simulation
        while true; do
          for file in /home/test/watched/*; do
            if [ -f "$file" ]; then
              echo "Event: file.created - $file" >> /tmp/sinex-events.log
              rm "$file"  # Clean up for continuous testing
            fi
          done
          sleep 1
        done
      '')
      
      (writeScriptBin "sinex" ''
        #!${bash}/bin/bash
        case "$1" in
          query)
            echo "Recent events:"
            tail -n 10 /tmp/sinex-events.log 2>/dev/null || echo "No events captured yet"
            ;;
          stats)
            count=$(wc -l < /tmp/sinex-events.log 2>/dev/null || echo "0")
            echo "Total events captured: $count"
            ;;
          *)
            echo "Usage: sinex {query|stats}"
            ;;
        esac
      '')
    ];
    
    # Systemd service for the collector
    systemd.services.sinex-collector = {
      description = "Sinex Event Collector";
      after = [ "postgresql.service" ];
      wantedBy = [ "multi-user.target" ];
      
      serviceConfig = {
        Type = "simple";
        User = "test";
        ExecStart = "${pkgs.bash}/bin/bash -c 'sinex-collector'";
        Restart = "on-failure";
      };
    };
  };
  
  testScript = ''
    start_all()
    
    # Wait for system to be ready
    machine.wait_for_unit("multi-user.target")
    machine.wait_for_unit("postgresql.service")
    
    # Verify PostgreSQL is running
    machine.succeed("systemctl is-active postgresql")
    
    # Start the collector
    machine.systemctl("start sinex-collector")
    machine.wait_for_unit("sinex-collector.service")
    
    # Test 1: Basic file creation event
    with subtest("File creation event capture"):
        # Create a test file
        machine.succeed("su - test -c 'echo \"Hello Sinex\" > /home/test/watched/test1.txt'")
        
        # Wait for event to be processed
        machine.sleep(2)
        
        # Query events
        output = machine.succeed("su - test -c 'sinex query'")
        print(f"Query output: {output}")
        
        # Verify event was captured
        assert "file.created" in output, "File creation event not captured"
        assert "test1.txt" in output, "Test file not mentioned in events"
    
    # Test 2: Multiple events
    with subtest("Multiple event capture"):
        # Create multiple files
        for i in range(5):
            machine.succeed(f"su - test -c 'touch /home/test/watched/file_{i}.txt'")
        
        # Wait for processing
        machine.sleep(3)
        
        # Check stats
        stats = machine.succeed("su - test -c 'sinex stats'")
        print(f"Stats output: {stats}")
        
        # Extract event count
        import re
        match = re.search(r'Total events captured: (\d+)', stats)
        assert match, "Could not parse stats output"
        count = int(match.group(1))
        assert count >= 6, f"Expected at least 6 events, got {count}"
    
    # Test 3: Service resilience
    with subtest("Service restart resilience"):
        # Restart the collector
        machine.systemctl("restart sinex-collector")
        machine.wait_for_unit("sinex-collector.service")
        
        # Generate new event
        machine.succeed("su - test -c 'echo \"After restart\" > /home/test/watched/restart-test.txt'")
        machine.sleep(2)
        
        # Verify it still works
        output = machine.succeed("su - test -c 'sinex query'")
        assert "restart-test.txt" in output, "Event not captured after restart"
    
    # Test 4: Database connectivity (basic check)
    with subtest("Database connectivity"):
        # Verify we can connect to the database
        machine.succeed("su - postgres -c 'psql -d sinex_test -c \"SELECT 1;\"'")
        
        # Check extensions
        extensions = machine.succeed("su - postgres -c 'psql -d sinex_test -c \"\\dx\"'")
        assert "timescaledb" in extensions, "TimescaleDB not installed"
        assert "uuid-ossp" in extensions, "UUID extension not installed"
  '';
}