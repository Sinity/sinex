# Failure recovery test for Sinex
{ pkgs
, sinex-ingestd
, sinex-gateway
, pg_jsonschema
, sinex ? null
, sinexCli ? null
, ...
}:

let
  sinexPackage = sinex or sinex-ingestd;
  sinexCliPackage = sinexCli or pkgs.python3;
  # Enhanced query tool with recovery testing support
  sinex-query = pkgs.writeScriptBin "sinex" ''
    #!${pkgs.python3}/bin/python3
    import subprocess
    import sys
    import json
    import time
    import os

    def query_events(limit=10, source=None, after=None):
        where_clause = ""
        if source:
            where_clause += f" AND source = '{source}'"
        if after:
            where_clause += f" AND ts_ingest > NOW() - INTERVAL '{after}'"
        
        cmd = f"psql -d sinex -t -c \"SELECT id, source, event_type, ts_ingest, payload FROM core.events WHERE 1=1{where_clause} ORDER BY ts_ingest DESC LIMIT {limit};\""
        result = subprocess.run([
            "su", "-", "postgres", "-c", cmd
        ], capture_output=True, text=True)
        
        if result.returncode == 0:
            lines = [line.strip() for line in result.stdout.split('\n') if line.strip()]
            if lines:
                print(f"Recent events ({len(lines)} found):")
                for line in lines:
                    print(f"  {line}")
            else:
                print("No events found")
        else:
            print(f"Query failed: {result.stderr}")

    def db_status():
        cmd = "psql -d sinex -t -c \"SELECT 'DB_CONNECTED' AS status;\""
        result = subprocess.run([
            "su", "-", "postgres", "-c", cmd
        ], capture_output=True, text=True)
        
        if result.returncode == 0:
            print("Database: CONNECTED")
            return True
        else:
            print(f"Database: DISCONNECTED ({result.stderr})")
            return False

    def service_status():
        services = ['sinex-ingestd', 'sinex-gateway', 'postgresql']
        statuses = {}
        for service in services:
            result = subprocess.run([
                "systemctl", "is-active", service
            ], capture_output=True, text=True)
            
            statuses[service] = result.stdout.strip()
            print(f"{service}: {statuses[service]}")
        
        return statuses

    def stats():
        cmd = "psql -d sinex -t -c 'SELECT COUNT(*) FROM core.events;'"
        result = subprocess.run([
            "su", "-", "postgres", "-c", cmd
        ], capture_output=True, text=True)
        
        if result.returncode == 0:
            count = result.stdout.strip()
            print(f"Total events captured: {count}")
            return int(count) if count.isdigit() else 0
        else:
            print(f"Stats failed: {result.stderr}")
            return -1

    def work_queue_stats():
        cmd = "psql -d sinex -t -c \"SELECT status, COUNT(*) FROM sinex_schemas.work_queue GROUP BY status ORDER BY status;\""
        result = subprocess.run([
            "su", "-", "postgres", "-c", cmd
        ], capture_output=True, text=True)
        
        if result.returncode == 0:
            lines = [line.strip() for line in result.stdout.split('\n') if line.strip()]
            print("Work queue by status:")
            for line in lines:
                print(f"  {line}")
        else:
            print(f"Work queue stats failed: {result.stderr}")

    # Parse command line arguments
    if len(sys.argv) > 1:
        if sys.argv[1] == "stats":
            stats()
        elif sys.argv[1] == "db-status":
            db_status()
        elif sys.argv[1] == "service-status":
            service_status()
        elif sys.argv[1] == "queue":
            work_queue_stats()
        elif sys.argv[1] == "query":
            limit = 10
            source = None
            after = None
            i = 2
            while i < len(sys.argv):
                arg = sys.argv[i]
                if arg == "--limit" and i + 1 < len(sys.argv):
                    limit = int(sys.argv[i + 1])
                    i += 2
                elif arg == "--source" and i + 1 < len(sys.argv):
                    source = sys.argv[i + 1]
                    i += 2
                elif arg == "--after" and i + 1 < len(sys.argv):
                    after = sys.argv[i + 1]
                    i += 2
                else:
                    i += 1
            query_events(limit, source, after)
        else:
            query_events()
    else:
        query_events()
  '';

  # Failure injection script
  failure-injector = pkgs.writeScriptBin "sinex-failure" ''
    #!${pkgs.bash}/bin/bash
    set -e
    
    FAILURE_TYPE=$1
    DURATION=''${2:-10}  # Default 10 seconds
    
    case $FAILURE_TYPE in
      db-disconnect)
        echo "Stopping PostgreSQL for $DURATION seconds..."
        systemctl stop postgresql
        sleep $DURATION
        echo "Restarting PostgreSQL..."
        systemctl start postgresql
        ;;
      collector-crash)
        echo "Stopping collector for $DURATION seconds..."
        systemctl stop sinex-ingestd
        sleep $DURATION
        echo "Restarting collector..."
        systemctl start sinex-ingestd
        ;;
      worker-crash)
        echo "Stopping worker for $DURATION seconds..."
        systemctl stop sinex-gateway
        sleep $DURATION
        echo "Restarting worker..."
        systemctl start sinex-gateway
        ;;
      disk-full)
        echo "Simulating disk full condition..."
        # Create large file to fill disk
        dd if=/dev/zero of=/tmp/diskfill bs=1M count=100 2>/dev/null || true
        sleep $DURATION
        echo "Cleaning up disk..."
        rm -f /tmp/diskfill
        ;;
      network-partition)
        echo "Simulating network partition for $DURATION seconds..."
        # Block PostgreSQL port
        iptables -A INPUT -p tcp --dport 5432 -j DROP
        iptables -A OUTPUT -p tcp --dport 5432 -j DROP
        sleep $DURATION
        echo "Restoring network..."
        iptables -D INPUT -p tcp --dport 5432 -j DROP
        iptables -D OUTPUT -p tcp --dport 5432 -j DROP
        ;;
      memory-pressure)
        echo "Creating memory pressure for $DURATION seconds..."
        # Allocate memory to create pressure
        stress --vm 2 --vm-bytes 128M --timeout ''${DURATION}s &
        wait
        echo "Memory pressure test completed"
        ;;
      *)
        echo "Unknown failure type: $FAILURE_TYPE"
        echo "Available types: db-disconnect, collector-crash, worker-crash, disk-full, network-partition, memory-pressure"
        exit 1
        ;;
    esac
    
    echo "Failure injection '$FAILURE_TYPE' completed"
  '';

  # Recovery verification script
  recovery-verify = pkgs.writeScriptBin "sinex-verify" ''
    #!${pkgs.bash}/bin/bash
    set -e
    
    MAX_WAIT=''${1:-60}  # Default 60 seconds max wait
    
    echo "Verifying system recovery (max wait: ''${MAX_WAIT}s)..."
    
    start_time=$(date +%s)
    
    # Check database connectivity
    while ! sinex db-status >/dev/null 2>&1; do
        current_time=$(date +%s)
        elapsed=$((current_time - start_time))
        if [ $elapsed -gt $MAX_WAIT ]; then
            echo "FAIL: Database not recovered within ''${MAX_WAIT}s"
            exit 1
        fi
        echo "Waiting for database... (''${elapsed}s)"
        sleep 2
    done
    
    # Check service status
    while ! systemctl is-active sinex-ingestd >/dev/null 2>&1; do
        current_time=$(date +%s)
        elapsed=$((current_time - start_time))
        if [ $elapsed -gt $MAX_WAIT ]; then
            echo "FAIL: Collector not recovered within ''${MAX_WAIT}s"
            exit 1
        fi
        echo "Waiting for collector... (''${elapsed}s)"
        sleep 2
    done
    
    while ! systemctl is-active sinex-gateway >/dev/null 2>&1; do
        current_time=$(date +%s)
        elapsed=$((current_time - start_time))
        if [ $elapsed -gt $MAX_WAIT ]; then
            echo "FAIL: Worker not recovered within ''${MAX_WAIT}s"
            exit 1
        fi
        echo "Waiting for worker... (''${elapsed}s)"
        sleep 2
    done
    
    # Test basic functionality
    echo "Testing basic functionality..."
    
    # Generate test event
    su - test -c 'echo "recovery-test-$(date +%s)" > /home/test/watched/recovery-test.txt'
    
    # Wait for event to be processed
    sleep 5
    
    # Verify event was captured
    if sinex query --limit 5 | grep -q "recovery-test"; then
        echo "SUCCESS: System fully recovered and operational"
        return 0
    else
        echo "FAIL: System not capturing new events after recovery"
        return 1
    fi
  '';
in
pkgs.nixosTest {
  name = "sinex-failure-recovery";

  nodes.machine =
    { config, pkgs, lib, ... }:
    {
      imports = [
        ../../../nixos
      ];

      services.sinex = {
        enable = true;
        package = sinexPackage;
        cliPackage = sinexCliPackage;
        targetUser = "test";

        serviceManagement.serviceGroups = {
          core = true;
          maintenance = false;
          monitoring = false;
        };

        satellite = {
          enable = true;
          coordination.enable = false;
          database.url = "postgresql:///sinex?host=/run/postgresql";
          logLevel = "info";

          coreServices.enable = true;

          eventSources = {
            filesystem = {
              enable = true;
              instances = 1;
              extraArgs = "";
            };
            terminal = {
              enable = true;
              instances = 1;
            };
            desktop = {
              enable = true;
              instances = 1;
            };
            system = {
              enable = true;
              instances = 1;
            };
          };

          automata = {
            canonicalCommandSynthesizer.enable = true;
            healthAggregator.enable = true;
          };
        };

        eventSources = {
          filesystem = {
            enable = true;
            watchPaths = [ "/home/test/watched" ];
          };
          atuin = {
            enable = true;
            databasePath = "/var/lib/sinex/.local/share/atuin/history.db";
          };
          shellHistory.enable = true;
          clipboard.enable = true;
          dbus.enable = true;
        };
      };

      # Test user setup
      users.users.test = {
        isNormalUser = true;
        createHome = true;
        shell = pkgs.zsh;
        uid = 1000;
      };
      
      services.dbus.enable = true;
      
      # Additional packages for failure testing
      environment.systemPackages = with pkgs; [
        atuin
        zsh
        bash
        file
        git
        sqlite
        sinex-query
        failure-injector
        recovery-verify
        stress-ng  # For memory pressure testing
        iptables   # For network partition testing
        procps     # Process monitoring
        psmisc     # killall and other utilities
        systemd    # systemctl commands
      ];
      
      programs.zsh.enable = true;
      
      # Enhanced tmpfiles for testing
      systemd.tmpfiles.rules = [
        "d /home/test/watched 0755 test users -"
        "f /var/lib/sinex/.zsh_history 0644 sinex sinex -"
        "f /var/lib/sinex/.bash_history 0644 sinex sinex -"
        "d /var/lib/sinex/.local 0755 sinex sinex -"
        "d /var/lib/sinex/.local/share 0755 sinex sinex -"
        "d /var/lib/sinex/.local/share/atuin 0755 sinex sinex -"
      ];
      
      # Package overlays
      nixpkgs.overlays = [(final: prev: {
        sinex-ingestd = sinex-ingestd;
        sinex-gateway = sinex-gateway;
        sinex = sinexPackage;
        sinexCli = sinexCliPackage;
        postgresql16Packages = prev.postgresql16Packages // {
          pg_jsonschema = pg_jsonschema;
        };
      })];

      # Enhanced service configuration for failure testing
      systemd.services.sinex-ingestd = {
        serviceConfig = {
          Restart = "always";
          RestartSec = "5";
          StartLimitInterval = "300";
          StartLimitBurst = "10";
        };
      };

      systemd.services.sinex-gateway = {
        serviceConfig = {
          Restart = "always";
          RestartSec = "5";
          StartLimitInterval = "300";
          StartLimitBurst = "10";
        };
      };
    };

  testScript = ''
    import time
    import re

    start_all()

    # Wait for system to be ready
    machine.wait_for_unit("multi-user.target")
    machine.wait_for_unit("postgresql.service")
    machine.wait_for_unit("sinex-migrate.service")
    machine.wait_for_unit("sinex-ingestd.service")
    machine.wait_for_unit("sinex-gateway.service")

    # Verify all services are active
    machine.succeed("systemctl is-active sinex-ingestd")
    machine.succeed("systemctl is-active sinex-gateway")

    # Initialize baseline system state
    with subtest("Initialize baseline system state"):
        # Initialize data sources
        machine.succeed("su - sinex -c 'cd /var/lib/sinex && atuin init zsh'")
        machine.succeed("su - sinex -c 'cd /var/lib/sinex && atuin import auto'")
        
        # Create initial test data
        machine.succeed("su - test -c 'echo baseline > /home/test/watched/baseline.txt'")
        machine.succeed("echo 'baseline_cmd' >> /var/lib/sinex/.zsh_history")
        
        # Wait for processing
        machine.sleep(5)
        
        baseline_stats = machine.succeed("sinex stats")
        print(f"Baseline stats: {baseline_stats}")
        
        baseline_match = re.search(r'Total events captured: (\d+)', baseline_stats)
        baseline_count = int(baseline_match.group(1)) if baseline_match else 0
        print(f"Baseline event count: {baseline_count}")

    # Test 1: Database disconnection recovery
    with subtest("Database disconnection recovery"):
        print("Testing database disconnection and recovery...")
        
        # Get pre-failure count
        pre_failure_stats = machine.succeed("sinex stats")
        pre_failure_match = re.search(r'Total events captured: (\d+)', pre_failure_stats)
        pre_failure_count = int(pre_failure_match.group(1)) if pre_failure_match else 0
        
        # Inject database failure
        machine.succeed("sinex-failure db-disconnect 15")
        
        # Generate events during outage (these should be queued/buffered)
        machine.succeed("su - test -c 'echo during-db-outage > /home/test/watched/db-outage.txt'")
        
        # Verify recovery
        machine.succeed("sinex-verify 30")
        
        # Check that events were eventually processed
        machine.sleep(10)  # Allow time for backlog processing
        
        post_recovery_stats = machine.succeed("sinex stats")
        post_recovery_match = re.search(r'Total events captured: (\d+)', post_recovery_stats)
        post_recovery_count = int(post_recovery_match.group(1)) if post_recovery_match else 0
        
        print(f"Pre-failure: {pre_failure_count}, Post-recovery: {post_recovery_count}")
        assert post_recovery_count > pre_failure_count, f"No new events after DB recovery: {pre_failure_count} -> {post_recovery_count}"
        
        # Verify the outage event was captured
        recent_events = machine.succeed("sinex query --limit 10")
        assert "db-outage" in recent_events, "Event generated during DB outage was lost"

    # Test 2: Collector crash recovery
    with subtest("Collector crash recovery"):
        print("Testing collector crash and recovery...")
        
        pre_crash_stats = machine.succeed("sinex stats")
        pre_crash_match = re.search(r'Total events captured: (\d+)', pre_crash_stats)
        pre_crash_count = int(pre_crash_match.group(1)) if pre_crash_match else 0
        
        # Inject collector crash
        machine.succeed("sinex-failure collector-crash 10")
        
        # Generate events during crash (these should be missed but system should recover)
        machine.succeed("su - test -c 'echo during-collector-crash > /home/test/watched/collector-crash.txt'")
        
        # Verify recovery
        machine.succeed("sinex-verify 30")
        
        # Generate post-recovery event to verify functionality
        machine.succeed("su - test -c 'echo post-collector-recovery > /home/test/watched/collector-recovery.txt'")
        machine.sleep(5)
        
        # Verify collector is capturing new events
        recent_events = machine.succeed("sinex query --limit 10")
        assert "collector-recovery" in recent_events, "Collector not capturing events after recovery"

    # Test 3: Worker crash recovery
    with subtest("Worker crash recovery"):
        print("Testing worker crash and recovery...")
        
        # Check work queue before crash
        pre_crash_queue = machine.succeed("sinex queue")
        print(f"Work queue before worker crash: {pre_crash_queue}")
        
        # Inject worker crash
        machine.succeed("sinex-failure worker-crash 10")
        
        # Generate events during worker crash (should accumulate in queue)
        for i in range(5):
            machine.succeed(f"su - test -c 'echo worker-crash-{i} > /home/test/watched/worker-crash-{i}.txt'")
            machine.sleep(1)
        
        # Verify recovery
        machine.succeed("sinex-verify 30")
        
        # Check that queued events were processed
        machine.sleep(10)  # Allow time for queue processing
        
        post_recovery_queue = machine.succeed("sinex queue")
        print(f"Work queue after worker recovery: {post_recovery_queue}")
        
        # Verify events were processed
        recent_events = machine.succeed("sinex query --limit 15")
        worker_crash_events = len([line for line in recent_events.split('\n') if 'worker-crash' in line])
        print(f"Worker crash events found: {worker_crash_events}")
        assert worker_crash_events > 0, "Worker crash events not processed after recovery"

    # Test 4: Memory pressure recovery
    with subtest("Memory pressure recovery"):
        print("Testing system behavior under memory pressure...")
        
        pre_pressure_stats = machine.succeed("sinex stats")
        
        # Start memory pressure in background and generate events
        machine.execute("sinex-failure memory-pressure 20 &")
        
        # Generate events during memory pressure
        for i in range(10):
            machine.succeed(f"su - test -c 'echo memory-pressure-{i} > /home/test/watched/memory-{i}.txt'")
            machine.sleep(1)
        
        # Wait for memory pressure to end
        machine.sleep(25)
        
        # Verify system is still functional
        machine.succeed("sinex service-status")
        machine.succeed("sinex stats")
        
        # Check that events were captured despite memory pressure
        recent_events = machine.succeed("sinex query --limit 20")
        memory_events = len([line for line in recent_events.split('\n') if 'memory-pressure' in line])
        print(f"Memory pressure events captured: {memory_events}")
        assert memory_events > 5, f"System dropped too many events under memory pressure: {memory_events}/10"

    # Test 5: Disk space recovery (simulated)
    with subtest("Disk space recovery"):
        print("Testing disk space exhaustion recovery...")
        
        # Get baseline
        pre_disk_stats = machine.succeed("sinex stats")
        
        # Simulate disk full condition
        machine.succeed("sinex-failure disk-full 15")
        
        # Generate events during disk issues
        machine.succeed("su - test -c 'echo disk-recovery-test > /home/test/watched/disk-recovery.txt'")
        
        # Wait for recovery and verify
        machine.sleep(5)
        machine.succeed("sinex stats")
        
        # Verify system recovered
        recent_events = machine.succeed("sinex query --limit 5")
        assert "disk-recovery" in recent_events, "System not functional after disk space recovery"

    # Test 6: Multiple simultaneous failures
    with subtest("Multiple simultaneous failures"):
        print("Testing recovery from multiple simultaneous failures...")
        
        baseline_stats = machine.succeed("sinex stats")
        baseline_match = re.search(r'Total events captured: (\d+)', baseline_stats)
        baseline_count = int(baseline_match.group(1)) if baseline_match else 0
        
        # Simultaneously crash collector and worker
        machine.execute("systemctl stop sinex-ingestd &")
        machine.execute("systemctl stop sinex-gateway &")
        machine.sleep(2)
        
        # Generate events during double failure
        machine.succeed("su - test -c 'echo multi-failure-test > /home/test/watched/multi-failure.txt'")
        
        # Restart both services
        machine.succeed("systemctl start sinex-ingestd")
        machine.succeed("systemctl start sinex-gateway")
        
        # Verify recovery
        machine.succeed("sinex-verify 30")
        
        # Generate post-recovery test event
        machine.succeed("su - test -c 'echo multi-recovery > /home/test/watched/multi-recovery.txt'")
        machine.sleep(5)
        
        # Verify full functionality restored
        final_stats = machine.succeed("sinex stats")
        final_match = re.search(r'Total events captured: (\d+)', final_stats)
        final_count = int(final_match.group(1)) if final_match else 0
        
        assert final_count > baseline_count, f"No new events after multi-failure recovery: {baseline_count} -> {final_count}"
        
        recent_events = machine.succeed("sinex query --limit 10")
        assert "multi-recovery" in recent_events, "System not capturing events after multi-failure recovery"

    # Test 7: Graceful degradation validation
    with subtest("Graceful degradation validation"):
        print("Validating graceful degradation behavior...")
        
        # Test partial service availability
        machine.succeed("systemctl stop sinex-gateway")
        
        # System should continue capturing events even without worker
        machine.succeed("su - test -c 'echo degraded-mode > /home/test/watched/degraded.txt'")
        machine.sleep(3)
        
        # Events should still be captured (just not processed by worker)
        pre_worker_stats = machine.succeed("sinex stats")
        print(f"Stats with worker stopped: {pre_worker_stats}")
        
        # Restart worker - events should be processed
        machine.succeed("systemctl start sinex-gateway")
        machine.sleep(5)
        
        # Verify degraded event was captured
        recent_events = machine.succeed("sinex query --limit 10")
        assert "degraded-mode" in recent_events, "Events not captured during degraded mode"

    print("✓ All failure recovery tests completed successfully")
    print("✓ System demonstrates resilience to database, service, and resource failures")
    print("✓ Graceful degradation and full recovery verified")
  '';
}
