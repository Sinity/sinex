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

    def extract_total_events():
        stats = machine.succeed("sinex stats")
        match = re.search(r"Total events captured: (\d+)", stats)
        if match:
            return int(match.group(1))
        return None

    def wait_for_event_pattern(pattern, timeout=60):
        deadline = time.time() + timeout
        while time.time() < deadline:
            output = machine.succeed("sinex query --limit 20")
            if pattern in output:
                return
            time.sleep(2)
        raise AssertionError(f"Timed out waiting for event containing '{pattern}'")

    def wait_for_services(units):
        for unit in units:
            machine.wait_for_unit(unit)
            machine.succeed(f"systemctl is-active {unit}")

    def run_failure(kind, duration=8):
        machine.succeed(f"sinex-failure {kind} {duration}")

    start_all()

    # Wait for system to be ready
    machine.wait_for_unit("multi-user.target")
    machine.wait_for_unit("postgresql.service")
    machine.wait_for_unit("sinex-ingestd.service")
    machine.wait_for_unit("sinex-gateway.service")
    machine.wait_for_unit("nats.service")

    # Ensure satellite instances are online
    satellite_units = [
        "sinex-fs-watcher-1.service",
        "sinex-terminal-satellite-1.service",
        "sinex-desktop-satellite-1.service",
        "sinex-system-satellite-1.service",
        "sinex-terminal-command-canonicalizer.service",
        "sinex-health-aggregator.service",
    ]
    wait_for_services(satellite_units)

    # Verify core hubs are active
    machine.succeed("systemctl is-active sinex-ingestd")
    machine.succeed("systemctl is-active sinex-gateway")

    # Initialize baseline system state
    with subtest("Initialize baseline system state"):
        machine.succeed("su - sinex -c 'cd /var/lib/sinex && atuin init zsh'")
        machine.succeed("su - sinex -c 'cd /var/lib/sinex && atuin import auto'")
        machine.succeed("su - test -c 'echo baseline > /home/test/watched/baseline.txt'")
        machine.succeed("echo 'baseline_cmd' >> /var/lib/sinex/.zsh_history")
        wait_for_event_pattern("baseline")
        baseline_count = extract_total_events() or 0
        print(f"Baseline event count: {baseline_count}")

    # Test 1: Database disconnection recovery
    with subtest("Database disconnection recovery"):
        baseline = extract_total_events() or 0
        run_failure('db-disconnect', 12)
        machine.succeed("su - test -c 'echo during-db-outage > /home/test/watched/db-outage.txt'")
        machine.succeed("sinex-verify 45")
        wait_for_event_pattern("db-outage")
        recovered = extract_total_events() or 0
        print(f"Recovered event count after DB outage: {recovered}")
        assert recovered > baseline, "No new events recorded after database recovery"

    # Test 2: Collector crash recovery
    with subtest("Collector crash recovery"):
        baseline = extract_total_events() or 0
        run_failure('collector-crash', 10)
        machine.succeed("sinex-verify 45")
        machine.succeed("su - test -c 'echo post-collector-recovery > /home/test/watched/collector-recovery.txt'")
        wait_for_event_pattern("collector-recovery")
        recovered = extract_total_events() or 0
        print(f"Collector recovery event count: {recovered}")
        assert recovered > baseline, "Collector did not resume ingesting events"

    # Test 3: Worker crash recovery
    with subtest("Worker crash recovery"):
        run_failure('worker-crash', 10)
        machine.succeed("sinex-verify 45")
        for i in range(3):
            machine.succeed(f"su - test -c 'echo worker-crash-{i} > /home/test/watched/worker-crash-{i}.txt'")
        wait_for_event_pattern("worker-crash-2")
        queue_snapshot = machine.succeed("sinex queue")
        print(f"Work queue after worker recovery:\n{queue_snapshot}")

    print("✓ Failure recovery smoke tests completed successfully")
  '';
}
