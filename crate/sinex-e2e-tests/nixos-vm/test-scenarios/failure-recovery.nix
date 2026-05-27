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
  sinexPackage = if sinex != null then sinex else sinex-ingestd;
  sinexCliPackage = if sinexCli != null then sinexCli else pkgs.python3;
  # Enhanced query tool with recovery testing support
  sinex-query = pkgs.writeScriptBin "sinex" ''
    #!${pkgs.python3}/bin/python3
    import subprocess
    import sys
    import json
    import time
    import os

    DB_NAME = os.environ.get("SINEX_TEST_DB_NAME", "sinex_dev")

    def query_events(limit=10, source=None, after=None):
        where_clause = ""
        if source:
            where_clause += f" AND source = '{source}'"
        if after:
            where_clause += f" AND ts_coided > NOW() - INTERVAL '{after}'"
        
        cmd = f"psql -d {DB_NAME} -t -c \"SELECT id, source, event_type, ts_coided, payload FROM core.events WHERE 1=1{where_clause} ORDER BY ts_coided DESC LIMIT {limit};\""
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
        cmd = f"psql -d {DB_NAME} -t -c \"SELECT 'DB_CONNECTED' AS status;\""
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
        cmd = f"psql -d {DB_NAME} -t -c 'SELECT COUNT(*) FROM core.events;'"
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

    def pipeline_activity():
        cmd = f"""
        psql -d {DB_NAME} -t -c "
        SELECT 
            source,
            event_type,
            COUNT(*) AS events,
            MIN(ts_coided) AS first_seen,
            MAX(ts_coided) AS last_seen
        FROM core.events
        WHERE ts_coided > NOW() - INTERVAL '10 minutes'
        GROUP BY source, event_type
        ORDER BY events DESC
        LIMIT 15;
        "
        """
        result = subprocess.run([
            "su", "-", "postgres", "-c", cmd
        ], capture_output=True, text=True)
        
        if result.returncode == 0:
            lines = [line.strip() for line in result.stdout.split('\n') if line.strip()]
            print("Recent pipeline activity:")
            for line in lines:
                print(f"  {line}")
        else:
            print(f"Pipeline activity query failed: {result.stderr}")

    # Parse command line arguments
    if len(sys.argv) > 1:
        if sys.argv[1] == "stats":
            stats()
        elif sys.argv[1] == "db-status":
            db_status()
        elif sys.argv[1] == "service-status":
            service_status()
        elif sys.argv[1] == "pipeline":
            pipeline_activity()
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
        systemctl start \
          sinex-ingestd \
          sinex-gateway \
          sinex-filesystem-1 \
          'sinex-source@terminal.atuin-history.service' \
          'sinex-source@terminal.bash-history.service' \
          'sinex-source@terminal.fish-history.service' \
          'sinex-source@terminal.zsh-history.service'
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
    DB_NAME=''${SINEX_TEST_DB_NAME:-sinex_dev}
    
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

    while ! systemctl is-active sinex-filesystem-1 >/dev/null 2>&1; do
        current_time=$(date +%s)
        elapsed=$((current_time - start_time))
        if [ $elapsed -gt $MAX_WAIT ]; then
            echo "FAIL: Filesystem node not recovered within ''${MAX_WAIT}s"
            exit 1
        fi
        echo "Waiting for filesystem node... (''${elapsed}s)"
        sleep 2
    done
    
    # Test basic functionality
    echo "Testing basic functionality..."
    
    # Generate test event
    su - test -c 'echo "recovery-test-$(date +%s)" > /var/lib/sinex/watched/recovery-test.txt'
    
    # Verify event was captured. Query the database directly so telemetry events
    # cannot push the marker out of a small CLI recency window.
    for _ in $(seq 1 15); do
        recovered_count=$(
            su - postgres -c "psql -d ''${DB_NAME} -tAc \"SELECT COUNT(*) FROM core.events WHERE payload::text LIKE '%recovery-test%';\"" \
                | tr -d '[:space:]'
        )
        if [ "''${recovered_count:-0}" -gt 0 ]; then
            echo "SUCCESS: System fully recovered and operational"
            exit 0
        fi
        sleep 2
    done

    echo "FAIL: System not capturing new events after recovery"
    exit 1
  '';
in
pkgs.testers.nixosTest {
  name = "sinex-failure-recovery";

  nodes.machine =
    { config, pkgs, lib, ... }:
    let
      stateDir = config.services.sinex.stateRoot;
    in {
      # Secrets/agenix can break evaluation in isolated VM builds; disable for tests
      disabledModules = [ ../../../../nixos/modules/secrets.nix ];
      imports = [
        ../../../../nixos
      ];

      services.sinex = {
        enable = true;
        package = sinexPackage;
        cliPackage = sinexCliPackage;
        users.target = "test";
        secrets.gatewayAdminTokenFile = "/etc/sinex/gateway-admin-token";
        core.gateway.autoGenerateTls = true;
        database.autoSetup = true;
        database.connectionPool.maxConnections = 20;
        lifecycle.preflight.enable = lib.mkForce false;
        nats.jetstreamMaxStore = "16G";

        nodes = {
          enable = true;
          coordination.enable = true;
          defaults = {
            instances = 1;
            logLevel = "info";
            env.SINEX_COORDINATION_DISABLED = "0";
          };

          filesystem = {
            enable = true;
            watchPaths = [ "/var/lib/sinex/watched" ];
          };

          terminal.enable = true;
          terminal.historySources = [
            {
              path = "/home/test/.bash_history";
              shell = "bash";
            }
            {
              path = "/home/test/.zsh_history";
              shell = "zsh";
            }
            {
              path = "/home/test/.local/share/atuin/history.db";
              shell = "atuin";
            }
            {
              path = "/home/test/.local/share/fish/fish_history";
              shell = "fish";
            }
          ];
          browser.enable = lib.mkForce false;
          desktop.enable = lib.mkForce false;
          system.enable = lib.mkForce false;
          document.enable = lib.mkForce false;

          automata = {
            enable = lib.mkForce false;
            canonicalizer.enable = lib.mkForce false;
            healthAggregator.enable = lib.mkForce false;
            analyticsAutomaton.enable = lib.mkForce false;
            sessionDetector.enable = lib.mkForce false;
          };
        };

        shell.kitty.enable = true;
      };

      # Test user setup
      users.users.test = {
        isNormalUser = true;
        createHome = true;
        shell = pkgs.zsh;
        uid = 1000;
      };
      
      environment.etc."sinex/gateway-admin-token".text = "test-admin-token:admin";
      
      services.postgresql.authentication = lib.mkForce ''
local   all             all                                     trust
host    all             all             127.0.0.1/32            trust
host    all             all             ::1/128                 trust
'';

      systemd.services.sinex-ingestd.after = [ "sinex-schema-apply.service" ];
      systemd.services.sinex-ingestd.requires = [ "sinex-schema-apply.service" ];
      systemd.services.sinex-gateway.after = [ "sinex-schema-apply.service" ];
      systemd.services.sinex-gateway.requires = [ "sinex-schema-apply.service" ];
      systemd.services.sinex-ingestd.path = [ pkgs.git pkgs.git-annex ];
      systemd.services.sinex-gateway.path = [ pkgs.git pkgs.git-annex ];
      systemd.services.sinex-blob-init.path = [ pkgs.git pkgs.git-annex ];
      systemd.services.sinex-system-1.enable = lib.mkForce false;
      systemd.services.sinex-system-1.wantedBy = lib.mkForce [ ];
      systemd.services.sinex-canonicalizer.enable = lib.mkForce false;
      systemd.services.sinex-canonicalizer.wantedBy = lib.mkForce [ ];
      systemd.services.sinex-health-automaton.enable = lib.mkForce false;
      systemd.services.sinex-health-automaton.wantedBy = lib.mkForce [ ];
      systemd.services.sinex-analytics-automaton.enable = lib.mkForce false;
      systemd.services.sinex-analytics-automaton.wantedBy = lib.mkForce [ ];
      systemd.services.sinex-session-detector.enable = lib.mkForce false;
      systemd.services.sinex-session-detector.wantedBy = lib.mkForce [ ];
      
      services.dbus.enable = true;
      
      # Keep Postgres memory small for the constrained VM
      services.postgresql.settings.shared_buffers = "128MB";
      services.postgresql.settings.max_connections = 50;
      
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

      environment.sessionVariables.SINEX_STATE_DIR = stateDir;
      
      programs.zsh.enable = true;
      
      # Enhanced tmpfiles for testing
      systemd.tmpfiles.rules = [
        "d /var/lib/sinex/watched 0755 test users -"
        "f /home/test/.zsh_history 0644 test users -"
        "f /home/test/.bash_history 0644 test users -"
        "d /home/test/.local 0755 test users -"
        "d /home/test/.local/share 0755 test users -"
        "d /home/test/.local/share/atuin 0755 test users -"
        "d /home/test/.local/share/fish 0755 test users -"
      ];

      system.activationScripts.sinexFailureHistoryFixture = ''
        mkdir -p /home/test/.local/share/atuin
        mkdir -p /home/test/.local/share/fish

        cat > /home/test/.zsh_history <<'EOF'
: 1700100000:0;echo failure_recovery_zsh_fixture
EOF
        cat > /home/test/.bash_history <<'EOF'
echo failure_recovery_bash_fixture
EOF

        rm -f /home/test/.local/share/atuin/history.db
        ${pkgs.sqlite}/bin/sqlite3 /home/test/.local/share/atuin/history.db <<'SQL'
CREATE TABLE history (
  id TEXT NOT NULL,
  timestamp INTEGER NOT NULL,
  duration INTEGER NOT NULL,
  exit INTEGER NOT NULL,
  command TEXT NOT NULL,
  cwd TEXT NOT NULL,
  session TEXT NOT NULL,
  hostname TEXT NOT NULL,
  deleted_at INTEGER
);
INSERT INTO history (id, timestamp, duration, exit, command, cwd, session, hostname, deleted_at)
VALUES
  ('failure-recovery-atuin-1', 1700100000000000000, 50000000, 0, 'echo failure_recovery_atuin_fixture', '/home/test', 'failure-recovery', 'sinex-vm', NULL);
SQL

        rm -f /home/test/.local/share/fish/fish_history
        ${pkgs.sqlite}/bin/sqlite3 /home/test/.local/share/fish/fish_history <<'SQL'
CREATE TABLE history (
  command TEXT NOT NULL,
  "when" INTEGER
);
INSERT INTO history (command, "when")
VALUES ('echo failure_recovery_fish_fixture', 1700100000);
SQL

        chown -R test:users /home/test/.zsh_history /home/test/.bash_history /home/test/.local/share/atuin /home/test/.local/share/fish
        chmod 0644 /home/test/.zsh_history /home/test/.bash_history /home/test/.local/share/atuin/history.db /home/test/.local/share/fish/fish_history
      '';
      
      # Package overlays
      nixpkgs.overlays = [(final: prev: {
        sinex-ingestd = sinex-ingestd;
        sinex-gateway = sinex-gateway;
        sinex = sinexPackage;
        sinexCli = sinexCliPackage;
        postgresql18Packages = prev.postgresql18Packages // {
          pg_jsonschema = pg_jsonschema;
        };
      })];

      # Enhanced service configuration for failure testing
      systemd.services.sinex-ingestd = {
        unitConfig = {
          StartLimitIntervalSec = lib.mkForce "300";
          StartLimitBurst = lib.mkForce "10";
        };
        serviceConfig = {
          Restart = lib.mkForce "always";
          RestartSec = lib.mkForce "5";
        };
      };

      systemd.services.sinex-gateway = {
        unitConfig = {
          StartLimitIntervalSec = lib.mkForce "300";
          StartLimitBurst = lib.mkForce "10";
        };
        serviceConfig = {
          Restart = lib.mkForce "always";
          RestartSec = lib.mkForce "5";
        };
        environment.SINEX_RPC_TOKEN_FILE = "/etc/sinex/gateway-admin-token";
      };
    };

  testScript = ''
    import time
    import re
    import shlex

    def extract_total_events():
        stats = machine.succeed("sinex stats")
        match = re.search(r"Total events captured: (\d+)", stats)
        if match:
            return int(match.group(1))
        return None

    def wait_for_event_pattern(pattern, timeout=60):
        escaped = pattern.replace("'", chr(39) + chr(39))
        psql_command = (
            "psql -d sinex_dev -tAc "
            + shlex.quote(
                "SELECT COUNT(*) FROM core.events "
                f"WHERE source LIKE '%{escaped}%' "
                f"OR event_type LIKE '%{escaped}%' "
                f"OR payload::text LIKE '%{escaped}%'"
            )
        )
        shell_command = "su - postgres -c " + shlex.quote(psql_command)
        deadline = time.time() + timeout
        while time.time() < deadline:
            output = machine.succeed(shell_command).strip()
            if output.isdigit() and int(output) > 0:
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

    # Ensure node instances are online
    terminal_source_units = [
        "sinex-source@terminal.atuin-history.service",
        "sinex-source@terminal.bash-history.service",
        "sinex-source@terminal.fish-history.service",
        "sinex-source@terminal.zsh-history.service",
    ]
    node_units = [
        "sinex-filesystem-1.service",
    ] + terminal_source_units
    wait_for_services(node_units)

    # Verify core hubs are active
    machine.succeed("systemctl is-active sinex-ingestd")
    machine.succeed("systemctl is-active sinex-gateway")

    # Initialize baseline system state
    with subtest("Initialize baseline system state"):
        machine.succeed("su - test -c 'echo baseline > /var/lib/sinex/watched/baseline.txt'")
        machine.succeed("su - test -c 'echo baseline_cmd >> /home/test/.zsh_history'")
        wait_for_event_pattern("baseline")
        baseline_count = extract_total_events() or 0
        print(f"Baseline event count: {baseline_count}")

    # Test 1: Database disconnection recovery
    with subtest("Database disconnection recovery"):
        baseline = extract_total_events() or 0
        run_failure('db-disconnect', 12)
        machine.succeed("su - test -c 'echo during-db-outage > /var/lib/sinex/watched/db-outage.txt'")
        machine.succeed("sinex-verify 120")
        wait_for_event_pattern("db-outage")
        recovered = extract_total_events() or 0
        print(f"Recovered event count after DB outage: {recovered}")
        assert recovered > baseline, "No new events recorded after database recovery"

    # Test 2: Collector crash recovery
    with subtest("Collector crash recovery"):
        baseline = extract_total_events() or 0
        run_failure('collector-crash', 10)
        machine.succeed("sinex-verify 120")
        machine.succeed("su - test -c 'echo post-collector-recovery > /var/lib/sinex/watched/collector-recovery.txt'")
        wait_for_event_pattern("collector-recovery")
        recovered = extract_total_events() or 0
        print(f"Collector recovery event count: {recovered}")
        assert recovered > baseline, "Collector did not resume ingesting events"

    # Test 3: Worker crash recovery
    with subtest("Worker crash recovery"):
        run_failure('worker-crash', 10)
        machine.succeed("sinex-verify 120")
        for i in range(3):
            machine.succeed(f"su - test -c 'echo worker-crash-{i} > /var/lib/sinex/watched/worker-crash-{i}.txt'")
        wait_for_event_pattern("worker-crash-2")
        queue_snapshot = machine.succeed("sinex pipeline")
        print(f"Pipeline activity after worker recovery:\n{queue_snapshot}")

    print("✓ Failure recovery smoke tests completed successfully")
  '';
}
