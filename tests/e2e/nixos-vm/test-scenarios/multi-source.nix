# Multi-source stress test for Sinex
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
  # Enhanced query tool with metrics support
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

    def stats_by_source():
        cmd = f"psql -d {DB_NAME} -t -c \"SELECT source, COUNT(*) FROM core.events GROUP BY source ORDER BY COUNT(*) DESC;\""
        result = subprocess.run([
            "su", "-", "postgres", "-c", cmd
        ], capture_output=True, text=True)
        
        if result.returncode == 0:
            lines = [line.strip() for line in result.stdout.split('\n') if line.strip()]
            print("Events by source:")
            for line in lines:
                print(f"  {line}")
        else:
            print(f"Source stats failed: {result.stderr}")

    def performance_stats():
        # Get event rate over last minute
        cmd = f"psql -d {DB_NAME} -t -c \"SELECT COUNT(*) FROM core.events WHERE ts_coided > NOW() - INTERVAL '1 minute';\""
        result = subprocess.run([
            "su", "-", "postgres", "-c", cmd
        ], capture_output=True, text=True)
        
        if result.returncode == 0:
            count = result.stdout.strip()
            print(f"Events in last minute: {count}")
            
            # Calculate events per second
            if count.isdigit():
                eps = int(count) / 60.0
                print(f"Average events per second: {eps:.2f}")
        else:
            print(f"Performance stats failed: {result.stderr}")

    def total_stats():
        cmd = f"psql -d {DB_NAME} -t -c 'SELECT COUNT(*) FROM core.events;'"
        result = subprocess.run([
            "su", "-", "postgres", "-c", cmd
        ], capture_output=True, text=True)
        
        if result.returncode == 0:
            count = result.stdout.strip()
            print(f"Total events captured: {count}")
        else:
            print(f"Stats failed: {result.stderr}")

    # Parse command line arguments
    if len(sys.argv) > 1:
        if sys.argv[1] == "stats":
            total_stats()
        elif sys.argv[1] == "sources":
            stats_by_source()
        elif sys.argv[1] == "perf":
            performance_stats()
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

  # Stress test generator script
  stress-generator = pkgs.writeScriptBin "sinex-stress" ''
    #!${pkgs.bash}/bin/bash
    set -euo pipefail

    DURATION="''${1:-30}"  # Default 30 seconds
    INTENSITY="''${2:-medium}"  # low, medium, high

    case "$INTENSITY" in
      low)
        FILE_OPS_PER_SEC=5
        SHELL_CMDS_PER_SEC=3
        CLIPBOARD_OPS_PER_SEC=0
        ;;
      medium)
        FILE_OPS_PER_SEC=12
        SHELL_CMDS_PER_SEC=6
        CLIPBOARD_OPS_PER_SEC=0
        ;;
      high)
        FILE_OPS_PER_SEC=20
        SHELL_CMDS_PER_SEC=8
        CLIPBOARD_OPS_PER_SEC=0
        ;;
      *)
        echo "Unknown intensity '$INTENSITY'" >&2
        exit 1
        ;;
    esac

    echo "Starting $INTENSITY stress test for $DURATION seconds"
    echo "File ops/sec: $FILE_OPS_PER_SEC, Shell cmds/sec: $SHELL_CMDS_PER_SEC, Clipboard ops/sec: $CLIPBOARD_OPS_PER_SEC"
    FILE_SLEEP=$(echo "scale=3; 1/$FILE_OPS_PER_SEC" | bc)
    SHELL_SLEEP=$(echo "scale=3; 1/$SHELL_CMDS_PER_SEC" | bc)
    if [ "$CLIPBOARD_OPS_PER_SEC" -gt 0 ]; then
      CLIPBOARD_SLEEP=$(echo "scale=3; 1/$CLIPBOARD_OPS_PER_SEC" | bc)
    else
      CLIPBOARD_SLEEP=0
    fi
    
    # Background jobs for parallel stress testing
    pids=()
    
    # File operations stress
    (
      for ((i=1; i<=DURATION*FILE_OPS_PER_SEC; i++)); do
        printf 'stress test %s\n' "$i" > "/var/lib/sinex/watched/stress_$i.txt"
        sleep "$FILE_SLEEP"
        rm -f "/var/lib/sinex/watched/stress_$i.txt"
      done
    ) &
    pids+=($!)
    
    # Shell history stress  
    (
      for ((i=1; i<=DURATION*SHELL_CMDS_PER_SEC; i++)); do
        echo "stress_cmd_$i /tmp/stress_$i" >> /home/test/.zsh_history
        echo "stress_bash_$i" >> /home/test/.bash_history
        sleep "$SHELL_SLEEP"
      done
    ) &
    pids+=($!)
    
    # Clipboard operations stress (if Wayland available)
    if [ "$CLIPBOARD_OPS_PER_SEC" -gt 0 ] && [ -e /run/user/1000/wayland-1 ]; then
      (
        for ((i=1; i<=DURATION*CLIPBOARD_OPS_PER_SEC; i++)); do
          timeout 2s su - test -c "XDG_RUNTIME_DIR=/run/user/1000 WAYLAND_DISPLAY=wayland-1 echo 'clipboard stress $i' | wl-copy" 2>/dev/null || true
          sleep "$CLIPBOARD_SLEEP"
        done
      ) &
      pids+=($!)
    fi
    
    # Atuin database stress
    (
      db_path="/home/test/.local/share/atuin/history.db"
      for ((i=1; i<=DURATION*5; i++)); do  # 5 atuin entries per second
        sqlite3 "$db_path" "INSERT INTO history (id, timestamp, duration, exit, command, cwd, session, hostname) VALUES ('stress$i', $(date +%s), 100, 0, 'stress-command-$i', '/tmp', 'stress-session', 'testhost');" 2>/dev/null || true
        sleep 0.2
      done
    ) &
    pids+=($!)
    
    # Asciinema file stress
    (
      for ((i=1; i<=DURATION*2; i++)); do  # 2 recording files per second
        cast_path="/home/test/.local/share/asciinema/stress_$i.cast"
        printf '{"version": 2, "width": 80, "height": 24}\n' > "$cast_path"
        printf '[0.0, "o", "stress command %s"]\n' "$i" >> "$cast_path"
        sleep 0.5
      done
    ) &
    pids+=($!)
    
    echo "Stress test started with PIDs: ''${pids[@]}"
    
    # Wait for all background processes
    for pid in "''${pids[@]}"; do
      wait "$pid"
    done
    
    echo "Stress test completed"
  '';
in
pkgs.testers.nixosTest {
  name = "sinex-multi-source-stress";

  nodes.machine =
    { config, pkgs, lib, ... }:
    let
      stateDir = config.services.sinex.stateRoot;
    in {
      imports = [
        (import ../common/test-base.nix {
          inherit config pkgs lib sinex-ingestd sinex-gateway pg_jsonschema sinex sinexCli;
        })
      ];

      services.sinex = {
        nodes = {
          filesystem = {
            instances = 2;
            watchPaths = [ "/var/lib/sinex/watched" ];
          };
          terminal.enable = true;
          desktop.enable = false;
          system.enable = false;
          document = {
            enable = true;
            allowedRoots = [ "/home/test/Documents" ];
          };

          automata = {
            enable = true;
            canonicalizer.enable = true;
            healthAggregator.enable = true;
            analyticsAutomaton.enable = true;
            sessionDetector.enable = true;
          };
        };
      };

      # Test user setup with additional stress directories
      users.users.test.shell = lib.mkForce pkgs.zsh;
      users.users.test.extraGroups = lib.mkForce [ "users" "video" "render" "seat" ];
      
      # Additional packages for stress testing
      environment.systemPackages = with pkgs; [
        atuin
        asciinema
        kitty
        zsh
        bash
        file
        git
        sqlite
        sinex-query
        stress-generator
        bc  # For floating point calculations
        procps  # For process monitoring
        htop    # For system monitoring
      ];
      
      # Configure all monitored services
      environment.etc."atuin/config.toml".text = ''
        auto_sync = false
        search_mode = "fuzzy"
        filter_mode = "global"
        style = "compact"
        inline_height = 30
        up_arrow = false
        show_preview = true
      '';
      
      programs.zsh.enable = true;
      
      # Enhanced tmpfiles for stress testing
      systemd.tmpfiles.rules = [
        # Test directories
        "d /var/lib/sinex/watched 0755 test users -"
        "d /tmp/sinex-stress 0755 test users -"
        
        # Shell history files
        "f /home/test/.zsh_history 0644 test users -"
        "f /home/test/.bash_history 0644 test users -"

        # Atuin directories
        "d /home/test/.local 0755 test users -"
        "d /home/test/.local/share 0755 test users -"
        "d /home/test/.local/share/atuin 0755 test users -"
        "d /home/test/.local/share/fish 0755 test users -"
        "d /home/test/.local/share/activitywatch 0755 test users -"
        "d /home/test/.local/share/activitywatch/aw-server-rust 0755 test users -"
        
        # Asciinema directories
        "d /home/test/.local 0755 test users -"
        "d /home/test/.local/share 0755 test users -"
        "d /home/test/.local/share/asciinema 0755 test users -"
        
        # Runtime directories
        "d /run/user 0755 root root -"
        "d /run/user/1000 0700 test users -"
        
        # Hyprland IPC socket directory
        "d /tmp/hypr 0755 test users -"
      ];

      system.activationScripts.sinexActivitywatchFixture = ''
        mkdir -p /home/test/.local/share/activitywatch/aw-server-rust
        rm -f /home/test/.local/share/activitywatch/aw-server-rust/sqlite.db
        ${pkgs.sqlite}/bin/sqlite3 /home/test/.local/share/activitywatch/aw-server-rust/sqlite.db <<'SQL'
CREATE TABLE buckets (
  id INTEGER PRIMARY KEY,
  name TEXT NOT NULL
);
CREATE TABLE events (
  bucketrow INTEGER NOT NULL,
  starttime INTEGER NOT NULL,
  endtime INTEGER NOT NULL,
  data TEXT,
  FOREIGN KEY(bucketrow) REFERENCES buckets(id)
);
INSERT INTO buckets (id, name) VALUES
  (1, 'aw-watcher-window_sinex-vm'),
  (2, 'aw-watcher-web_sinex-vm'),
  (3, 'aw-watcher-afk_sinex-vm');
INSERT INTO events (bucketrow, starttime, endtime, data) VALUES
  (1, 1000000000, 4000000000, '{"app":"kitty","title":"multi-source"}'),
  (2, 5000000000, 9000000000, '{"app":"Firefox","title":"Docs","url":"https://example.com"}'),
  (3, 10000000000, 16000000000, '{"status":"afk"}');
SQL
        chown -R test:users /home/test/.local/share/activitywatch
      '';

      system.activationScripts.sinexTerminalHistoryFixture = ''
        mkdir -p /home/test/.local/share/atuin
        mkdir -p /home/test/.local/share/fish

        cat > /home/test/.zsh_history <<'EOF'
: 1700100000:0;echo multi_source_zsh_fixture
EOF
        cat > /home/test/.bash_history <<'EOF'
echo multi_source_bash_fixture
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
  ('multi-source-atuin-1', 1700100000000000000, 50000000, 0, 'echo multi_source_atuin_fixture', '/home/test', 'multi-source', 'sinex-vm', NULL);
SQL

        rm -f /home/test/.local/share/fish/fish_history
        ${pkgs.sqlite}/bin/sqlite3 /home/test/.local/share/fish/fish_history <<'SQL'
CREATE TABLE history (
  command TEXT NOT NULL,
  "when" INTEGER
);
INSERT INTO history (command, "when")
VALUES ('echo multi_source_fish_fixture', 1700100000);
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
    };

  testScript = ''
    import time
    import shlex

    def query_event_count(where_clause="TRUE"):
        psql_command = (
            "psql -d sinex_dev -tAc "
            + shlex.quote(f"SELECT COUNT(*) FROM core.events WHERE {where_clause}")
        )
        shell_command = "timeout 10s su - postgres -c " + shlex.quote(psql_command)
        output = machine.succeed(shell_command).strip()
        if output.isdigit():
            return int(output)
        raise AssertionError(f"Unexpected event count output for {where_clause!r}: {output!r}")

    def extract_total_events():
        return query_event_count()

    def wait_for_count_increase(previous, delta, timeout=120):
        deadline = time.time() + timeout
        last = extract_total_events()
        while time.time() < deadline:
            current = extract_total_events()
            if current is not None and current >= previous + delta:
                return current
            last = current
            time.sleep(2)
        raise AssertionError(
            f"Timed out waiting for event count to grow by {delta} (baseline={previous}, last_seen={last})"
        )

    def wait_for_event_pattern(pattern, timeout=60):
        escaped = pattern.replace("'", chr(39) + chr(39))
        where_clause = (
            f"source LIKE '%{escaped}%' "
            f"OR event_type LIKE '%{escaped}%' "
            f"OR payload::text LIKE '%{escaped}%'"
        )
        deadline = time.time() + timeout
        while time.time() < deadline:
            if query_event_count(where_clause) > 0:
                return
            time.sleep(2)
        raise AssertionError(f"Timed out waiting for event containing '{pattern}'")

    def wait_for_event_type_like(pattern, timeout=60):
        escaped = pattern.replace("'", chr(39) + chr(39))
        deadline = time.time() + timeout
        while time.time() < deadline:
            if query_event_count(f"event_type LIKE '{escaped}'") > 0:
                return
            time.sleep(2)
        raise AssertionError(f"Timed out waiting for event_type LIKE '{pattern}'")

    def wait_for_terminal_event(timeout=60):
        where_clause = (
            "event_type LIKE 'shell.%' "
            "OR source LIKE '%terminal%' "
            "OR source LIKE '%shell%'"
        )
        deadline = time.time() + timeout
        while time.time() < deadline:
            if query_event_count(where_clause) > 0:
                return
            time.sleep(2)
        raise AssertionError("Timed out waiting for terminal/shell events")

    def safe_perf():
        try:
            return machine.succeed("timeout 10s sinex perf || true")
        except Exception as exc:
            return f"perf unavailable: {exc}"

    start_all()

    # Wait for system to be ready
    machine.wait_for_unit("multi-user.target")
    machine.wait_for_unit("postgresql.service")

    # Wait for Sinex services
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
        "sinex-filesystem-2.service",
        "sinex-canonicalizer.service",
        "sinex-health-automaton.service",
        "sinex-analytics-automaton.service",
        "sinex-session-detector.service",
    ] + terminal_source_units
    for unit in node_units:
        machine.wait_for_unit(unit)
        machine.succeed(f"systemctl is-active {unit}")

    # Verify core hubs are active
    machine.succeed("systemctl is-active sinex-ingestd")
    machine.succeed("systemctl is-active sinex-gateway")

    # Initialize all data sources
    with subtest("Initialize all event sources"):
        machine.succeed("su - test -c 'echo initial > /var/lib/sinex/watched/initial.txt'")
        machine.succeed("printf '# initial document\\n' > /home/test/Documents/initial.md")
        machine.succeed("chown test:users /home/test/Documents/initial.md")
        machine.succeed("su - test -c 'echo initial_cmd >> /home/test/.zsh_history'")
        machine.succeed("systemctl start sinex-document-scan.service")
        machine.wait_until_succeeds(
            "su - postgres -c \"psql -d sinex_dev -tAc \\\"SELECT COUNT(*) FROM core.events WHERE event_type = 'document.ingested'\\\"\" | grep -Eq '^[1-9][0-9]*$'"
        )
        wait_for_event_pattern("initial")
        baseline_count = extract_total_events() or 0
        print(f"Baseline event count: {baseline_count}")

    # Test 1: Low intensity stress (warm-up)
    with subtest("Low intensity multi-source stress test"):
        print("Starting low intensity stress test...")
        machine.succeed("timeout 120s sinex-stress 6 low")
        low_count = wait_for_count_increase(baseline_count, 6, timeout=60)
        print(f"Low intensity event count: {low_count}")
        wait_for_event_type_like("file.%")

    # Test 2: Medium intensity stress
    with subtest("Medium intensity multi-source stress test"):
        print("Starting medium intensity stress test...")
        baseline_count = extract_total_events() or 0
        machine.succeed("timeout 120s sinex-stress 8 medium")
        time.sleep(5)
        print(f"Mid-test performance: {safe_perf()}")
        medium_count = wait_for_count_increase(baseline_count, 25, timeout=75)
        print(f"Medium intensity event count: {medium_count}")
        source_stats = machine.succeed("timeout 10s sinex sources || true")
        print(f"Source distribution: {source_stats}")
        wait_for_event_type_like("file.%")

    # Test 3: Bounded burst load
    with subtest("Bounded burst multi-source load test"):
        print("Starting bounded burst load test...")
        baseline_count = extract_total_events() or 0
        machine.succeed("timeout 120s sinex-stress 6 high")
        for i in range(3):
            time.sleep(5)
            print(f"Performance check {i+1}: {safe_perf()}")
        high_count = wait_for_count_increase(baseline_count, 30, timeout=90)
        print(f"Bounded burst event count: {high_count}")
        print(f"Final performance: {safe_perf()}")

    # Test 4: Concurrent source validation
    with subtest("Concurrent source validation"):
        source_stats = machine.succeed("timeout 10s sinex sources || true")
        print(f"Final source statistics:\n{source_stats}")
        sources_found = []
        for line in source_stats.split('\n'):
            if '|' in line and line.strip():
                parts = line.split('|')
                if len(parts) >= 2:
                    source = parts[0].strip()
                    count = parts[1].strip()
                    if source and count.isdigit() and int(count) > 0:
                        sources_found.append(source)
        print(f"Active sources: {sources_found}")
        wait_for_event_type_like("file.%")
        wait_for_terminal_event()

    # Test 5: Post-stress stability
    with subtest("System stability after stress test"):
        machine.succeed("systemctl is-active sinex-ingestd")
        machine.succeed("systemctl is-active sinex-gateway")
        machine.succeed("systemctl is-active postgresql")
        machine.succeed("su - test -c 'echo post-stress-test > /var/lib/sinex/watched/post-stress.txt'")
        wait_for_event_pattern("post-stress")
        print("System remains stable and continues to ingest events after stress testing")

    print("✓ Multi-source stress test completed successfully")
  '';
}
