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

    def stats_by_source():
        cmd = "psql -d sinex -t -c \"SELECT source, COUNT(*) FROM core.events GROUP BY source ORDER BY COUNT(*) DESC;\""
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
        cmd = "psql -d sinex -t -c \"SELECT COUNT(*) FROM core.events WHERE ts_ingest > NOW() - INTERVAL '1 minute';\""
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
        cmd = "psql -d sinex -t -c 'SELECT COUNT(*) FROM core.events;'"
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
    set -e
    
    DURATION=''${1:-30}  # Default 30 seconds
    INTENSITY=''${2:-medium}  # low, medium, high
    
    case $INTENSITY in
      low)
        FILE_OPS_PER_SEC=10
        SHELL_CMDS_PER_SEC=5
        CLIPBOARD_OPS_PER_SEC=2
        ;;
      medium)
        FILE_OPS_PER_SEC=50
        SHELL_CMDS_PER_SEC=20
        CLIPBOARD_OPS_PER_SEC=5
        ;;
      high)
        FILE_OPS_PER_SEC=200
        SHELL_CMDS_PER_SEC=50
        CLIPBOARD_OPS_PER_SEC=10
        ;;
    esac
    
    echo "Starting $INTENSITY stress test for $DURATION seconds"
    echo "File ops/sec: $FILE_OPS_PER_SEC, Shell cmds/sec: $SHELL_CMDS_PER_SEC, Clipboard ops/sec: $CLIPBOARD_OPS_PER_SEC"
    
    # Background jobs for parallel stress testing
    pids=()
    
    # File operations stress
    (
      for ((i=1; i<=DURATION*FILE_OPS_PER_SEC; i++)); do
        su - test -c "echo 'stress test $i' > /home/test/watched/stress_$i.txt"
        su - test -c "rm /home/test/watched/stress_$i.txt" 2>/dev/null || true
        sleep $(echo "scale=3; 1/$FILE_OPS_PER_SEC" | bc)
      done
    ) &
    pids+=($!)
    
    # Shell history stress  
    (
      for ((i=1; i<=DURATION*SHELL_CMDS_PER_SEC; i++)); do
        echo "stress_cmd_$i /tmp/stress_$i" >> /var/lib/sinex/.zsh_history
        echo "stress_bash_$i" >> /var/lib/sinex/.bash_history
        sleep $(echo "scale=3; 1/$SHELL_CMDS_PER_SEC" | bc)
      done
    ) &
    pids+=($!)
    
    # Clipboard operations stress (if Wayland available)
    if [ -e /run/user/1000/wayland-1 ]; then
      (
        for ((i=1; i<=DURATION*CLIPBOARD_OPS_PER_SEC; i++)); do
          su - test -c "XDG_RUNTIME_DIR=/run/user/1000 WAYLAND_DISPLAY=wayland-1 echo 'clipboard stress $i' | wl-copy" 2>/dev/null || true
          sleep $(echo "scale=3; 1/$CLIPBOARD_OPS_PER_SEC" | bc)
        done
      ) &
      pids+=($!)
    fi
    
    # Atuin database stress
    (
      db_path="/var/lib/sinex/.local/share/atuin/history.db"
      for ((i=1; i<=DURATION*5; i++)); do  # 5 atuin entries per second
        sqlite3 "$db_path" "INSERT INTO history (id, timestamp, duration, exit, command, cwd, session, hostname) VALUES ('stress$i', $(date +%s), 100, 0, 'stress-command-$i', '/tmp', 'stress-session', 'testhost');" 2>/dev/null || true
        sleep 0.2
      done
    ) &
    pids+=($!)
    
    # Asciinema file stress
    (
      for ((i=1; i<=DURATION*2; i++)); do  # 2 recording files per second
        su - test -c "echo '{\"version\": 2, \"width\": 80, \"height\": 24}' > /home/test/.local/share/asciinema/stress_$i.cast"
        su - test -c "echo '[0.0, \"o\", \"stress command $i\"]' >> /home/test/.local/share/asciinema/stress_$i.cast"
        sleep 0.5
      done
    ) &
    pids+=($!)
    
    echo "Stress test started with PIDs: ''${pids[@]}"
    
    # Wait for all background processes
    for pid in "''${pids[@]}"; do
      wait $pid
    done
    
    echo "Stress test completed"
  '';
in
pkgs.nixosTest {
  name = "sinex-multi-source-stress";

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
              instances = 2;
              extraArgs = "";
              watchPaths = [ "/home/test/watched" "/tmp/sinex-stress" ];
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

        shell = {
          asciinema.recordingsPath = "/home/test/.local/share/asciinema";
          kitty.enable = true;
        };

      };

      # Test user setup with additional stress directories
      users.users.test = {
        isNormalUser = true;
        createHome = true;
        shell = pkgs.zsh;
        uid = 1000;
      };
      
      # Enhanced Hyprland setup for IPC testing
      services.dbus.enable = true;
      
      systemd.services.hyprland-headless = {
        description = "Hyprland Wayland compositor (headless mode for testing)";
        wantedBy = [ "multi-user.target" ];
        after = [ "systemd-user-sessions.service" ];
        
        serviceConfig = {
          ExecStart = "${pkgs.hyprland}/bin/Hyprland";
          Restart = "always";
          RestartSec = "2";
          User = "test";
          Group = "users";
          Environment = [
            "WAYLAND_DISPLAY=wayland-1"
            "XDG_RUNTIME_DIR=/run/user/1000"
            "XDG_SESSION_TYPE=wayland"
            "WLR_BACKENDS=headless"
            "WLR_RENDERER=pixman"
            "WLR_RENDERER_ALLOW_SOFTWARE=1"
            "HYPRLAND_NO_RT=1"
            "HYPRLAND_NO_SD_NOTIFY=1"
            "LIBGL_ALWAYS_SOFTWARE=1"
            "WLR_NO_HARDWARE_CURSORS=1"
            "HYPRLAND_INSTANCE_SIGNATURE=test"
          ];
        };
        
        preStart = ''
          mkdir -p /run/user/1000
          chown test:users /run/user/1000
          chmod 0700 /run/user/1000
          
          # Create IPC socket directory
          mkdir -p /tmp/hypr
          chown test:users /tmp/hypr
          
          # Enhanced Hyprland configuration for IPC testing
          mkdir -p /home/test/.config/hypr
          cat > /home/test/.config/hypr/hyprland.conf <<EOF
monitor=HEADLESS-1,1920x1080@60,0x0,1

input {
    kb_layout = us
}

general {
    gaps_in = 5
    gaps_out = 20
    border_size = 2
}

# Enable IPC and events for stress testing
misc {
    disable_hyprland_logo = true
    enable_swallow = false
    vfr = false
}

# Window rules for stress testing
windowrulev2 = float,class:.*
windowrulev2 = size 800 600,class:.*
EOF
          chown -R test:users /home/test/.config
        '';
      };
      
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
        wl-clipboard
        wl-clip-persist
        sinex-query
        stress-generator
        hyprland
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
      
      environment.sessionVariables = {
        WAYLAND_DISPLAY = "wayland-1";
        XDG_SESSION_TYPE = "wayland";
      };
      
      programs.zsh.enable = true;
      
      # Enhanced tmpfiles for stress testing
      systemd.tmpfiles.rules = [
        # Test directories
        "d /home/test/watched 0755 test users -"
        "d /tmp/sinex-stress 0755 test users -"
        
        # Shell history files
        "f /var/lib/sinex/.zsh_history 0644 sinex sinex -"
        "f /var/lib/sinex/.bash_history 0644 sinex sinex -"
        
        # Atuin directories
        "d /var/lib/sinex/.local 0755 sinex sinex -"
        "d /var/lib/sinex/.local/share 0755 sinex sinex -"
        "d /var/lib/sinex/.local/share/atuin 0755 sinex sinex -"
        
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
        deadline = time.time() + timeout
        while time.time() < deadline:
            output = machine.succeed("sinex query --limit 20")
            if pattern in output:
                return
            time.sleep(2)
        raise AssertionError(f"Timed out waiting for event containing '{pattern}'")

    def safe_perf():
        try:
            return machine.succeed("sinex perf")
        except Exception as exc:
            return f"perf unavailable: {exc}"

    def start_optional_hyprland():
        try:
            machine.systemctl("start hyprland-headless")
            machine.wait_until_succeeds("systemctl is-active hyprland-headless.service")
            print("Hyprland headless started for desktop event coverage")
        except Exception as exc:
            print(f"Hyprland optional start failed (continuing without Wayland sources): {exc}")

    start_all()

    # Wait for system to be ready
    machine.wait_for_unit("multi-user.target")
    machine.wait_for_unit("postgresql.service")

    # Wait for Sinex services
    machine.wait_for_unit("sinex-ingestd.service")
    machine.wait_for_unit("sinex-gateway.service")
    machine.wait_for_unit("nats.service")

    # Ensure satellite instances are online
    satellite_units = [
        "sinex-fs-watcher-1.service",
        "sinex-fs-watcher-2.service",
        "sinex-terminal-satellite-1.service",
        "sinex-desktop-satellite-1.service",
        "sinex-system-satellite-1.service",
        "sinex-terminal-command-canonicalizer.service",
        "sinex-health-aggregator.service",
    ]
    for unit in satellite_units:
        machine.wait_for_unit(unit)
        machine.succeed(f"systemctl is-active {unit}")

    # Start optional desktop integration
    start_optional_hyprland()

    # Verify core hubs are active
    machine.succeed("systemctl is-active sinex-ingestd")
    machine.succeed("systemctl is-active sinex-gateway")

    # Initialize all data sources
    with subtest("Initialize all event sources"):
        machine.succeed("su - sinex -c 'cd /var/lib/sinex && atuin init zsh'")
        machine.succeed("su - sinex -c 'cd /var/lib/sinex && atuin import auto'")
        machine.succeed("su - test -c 'echo initial > /home/test/watched/initial.txt'")
        machine.succeed("echo 'initial_cmd' >> /var/lib/sinex/.zsh_history")
        wait_for_event_pattern("initial")
        baseline_count = extract_total_events() or 0
        print(f"Baseline event count: {baseline_count}")

    # Test 1: Low intensity stress (warm-up)
    with subtest("Low intensity multi-source stress test"):
        print("Starting low intensity stress test...")
        machine.succeed("sinex-stress 10 low")
        low_count = wait_for_count_increase(baseline_count, 20, timeout=90)
        print(f"Low intensity event count: {low_count}")
        machine.wait_until_succeeds("sinex sources | grep filesystem")

    # Test 2: Medium intensity stress
    with subtest("Medium intensity multi-source stress test"):
        print("Starting medium intensity stress test...")
        baseline_count = extract_total_events() or 0
        machine.succeed("sinex-stress 20 medium")
        time.sleep(5)
        print(f"Mid-test performance: {safe_perf()}")
        medium_count = wait_for_count_increase(baseline_count, 250, timeout=120)
        print(f"Medium intensity event count: {medium_count}")
        source_stats = machine.succeed("sinex sources")
        print(f"Source distribution: {source_stats}")
        assert "filesystem" in source_stats, "Filesystem events not captured"

    # Test 3: High intensity stress (performance limit test)
    with subtest("High intensity performance limit test"):
        print("Starting high intensity stress test...")
        baseline_count = extract_total_events() or 0
        machine.succeed("sinex-stress 15 high")
        for i in range(3):
            time.sleep(5)
            print(f"Performance check {i+1}: {safe_perf()}")
        high_count = wait_for_count_increase(baseline_count, 500, timeout=150)
        print(f"High intensity event count: {high_count}")
        print(f"Final performance: {safe_perf()}")

    # Test 4: Concurrent source validation
    with subtest("Concurrent source validation"):
        source_stats = machine.succeed("sinex sources")
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
        assert any('filesystem' in found for found in sources_found), f"Filesystem events missing: {sources_found}"
        assert any('terminal' in found or 'shell' in found for found in sources_found), f"Terminal events missing: {sources_found}"

    # Test 5: Post-stress stability
    with subtest("System stability after stress test"):
        machine.succeed("systemctl is-active sinex-ingestd")
        machine.succeed("systemctl is-active sinex-gateway")
        machine.succeed("systemctl is-active postgresql")
        machine.succeed("su - test -c 'echo post-stress-test > /home/test/watched/post-stress.txt'")
        wait_for_event_pattern("post-stress")
        print("System remains stable and continues to ingest events after stress testing")

    print("✓ Multi-source stress test completed successfully")
  '';
}
