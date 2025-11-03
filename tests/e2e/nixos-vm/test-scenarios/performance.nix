# Performance validation test for Sinex - Optimized version
{ pkgs
, sinex-ingestd
, sinex-gateway
, pg_jsonschema
, sinex ? null
, sinexCli ? null
, ...
}:

let
  inherit (pkgs) lib;
  stateDir = "/var/lib/sinex";
  
  # Enhanced performance monitoring and query tool
  sinex-query = pkgs.writeScriptBin "sinex" ''
    #!${pkgs.python3}/bin/python3
    import subprocess
    import sys
    import json
    import time
    import os
    import statistics

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

    def performance_metrics():
        # Events per second over different time windows
        windows = [
            ('1 minute', 60),
            ('5 minutes', 300),
            ('15 minutes', 900)
        ]
        
        for window_name, seconds in windows:
            cmd = f"psql -d sinex -t -c \"SELECT COUNT(*) FROM core.events WHERE ts_ingest > NOW() - INTERVAL '{window_name}';\""
            result = subprocess.run([
                "su", "-", "postgres", "-c", cmd
            ], capture_output=True, text=True)
            
            if result.returncode == 0:
                count = result.stdout.strip()
                if count.isdigit():
                    events_per_sec = int(count) / seconds
                    print(f"{window_name}: {count} events ({events_per_sec:.2f} events/sec)")
                else:
                    print(f"{window_name}: {count}")
            else:
                print(f"{window_name} query failed: {result.stderr}")

    def latency_analysis():
        # Analyze event processing latency
        cmd = """
        psql -d sinex -t -c "
        SELECT 
            source,
            COUNT(*) as event_count,
            AVG(EXTRACT(EPOCH FROM (ts_ingest - ts_event))) as avg_latency_sec,
            MAX(EXTRACT(EPOCH FROM (ts_ingest - ts_event))) as max_latency_sec,
            MIN(EXTRACT(EPOCH FROM (ts_ingest - ts_event))) as min_latency_sec
        FROM core.events 
        WHERE ts_ingest > NOW() - INTERVAL '5 minutes'
        GROUP BY source 
        ORDER BY avg_latency_sec DESC;
        "
        """
        result = subprocess.run([
            "su", "-", "postgres", "-c", cmd
        ], capture_output=True, text=True)
        
        if result.returncode == 0:
            print("Event processing latency by source:")
            lines = [line.strip() for line in result.stdout.split('\n') if line.strip()]
            for line in lines:
                print(f"  {line}")
        else:
            print(f"Latency analysis failed: {result.stderr}")

    def database_performance():
        # Database performance metrics
        metrics = [
            ("Active connections", "SELECT count(*) FROM pg_stat_activity WHERE state = 'active';"),
            ("Database size", "SELECT pg_size_pretty(pg_database_size('sinex'));"),
            ("Events table size", "SELECT pg_size_pretty(pg_total_relation_size('core.events'));"),
            ("Index usage", "SELECT schemaname, tablename, indexname, idx_scan FROM pg_stat_user_indexes WHERE tablename = 'events';"),
            ("Cache hit ratio", "SELECT round(blks_hit::numeric / (blks_hit + blks_read) * 100, 2) as cache_hit_ratio FROM pg_stat_database WHERE datname = 'sinex';")
        ]
        
        for metric_name, query in metrics:
            result = subprocess.run([
                "su", "-", "postgres", "-c", f"psql -d sinex -t -c \"{query}\""
            ], capture_output=True, text=True)
            
            if result.returncode == 0:
                value = result.stdout.strip()
                print(f"{metric_name}: {value}")
            else:
                print(f"{metric_name}: Query failed")

    def work_queue_metrics():
        # Work queue performance metrics
        cmd = """
        psql -d sinex -t -c "
        SELECT 
            status,
            COUNT(*) as count,
            AVG(EXTRACT(EPOCH FROM (COALESCE(processed_at, NOW()) - created_at))) as avg_processing_time_sec
        FROM sinex_schemas.work_queue 
        WHERE created_at > NOW() - INTERVAL '15 minutes'
        GROUP BY status 
        ORDER BY status;
        "
        """
        result = subprocess.run([
            "su", "-", "postgres", "-c", cmd
        ], capture_output=True, text=True)
        
        if result.returncode == 0:
            print("Work queue processing metrics:")
            lines = [line.strip() for line in result.stdout.split('\n') if line.strip()]
            for line in lines:
                print(f"  {line}")
        else:
            print(f"Work queue metrics failed: {result.stderr}")

    def system_resources():
        # System resource utilization
        try:
            # Memory usage
            with open('/proc/meminfo', 'r') as f:
                meminfo = f.read()
                for line in meminfo.split('\n'):
                    if 'MemTotal:' in line or 'MemAvailable:' in line or 'MemFree:' in line:
                        print(line.strip())
            
            # Load average
            with open('/proc/loadavg', 'r') as f:
                loadavg = f.read().strip()
                print(f"Load average: {loadavg}")
            
            # Disk usage
            disk_result = subprocess.run(['df', '-h', '/'], capture_output=True, text=True)
            if disk_result.returncode == 0:
                print(f"Disk usage: {disk_result.stdout.strip()}")
                
        except Exception as e:
            print(f"System resource check failed: {e}")

    def measure_query_performance():
        # Measure query response times
        query_times = []
        
        for i in range(10):
            start_time = time.time()
            subprocess.run([
                "su", "-", "postgres", "-c", 
                "psql -d sinex -t -c \"SELECT COUNT(*) FROM core.events;\""
            ], capture_output=True, text=True)
            end_time = time.time()
            query_times.append(end_time - start_time)
        
        if query_times:
            avg_time = statistics.mean(query_times)
            min_time = min(query_times)
            max_time = max(query_times)
            print(f"Query performance (10 runs):")
            print(f"  Average: {avg_time:.3f}s")
            print(f"  Min: {min_time:.3f}s")
            print(f"  Max: {max_time:.3f}s")

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

    # Parse command line arguments
    if len(sys.argv) > 1:
        cmd = sys.argv[1]
        if cmd == "stats":
            stats()
        elif cmd == "perf":
            performance_metrics()
        elif cmd == "latency":
            latency_analysis()
        elif cmd == "db-perf":
            database_performance()
        elif cmd == "queue-perf":
            work_queue_metrics()
        elif cmd == "resources":
            system_resources()
        elif cmd == "query-perf":
            measure_query_performance()
        elif cmd == "full-report":
            print("=== SINEX PERFORMANCE REPORT ===")
            print("\n--- Event Statistics ---")
            stats()
            print("\n--- Performance Metrics ---")
            performance_metrics()
            print("\n--- Latency Analysis ---")
            latency_analysis()
            print("\n--- Database Performance ---")
            database_performance()
            print("\n--- Work Queue Metrics ---")
            work_queue_metrics()
            print("\n--- System Resources ---")
            system_resources()
            print("\n--- Query Performance ---")
            measure_query_performance()
        elif cmd == "query":
            query_events()
        else:
            query_events()
    else:
        query_events()
  '';

  # High-frequency event generator
  perf-generator = pkgs.writeScriptBin "sinex-perf" ''
    #!${pkgs.bash}/bin/bash
    set -e
    
    MODE=''${1:-burst}  # burst, sustained, ramp
    DURATION=''${2:-60}  # Default 60 seconds
    TARGET_EPS=''${3:-100}  # Target events per second
    
    echo "Performance test: $MODE mode, $DURATION seconds, target $TARGET_EPS events/sec"
    
    case $MODE in
      burst)
        # Generate bursts of events with pauses
        for ((burst=1; burst<=10; burst++)); do
          echo "Burst $burst/10..."
          for ((i=1; i<=TARGET_EPS; i++)); do
            su - test -c "echo 'burst-$burst-event-$i-$(date +%s%3N)' > /home/test/watched/burst_''${burst}_''${i}.tmp && mv /home/test/watched/burst_''${burst}_''${i}.tmp /home/test/watched/burst_''${burst}_''${i}.txt" &
            if (( i % 20 == 0 )); then
              wait  # Prevent too many background processes
            fi
          done
          wait  # Wait for all events in this burst
          sleep $((DURATION / 10))  # Pause between bursts
        done
        ;;
        
      sustained)
        # Generate steady stream of events
        interval=$(echo "scale=6; 1/$TARGET_EPS" | bc)
        total_events=$((TARGET_EPS * DURATION))
        
        echo "Generating $total_events events over $DURATION seconds (interval: $interval)"
        
        for ((i=1; i<=total_events; i++)); do
          su - test -c "echo 'sustained-event-$i-$(date +%s%3N)' > /home/test/watched/sustained_$i.txt" &
          
          # Throttle background processes
          if (( i % 50 == 0 )); then
            wait
          fi
          
          # Sleep to maintain target rate
          sleep $interval
        done
        wait
        ;;
        
      ramp)
        # Gradually increase event rate
        echo "Ramping from 10 to $TARGET_EPS events/sec over $DURATION seconds"
        
        steps=10
        step_duration=$((DURATION / steps))
        
        for ((step=1; step<=steps; step++)); do
          current_eps=$(( 10 + (TARGET_EPS - 10) * step / steps ))
          echo "Step $step/$steps: $current_eps events/sec for $step_duration seconds"
          
          interval=$(echo "scale=6; 1/$current_eps" | bc)
          events_in_step=$((current_eps * step_duration))
          
          for ((i=1; i<=events_in_step; i++)); do
            su - test -c "echo 'ramp-step-$step-event-$i-$(date +%s%3N)' > /home/test/watched/ramp_''${step}_''${i}.txt" &
            
            if (( i % 20 == 0 )); then
              wait
            fi
            
            sleep $interval
          done
          wait
        done
        ;;
        
      spike)
        # Generate normal load with occasional spikes
        base_eps=10
        spike_eps=$TARGET_EPS
        spike_duration=5  # 5 second spikes
        
        echo "Spike test: base $base_eps eps with spikes to $spike_eps eps every 15 seconds"
        
        start_time=$(date +%s)
        event_counter=1
        
        while [ $(($(date +%s) - start_time)) -lt $DURATION ]; do
          current_time=$(($(date +%s) - start_time))
          
          # Spike every 15 seconds for 5 seconds
          if (( (current_time % 15) < spike_duration )); then
            current_eps=$spike_eps
            mode_label="spike"
          else
            current_eps=$base_eps
            mode_label="base"
          fi
          
          su - test -c "echo '$mode_label-event-$event_counter-$(date +%s%3N)' > /home/test/watched/spike_$event_counter.txt" &
          
          if (( event_counter % 20 == 0 )); then
            wait
          fi
          
          interval=$(echo "scale=6; 1/$current_eps" | bc)
          sleep $interval
          
          event_counter=$((event_counter + 1))
        done
        wait
        ;;
        
      *)
        echo "Unknown mode: $MODE"
        echo "Available modes: burst, sustained, ramp, spike"
        exit 1
        ;;
    esac
    
    echo "Performance test '$MODE' completed"
  '';

  # Load testing script
  load-tester = pkgs.writeScriptBin "sinex-load" ''
    #!${pkgs.bash}/bin/bash
    set -e
    
    TEST_TYPE=''${1:-mixed}  # mixed, filesystem-only, multi-source
    INTENSITY=''${2:-medium}  # low, medium, high, extreme
    DURATION=''${3:-120}  # Default 2 minutes
    
    case $INTENSITY in
      low)
        FS_EPS=20
        SHELL_EPS=5
        CLIPBOARD_EPS=2
        ;;
      medium)
        FS_EPS=100
        SHELL_EPS=20
        CLIPBOARD_EPS=5
        ;;
      high)
        FS_EPS=500
        SHELL_EPS=50
        CLIPBOARD_EPS=10
        ;;
      extreme)
        FS_EPS=1000
        SHELL_EPS=100
        CLIPBOARD_EPS=20
        ;;
    esac
    
    STATE_DIR="${stateDir}"

    echo "Load test: $TEST_TYPE, $INTENSITY intensity, $DURATION seconds"
    echo "Target rates - Filesystem: $FS_EPS eps, Shell: $SHELL_EPS eps, Clipboard: $CLIPBOARD_EPS eps"
    
    pids=()
    
    # Filesystem load
    if [[ "$TEST_TYPE" == "mixed" || "$TEST_TYPE" == "filesystem-only" || "$TEST_TYPE" == "multi-source" ]]; then
      (
        interval=$(echo "scale=6; 1/$FS_EPS" | bc)
        for ((i=1; i<=DURATION*FS_EPS; i++)); do
          su - test -c "echo 'load-fs-$i-$(date +%s%3N)' > /home/test/watched/load_fs_$i.txt"
          sleep $interval
        done
      ) &
      pids+=($!)
    fi
    
    # Shell history load
    if [[ "$TEST_TYPE" == "mixed" || "$TEST_TYPE" == "multi-source" ]]; then
      (
        interval=$(echo "scale=6; 1/$SHELL_EPS" | bc)
        for ((i=1; i<=DURATION*SHELL_EPS; i++)); do
          echo "load_shell_cmd_$i /tmp/load_$i" >> "$STATE_DIR"/.zsh_history
          sleep $interval
        done
      ) &
      pids+=($!)
    fi
    
    # Clipboard load (if available)
    if [[ "$TEST_TYPE" == "mixed" || "$TEST_TYPE" == "multi-source" ]] && [ -e /run/user/1000/wayland-1 ]; then
      (
        interval=$(echo "scale=6; 1/$CLIPBOARD_EPS" | bc)
        for ((i=1; i<=DURATION*CLIPBOARD_EPS; i++)); do
          su - test -c "XDG_RUNTIME_DIR=/run/user/1000 WAYLAND_DISPLAY=wayland-1 echo 'load-clipboard-$i' | wl-copy" 2>/dev/null || true
          sleep $interval
        done
      ) &
      pids+=($!)
    fi
    
    # Atuin database load
    if [[ "$TEST_TYPE" == "multi-source" ]]; then
      (
        db_path="$STATE_DIR/.local/share/atuin/history.db"
        interval=$(echo "scale=6; 1/10" | bc)  # 10 atuin entries per second
        for ((i=1; i<=DURATION*10; i++)); do
          sqlite3 "$db_path" "INSERT INTO history (id, timestamp, duration, exit, command, cwd, session, hostname) VALUES ('load$i', $(date +%s), 100, 0, 'load-command-$i', '/tmp', 'load-session', 'loadhost');" 2>/dev/null || true
          sleep $interval
        done
      ) &
      pids+=($!)
    fi
    
    echo "Load test started with PIDs: ''${pids[@]}"
    
    # Monitor progress
    start_time=$(date +%s)
    while [ $(($(date +%s) - start_time)) -lt $DURATION ]; do
      elapsed=$(($(date +%s) - start_time))
      echo "Load test progress: $elapsed/$DURATION seconds"
      sleep 10
    done
    
    # Wait for all background processes
    for pid in "''${pids[@]}"; do
      wait $pid
    done
    
    echo "Load test completed"
  '';
in
pkgs.nixosTest {
  name = "sinex-performance";

  nodes.machine = { config, pkgs, lib, ... }: {
    imports = [
      (import ../common/test-base.nix {       services.sinex = {
        serviceManagement.serviceGroups = {
          core = true;
          maintenance = true;
          monitoring = false;
        };

        database = {
          autoSetup = true;
          name = "sinex_perf";
          user = "sinex";
        };

        satellite = {
          enable = true;
          coordination.enable = false;
          database.url = "postgresql:///sinex_perf?host=/run/postgresql";
          logLevel = "info";

          coreServices.enable = true;

          eventSources = {
            filesystem = {
              enable = true;
              instances = 2;
            };
            terminal = {
              enable = true;
              instances = 2;
            };
            desktop.enable = false;
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
      };

        inherit config pkgs lib sinex-ingestd sinex-gateway pg_jsonschema sinex sinexCli; 
      })
    ];

    # Override for performance testing
    services.sinex = {
      serviceManagement.serviceGroups = {
        core = true;
        maintenance = true;
        monitoring = false;
      };

      database = {
        autoSetup = true;
        name = "sinex_perf";
        user = "sinex";
      };

      satellite = {
        enable = true;
        coordination.enable = false;
        database.url = "postgresql:///sinex_perf?host=/run/postgresql";
        logLevel = "info";

        coreServices.enable = true;

        eventSources = {
          filesystem = {
            enable = true;
            instances = 2;
            batchSize = 150;
            batchTimeout = 2;
          };
          terminal = {
            enable = true;
            instances = 2;
            batchSize = 120;
            batchTimeout = 2;
          };
          desktop.enable = false;
          system = {
            enable = true;
            instances = 1;
            batchSize = 150;
            batchTimeout = 3;
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
          watchPaths = lib.mkAfter [ "/tmp/perf-test" ];
        };
        shellHistory.enable = true;
      atuin = {
        enable = true;
        databasePath = "${stateDir}/.local/share/atuin/history.db";
      };
        dbus.enable = true;
      };

      monitoring.observabilityStack.enable = false;
    };
    
      # Performance test packages
    environment.systemPackages = with pkgs; [
      atuin
      zsh
      sinex-query
      perf-generator
      load-tester
      bc
      sysstat
    ];
    
    programs.zsh.enable = true;
    services.dbus.enable = true;
    
    # Additional tmpfiles for performance testing
    systemd.tmpfiles.rules = lib.mkAfter (
      [
        "d /tmp/perf-test 0755 test users -"
        "d ${stateDir}/.local 0755 sinex sinex -"
        "d ${stateDir}/.local/share 0755 sinex sinex -"
        "d ${stateDir}/.local/share/atuin 0755 sinex sinex -"
      ]
    );

    # PostgreSQL performance tuning
    services.postgresql.settings = {
      shared_buffers = "512MB";
      wal_buffers = "16MB";
      max_wal_size = "2GB";
      min_wal_size = "80MB";
      work_mem = "8MB";
      maintenance_work_mem = "128MB";
      max_parallel_workers = "4";
      max_parallel_workers_per_gather = "2";
      checkpoint_completion_target = "0.9";
      track_io_timing = "on";
      track_functions = "all";
      # Aggressive autovacuum for performance tests
      autovacuum_naptime = "10s";
      autovacuum_vacuum_scale_factor = "0.05";
      autovacuum_analyze_scale_factor = "0.02";
    };
    
    # Optimize VM for performance testing
    virtualisation = {
      memorySize = 4096;
      diskSize = 8192;
      cores = 4;
    };
  };

  testScript = ''
    import time
    import re
    from test_helpers import TestHelpers

    state_dir = "${stateDir}"

    start_all()
    helpers = TestHelpers(machine)

    def wait_for_delta(baseline: int, delta: int, timeout: int = 120) -> int:
        target = baseline + delta
        if not helpers.wait_for_event_processing(target, timeout):
            current = helpers.get_event_count()
            raise AssertionError(
                f"Timed out waiting for event count to reach {target} (last={current})"
            )
        return helpers.get_event_count()

    def ensure_event(pattern: str, timeout: int = 60) -> None:
        deadline = time.time() + timeout
        while time.time() < deadline:
            output = machine.succeed("sinex query --limit 25")
            if pattern in output:
                return
            time.sleep(2)
        raise AssertionError(f"Timed out waiting for event containing '{pattern}'")

    with subtest("System initialization for performance testing"):
        machine.wait_for_unit("multi-user.target")
        helpers.wait_for_sinex_ready(timeout=120)
        satellites = helpers.wait_for_satellites(timeout=120)
        print(f"Active satellites: {satellites}")

    with subtest("Initialize performance testing environment"):
        machine.wait_until_succeeds(f"su - sinex -c 'cd {state_dir} && atuin init zsh'", timeout=45)
        machine.wait_until_succeeds(f"su - sinex -c 'cd {state_dir} && atuin import auto'", timeout=45)
        machine.succeed("su - test -c 'echo baseline > /home/test/watched/baseline.txt'")
        machine.succeed(f"echo 'baseline_cmd' >> {state_dir}/.zsh_history")
        ensure_event("baseline")
        baseline_count = helpers.get_event_count()
        print(f"Baseline event count: {baseline_count}")

    with subtest("Burst load performance test"):
        pre_burst = helpers.get_event_count()
        duration = helpers.measure_operation_time(lambda: machine.succeed("sinex-perf burst 6 80"))
        print(f"Burst generation duration: {duration:.2f}s")
        post_burst = wait_for_delta(pre_burst, 300, timeout=120)
        print(f"Burst captured {post_burst - pre_burst} events")
        print(machine.succeed("sinex perf"))

    with subtest("Sustained throughput test"):
        pre_sustained = helpers.get_event_count()
        run_duration = helpers.measure_operation_time(lambda: machine.succeed("sinex-perf sustained 20 150"))
        print(f"Sustained load generation duration: {run_duration:.2f}s")
        post_sustained = wait_for_delta(pre_sustained, 2200, timeout=150)
        captured = post_sustained - pre_sustained
        print(f"Sustained captured {captured} events")
        perf_summary = helpers.get_event_count_since(30)
        print(f"Events in last 30s: {perf_summary}")

    with subtest("Multi-source concurrent load"):
        pre_multi = helpers.get_event_count()
        helpers.measure_operation_time(lambda: machine.succeed("sinex-load multi-source medium 20"))
        post_multi = wait_for_delta(pre_multi, 2500, timeout=150)
        multi_events = post_multi - pre_multi
        print(f"Multi-source captured {multi_events} events")
        source_stats = machine.succeed("sinex sources")
        print(f"Source distribution:\n{source_stats}")
        required_sources = ["filesystem", "shell"]
        for source in required_sources:
            if source not in source_stats and source.replace('shell', 'terminal') not in source_stats:
                raise AssertionError(f"Expected source '{source}' missing in stats")

    with subtest("Post-load stability check"):
        machine.succeed("systemctl is-active sinex-ingestd")
        machine.succeed("systemctl is-active sinex-gateway")
        pre_final = helpers.get_event_count()
        machine.succeed("su - test -c 'echo post-load > /home/test/watched/post-load.txt'")
        ensure_event("post-load", timeout=45)
        post_final = helpers.get_event_count()
        print(f"Post-load delta: {post_final - pre_final} events")

    print("✓ Performance scenarios completed successfully")
  '';
}
