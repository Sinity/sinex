# Performance validation test for Sinex
{ pkgs, sinex-collector, sinex-promo-worker, pg_jsonschema, ... }:

let
  # Performance monitoring and query tool
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
        
        cmd = f"psql -d sinex -t -c \"SELECT id, source, event_type, ts_ingest, payload FROM raw.events WHERE 1=1{where_clause} ORDER BY ts_ingest DESC LIMIT {limit};\""
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
            cmd = f"psql -d sinex -t -c \"SELECT COUNT(*) FROM raw.events WHERE ts_ingest > NOW() - INTERVAL '{window_name}';\""
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
        FROM raw.events 
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
            ("Events table size", "SELECT pg_size_pretty(pg_total_relation_size('raw.events'));"),
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
                "psql -d sinex -t -c \"SELECT COUNT(*) FROM raw.events;\""
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
        cmd = "psql -d sinex -t -c 'SELECT COUNT(*) FROM raw.events;'"
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
          echo "load_shell_cmd_$i /tmp/load_$i" >> /var/lib/sinex/.zsh_history
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
        db_path="/var/lib/sinex/.local/share/atuin/history.db"
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

  nodes.machine =
    { config, pkgs, lib, ... }:
    {
      imports = [
        ../../../nixos
      ];

      services.sinex = {
        enable = true;
        package = sinex-collector;
        promoWorker.enable = true;

        unifiedCollector = {
          enable = true;
          
          # Enable all sources for comprehensive performance testing
          sources.filesystem = {
            enable = true;
            watchPaths = [ "/home/test/watched" "/tmp/perf-test" ];
          };
          sources.atuin = {
            enable = true;
            databasePath = "/var/lib/sinex/.local/share/atuin/history.db";
          };
          sources.shellHistory.enable = true;
          sources.asciinema = {
            enable = true;
            recordingsPath = "/home/test/.local/share/asciinema";
            autoRecord = false;
          };
          sources.clipboard.enable = true;
          sources.dbus.enable = true;
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
      
      # Enhanced system for performance testing
      environment.systemPackages = with pkgs; [
        atuin
        asciinema
        zsh
        bash
        file
        git
        sqlite
        wl-clipboard
        sinex-query
        perf-generator
        load-tester
        bc            # For floating point calculations
        procps        # Process monitoring
        htop          # System monitoring
        iotop         # IO monitoring
        sysstat       # System statistics
        time          # Command timing
      ];
      
      programs.zsh.enable = true;
      
      # Performance-oriented tmpfiles
      systemd.tmpfiles.rules = [
        "d /home/test/watched 0755 test users -"
        "d /tmp/perf-test 0755 test users -"
        "f /var/lib/sinex/.zsh_history 0644 sinex sinex -"
        "f /var/lib/sinex/.bash_history 0644 sinex sinex -"
        "d /var/lib/sinex/.local 0755 sinex sinex -"
        "d /var/lib/sinex/.local/share 0755 sinex sinex -"
        "d /var/lib/sinex/.local/share/atuin 0755 sinex sinex -"
        "d /home/test/.local 0755 test users -"
        "d /home/test/.local/share 0755 test users -"
        "d /home/test/.local/share/asciinema 0755 test users -"
        "d /run/user 0755 root root -"
        "d /run/user/1000 0700 test users -"
      ];
      
      # Package overlays
      nixpkgs.overlays = [(final: prev: {
        sinex-unified-collector = sinex-collector;
        sinex-promo-worker = sinex-promo-worker;
        postgresql16Packages = prev.postgresql16Packages // {
          pg_jsonschema = pg_jsonschema;
        };
      })];

      # PostgreSQL performance tuning for testing
      services.postgresql = {
        settings = {
          # Increase shared buffers for better performance
          shared_buffers = "256MB";
          # Increase WAL buffers
          wal_buffers = "16MB";
          # Increase checkpoint segments
          max_wal_size = "1GB";
          min_wal_size = "80MB";
          # Increase work memory
          work_mem = "4MB";
          # Increase maintenance work memory
          maintenance_work_mem = "64MB";
          # Enable parallel workers
          max_parallel_workers = "4";
          max_parallel_workers_per_gather = "2";
          # Tune checkpoint behavior
          checkpoint_completion_target = "0.9";
          # Enable statistics collection
          track_io_timing = "on";
          track_functions = "all";
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
    machine.wait_for_unit("sinex-unified-collector.service")
    machine.wait_for_unit("sinex-promo-worker.service")

    # Verify all services are active
    machine.succeed("systemctl is-active sinex-unified-collector")
    machine.succeed("systemctl is-active sinex-promo-worker")

    # Initialize system for performance testing
    with subtest("Initialize performance testing environment"):
        # Initialize data sources
        machine.succeed("su - sinex -c 'cd /var/lib/sinex && atuin init zsh'")
        machine.succeed("su - sinex -c 'cd /var/lib/sinex && atuin import auto'")
        
        # Create baseline test data
        machine.succeed("su - test -c 'echo baseline > /home/test/watched/baseline.txt'")
        machine.sleep(3)
        
        baseline_stats = machine.succeed("sinex stats")
        print(f"Baseline stats: {baseline_stats}")

    # Test 1: Burst load performance
    with subtest("Burst load performance test"):
        print("Testing burst load performance...")
        
        pre_burst_stats = machine.succeed("sinex stats")
        pre_burst_match = re.search(r'Total events captured: (\d+)', pre_burst_stats)
        pre_burst_count = int(pre_burst_match.group(1)) if pre_burst_match else 0
        
        # Generate burst load - 100 events/sec for 30 seconds
        machine.succeed("sinex-perf burst 30 100")
        
        # Wait for processing
        machine.sleep(15)
        
        # Measure performance
        burst_perf = machine.succeed("sinex perf")
        print(f"Burst performance metrics: {burst_perf}")
        
        # Check final count
        post_burst_stats = machine.succeed("sinex stats")
        post_burst_match = re.search(r'Total events captured: (\d+)', post_burst_stats)
        post_burst_count = int(post_burst_match.group(1)) if post_burst_match else 0
        
        events_captured = post_burst_count - pre_burst_count
        print(f"Burst test captured {events_captured} events")
        
        # Should capture most events (allow for some processing delay)
        assert events_captured > 2500, f"Low event capture rate in burst test: {events_captured}/3000 expected"

    # Test 2: Sustained high-frequency load
    with subtest("Sustained high-frequency load test"):
        print("Testing sustained load performance...")
        
        pre_sustained_stats = machine.succeed("sinex stats")
        pre_sustained_match = re.search(r'Total events captured: (\d+)', pre_sustained_stats)
        pre_sustained_count = int(pre_sustained_match.group(1)) if pre_sustained_match else 0
        
        # Start sustained load in background
        machine.execute("sinex-perf sustained 60 200 &")  # 200 events/sec for 60 seconds
        
        # Monitor performance every 15 seconds
        for i in range(4):
            machine.sleep(15)
            current_perf = machine.succeed("sinex perf")
            print(f"Sustained load checkpoint {i+1}: {current_perf}")
            
            # Check system resources
            resources = machine.succeed("sinex resources")
            print(f"System resources at checkpoint {i+1}: {resources}")
        
        # Wait for completion
        machine.sleep(10)
        
        # Final measurements
        post_sustained_stats = machine.succeed("sinex stats")
        post_sustained_match = re.search(r'Total events captured: (\d+)', post_sustained_stats)
        post_sustained_count = int(post_sustained_match.group(1)) if post_sustained_match else 0
        
        sustained_events = post_sustained_count - pre_sustained_count
        target_events = 200 * 60  # 12000 events
        capture_rate = (sustained_events / target_events) * 100
        
        print(f"Sustained test: {sustained_events}/{target_events} events ({capture_rate:.1f}% capture rate)")
        
        # Should maintain good capture rate under sustained load
        assert capture_rate > 90, f"Low sustained capture rate: {capture_rate:.1f}%"

    # Test 3: Ramp-up load testing
    with subtest("Ramp-up load testing"):
        print("Testing ramp-up performance...")
        
        pre_ramp_stats = machine.succeed("sinex stats")
        pre_ramp_match = re.search(r'Total events captured: (\d+)', pre_ramp_stats)
        pre_ramp_count = int(pre_ramp_match.group(1)) if pre_ramp_match else 0
        
        # Ramp from 10 to 500 events/sec over 90 seconds
        machine.succeed("sinex-perf ramp 90 500")
        
        # Wait for processing
        machine.sleep(10)
        
        # Analyze latency during ramp-up
        latency_analysis = machine.succeed("sinex latency")
        print(f"Ramp-up latency analysis: {latency_analysis}")
        
        post_ramp_stats = machine.succeed("sinex stats")
        post_ramp_match = re.search(r'Total events captured: (\d+)', post_ramp_stats)
        post_ramp_count = int(post_ramp_match.group(1)) if post_ramp_match else 0
        
        ramp_events = post_ramp_count - pre_ramp_count
        print(f"Ramp-up test captured {ramp_events} events")
        
        # Should handle ramp-up gracefully
        assert ramp_events > 15000, f"Low event capture in ramp-up test: {ramp_events}"

    # Test 4: Multi-source load testing
    with subtest("Multi-source concurrent load"):
        print("Testing multi-source concurrent load...")
        
        pre_multi_stats = machine.succeed("sinex stats")
        pre_multi_match = re.search(r'Total events captured: (\d+)', pre_multi_stats)
        pre_multi_count = int(pre_multi_match.group(1)) if pre_multi_match else 0
        
        # High intensity multi-source load
        machine.succeed("sinex-load multi-source high 90")
        
        # Monitor during load
        machine.sleep(30)
        mid_test_perf = machine.succeed("sinex perf")
        print(f"Mid multi-source test performance: {mid_test_perf}")
        
        # Wait for completion and processing
        machine.sleep(15)
        
        # Final analysis
        post_multi_stats = machine.succeed("sinex stats")
        post_multi_match = re.search(r'Total events captured: (\d+)', post_multi_stats)
        post_multi_count = int(post_multi_match.group(1)) if post_multi_match else 0
        
        multi_events = post_multi_count - pre_multi_count
        print(f"Multi-source test captured {multi_events} events")
        
        # Verify all sources contributed
        final_latency = machine.succeed("sinex latency")
        print(f"Multi-source latency breakdown: {final_latency}")
        
        # Should handle multiple concurrent sources
        assert multi_events > 30000, f"Low multi-source capture: {multi_events}"

    # Test 5: Spike load testing
    with subtest("Spike load handling"):
        print("Testing spike load handling...")
        
        pre_spike_stats = machine.succeed("sinex stats")
        pre_spike_match = re.search(r'Total events captured: (\d+)', pre_spike_stats)
        pre_spike_count = int(pre_spike_match.group(1)) if pre_spike_match else 0
        
        # Spike test: base 10 eps with spikes to 1000 eps
        machine.succeed("sinex-perf spike 60 1000")
        
        # Wait for processing
        machine.sleep(10)
        
        post_spike_stats = machine.succeed("sinex stats")
        post_spike_match = re.search(r'Total events captured: (\d+)', post_spike_stats)
        post_spike_count = int(post_spike_match.group(1)) if post_spike_match else 0
        
        spike_events = post_spike_count - pre_spike_count
        print(f"Spike test captured {spike_events} events")
        
        # Should handle spikes without significant loss
        assert spike_events > 8000, f"Poor spike handling: {spike_events}"

    # Test 6: Query performance under load
    with subtest("Query performance under load"):
        print("Testing query performance under concurrent load...")
        
        # Start background load
        machine.execute("sinex-load filesystem-only medium 60 &")
        
        # Measure query performance during load
        machine.sleep(10)  # Let load stabilize
        
        query_perf = machine.succeed("sinex query-perf")
        print(f"Query performance under load: {query_perf}")
        
        # Parse query times to ensure responsiveness
        lines = query_perf.split('\n')
        for line in lines:
            if 'Average:' in line:
                avg_time = float(line.split(':')[1].strip().replace('s', ''))
                assert avg_time < 1.0, f"Queries too slow under load: {avg_time}s average"
                print(f"✓ Query performance acceptable: {avg_time}s average")

    # Test 7: Database performance analysis
    with subtest("Database performance analysis"):
        # Wait for any background processes to finish
        machine.sleep(10)
        
        # Comprehensive database performance report
        db_perf = machine.succeed("sinex db-perf")
        print(f"Database performance metrics: {db_perf}")
        
        # Work queue performance
        queue_perf = machine.succeed("sinex queue-perf")
        print(f"Work queue performance: {queue_perf}")
        
        # System resource utilization
        final_resources = machine.succeed("sinex resources")
        print(f"Final system resources: {final_resources}")

    # Test 8: Performance regression check
    with subtest("Performance regression validation"):
        # Generate final performance report
        full_report = machine.succeed("sinex full-report")
        print(f"=== FINAL PERFORMANCE REPORT ===\n{full_report}")
        
        # Validate key performance indicators
        final_stats = machine.succeed("sinex stats")
        final_match = re.search(r'Total events captured: (\d+)', final_stats)
        total_events = int(final_match.group(1)) if final_match else 0
        
        print(f"Total events captured in performance tests: {total_events}")
        
        # Should have captured a significant number of events
        assert total_events > 100000, f"Low total event count: {total_events}"
        
        # Verify system is still responsive
        quick_query_start = time.time()
        machine.succeed("sinex stats")
        quick_query_time = time.time() - quick_query_start
        
        assert quick_query_time < 2.0, f"System unresponsive after load testing: {quick_query_time}s"
        
        print(f"✓ System remains responsive after extensive performance testing ({quick_query_time:.2f}s)")

    print("✓ All performance tests completed successfully")
    print("✓ System demonstrated high-throughput event processing capability")
    print("✓ Performance remains stable under various load patterns")
    print("✓ Query responsiveness maintained during concurrent operations")
  '';
}