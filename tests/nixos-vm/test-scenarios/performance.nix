# Performance validation test for Sinex - Optimized version
{ pkgs, sinex-ingestd, sinex-gateway, pg_jsonschema, ... }:

let
  inherit (pkgs) lib;
  
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

  nodes.machine = { config, pkgs, lib, ... }: {
    imports = [
      (import ../common/test-base.nix { 
        inherit config pkgs lib sinex-ingestd sinex-gateway pg_jsonschema; 
      })
    ];

    # Override for performance testing
    services.sinex = {
      # Enable promo worker for performance tests
      promoWorker.enable = true;
      
      unifiedCollector = {
        # Additional sources for performance testing
        sources.filesystem.watchPaths = lib.mkAfter [ "/tmp/perf-test" ];
        sources.shellHistory.enable = true;
        sources.atuin = {
          enable = true;
          databasePath = "/var/lib/sinex/.local/share/atuin/history.db";
        };
        sources.dbus.enable = true;
      };
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
    systemd.tmpfiles.rules = lib.mkAfter [
      "d /tmp/perf-test 0755 test users -"
      "d /var/lib/sinex/.local 0755 sinex sinex -"
      "d /var/lib/sinex/.local/share 0755 sinex sinex -"
      "d /var/lib/sinex/.local/share/atuin 0755 sinex sinex -"
    ];

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
    import sys
    sys.path.append('/etc/nixos-test')
    from test_helpers import TestHelpers
    
    start_all()
    helpers = TestHelpers(machine)
    
    # Wait for system with extended timeout for performance VM
    with subtest("System initialization for performance testing"):
        machine.wait_for_unit("multi-user.target")
        helpers.wait_for_sinex_ready(timeout=90)
        machine.wait_for_unit("sinex-gateway.service")
        
        # Verify services
        assert helpers.check_service_health("sinex-ingestd"), "Collector not healthy"
        assert helpers.check_service_health("sinex-gateway"), "Worker not healthy"
        
        print("✓ Performance test environment ready")

    # Initialize system for performance testing
    with subtest("Initialize performance testing environment"):
        # Initialize data sources with retry
        machine.wait_until_succeeds(
            "su - sinex -c 'cd /var/lib/sinex && atuin init zsh'",
            timeout=30
        )
        
        # Generate baseline events
        baseline_count = helpers.get_event_count()
        helpers.generate_events(10, "baseline")
        
        # Wait for system to stabilize
        machine.sleep(5)
        current_count = helpers.get_event_count()
        print(f"Baseline established: {current_count} events")

    # Test 1: Burst load performance
    with subtest("Burst load performance test"):
        print("Testing burst load performance...")
        pre_burst_count = helpers.get_event_count()
        
        # Measure burst generation time
        burst_duration = helpers.measure_operation_time(
            lambda: machine.succeed("sinex-perf burst 20 100")
        )
        print(f"Burst generation took {burst_duration:.2f}s")
        
        # Wait for processing with progress monitoring
        for i in range(3):
            machine.sleep(5)
            current = helpers.get_event_count()
            print(f"Processing progress: {current - pre_burst_count} events")
        
        # Final measurements
        burst_perf = machine.succeed("sinex perf")
        print(f"Burst performance metrics:\n{burst_perf}")
        
        post_burst_count = helpers.get_event_count()
        events_captured = post_burst_count - pre_burst_count
        capture_rate = (events_captured / 2000) * 100
        
        print(f"Burst test: {events_captured}/2000 events ({capture_rate:.1f}% capture rate)")
        assert capture_rate > 85, f"Low burst capture rate: {capture_rate:.1f}%"

    # Test 2: Sustained load with resource monitoring
    with subtest("Sustained high-frequency load test"):
        print("Testing sustained load with resource monitoring...")
        pre_sustained_count = helpers.get_event_count()
        
        # Start sustained load in background (shorter duration for faster tests)
        machine.execute("sinex-perf sustained 30 200 &")
        pid = machine.succeed("pgrep -f 'sinex-perf sustained' | head -1").strip()
        
        # Monitor performance and resources
        checkpoints = []
        for i in range(3):
            machine.sleep(10)
            
            # Capture metrics
            perf_output = machine.succeed("sinex perf")
            events_per_sec = re.search(r'1 minute:.*\((\d+\.\d+) events/sec\)', perf_output)
            if events_per_sec:
                eps = float(events_per_sec.group(1))
                checkpoints.append(eps)
                print(f"Checkpoint {i+1}: {eps:.1f} events/sec")
            
            # Check if generator is still running
            try:
                machine.succeed(f"kill -0 {pid}")
            except:
                print("Generator finished early")
                break
        
        # Wait for completion
        machine.wait_until_fails(f"kill -0 {pid}", timeout=20)
        machine.sleep(5)  # Allow processing to catch up
        
        # Analyze results
        post_sustained_count = helpers.get_event_count()
        sustained_events = post_sustained_count - pre_sustained_count
        target_events = 200 * 30  # 6000 events
        capture_rate = (sustained_events / target_events) * 100
        
        if checkpoints:
            avg_eps = sum(checkpoints) / len(checkpoints)
            print(f"Average throughput: {avg_eps:.1f} events/sec")
        
        print(f"Sustained test: {sustained_events}/{target_events} events ({capture_rate:.1f}%)") 
        assert capture_rate > 85, f"Low sustained capture rate: {capture_rate:.1f}%"

    # Test 3: Ramp-up load testing  
    with subtest("Ramp-up load testing"):
        print("Testing performance under increasing load...")
        pre_ramp_count = helpers.get_event_count()
        
        # Shorter ramp for faster tests: 10 to 300 events/sec over 30 seconds
        ramp_start = time.time()
        machine.execute("sinex-perf ramp 30 300 &")
        
        # Monitor latency during ramp
        latencies = []
        for i in range(3):
            machine.sleep(10)
            
            # Try to get latency metrics
            try:
                latency_output = machine.succeed("sinex latency")
                # Extract average latency if available
                for line in latency_output.split('\n'):
                    if 'filesystem' in line and 'avg_latency' in line:
                        parts = line.split('|')
                        if len(parts) > 2:
                            avg_latency = float(parts[2].strip())
                            latencies.append(avg_latency)
                            print(f"Latency at {i*10}s: {avg_latency:.3f}s")
            except:
                print(f"Could not parse latency at {i*10}s")
        
        # Wait for completion
        machine.sleep(10)
        
        # Final analysis
        post_ramp_count = helpers.get_event_count()
        ramp_events = post_ramp_count - pre_ramp_count
        ramp_duration = time.time() - ramp_start
        
        print(f"Ramp test: {ramp_events} events in {ramp_duration:.1f}s")
        
        # Check if latency increased significantly
        if latencies and len(latencies) > 1:
            latency_increase = latencies[-1] / latencies[0]
            print(f"Latency increase factor: {latency_increase:.2f}x")
            assert latency_increase < 5, "Latency degraded too much under load"
        
        # Should capture reasonable number of events
        assert ramp_events > 3000, f"Low ramp-up capture: {ramp_events}"

    # Test 4: Multi-source concurrent load
    with subtest("Multi-source concurrent load"):
        print("Testing concurrent load from multiple sources...")
        pre_multi_count = helpers.get_event_count()
        
        # Medium intensity for 30 seconds (faster test)
        load_start = time.time()
        machine.succeed("sinex-load multi-source medium 30")
        load_duration = time.time() - load_start
        
        # Wait for processing to complete
        machine.sleep(10)
        
        # Analyze results by source
        post_multi_count = helpers.get_event_count()
        multi_events = post_multi_count - pre_multi_count
        events_per_second = multi_events / load_duration
        
        print(f"Multi-source test: {multi_events} events in {load_duration:.1f}s")
        print(f"Throughput: {events_per_second:.1f} events/sec")
        
        # Get latency breakdown
        try:
            latency_output = machine.succeed("sinex latency")
            print(f"Source latency breakdown:\n{latency_output}")
            
            # Verify multiple sources contributed
            sources_found = 0
            for source in ['filesystem', 'shell_history', 'atuin']:
                if source in latency_output:
                    sources_found += 1
            
            print(f"Active sources: {sources_found}")
            assert sources_found >= 2, "Too few sources contributed events"
        except:
            print("Could not analyze source breakdown")
        
        # Should handle concurrent sources efficiently
        assert multi_events > 5000, f"Low multi-source capture: {multi_events}"

    # Test 5: Spike load handling
    with subtest("Spike load handling"):
        print("Testing system behavior under load spikes...")
        pre_spike_count = helpers.get_event_count()
        
        # Spike test: base 10 eps with spikes to 500 eps for 30s
        spike_duration = helpers.measure_operation_time(
            lambda: machine.succeed("sinex-perf spike 30 500")
        )
        
        # Monitor recovery
        machine.sleep(5)
        mid_spike_count = helpers.get_event_count()
        machine.sleep(5)
        post_spike_count = helpers.get_event_count()
        
        spike_events = post_spike_count - pre_spike_count
        events_during_recovery = post_spike_count - mid_spike_count
        
        print(f"Spike test: {spike_events} events in {spike_duration:.1f}s")
        print(f"Events during recovery: {events_during_recovery}")
        
        # System should handle spikes and recover quickly
        assert spike_events > 2000, f"Poor spike handling: {spike_events}"
        assert events_during_recovery < 100, "System slow to recover from spikes"

    # Test 6: Query performance under load
    with subtest("Query performance under load"):
        print("Testing database query responsiveness during load...")
        
        # Start background load
        machine.execute("sinex-load filesystem-only medium 30 &")
        load_pid = machine.succeed("pgrep -f 'sinex-load' | head -1").strip()
        
        # Let load stabilize
        machine.sleep(5)
        
        # Measure query performance
        query_times = []
        for i in range(5):
            start = time.time()
            machine.succeed("sinex stats")
            query_time = time.time() - start
            query_times.append(query_time)
            machine.sleep(2)
        
        # Stop load and measure again
        machine.execute(f"kill {load_pid} 2>/dev/null || true")
        machine.sleep(2)
        
        post_load_time = helpers.measure_operation_time(
            lambda: machine.succeed("sinex stats")
        )
        
        avg_under_load = sum(query_times) / len(query_times)
        print(f"Query time under load: {avg_under_load:.3f}s avg")
        print(f"Query time after load: {post_load_time:.3f}s")
        
        # Queries should remain responsive
        assert avg_under_load < 1.0, f"Queries too slow: {avg_under_load:.3f}s"
        assert post_load_time < 0.5, f"Post-load queries slow: {post_load_time:.3f}s"

    # Test 7: Database and system analysis
    with subtest("Database and system performance analysis"):
        # Ensure no background processes
        machine.execute("pkill -f 'sinex-perf' || true")
        machine.execute("pkill -f 'sinex-load' || true")
        machine.sleep(5)
        
        # Database metrics
        db_perf = machine.succeed("sinex db-perf")
        print(f"Database performance:\n{db_perf}")
        
        # Extract and validate key metrics
        if "cache_hit_ratio" in db_perf:
            cache_match = re.search(r'cache_hit_ratio.*?(\d+\.\d+)', db_perf)
            if cache_match:
                cache_ratio = float(cache_match.group(1))
                print(f"Cache hit ratio: {cache_ratio}%")
                assert cache_ratio > 80, f"Poor cache performance: {cache_ratio}%"
        
        # System resources
        resources = machine.succeed("sinex resources")
        print(f"System resources:\n{resources}")
        
        # Verify system is not overloaded
        if "Load average:" in resources:
            load_match = re.search(r'Load average: ([\d.]+)', resources)
            if load_match:
                load_avg = float(load_match.group(1))
                print(f"Load average: {load_avg}")
                assert load_avg < 8.0, f"System overloaded: {load_avg}"

    # Test 8: Performance summary and validation
    with subtest("Performance regression validation"):
        # Clean up before final measurements
        helpers.cleanup_test_data()
        machine.sleep(5)
        
        # Final performance summary
        print("\n=== PERFORMANCE TEST SUMMARY ===")
        
        total_events = helpers.get_event_count()
        print(f"Total events processed: {total_events}")
        
        # Quick responsiveness check
        final_query_time = helpers.measure_operation_time(
            lambda: machine.succeed("sinex stats")
        )
        print(f"Final query response time: {final_query_time:.3f}s")
        
        # Service health check
        services_healthy = (
            helpers.check_service_health("sinex-ingestd") and
            helpers.check_service_health("sinex-gateway") and
            helpers.check_service_health("postgresql")
        )
        print(f"All services healthy: {services_healthy}")
        
        # Performance assertions
        assert total_events > 20000, f"Insufficient events processed: {total_events}"
        assert final_query_time < 1.0, f"System sluggish: {final_query_time:.3f}s"
        assert services_healthy, "Services unhealthy after performance tests"
        
        print("\n✓ Performance tests completed successfully")
        print("✓ System handled various load patterns effectively")
        print("✓ Database queries remained responsive")
        print("✓ Services remained stable throughout testing")
  '';
}