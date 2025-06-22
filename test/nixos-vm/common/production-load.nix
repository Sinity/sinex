{ pkgs, ... }:
{
  environment.systemPackages = with pkgs; [
    (writeScriptBin "production-load-generator" ''
      #!${pkgs.bash}/bin/bash
      set -euo pipefail
      
      LOAD_TYPE="$1"
      RATE="''${2:-1000}"
      DURATION="''${3:-60}"
      
      case "$LOAD_TYPE" in
        "--filesystem")
          echo "Generating filesystem load: ''$RATE ops/sec for ''$DURATION seconds"
          
          # Create 50 watched directories (reduced for testing)
          for i in {1..50}; do
            mkdir -p "/watched/dir''$i"
          done
          
          # Generate file operations
          ${pkgs.python3}/bin/python3 -c "
import os
import time
import random
import threading
from pathlib import Path

def generate_operations(dir_num, rate_per_dir, duration):
    base_dir = f'/watched/dir{dir_num}'
    end_time = time.time() + duration
    
    while time.time() < end_time:
        op_type = random.choice(['create', 'modify', 'delete'])
        file_num = random.randint(1, 20)
        file_path = f'{base_dir}/file_{file_num}.txt'
        
        try:
            if op_type == 'create':
                Path(file_path).write_text(f'Content at {time.time()}')
            elif op_type == 'modify' and os.path.exists(file_path):
                with open(file_path, 'a') as f:
                    f.write(f'\nModified at {time.time()}')
            elif op_type == 'delete' and os.path.exists(file_path):
                os.unlink(file_path)
        except:
            pass  # Ignore errors during load testing
        
        time.sleep(max(0.1, 1.0 / rate_per_dir))

# Start threads for each directory
threads = []
rate_per_dir = max(1, ''$RATE / 50)  # Distribute across 50 directories

for i in range(1, 51):
    t = threading.Thread(target=generate_operations, args=(i, rate_per_dir, ''$DURATION))
    t.start()
    threads.append(t)

# Wait for completion
for t in threads:
    t.join()

print(f'Generated filesystem operations at ~''$RATE ops/sec')
"
          ;;
          
        "--mixed")
          echo "Generating mixed production load"
          
          # Run multiple load types concurrently
          production-load-generator --filesystem $((''$RATE / 2)) ''$DURATION &
          production-load-generator --terminal $((''$RATE / 2)) ''$DURATION &
          wait
          ;;
          
        "--terminal")
          echo "Generating terminal command load"
          for i in {1..5}; do
            ${pkgs.tmux}/bin/tmux new-session -d -s "load''$i" "
              count=0
              end_time=\$((SECONDS + ''$DURATION))
              while [ \$SECONDS -lt \$end_time ]; do
                echo 'Command \$count at \$(date)' 
                ls -la /tmp >/dev/null
                count=\$((count + 1))
                sleep \$((100 / ''$RATE))
              done
            " 2>/dev/null || true
          done
          sleep ''$DURATION
          for i in {1..5}; do
            ${pkgs.tmux}/bin/tmux kill-session -t "load''$i" 2>/dev/null || true
          done
          ;;
          
        "--clipboard")
          echo "Generating clipboard activity"
          ${pkgs.python3}/bin/python3 -c "
import time
import random
import string

duration = ''$DURATION
rate = max(1, ''$RATE)
end_time = time.time() + duration

count = 0
while time.time() < end_time:
    # Generate random content
    content = 'test_content_' + str(count)
    
    # Simulate clipboard by writing to file
    with open('/tmp/clipboard_sim.txt', 'w') as f:
        f.write(content)
    
    count += 1
    time.sleep(max(0.01, 1.0 / rate))

print(f'Generated {count} clipboard operations')
"
          ;;
      esac
    '')
    
    (writeScriptBin "sinex-metrics" ''
      #!${pkgs.bash}/bin/bash
      # Output system metrics in JSON format
      
      # Calculate ingestion rate
      EVENTS_1=$(sinex-query --format csv 2>/dev/null | wc -l || echo "0")
      sleep 5
      EVENTS_2=$(sinex-query --format csv 2>/dev/null | wc -l || echo "0")
      INGESTION_RATE=$(( (EVENTS_2 - EVENTS_1) / 5 ))
      
      # Get memory usage
      MEMORY_MB=$(free -m | grep Mem | awk '{print $3}')
      
      # Get CPU load
      CPU_LOAD=$(uptime | awk -F'load average:' '{print $2}' | awk -F, '{print $1}' | xargs)
      
      # Database query latency (simplified)
      LATENCY_START=$(date +%s%N)
      sinex-query --limit 1 >/dev/null 2>&1 || true
      LATENCY_END=$(date +%s%N)
      LATENCY_MS=$(( (LATENCY_END - LATENCY_START) / 1000000 ))
      
      cat <<EOF
{
  "ingestion_rate": $INGESTION_RATE,
  "memory_usage": $MEMORY_MB,
  "cpu_load": $CPU_LOAD,
  "query_latency_ms": $LATENCY_MS,
  "timestamp": "$(date -Iseconds)"
}
EOF
    '')
  ];
}