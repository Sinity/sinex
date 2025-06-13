#!/usr/bin/env bash
# Test script to determine when Hyprland emits closewindow events
# and whether we can still access the terminal at that point

echo "=== Terminal Exit Capture Timing Test ==="
echo "This test will help determine if we can capture scrollback when windows close"
echo

# Start monitoring Hyprland events
echo "Starting Hyprland event monitor..."
socat -u UNIX-CONNECT:$XDG_RUNTIME_DIR/hypr/$HYPRLAND_INSTANCE_SIGNATURE/.socket2.sock - | while IFS= read -r line; do
    echo "[$(date +%s.%N)] Hyprland: $line"
    
    # If it's a closewindow event, immediately try to capture from all Kitty windows
    if [[ "$line" == closewindow* ]]; then
        echo "[$(date +%s.%N)] CLOSEWINDOW DETECTED! Attempting immediate capture..."
        
        # Try to list all Kitty windows
        if kitty @ ls 2>/dev/null; then
            echo "[$(date +%s.%N)] Kitty is still responsive!"
            
            # Try to get scrollback from all windows
            kitty @ ls | jq -r '.[].tabs[].windows[].id' 2>/dev/null | while read -r window_id; do
                if [[ -n "$window_id" ]]; then
                    echo "[$(date +%s.%N)] Attempting to capture window $window_id..."
                    if scrollback=$(kitty @ get-text --match "id:$window_id" --extent all 2>/dev/null); then
                        lines=$(echo "$scrollback" | wc -l)
                        echo "[$(date +%s.%N)] SUCCESS: Captured $lines lines from window $window_id"
                    else
                        echo "[$(date +%s.%N)] FAILED: Could not capture window $window_id"
                    fi
                fi
            done
        else
            echo "[$(date +%s.%N)] Kitty is NOT responsive - too late!"
        fi
    fi
done &

MONITOR_PID=$!

echo
echo "Monitor running (PID: $MONITOR_PID)"
echo
echo "TEST INSTRUCTIONS:"
echo "1. Open a new Kitty terminal window"
echo "2. Type some commands to create scrollback"
echo "3. Close the window (Ctrl+D or close button)"
echo "4. Watch the output above to see timing"
echo
echo "Press Ctrl+C to stop the test"

# Keep the script running
trap "kill $MONITOR_PID 2>/dev/null; exit" INT
wait