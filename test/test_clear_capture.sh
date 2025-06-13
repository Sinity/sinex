#!/usr/bin/env bash
# Test script to verify clear command capture works

echo "=== Clear Command Capture Test ==="
echo
echo "This test will verify that we can capture scrollback before clear commands"
echo

# Create some test content
echo "Line 1: This is test content"
echo "Line 2: More test content"
echo "Line 3: Even more content"
ls -la
echo "Line 5: After ls command"

echo
echo "In 3 seconds, we'll run 'clear' command..."
echo "The scrollback monitor should capture everything above before it's cleared"
sleep 3

# This should trigger urgent capture
clear

echo "Screen cleared! Check if scrollback was captured in the database."
echo
echo "Query to check:"
echo "SELECT event_type, payload->>'trigger', payload->>'scrollback_lines' FROM raw.events WHERE event_type = 'terminal.scrollback.captured' ORDER BY ts_ingest DESC LIMIT 5;"