# Terminal Exit Capture - The Reality

## The Problem with closewindow Events

After thinking harder about this, the fundamental issue is timing:

1. When Hyprland emits `closewindow`, the window is closing or already closed
2. The terminal process may already be terminated
3. Even if it's still alive, it's in the process of shutting down
4. By the time our worker processes the event, it's almost certainly too late

## Real Solutions

### 1. Continuous Incremental Capture (Most Realistic)
Instead of trying to capture on exit, capture continuously:

```toml
[event.terminal_scrollback]
# Capture more frequently
capture_interval_secs = 60  # Every minute instead of 5 minutes
# Store only new content since last capture
incremental_capture = true
```

Implementation:
- Track last captured position per window
- Store only new lines in each event
- Link events together for full reconstruction
- Accept that the last 0-60 seconds might be lost on sudden exit

### 2. Shell Integration Hooks (Most Reliable)
The ONLY reliable way to capture before exit is from inside the shell:

```bash
# .zshrc / .bashrc
_sinex_capture_scrollback() {
    if [[ -n "$KITTY_WINDOW_ID" ]] && command -v kitty >/dev/null 2>&1; then
        # Capture in background to not delay exit
        (
            kitty @ get-text --match "id:$KITTY_WINDOW_ID" --extent all 2>/dev/null | \
            curl -s -X POST http://localhost:9091/api/scrollback \
                -H "Content-Type: text/plain" \
                -d @- >/dev/null 2>&1 &
        )
    fi
}

# Bash: trap on EXIT
trap '_sinex_capture_scrollback' EXIT

# Zsh: use zshexit hook
zshexit() {
    _sinex_capture_scrollback
}
```

### 3. Prompt Command Capture (Compromise)
Capture after every command execution:

```bash
# Capture after each command
_sinex_prompt_capture() {
    # Only capture if last command took > 2 seconds
    if [[ $SECONDS -gt 2 ]]; then
        _sinex_capture_scrollback
    fi
}

# Bash
PROMPT_COMMAND="_sinex_prompt_capture; $PROMPT_COMMAND"

# Zsh
precmd_functions+=(_sinex_prompt_capture)
```

### 4. Terminal Wrapper Script (Clean but Requires User Action)
Create a wrapper that ensures capture:

```bash
#!/usr/bin/env bash
# /usr/local/bin/kitty-sinex

# Start kitty with a unique session ID
SESSION_ID=$(uuidgen)
export SINEX_SESSION_ID=$SESSION_ID

# Record start
curl -s -X POST http://localhost:9091/api/session/start \
    -d "{\"session_id\": \"$SESSION_ID\", \"type\": \"kitty\"}"

# Run kitty
kitty "$@"

# Capture on exit (kitty has exited at this point)
# This won't work! The window is gone!

# This is why we need the shell hook instead
```

### 5. Clear Command Detection (This Works!)
We CAN detect and capture before clear:

```rust
// In existing shell command worker
impl Worker for ShellCommandWorker {
    async fn process_event(&mut self, event: &RawEvent) -> Result<()> {
        if event.event_type == "command.executed" {
            let payload: CommandPayload = serde_json::from_value(event.payload)?;
            
            // Check if it's a clear command
            if payload.command == "clear" || payload.command.starts_with("clear ") {
                // Emit urgent capture request
                let capture_request = RawEvent {
                    event_type: "terminal.capture.urgent".to_string(),
                    payload: json!({
                        "window_id": payload.window_id,
                        "reason": "clear_command",
                        "command_event_id": event.id,
                    }),
                    // ... other fields
                };
                self.event_tx.send(capture_request).await?;
            }
        }
        Ok(())
    }
}
```

## Recommended Implementation

### Phase 1: Improve Continuous Capture
1. Reduce capture interval to 60 seconds
2. Implement incremental capture to reduce data size
3. Add configuration option for capture frequency

### Phase 2: Shell Integration
1. Provide shell configuration snippets
2. Document in setup instructions
3. Make it optional but recommended

### Phase 3: Clear Command Handling
1. Detect clear commands in shell worker
2. Emit urgent capture events
3. Scrollback monitor processes urgent captures immediately

### Phase 4: Hybrid Approach
1. Continuous capture every 60s (background safety net)
2. Shell exit hooks (reliable when configured)
3. Clear command detection (prevent data loss)
4. Command-triggered capture for long operations

## Configuration

```toml
[event.terminal_scrollback]
# More frequent captures as safety net
capture_interval_secs = 60

# Capture after commands that run > N seconds
capture_on_long_commands = true
command_duration_threshold_secs = 5

# Enable incremental capture
incremental_capture = true

# Listen for urgent capture requests
process_urgent_requests = true
urgent_request_timeout_ms = 500

# Store in database only
save_to_files = false
```

## The Hard Truth

**We cannot reliably capture scrollback when a window closes through window manager events alone.** The window manager tells us about the close too late. The only reliable approaches are:

1. Capture frequently (accept some data loss)
2. Use shell hooks (requires user configuration)
3. Capture on specific triggers (clear, long commands)

The best solution is a combination of all three.