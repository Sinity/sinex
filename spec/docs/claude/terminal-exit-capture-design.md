# Terminal Exit Capture Design

## Problem
When a terminal window closes, its scrollback buffer is lost forever. We need to capture it before the window closes.

## Solution: Cross-Source Event Correlation

### Current Architecture
We already have:
1. **Window Manager Events**: `window.closed` with `window_class` field
2. **Scrollback Capture**: Can capture on-demand from running terminals
3. **Event Source Independence**: Each source operates independently

### Proposed Implementation

#### Option 1: Event-Driven Worker (Recommended)
Create a new worker that:
1. Subscribes to `window.closed` events
2. Checks if `window_class == "kitty"` (or other terminals)
3. Immediately attempts scrollback capture via Kitty API
4. Stores as `terminal.scrollback.captured` with `trigger: "window_closing"`

```rust
// New worker: terminal-exit-capture-worker
impl Worker for TerminalExitCaptureWorker {
    async fn process_event(&mut self, event: &RawEvent) -> Result<()> {
        if event.event_type == "window.closed" {
            let payload: WindowClosedPayload = serde_json::from_value(event.payload)?;
            
            // Check if it's a terminal (would need window_class in payload)
            if is_terminal_window(&payload) {
                // Extract window_id from address
                let window_id = extract_kitty_window_id(&payload.window_address)?;
                
                // Attempt immediate capture
                if let Ok(scrollback) = capture_kitty_scrollback(window_id).await {
                    // Emit new event
                    self.emit_event(create_scrollback_event(scrollback, "window_closing"));
                }
            }
        }
        Ok(())
    }
}
```

#### Option 2: Shell Exit Hooks
Add to shell configuration:
```bash
# .zshrc / .bashrc
sinex_capture_scrollback() {
    if [ -n "$KITTY_WINDOW_ID" ]; then
        # Call sinex API or use kitty directly
        kitty @ get-text --match id:$KITTY_WINDOW_ID | \
            curl -s -X POST http://localhost:9001/api/events \
                -H "Content-Type: application/json" \
                -d @- > /dev/null 2>&1
    fi
}
trap sinex_capture_scrollback EXIT
```

#### Option 3: Enhanced Scrollback Monitor
Modify the existing scrollback source to:
1. Subscribe to window manager events internally
2. Maintain a priority queue of windows to capture
3. Prioritize windows that are closing

### Implementation Challenges

1. **Race Condition**: Window might close before capture completes
   - Solution: Very fast capture, accept some losses
   
2. **Window ID Mapping**: Window manager IDs != Kitty window IDs
   - Solution: Maintain mapping table or use window title/PID matching

3. **Performance**: Many windows closing at once (e.g., system shutdown)
   - Solution: Rate limiting, best-effort capture

### Clear Command Handling

Similar approach for `clear` command:
1. Monitor shell commands (already done via Atuin)
2. When `clear` detected → immediate scrollback capture
3. Tag with `pre_clear: true` metadata

```rust
// In shell command worker
if command.text == "clear" {
    // Emit high-priority capture request
    emit_event(Event {
        event_type: "terminal.capture_requested",
        payload: json!({
            "window_id": command.window_id,
            "reason": "clear_command",
            "priority": "high"
        })
    });
}
```

## Recommended Approach

1. **Phase 1**: Implement Option 1 (Event-Driven Worker)
   - Cleanest separation of concerns
   - Works with existing architecture
   - No shell configuration needed

2. **Phase 2**: Add clear command detection
   - Enhance existing shell command monitoring

3. **Phase 3**: Consider shell hooks as fallback
   - For terminals that close without window manager events

## Configuration

```toml
# New worker configuration
[worker.terminal_exit_capture]
enabled = true
capture_on_close = true
capture_on_clear = true
terminal_classes = ["kitty", "alacritty", "gnome-terminal"]
max_capture_time_ms = 500  # Timeout for capture attempts
```