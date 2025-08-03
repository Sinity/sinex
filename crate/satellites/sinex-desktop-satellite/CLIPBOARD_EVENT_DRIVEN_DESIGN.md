# Event-Driven Clipboard Monitoring Design

This document describes the event-driven approach for clipboard monitoring that would be more efficient than the current polling implementation.

## Efficiency Comparison

- **Event-driven**: CPU <0.1%, ~95% less power consumption
- **Polling**: Higher CPU usage, continuous wake-ups affect battery life

## Wayland Implementation

### Protocol
Uses `wlr-data-control-unstable-v1` protocol for compositor data transfer interaction.

### Implementation with wl-clipboard
```bash
# Monitor regular clipboard
wl-paste --watch /opt/sinex/bin/sinex_clipboard_handler_wayland

# Monitor primary selection (middle-click paste)
wl-paste --primary --watch /opt/sinex/bin/sinex_clipboard_handler_wayland
```

### Handler Script Logic
1. Invoked by `wl-paste --watch` on clipboard changes
2. Query available MIME types: `wl-paste --list-types`
3. Prioritize MIME types:
   - `text/plain;charset=utf-8` (preferred)
   - `text/html`
   - `image/png`
4. Retrieve content: `wl-paste --type <chosen_mime_type>`
5. Construct event payload with all available metadata

### Wayland Limitations
- Source application detection is restricted by security model
- Some compositors may limit available metadata

## X11 Implementation

### XFIXES Extension
Uses X Fixes Extension for selection change notifications.

### Implementation Steps
1. Connect to X server
2. Query XFIXES version: `XFixesQueryVersion()`
3. Register for selection notifications:
   ```c
   XFixesSelectSelectionInput(display, window, selection_atom, 
                              XFixesSetSelectionOwnerNotifyMask)
   ```
   - `XA_CLIPBOARD`: Ctrl+C/V clipboard
   - `XA_PRIMARY`: Middle-mouse selection

4. Handle `XFixesSelectionNotifyEvent` in event loop
5. Request selection content:
   ```c
   XConvertSelection(display, selection_atom, target_atom, 
                     property_atom, requestor_window, time)
   ```

### INCR Protocol for Large Payloads

For data too large for single transfer:
1. `SelectionNotify` property type is `INCR`
2. Delete property to signal readiness: `XDeleteProperty()`
3. Receive data in multiple `PropertyNotify` events
4. Zero-length chunk signals completion

### X11 Advantages
- Can often determine source application from selection owner window
- Rich metadata available through window properties

## Non-Blocking Event Processing Example

```rust
use tokio::process::Command;
use tokio::io::{BufReader, AsyncBufReadExt};
use std::process::Stdio;

async fn monitor_wl_clipboard_with_handler(
    handler_path: &str, 
    selection_type: &str
) {
    let mut cmd_args = vec!["--watch", handler_path];
    if selection_type == "primary" {
        cmd_args.insert(0, "--primary");
    }

    let mut child = Command::new("wl-paste")
        .args(&cmd_args)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("Failed to start wl-paste --watch");

    let stdout = child.stdout.take()
        .expect("Failed to capture stdout");
    let stderr = child.stderr.take()
        .expect("Failed to capture stderr");

    let mut stdout_reader = BufReader::new(stdout).lines();
    let mut stderr_reader = BufReader::new(stderr).lines();

    loop {
        tokio::select! {
            Ok(Some(line)) = stdout_reader.next_line() => {
                println!("[wl-paste-handler-{}] STDOUT: {}", 
                         selection_type, line);
            }
            Ok(Some(line)) = stderr_reader.next_line() => {
                eprintln!("[wl-paste-handler-{}] STDERR: {}", 
                          selection_type, line);
            }
            status = child.wait() => {
                match status {
                    Ok(exit_status) => {
                        eprintln!("[wl-paste-handler-{}] exited: {}", 
                                  selection_type, exit_status);
                    }
                    Err(e) => {
                        eprintln!("[wl-paste-handler-{}] error: {}", 
                                  selection_type, e);
                    }
                }
                break; // TODO: Implement restart logic
            }
        }
    }
}

// Usage in main:
// tokio::spawn(monitor_wl_clipboard_with_handler(
//     "/opt/sinex/bin/clipboard_handler", "clipboard"));
// tokio::spawn(monitor_wl_clipboard_with_handler(
//     "/opt/sinex/bin/clipboard_handler", "primary"));
```

## Benefits of Event-Driven Approach

1. **Power Efficiency**: No continuous polling, CPU sleeps between events
2. **Low Latency**: Immediate notification of clipboard changes
3. **Resource Usage**: Minimal memory and CPU footprint
4. **Battery Life**: Significant improvement on laptops/mobile devices

## Integration Considerations

To integrate with current architecture:
1. Replace polling loop with event handlers
2. Keep existing content analysis and blob storage
3. Add platform detection to choose appropriate backend
4. Maintain backward compatibility with polling fallback