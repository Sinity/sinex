# TIM-KittyTerminalIntegration: Kitty Terminal Specific Integration

*   **Relevant ADR:** `[ADR-008-TerminalActivityCaptureStrategy.md](docs/adr/ADR-008-TerminalActivityCaptureStrategy.md)` (Kitty RC is part of layered strategy)
*   **Original UG Context:** Section 8.1

This TIM details the technical implementation for integrating with the Kitty terminal emulator, leveraging its Remote Control protocol and OSC escape sequences for capturing semantic terminal activity.

## 1. Rationale Summary

Kitty's advanced features provide richer semantic data than generic terminal logging alone, such as OS window/tab/pane management, CWD of active panes, and scrollback access. This complements Atuin (command history) and Asciinema (full session replay) as per ADR-008.

## 2. OSC 52 and OSC 5522 Clipboard Protocols [UG Sec 8.1.1, CR4]

While the primary clipboard monitoring is handled by `TIM-ClipboardMonitoring.md`, Kitty's OSC clipboard protocols are relevant if interacting with Kitty's internal clipboard or if Kitty is configured to emit OSC sequences on system clipboard changes.

*   **OSC 52 (Standard Clipboard):**
    *   Sequence: `\x1b]52;<clipboard_spec>;<base64_data>\x07` (or `\x1b\\` ST).
    *   `<clipboard_spec>`: `c` (system clipboard), `p` (primary), `s<name>` (named).
    *   Request content: `\x1b]52;c;?\x07`. Kitty might respond with content.
    *   Payload Limit: ~74,994 bytes raw data before Base64.
*   **Kitty OSC 5522 (Enhanced Clipboard & Data Transfer):**
    *   Purpose: Overcomes OSC 52 limits, multiple formats, chunking for large data.
    *   Chunking: Data transferred to/from Kitty via OSC 5522 should use **4KB chunks**, each as a separate OSC 5522 sequence with `m=1` (more data) or `m=0` (final chunk) flags.
*   **Exocortex Use:** Primarily for awareness if the Exocortex Kitty ingestor needs to parse these if observed (e.g., from scrollback or PTY stream if Kitty is configured to emit them). Direct clipboard manipulation is usually via Kitty RC.

## 3. Kitty Remote Control (RC) Protocol [UG Sec 8.1.2, CR4, SA4]

This is the primary mechanism for the Exocortex Kitty ingestor.

### 3.1. Communication Methods

1.  **UNIX Domain Socket (Preferred for Exocortex Ingestor):**
    *   Launch Kitty: `kitty --listen-on unix:/tmp/my_kitty_socket_$$` (or a fixed, permission-controlled path like `/run/user/$UID/sinex_kitty_rc.sock`).
    *   Ingestor connects to this socket.
    *   Protocol: Send `\x1bP@kitty-cmd<JSON_payload_as_string>\x1b\` messages over the socket.
        *   `<JSON_payload_as_string>`: The JSON command object serialized to a string.
    *   Responses are JSON strings, also newline-terminated or within a similar DCS/ST envelope if Kitty uses that over socket.
2.  **Escape Sequence Method (via PTY - Fallback/Less Robust):**
    *   Send `\x1bP@kitty-cmd<JSON_payload_as_string>\x1b\` to Kitty's PTY.
    *   Less reliable for complex JSON or bidirectional communication.

### 3.2. RC Protocol Performance [CR4]

*   Simple commands (list windows, get active ID): ~1-2ms latency.
*   Large data (get scrollback): ~100ms per MB.
*   Socket method is ~2x faster and more reliable than PTY escapes.

### 3.3. RC Command Examples (JSON Payloads)

*   List all Kitty OS windows, tabs, and windows (panes):
    `{"cmd": "ls"}`
*   Get text from focused Kitty window (pane):
    `{"cmd": "get-text", "match": "focused:true", "formatted": "false"}` (for raw text)
    `{"cmd": "get-text", "match": "id:N", "extent": "scrollback"}` (get full scrollback for window ID N)
*   Get window state (title, PID, CWD, foreground process) for focused window:
    `{"cmd": "get-window-state", "match": "focused:true"}`
*   Set window title:
    `{"cmd": "set-window-title", "match": "focused:true", "title": "New Title"}`
*   Launch a new command in a new Kitty window/tab/OS window:
    `{"cmd": "launch", "type": "os-window|tab|window", "cwd": "/path/to/start", "args": ["htop"]}`
*   Send text to a window (as if typed):
    `{"cmd": "send-text", "match": "focused:true", "text": "echo 'hello'\\n"}`
*   Get/Set Kitty internal clipboard:
    `{"cmd": "get-clipboard", "kitty": "true"}`
    `{"cmd": "set-clipboard", "kitty": "true", "text": "internal clip text"}`

### 3.4. Rust Client for Kitty RC (Socket Mode - Conceptual)

```rust
use tokio::net::UnixStream;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader, AsyncBufReadExt};
use serde_json::{json, Value as JsonValue};
use std::path::Path;

// async fn send_kitty_rc_command(socket_path: &Path, command_payload: JsonValue) -> Result<JsonValue, anyhow::Error> {
//     let mut stream = UnixStream::connect(socket_path).await?;

//     // Kitty's socket protocol uses a specific framing for request and response:
//     // Request: <ESC>_Ga=1,q=1,s=1,a=1,p=1<ESC>\ (optional handshake/graphics query, may not be needed for simple cmd)
//     //          <ESC>P@kitty-cmd<JSON_STRING><ESC>\
//     // Response: <ESC>_Gok,aid=N,s=1,a=1,p=1,q=1,i=N,f=100,c=N,r=N,m=0<ESC>\ (graphics protocol ACK - might not appear for all cmd types)
//     //           <ESC>P1$r<JSON_RESPONSE_STRING><ESC>\ (this is the actual command response)
//     // Or sometimes just the JSON response string directly for simpler commands.
//     // The exact framing needs to be verified with current Kitty versions for socket mode.
//     // For simplicity, this example assumes direct JSON request/response after initial connection,
//     // or that the DCS/ST framing is handled by a wrapper.

//     let cmd_str = command_payload.to_string();
//     // Actual Kitty RC over socket often wraps the JSON:
//     let framed_cmd = format!("\x1bP@kitty-cmd{}\x1b\\", cmd_str);

//     stream.write_all(framed_cmd.as_bytes()).await?;
//     stream.flush().await?; // Ensure it's sent

//     // Reading response is tricky due to potential multiple parts and DCS/ST framing.
//     // A robust parser would look for <ESC>P1$r ... <ESC>\ or handle raw JSON if that's what Kitty sends.
//     let mut reader = BufReader::new(stream);
//     let mut response_buffer = Vec::new();
    
//     // This is a simplified read, assuming response is newline-terminated JSON or within a single buffer read.
//     // A real implementation needs a more robust framed message parser.
//     // reader.read_to_end(&mut response_buffer).await?; // Reads until EOF, might be too much if stream is kept open.

//     // Let's assume for simpler commands, Kitty might send a newline-terminated JSON string or close after response.
//     // Or, we need to parse the specific Kitty framing.
//     // For example, if response is <ESC>P1$rJSON_DATA<ESC>\, parse that.
//     // This is a placeholder for robust response parsing.
//     let mut raw_response_str = String::new();
//     reader.read_to_string(&mut raw_response_str).await?; // This might hang if Kitty doesn't close or newline-terminate appropriately.

//     // Attempt to extract JSON from the raw response (stripping potential framing)
//     let json_response_str = if let Some(start_json) = raw_response_str.find('{') {
//         if let Some(end_json) = raw_response_str.rfind('}') {
//             raw_response_str[start_json..=end_json].to_string()
//         } else {
//             raw_response_str // Assume it's just JSON if no clear start/end
//         }
//     } else {
//         raw_response_str
//     };
    
//     let response_json: JsonValue = serde_json::from_str(&json_response_str)?;
//     Ok(response_json)
// }

// Example Usage:
// async fn get_kitty_ls(socket_path: &Path) -> Result<JsonValue, anyhow::Error> {
//     let cmd = json!({"cmd": "ls"});
//     send_kitty_rc_command(socket_path, cmd).await
// }
```
**Note:** The exact framing for Kitty's socket remote control needs careful verification against Kitty's documentation or source, as it might involve specific DCS/ST sequences for requests and responses, similar to its graphics protocol or PTY control sequences. The example above simplifies this for brevity. A robust client needs a proper state machine to parse these frames.

## 4. Security Considerations [UG Sec 8.1.3, CR4]

*   **Risk:** Escape sequence injection if `allow_remote_control yes` and not socket-only.
*   **Mitigations (Mandatory for Exocortex):**
    1.  **Socket-Only Mode:** Launch Kitty with `--listen-on unix:/path/to/secure_socket` and set `allow_remote_control no` in `kitty.conf`. The Exocortex ingestor *only* connects to this socket.
    2.  **Socket Permissions:** Ensure the UNIX domain socket file has restrictive permissions (e.g., `0600` owned by user, or accessible only by `sinex` group if ingestor runs as different user).
    3.  **Password Protection (Optional):** Set `remote_control_password YOUR_PASSWORD` in `kitty.conf`. Client sends auth command first.
    4.  **Input Sanitization (If Applicable):** Not generally needed for sending well-defined JSON commands to Kitty RC. If sending text that Kitty might interpret in a shell context (e.g., via `send-text` that then triggers a shell command in Kitty), sanitize that text.

## 5. Race Condition Handling [UG Sec 8.1.4, CR4]

*   **Issue:** Rapid-fire RC commands might be dropped or misordered by Kitty.
*   **Mitigation:** Client-side serialization for commands. Implement a queue in the ingestor with a small delay (e.g., 1-5ms) between sending consecutive commands if issues are observed. For critical sequences, wait for response from one command before sending the next.

## 6. Kitty Ingestor (`ingestor/kitty`) Implementation Details [UG Sec 8.1.5]

The Rust `ingestor/kitty` agent uses the RC protocol (socket mode) to capture:

*   **Window/Tab/OS-Window State:**
    *   Periodically (e.g., every 1-5 seconds, or on `focus_changed` events from Hyprland if Kitty is active) poll `kitty @ ls` (JSON format).
    *   Diff against previous state to generate `raw.events` for:
        *   `app.terminal.kitty.os_window_created/closed/focused`
        *   `app.terminal.kitty.tab_created/closed/focused/title_changed`
        *   `app.terminal.kitty.window_created/closed/focused/layout_changed` (panes)
    *   For focused Kitty window (pane), poll `kitty @ get-window-state --match focused:true` to get:
        *   `app.terminal.kitty.cwd_changed`
        *   `app.terminal.kitty.foreground_process_changed` (PID, name, args)
*   **Scrollback Buffer Changes:**
    *   Periodically (e.g., every 5-15 seconds, or on prompt appearance if detectable from foreground process changes or heuristics):
        *   `kitty @ get-text --match focused:true --extent scrollback` (or for specific window IDs).
        *   Compute BLAKE3 hash of the retrieved scrollback text.
        *   If hash differs from previously stored hash for this Kitty window:
            1.  Store the full scrollback text as a new blob in `git-annex` (via `core_blobs`).
            2.  Emit `app.terminal.kitty.scrollback_captured` event to `raw.events`. Payload: `{ "kitty_window_id": N, "scrollback_annex_key": "...", "scrollback_blake3_hash": "...", "line_count": M }`.
            3.  Update stored hash for this window.
    *   On Kitty window closure (detected from `kitty @ ls` diff or `closewindow` event from Hyprland if Kitty was active): Capture final scrollback.
*   **Internal Clipboard (If distinct from system and relevant):**
    *   Periodically poll `kitty @ get-clipboard --kitty`. Diff content.
    *   Emit `app.terminal.kitty.internal_clipboard_changed` event.
*   **Command Execution (Indirect Inference):**
    *   The Kitty RC protocol does *not* directly emit "shell command executed" events with exit status. This is primarily captured by **Atuin** (see `TIM-GenericTerminalLogging.md`).
    *   The Kitty ingestor can infer *potential* command boundaries by:
        *   Monitoring `app.terminal.kitty.foreground_process_changed` for new shell instances or common command PIDs.
        *   Analyzing scrollback for shell prompt patterns and subsequent output.
        *   This is heuristic and less reliable than Atuin. It primarily provides context around commands Atuin logs.
*   **Event Payloads:** All generated `raw.events` should include relevant Kitty identifiers (OS window ID, tab ID, window/pane ID) and timestamps.

