# TIM-ClipboardMonitoring: Event-Driven Clipboard Capture

## Status Dashboard
**Maturity Level**: L4 - Implemented
**Implementation**: 85% (Wayland and X11 core functionality working)
**Dependencies**: wl-clipboard package, XFIXES extension, StatefulStreamProcessor trait
**Blocks**: Desktop context analysis, AI-powered clipboard content analysis

## MVP Specification
- Event-driven clipboard monitoring on Wayland (wl-paste --watch)
- Basic X11 clipboard monitoring via XFIXES
- Raw event logging to core.events table
- MIME type detection and content capture
- Primary selection and clipboard distinction

## Enhanced Features
- Advanced INCR protocol handling for large payloads
- Source application detection (Wayland limitations)
- Intelligent content type prioritization
- Cross-platform abstraction layer
- Rich context extraction from clipboard metadata

## Implementation Checklist
- [x] Database schema (core.events)
- [x] Wayland implementation (wl-paste integration)
- [x] X11 implementation (XFIXES)
- [x] StatefulStreamProcessor trait implementation
- [x] MIME type handling
- [x] Basic testing
- [ ] INCR protocol completion
- [ ] Source app detection
- [ ] Enhanced error handling

*   **Relevant ADR:** (N/A directly, core ingestor)
*   **Original UG Context:** Section 7

This TIM details the technical implementation for event-driven clipboard monitoring on Wayland and X11.

## 1. Rationale Summary [UG Sec 7.1, CR4]

Event-driven clipboard monitoring is vastly more efficient (CPU <0.1%, ~95% less power) than polling. It's essential for capturing copied/pasted text and other data types without excessive resource use.

## 2. Wayland Implementation [UG Sec 7.2, CR4]

*   **Protocol:** `wlr-data-control-unstable-v1` allows interaction with compositor's data transfer.
*   **Tooling (`wl-clipboard` package):**
    *   `wl-paste --watch /opt/sinex/bin/sinex_clipboard_handler_wayland`
        *   `wl-paste --watch` executes the handler script whenever clipboard content (regular selection) changes.
        *   A separate `wl-paste --primary --watch ...` is needed for primary selection.
*   **`sinex_clipboard_handler_wayland` Script/Binary (e.g., Rust or Python):**
    1.  Is invoked by `wl-paste --watch`.
    2.  Calls `wl-paste --list-types` to get available MIME types on the clipboard.
    3.  Chooses preferred MIME type(s) (e.g., `text/plain;charset=utf-8` first, then `text/html`, then `image/png`).
    4.  Calls `wl-paste --type <chosen_mime_type>` to get the actual content for each desired type.
    5.  Constructs a `core.events` payload:
        *   `source`: `"desktop.wayland.clipboard_monitor"`
        *   `event_type`: `"clipboard_content_changed"` (or `primary_selection_changed`)
        *   `payload`: `{ "selection_type": "clipboard" | "primary", "available_mime_types": ["type1", "type2"], "retrieved_content": {"mime_type1": "data_base64_if_binary_or_text", "mime_type2": "..."}, "source_application_hint": "..." (if obtainable, Wayland makes this hard generally) }`
    6.  Sends event to `core.events` (e.g., via `exo log` CLI or direct DB insert).
*   **Payloads & MIME Types [CR4]:** Wayland is MIME type-based. No inherent protocol size limits for data, but apps/compositor might have practical limits or use streaming.

## 3. X11 Implementation [UG Sec 7.3, CR4]

*   **Extension:** XFIXES (X Fixes Extension).
*   **Mechanism (e.g., Python with `python-xlib`):**
    1.  Connect to X server.
    2.  `XFixesQueryVersion()`.
    3.  `XFixesSelectSelectionInput(display, window, selection_atom, XFixesSetSelectionOwnerNotifyMask)`:
        *   `selection_atom`: `XA_CLIPBOARD` (for Ctrl+C/V) or `XA_PRIMARY` (for middle-mouse).
        *   `window`: An agent-owned window to receive notifications.
    4.  In X event loop, listen for `XFixesSelectionNotifyEvent`. This indicates selection owner changed.
    5.  On event, request selection content:
        *   `XConvertSelection(display, selection_atom, target_atom, property_atom, requestor_window, time)`:
            *   `target_atom`: `UTF8_STRING` (for text), `TARGETS` (to get list of available formats), `text/uri-list` (for files), `image/png`, etc.
        *   Handle subsequent `SelectionNotify` event containing the data (or `INCR` marker).
*   **INCR Protocol for Large Payloads [CR4]:**
    *   If `SelectionNotify` property type is `INCR`, data is too large for one chunk.
    *   Client must then delete the property on its window (`XDeleteProperty`) to signal readiness for the next chunk.
    *   Selection owner sends data in multiple `PropertyNotify` events until a zero-length chunk signals end.
    *   The Exocortex X11 clipboard ingestor must correctly implement the INCR protocol client side.
*   **Payload Construction:** Similar to Wayland, log `available_mime_types` (from `TARGETS`), `retrieved_content` for key types. Source application often inferable from selection owner window properties.

## 4. Cross-Platform Abstraction and Display Server Detection [UG Sec 7.4, CR4]

*   The Exocortex clipboard ingestor (e.g., a single Rust binary) should:
    1.  On startup, detect active display server:
        *   Check `WAYLAND_DISPLAY` env var.
        *   Else, check `DISPLAY` env var for X11.
    2.  Initialize and run the appropriate backend (Wayland or X11) logic.

## 5. Non-Blocking Event Processing [UG Sec 7.5, CR4]

*   The ingestor's main event loop (whether Wayland or X11) must use non-blocking I/O multiplexing (`epoll` on Linux, `mio` in Rust, or async runtime like Tokio).
*   This allows waiting for clipboard change notifications and potentially other IPC/timer events without blocking.
*   **Rust (Tokio) example for Wayland `wl-paste --watch` subprocess monitoring:**
    ```rust
    // use tokio::process::Command;
    // use tokio::io::{BufReader, AsyncBufReadExt};
    // use std::process::Stdio;

    // async fn monitor_wl_clipboard_with_handler(handler_path: &str, selection_type: &str) {
    //     let mut cmd_args = vec!["--watch", handler_path];
    //     if selection_type == "primary" {
    //         cmd_args.insert(0, "--primary");
    //     }

    //     let mut child = Command::new("wl-paste")
    //         .args(&cmd_args)
    //         .stdout(Stdio::piped()) // Capture stdout of handler if it prints status/errors
    //         .stderr(Stdio::piped()) // Capture stderr
    //         .spawn()
    //         .expect("Failed to start wl-paste --watch");

    //     let stdout = child.stdout.take().expect("Failed to capture stdout of wl-paste handler");
    //     let stderr = child.stderr.take().expect("Failed to capture stderr of wl-paste handler");

    //     let mut stdout_reader = BufReader::new(stdout).lines();
    //     let mut stderr_reader = BufReader::new(stderr).lines();

    //     // Concurrently read stdout and stderr from the handler script
    //     // The handler script itself does the actual clipboard processing and Exocortex logging.
    //     // This loop just monitors the handler's lifecycle and its diagnostic output.
    //     loop {
    //         tokio::select! {
    //             Ok(Some(line)) = stdout_reader.next_line() => {
    //                 println!("[wl-paste-handler-{}] STDOUT: {}", selection_type, line);
    //             }
    //             Ok(Some(line)) = stderr_reader.next_line() => {
    //                 eprintln!("[wl-paste-handler-{}] STDERR: {}", selection_type, line);
    //             }
    //             status = child.wait() => { // wl-paste --watch process exited
    //                 match status {
    //                     Ok(exit_status) => eprintln!("[wl-paste-handler-{}] exited with status: {}", selection_type, exit_status),
    //                     Err(e) => eprintln!("[wl-paste-handler-{}] failed to wait for exit: {}", selection_type, e),
    //                 }
    //                 break; // TODO: Implement restart logic for wl-paste --watch
    //             }
    //         }
    //     }
    // }
    // // In main:
    // // tokio::spawn(monitor_wl_clipboard_with_handler("/opt/sinex/bin/clipboard_handler", "clipboard"));
    // // tokio::spawn(monitor_wl_clipboard_with_handler("/opt/sinex/bin/clipboard_handler", "primary"));
    ```

