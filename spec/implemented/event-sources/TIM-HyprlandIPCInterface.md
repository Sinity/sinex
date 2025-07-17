# TIM-HyprlandIPCInterface: Hyprland Compositor IPC Socket Integration

## Status Dashboard
**Maturity Level**: L4 - Implemented
**Implementation**: 90% (Core IPC integration working, state snapshots implemented)
**Dependencies**: Hyprland compositor, unix sockets, hyprctl binary, StatefulStreamProcessor trait
**Blocks**: Desktop context analysis, window-based activity correlation, workspace insights

## MVP Specification
- Socket2 event stream monitoring
- Real-time window focus events
- Workspace change detection
- Basic window lifecycle tracking
- hyprctl state querying integration

## Enhanced Features
- Advanced window property augmentation
- Workspace layout analysis
- Monitor configuration tracking
- Performance-optimized event filtering
- Historical state reconstruction

## Implementation Checklist
- [x] Socket2 IPC connection
- [x] Event stream parsing
- [x] Window focus tracking
- [x] Workspace change events
- [x] hyprctl integration
- [x] State snapshot system
- [ ] Advanced window properties
- [ ] Performance optimization
- [ ] Historical state correlation

* **Relevant ADR:** `[ADR-003-HyprlandCompositorIntegrationPath.md](docs/adr/ADR-003-HyprlandCompositorIntegrationPath.md)` (Decision: IPC first)
* **Original UG Context:** Section 4.1

This TIM details the technical implementation for integrating with the Hyprland Wayland compositor using its Inter-Process Communication (IPC) socket interface. This is the primary method for capturing desktop context as per ADR-003.

## 1. Rationale Summary

Hyprland's IPC sockets provide a rich, text-based stream of compositor events and a command interface for querying state, allowing for comprehensive desktop context capture without the complexities of a native plugin. See ADR-003 for the full rationale.

## 2. Socket Locations and Access [UG Sec 4.1.1]

* **Base Path:** `$XDG_RUNTIME_DIR/hypr/`
* **Instance Directory:** `$XDG_RUNTIME_DIR/hypr/$HYPRLAND_INSTANCE_SIGNATURE/`
  * The `HYPRLAND_INSTANCE_SIGNATURE` environment variable is crucial for clients to find the correct socket path. It is set by Hyprland.
* **Sockets:** UNIX domain sockets.
  * **Command/Query Socket (`socket1`):** `$XDG_RUNTIME_DIR/hypr/$HYPRLAND_INSTANCE_SIGNATURE/.socket.sock`
  * **Event Broadcast Socket (`socket2`):** `$XDG_RUNTIME_DIR/hypr/$HYPRLAND_INSTANCE_SIGNATURE/.socket2.sock`
* **Access [OR2]:** Sockets are owned by the user running Hyprland. Exocortex ingestors running as the same user can access them.
* **API Signature:**
    ```rust
    fn get_hyprland_socket_paths() -> Result<(PathBuf, PathBuf), String>
    ```
    Discovers socket paths using `XDG_RUNTIME_DIR` and `HYPRLAND_INSTANCE_SIGNATURE` environment variables.

## 3. IPC Protocol Details [UG Sec 4.1.2]

### 3.1. Command Socket Protocol (`socket1`) [CR2, OR2]

* **Pattern:** Request-response. Client connects, sends command, reads response, typically closes connection (or keeps persistent for batching if Hyprland IPC server supports).
* **Command Format:** `[<flags>/]<command_or_keyword> <arguments>\n`
  * **Flags:**
    * `j/`: Requests JSON output (e.g., `j/clients` for `hyprctl -j clients`).
  * **Command:** A Hyprland dispatcher command (e.g., `dispatch exec kitty`).
  * **Keyword:** A Hyprland keyword for querying state (e.g., `activewindow`, `monitors`, `workspaces`, `clients`).
* **Response:** Plain text for regular commands/keywords, JSON string for `j/` prefixed queries.
* **API Signature:**
    ```rust
    async fn query_hyprland_socket1(socket1_path: &PathBuf, command: &str) -> Result<String, std::io::Error>
    ```
    Sends command to socket1 and returns response. Supports JSON output with `j/` prefix.

### 3.2. Event Socket Protocol (`socket2`) [CR2, OR2, SR1]

* **Pattern:** Read-only streaming. Client connects and continuously reads newline-terminated messages.
* **Format:** `EVENTNAME>>DATA\n`
  * `EVENTNAME`: String identifying event type (e.g., `activewindow`, `openwindow`).
  * `DATA`: Comma-separated values specific to the event type.
  * Example: `activewindowv2>>0x123abc`
* **Encoding:** UTF-8.
* **API Signature:**
    ```rust
    async fn listen_hyprland_socket2(socket2_path: &PathBuf) -> Result<(), std::io::Error>
    ```
    Continuously reads newline-terminated events from socket2 in format `EVENTNAME>>DATA\n`.

## 4. Reliability Considerations [UG Sec 4.1.3]

* **`EAGAIN`/`EWOULDBLOCK` [SA1]:** When reading `socket2` in non-blocking mode (typical with async libraries like Tokio), these errors indicate no data is immediately available. Tokio's `AsyncReadExt` handles this internally by waiting for readability.
* **Missed Events [SR1]:** `socket2` event delivery is best-effort. Events can be missed, especially under high system load or for certain event types (e.g., fullscreen changes reported historically). The ingestor cannot rely on guaranteed delivery from `socket2` alone.
  * **Mitigation:** Periodic full state snapshots (e.g., `hyprctl -j clients,workspaces,monitors`) ingested as `hyprland.state_snapshot` events can help reconcile missed transient events over time. A reconciliation agent can compare snapshots to find discrepancies.
* **Event Ordering [SA1]:** Generally events are in order, but clients should be somewhat robust to minor reordering if it occurs under extreme conditions, although Hyprland aims for correct ordering (e.g., XDG shell map event ordering fixed in commit `02772fe8`).

## 5. Key Event Types and Payload Structures (from `socket2`) [UG Sec 4.1.4, CR2]

The Hyprland ingestor must parse these events and their comma-separated data payloads. These events are processed through the StatefulStreamProcessor interface and stored in `core.events`.

* `workspace>>WORKSPACENAME`
* `focusedmon>>MONITORNAME,WORKSPACENAME`
* `activewindow>>WINDOWCLASS,WINDOWTITLE` (Legacy, less specific)
* `activewindowv2>>WINDOWADDRESS` (Hex string, e.g., "0x123abc". Preferred for unique window ID)
* `fullscreen>>STATE` (STATE: `1` for enter, `0` for exit)
* `monitoradded>>MONITORNAME`
* `monitorremoved>>MONITORNAME`
* `createworkspace>>WORKSPACENAME`
* `destroyworkspace>>WORKSPACENAME`
* `openwindow>>WINDOWADDRESS,WORKSPACENAME,WINDOWCLASS,WINDOWTITLE`
* `closewindow>>WINDOWADDRESS`
* `movewindow>>WINDOWADDRESS,WORKSPACENAME` (Legacy)
* `movewindowv2>>WINDOWADDRESS,WORKSPACEID_STRING,WORKSPACENAME`
* `openlayer>>LAYER_NAMESPACE` (e.g., for panels, notifications)
* `closelayer>>LAYER_NAMESPACE`
* `submap>>SUBMAPNAME` (e.g., "resize" keybinding submap)
* `changefloatingmode>>WINDOWADDRESS,FLOATING_STATE(0_OR_1)`
* `urgent>>WINDOWADDRESS`
* `minimize>>WINDOWADDRESS,MINIMIZED_STATE(0_OR_1)` (Hyprland v0.33.0+)
* `screencast>>OWNER_STATE(0_OR_1),SCREENCAST_STATE(0_OR_1)` (Owner 0=no, 1=yes; State 0=inactive, 1=active)
* `windowtitle>>WINDOWADDRESS` (Signals title changed. Ingestor should then query `hyprctl -j clients` for full details of this `WINDOWADDRESS` to get the new title.)

**Ingestor Strategy for Rich Payloads:**
When an event from `socket2` provides only a `WINDOWADDRESS` (e.g., `activewindowv2`, `windowtitle`), the ingestor should make a subsequent asynchronous call to `hyprctl -j clients` (via `socket1`), find the entry for that `WINDOWADDRESS`, and extract full details (class, title, PID, geometry, workspace, monitor, floating/fullscreen state, etc.) to populate the `raw.events.payload`.
A local cache of window states (keyed by `WINDOWADDRESS`) can be maintained by the ingestor, updated by `openwindow`, `closewindow`, `movewindowv2`, etc., and from `hyprctl clients` responses. This can reduce redundant `hyprctl` calls, but the cache must be carefully managed for consistency.

## 6. Minimum Hyprland Version Requirements [UG Sec 4.1.5, CR2]

* **Hyprland v0.33.1+** is recommended as a minimum for some documented features or stable event formats (e.g., `minimize` event).
* The ingestor should ideally log the Hyprland version it's connected to (e.g., by parsing output of `hyprctl version` on startup) for debugging.
