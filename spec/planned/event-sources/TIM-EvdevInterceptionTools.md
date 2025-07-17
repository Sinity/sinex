# TIM-EvdevInterceptionTools: Low-Level Input Capture (`evdev`)

*   **Relevant ADR:** (N/A directly, but implements part of ADR-008 strategy for terminal/input capture, with security caveats)
*   **Original UG Context:** Section 6
*   **Security Warning:** Direct `evdev` capture is effectively a keylogger/mouselogger. Implement with extreme caution, user opt-in, clear notifications, and strict privilege separation as detailed in UG Sec 22.3 and the original UG Section 6.3. This TIM focuses on the *technical mechanism*, assuming security mitigations are applied.

This TIM details using the Linux `evdev` interface and the Interception Tools suite for low-level input capture. This is a *secondary/redundant* input capture method, complementing compositor-level input capture (e.g., via Hyprland IPC, see `TIM-HyprlandIPCInterface.md`).

## 1. Rationale Summary

`evdev` provides raw hardware input events (keyboard scancodes, mouse movements/buttons) bypassing higher-level abstractions. This can be useful for:
*   Ground-truth input recording for debugging or specific analyses.
*   Input capture if the compositor or X/Wayland server is unavailable or not providing events.
*   Advanced input remapping or filtering (though Exocortex primarily uses it for observation).
The Interception Tools suite (`intercept`, `uinput`, `udevmon`) provides user-space utilities for working with `evdev`.

## 2. `evdev` Interface Overview [UG Sec 6.1]

*   **Mechanism [OR2]:** Linux kernel exposes input devices as `/dev/input/event*`. Stable symlinks at `/dev/input/by-id/` or `/dev/input/by-path/` should be used.
*   **Event Structure (`struct input_event` from `<linux/input.h>`):**
    ```c
    // struct input_event {
    //     struct timeval time; // Timestamp (seconds, microseconds)
    //     __u16 type;          // EV_KEY, EV_REL, EV_ABS, EV_MSC, etc.
    //     __u16 code;          // KEY_A, BTN_LEFT, REL_X, MSC_SCAN, etc.
    //     __s32 value;         // 0=release, 1=press, 2=repeat; relative motion; abs coordinate
    // };
    ```
*   **Libraries:** `libevdev` (C), Python `evdev` simplify parsing.

## 3. Interception Tools Suite [UG Sec 6.2]

*   **`intercept` [OR2]:** Reads events from an `evdev` device. Outputs to `stdout` (textual or binary).
    *   `intercept /dev/input/by-id/YOUR_KEYBOARD_ID-event-kbd`
    *   With `-g` (grab): `intercept -g ...` requests exclusive grab (events not passed to system). Used for remapping, not typically for passive Exocortex logging.
*   **`uinput` [OR2]:** Reads `intercept`-formatted events from `stdin`, creates a virtual input device via `/dev/uinput`, injects events. Used for remapping output.
*   **`udevmon` [OR2]:** Daemon monitoring `udev` for device hotplug. Starts/stops `intercept`/`uinput` pipelines based on YAML config.
    *   **`udevmon.yaml` for Exocortex Logging (Conceptual, from UG Sec 6.2.2):**
        A custom Exocortex tool `sinex_evdev_to_raw_events_ingestor` (Rust binary using `libevdev` or parsing `intercept` output) converts `evdev` events to JSON and sends to `core.events`.
        ```yaml
        # Example /etc/interception/udevmon.yaml or user-specific config
        # This job starts the custom ingestor when a keyboard is plugged in.
        # The ingestor itself handles connecting to PostgreSQL.
        # The 'intercept' part might be integrated into the custom ingestor itself
        # to avoid piping if the ingestor uses libevdev directly.

        # - JOB: "intercept /dev/input/DEVNODE | /opt/sinex/bin/sinex_evdev_to_raw_events_ingestor --device-type keyboard --device-id DEVNODE_ID_FROM_BY_ID"
        #   DEVICE:
        #     EVENTS: # Optional: Filter events processed by 'intercept'
        #       EV_KEY: [] # Process all key events
        #     MATCH_NAME: "*Keyboard*" # Match device by name pattern (adjust for specific keyboards)
        #     # Or use MATCH_VENDOR, MATCH_PRODUCT for more specificity

        # - JOB: "intercept /dev/input/DEVNODE | /opt/sinex/bin/sinex_evdev_to_raw_events_ingestor --device-type mouse --device-id DEVNODE_ID_FROM_BY_ID"
        #   DEVICE:
        #     EVENTS:
        #       EV_REL: [] # Relative motion
        #       EV_KEY: [] # Mouse buttons (BTN_LEFT etc. are EV_KEY type)
        //     MATCH_NAME: "*Mouse*"
        ```
        *Note: `DEVNODE` is a placeholder `udevmon` uses. The `sinex_evdev_to_raw_events_ingestor` would receive the actual device node path and its stable ID (e.g., from `/dev/input/by-id/`) as arguments from `udevmon`'s job expansion.*
        *The `ingestor/keyboard` via `journald_bridge` method (Vision Doc III.2.2.A) uses a simpler `interception-tools` plugin that just prints JSON to stdout, which `journald` captures.*

## 4. Permissions and Security [UG Sec 6.3, SR1, CR4]

**This is the most critical section for `evdev` capture.**

*   **Permissions Required:**
    *   Reading `/dev/input/event*`: `root` or user in `input` group. Modern systems with `logind` may use ACLs/`uaccess` tags for active session user access, but this is often restricted for *all* input devices simultaneously.
    *   Grabbing (EVIOCGRAB): Usually `root`.
    *   `/dev/uinput` access: `root` or specific group (`uinput`).
*   **Security Risks [SR1]:** System-wide keylogger. Compromise can lead to exfiltration of all typed data (passwords, PII). Significant privilege escalation risk.
*   **Mitigation Strategies (Implement these rigorously, see UG Sec 22.3 for more detail):**
    1.  **Privilege Separation (Mandatory):**
        *   Minimal `evdev` reader component (e.g., `interception-tools` plugin, or small C/Rust binary using `libevdev`). Runs with minimal privileges (e.g., only access to specific keyboard `evdev` node if possible).
        *   Heavily sandboxed (seccomp, AppArmor).
        *   Forwards raw data via secure local IPC (permissioned UNIX socket) to an unprivileged Exocortex agent (e.g., `EvdevEventProcessorAgent`).
        *   This processor agent parses, (attempts context filtering if enabled), structures for `core.events`, and inserts into DB. It has NO direct `evdev` access.
    2.  **User Opt-In & Clear Persistent Notification (Mandatory):** `evdev` capture disabled by default. Visible UI indicator when active.
    3.  **Prefer Higher-Level Input Capture (Default):** Use Hyprland IPC (TIM-HyprlandIPCInterface), AT-SPI2 (TIM-ATSPI2Integration), app-specific ingestors as primary sources. `evdev` is supplemental/fallback.
    4.  **Context-Aware Filtering (Best-Effort, Unreliable):** The `EvdevEventProcessorAgent` might *attempt* to suppress logging when password fields are active (correlating with AT-SPI2/Hyprland focus data). **Not a primary security control.**

## 5. Performance, Reliability, Device ID [UG Sec 6.4]

*   **Performance [OR2]:** `evdev` reading itself is low overhead. Processing/logging load is on Exocortex agent.
*   **Reliability [OR2]:** `udevmon` (if used) should run with high priority.
*   **Robust Device Identification [OR2]:** **Always use stable symlinks** from `/dev/input/by-id/` or `/dev/input/by-path/` in configurations. Do not use `/dev/input/eventX`.
*   **Mouse Event Volume [OR2]:** High frequency `EV_REL` (motion) events. Ingestor should filter (e.g., only buttons/scroll) or sample/throttle motion events if full path logging isn't needed or causes too much data.

## 6. Keyboard Ingestor Implementation via `journald_bridge` (Vision Doc III.2.2.A)

This specific implementation pattern was chosen for simplicity and leveraging existing `journald` infrastructure.

1.  **`interception-tools` Plugin (`evdev-json-logger.so` - conceptual name for a custom C plugin):**
    *   Loaded by `udevmon` for target keyboard device(s).
    *   Reads `evdev` events for the device.
    *   Formats each event as a single line of JSON (e.g., `{"device_id_by_path": "...", "device_name": "...", "time_sec": ..., "time_usec": ..., "type": ..., "code": ..., "value": ...}`).
    *   Prints this JSON line to `stdout`.
2.  **Systemd Journal (`journald`):**
    *   `udevmon` (and its child `intercept` process running the plugin) logs its `stdout` to the journal.
3.  **`ingestor/journald_bridge` Agent (Rust):**
    *   Reads journal entries (e.g., using `sd-journal` crate).
    *   Filters for entries from `udevmon` or the specific `intercept` job related to keyboard events (e.g., based on `_SYSTEMD_UNIT` or a specific `SYSLOG_IDENTIFIER` set for the `intercept` command).
    *   Parses the JSON line from the journal message.
    *   Constructs a `core.events` payload (source `desktop.input.evdev_keyboard`, event_type `key_pressed` or `key_event`).
    *   Inserts into `core.events` table.

This decouples direct `evdev` reading (by the sandboxed plugin) from database interaction (by the `journald_bridge` agent).

