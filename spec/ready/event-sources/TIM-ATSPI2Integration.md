# TIM-ATSPI2Integration: GUI Accessibility Framework (AT-SPI2)

## Status Dashboard
**Maturity Level**: L2 - Ready for Implementation
**Implementation**: 0% (Design complete, implementation not started)
**Dependencies**: AT-SPI2 libraries, D-Bus integration, EventSource trait, accessibility bus
**Blocks**: UI semantic capture, accessibility event monitoring, GUI context analysis

## MVP Specification
- AT-SPI2 D-Bus connection and accessibility bus integration
- Basic UI element discovery and enumeration
- Focus change and window activation event capture
- Widget property extraction (name, role, value, state)
- Integration with EventSource pattern

## Enhanced Features
- Advanced UI hierarchy traversal
- Text content extraction from UI elements
- Coordinate-based UI element lookup
- Performance-optimized event filtering
- Cross-application UI relationship mapping
- Privacy-aware sensitive UI filtering

## Implementation Checklist
- [ ] AT-SPI2 library bindings and D-Bus integration
- [ ] Accessibility bus connection management
- [ ] EventSource trait implementation
- [ ] UI element discovery and enumeration
- [ ] Focus and activation event monitoring
- [ ] Widget property extraction pipeline
- [ ] Event filtering and privacy controls
- [ ] Performance optimization and caching
- [ ] Integration testing with common applications

*   **Relevant ADR:** (N/A directly, core ingestor for UI semantics)
*   **Original UG Context:** Section 5

This TIM details the technical implementation for integrating with the AT-SPI2 accessibility framework to capture UI element information and events.

## 1. Rationale Summary

AT-SPI2 is the standard accessibility framework on Linux desktops, allowing Exocortex to query UI hierarchy, widget properties (name, role, value, state), and listen for UI events (focus changes, text input, window activation) from accessible applications (GTK, Qt, Electron, browsers). This provides rich semantic context about user interactions with GUIs.

## 2. Mechanism and Capabilities [UG Sec 5.1]

*   **Core Mechanism [OR2]:** Operates over a dedicated D-Bus connection, the **accessibility bus**.
    *   Address usually in `AT_SPI_BUS_ADDRESS` environment variable or X11 root window property.
*   **Daemons [OR2]:**
    *   `at-spi-bus-launcher`: Manages the accessibility D-Bus daemon.
    *   `at-spi2-registryd`: Registry of accessible applications.
*   **Capabilities via AT-SPI2 Ingestor [OR2]:**
    *   **Enumerate UI Elements:** Discover accessible apps, windows, widget trees.
    *   **Query Widget Properties:** Accessible Name, Role, Value, States (focused, enabled, visible, checked, etc.), Description, Bounding Box (screen coordinates).
    *   **Listen for Accessibility Events:**
        *   Focus changes: `object:state-changed:focused`.
        *   Text input changes: `object:text-changed:insert`, `object:text-changed:delete`.
        *   Value changes: `object:property-change:accessible-value`.
        *   Selection changes: `object:text-selection-changed`.
        *   Window lifecycle: `window:activate`, `window:deactivate`, `window:minimize`, `window:close`.
        *   Object lifecycle: `object:children-changed:add/remove`.
    *   **Invoke Actions (Less common for passive ingestor):** Programmatically "click" buttons, set text.

## 3. Integration Libraries and Methods [UG Sec 5.2]

*   **Python (`pyatspi2`) (Preferred for Exocortex Ingestor):**
    *   Often packaged as `pyatspi` or `python3Packages.pyatspi` in NixOS.
    *   Provides a high-level Pythonic interface over D-Bus.
    *   **Example Python Event Listener (from UG Sec 5.2, refined):**
        ```python
        import gi
        gi.require_version('Atspi', '2.0')
        from gi.repository import Atspi, GLib
        import json # For payload construction
        import datetime

        # Placeholder: Function to send event to Exocortex raw.events
        # In a real agent, this would use psycopg2 or an HTTP client to an ingest API.
        def send_to_exocortex_raw_events(source, event_type, payload_dict):
            # Construct full raw.events structure
            event_data = {
                "source": source,
                "event_type": event_type,
                "ts_orig": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                "host": "my-desktop", # Get actual hostname
                "ingestor_version": "pyatspi_ingestor_v0.1.0",
                # "payload_schema_id": "ULID_of_schema_for_this_event", # Look up from schema registry
                "payload": payload_dict
            }
            # Simulate sending by printing JSON
            print(json.dumps(event_data))


        def on_focus_changed(event: Atspi.Event):
            # event.source is an Atspi.Accessible object for the newly focused widget
            if not event.source or not hasattr(event.source, 'get_name'): # Check if object is valid
                return

            try:
                app_accessible = Atspi.get_application_for_object(event.source)
                app_name = app_accessible.get_name() if app_accessible and hasattr(app_accessible, 'get_name') else "UnknownApp"
                app_id = app_accessible.get_id() if app_accessible and hasattr(app_accessible, 'get_id') else -1
                
                widget_name = event.source.get_name()
                widget_role_num = event.source.get_role()
                widget_role_name = Atspi.role_get_name(widget_role_num)
                widget_path = event.source.get_path() # D-Bus path, can be useful for identification

                # Get window title if possible (ancestor traversal)
                window_title = "UnknownWindow"
                current_obj = event.source
                for _ in range(10): # Limit depth to avoid infinite loops
                    if not current_obj: break
                    if Atspi.role_get_name(current_obj.get_role()) in ["application", "frame", "dialog", "window"]:
                        if hasattr(current_obj, 'get_name') and current_obj.get_name():
                             window_title = current_obj.get_name()
                             break
                    parent = current_obj.get_parent()
                    if parent == current_obj: break # Avoid self-parent loops
                    current_obj = parent
                    if not hasattr(current_obj, 'get_role'): break # Parent not accessible


                payload = {
                    "application_name": app_name,
                    "application_id_atspi": app_id, # AT-SPI specific ID
                    "window_title_atspi": window_title,
                    "widget_name": widget_name,
                    "widget_role": widget_role_name,
                    "widget_role_code_atspi": widget_role_num,
                    "widget_dbus_path_atspi": widget_path,
                    "_provenance": { /* ... correlation_id if available from broader context ... */ }
                }
                send_to_exocortex_raw_events("desktop.atspi.ingestor", "widget_focused", payload)

            except Exception as e:
                print(f"Error processing focus event: {e} for source: {event.source}")


        def on_text_changed_insert(event: Atspi.Event):
            # event.detail1: start_offset, event.detail2: length, event.any_data: inserted_text (string)
            # event.source is the text widget
            if not event.source or not hasattr(event.source, 'get_name'): return
            try:
                app_accessible = Atspi.get_application_for_object(event.source)
                app_name = app_accessible.get_name() if app_accessible else "UnknownApp"
                widget_name = event.source.get_name()
                widget_role_name = Atspi.role_get_name(event.source.get_role())
                
                # For password fields, text might be masked or not available.
                # Check state: event.source.get_state_set().contains(Atspi.StateType.EDITABLE) etc.
                # ATK_STATE_SENSITIVE might indicate password field.
                is_sensitive = event.source.get_state_set().contains(Atspi.StateType.SENSITIVE)

                inserted_text = event.any_data_as_string # any_data is GValue, need to extract string
                
                payload = {
                    "application_name": app_name,
                    "widget_name": widget_name,
                    "widget_role": widget_role_name,
                    "start_offset": event.detail1,
                    "length_inserted": event.detail2,
                    "inserted_text": "REDACTED_SENSITIVE" if is_sensitive else inserted_text, # Basic redaction
                    "is_sensitive_widget": is_sensitive,
                }
                send_to_exocortex_raw_events("desktop.atspi.ingestor", "text_inserted", payload)
            except Exception as e:
                print(f"Error processing text_changed_insert event: {e}")


        # main_loop = None
        # def run_atspi_listener():
        //     global main_loop
        //     if not Atspi.is_initialized():
        //         Atspi.init() # Initialize registry connection

        //     # Register for focus events on any object
        //     Atspi.Registry.register_event_listener(on_focus_changed, "object:state-changed:focused")
        //     # Register for window activation (often precedes focus on first widget)
        //     Atspi.Registry.register_event_listener(on_focus_changed, "window:activate") # Use same handler for now

        //     # Register for text insertion events
        //     Atspi.Registry.register_event_listener(on_text_changed_insert, "object:text-changed:insert")
        //     # Also consider "object:text-changed:delete"

        //     main_loop = GLib.MainLoop()
        //     try:
        //         print("Listening for AT-SPI2 events...")
        //         main_loop.run()
        //     except KeyboardInterrupt:
        //         print("Stopping AT-SPI2 listener.")
        //     finally:
        //         if Atspi.is_initialized():
        //             Atspi.Registry.deregister_event_listener(on_focus_changed, "object:state-changed:focused")
        //             Atspi.Registry.deregister_event_listener(on_focus_changed, "window:activate")
        //             Atspi.Registry.deregister_event_listener(on_text_changed_insert, "object:text-changed:insert")
        //             # Atspi.exit() # Usually not needed if desktop manages it
        //         if main_loop and main_loop.is_running():
        //             main_loop.quit()

        // if __name__ == "__main__":
        //     run_atspi_listener()
        ```
*   **C/C++ (`libatspi`):** Official C library. Lower-level, more complex.
*   **Direct D-Bus:** Possible with generic D-Bus libraries (e.g., `zbus` in Rust) but significantly more complex than using `pyatspi2` or `libatspi`.

## 4. Permissions, Setup, NixOS Considerations [UG Sec 5.3]

*   **Permissions [OR2]:** Runs with user privileges. No `root` needed for agent.
*   **Desktop Environment Enablement [OR2]:** Accessibility must be enabled in DE (usually default in GNOME/KDE). Check `AT_SPI_BUS_ADDRESS`. Avoid `NO_AT_BRIDGE=1`.
*   **Wayland Application Integration [OR2]:**
    *   Qt apps: `QT_ACCESSIBILITY=1` env var.
    *   GTK apps: `GTK_MODULES` includes `gail:atk-bridge`.
*   **NixOS Packages [OR2]:**
    *   `at-spi2-core` (daemons), `at-spi2-atk` (GTK bridge).
    *   For Python ingestor: `python3Packages.pyatspi` (or `pyatspi2`).
    *   Include these in system or user environment for both ingestor and target applications.
*   **Sandboxed Applications (Flatpak, Snap) [OR2]:** Sandbox must grant AT-SPI2 bus access (e.g., Flatpak `--talk-name=org.a11y.Bus`).

## 5. Reliability Issues and Fallback Strategies [UG Sec 5.4]

*   **Reliability Variability [SR1]:** Depends on DE, app toolkit, AT-SPI2 library versions.
*   **Reported Issues [SR1]:** Service crashes/hangs, delays (25s+ observed). AT-SPI2 v2.48.2+ reported more stable.
*   **Application Support Gaps [OR2]:** Custom UIs, games, older toolkits may have poor accessibility.
*   **Event Volume [OR2]:** Can be high (e.g., `text-changed` per keypress). Ingestor needs efficient processing, filtering, debouncing.
*   **Fallback Strategies [SR1, OR2]:**
    1.  **OCR for Text:** If AT-SPI2 fails to get text, use OCR on a screenshot of the app window/widget (see `TIM-OCR_Tesseract.md`).
    2.  **Monitor AT-SPI2 Service Health:** Ingestor monitors bus connection. Attempts reconnect, logs `sinex.agent.error` on persistent failure.
    3.  **Ignore Problematic Applications:** Configure ingestor to ignore events from apps known to cause instability or flood the bus.
    4.  **Prioritize Application-Specific Ingestors:** For critical apps (Browser, Neovim), their dedicated ingestors (UG Sec 10, 15) usually provide richer and more reliable data than generic AT-SPI2 inspection of their UI chrome. AT-SPI2 is valuable for apps *without* dedicated Exocortex ingestors.

