# ADR-003: Hyprland Compositor Integration Path

*   **Status:** Implemented
*   **Date:** 2024-03-11
*   **Implementation Date:** 2025-07-17
*   **Context & Problem Statement:**
    The Hyprland Wayland compositor is a rich source of information about the user's desktop activity, including window management, focus changes, input events, and workspace state. The Exocortex needs a robust and comprehensive way to ingest this data. Two primary integration paths exist:
    1.  Utilizing Hyprland's existing Inter-Process Communication (IPC) sockets.
    2.  Developing a native C++ Hyprland plugin that runs within the compositor's process space.

    The choice involves trade-offs between ease of implementation, data richness/fidelity, performance impact, stability risks, and development/maintenance effort.

*   **Discussed Options:**

    1.  **Hyprland IPC Sockets (`socket1` for commands/queries, `socket2` for events):**
        *   **Description:** An external Exocortex ingestor process (e.g., written in Rust) connects to Hyprland's UNIX domain sockets (`$XDG_RUNTIME_DIR/hypr/$HYPRLAND_INSTANCE_SIGNATURE/.socket.sock` and `.socket2.sock`). It listens for newline-terminated event strings (`EVENTNAME>>DATA`) on `socket2` and can send commands or queries (e.g., `hyprctl -j clients`) to `socket1`.
        *   **Pros:**
            *   **Easier Implementation:** Simpler to develop an external client that parses text/JSON streams compared to a C++ plugin. Less direct coupling with Hyprland internals.
            *   **Lower Stability Risk (to Compositor):** A bug in the external ingestor process is unlikely to crash the Hyprland compositor itself.
            *   **Good Coverage of Core Events:** `socket2` provides a wide range of events for window lifecycle, focus, workspace changes, monitor events, etc. [CR2: ~47 event types]. `socket1` allows querying detailed state.
            *   **Language Flexibility:** Ingestor can be written in any language that supports UNIX sockets.
        *   **Cons:**
            *   **Limited Data Fidelity/Granularity:**
                *   Event payloads on `socket2` are often summary-level (e.g., `activewindowv2>>WINDOWADDRESS`). Fetching full details requires an additional query to `socket1`, introducing latency and potential race conditions.
                *   No access to internal compositor metrics (e.g., render timings, GPU load, precise input event latencies within compositor).
                *   No direct access to window content/textures for advanced visual capture or analysis.
            *   **Reliability Concerns for `socket2`:** Some community reports of `socket2` occasionally missing events, especially under high load or for certain event types (e.g., fullscreen changes [SR1]). No guaranteed delivery. Client must handle `EAGAIN` robustly.
            *   **Performance for High-Frequency Queries:** Repeatedly querying `socket1` for detailed state for many windows or at high frequency can add overhead.

    2.  **Native C++ Hyprland Plugin:**
        *   **Description:** A shared object (`.so`) file compiled against Hyprland's C++ headers and loaded by Hyprland at startup (`plugin = /path/to/plugin.so` in `hyprland.conf`). The plugin code runs within the compositor's process space and can use Hyprland's internal C++ APIs and data structures (`CWindow`, `CMonitor`, `g_pCompositor`, etc.) and register callbacks for internal events.
        *   **Pros:**
            *   **Maximum Data Fidelity & Granularity:** Direct access to all internal compositor state, including precise window geometry, animation states, layer-shell surfaces, input event details with compositor-resolved context, render timings, and potentially window textures/framebuffers.
            *   **Lower Latency for Event Access:** Callbacks are invoked directly within the compositor's event loop.
            *   **Potential for Advanced Features:** Enables capabilities like efficient damage-aware screen recording via DMA-BUF, VLM/OCR hooks on rendered frames, modification of input events (with extreme care), or custom rendering overlays.
            *   **Reduced IPC Overhead:** Accesses data directly in memory rather than through socket communication and text/JSON parsing.
        *   **Cons:**
            *   **Higher Development Complexity:** Requires C++ development, understanding of Hyprland's internal architecture, and careful memory management.
            *   **Significant Stability Risk (to Compositor):** A bug in the plugin (e.g., unhandled exception, null pointer, memory corruption) will crash the entire Hyprland compositor, leading to loss of the graphical session [SR1, SA1]. This is the primary drawback.
            *   **Security Risks:** Malicious or compromised plugin code runs with compositor privileges (user level) and can act as a keylogger, screen scraper, or execute arbitrary code.
            *   **API Instability & Maintenance Burden:** Hyprland's internal C++ API is not stable and can change between releases, requiring plugin recompilation and potential code adaptation. Requires careful version pinning and monitoring of Hyprland development [SA1].
            *   **Performance Impact if Not Optimized:** Poorly written plugin code (e.g., blocking operations in callbacks) can degrade compositor performance.

    3.  **Hybrid Approach (IPC First, Plugin for Advanced Features):**
        *   **Description:** Start by implementing a robust ingestor using the IPC sockets to capture the broad range of available events. Later, develop a C++ plugin to supplement this with data not available via IPC (e.g., frame capture, precise input latencies, internal metrics) or to optimize specific high-frequency data capture paths. Data from both sources would be correlated in the Exocortex backend.
        *   **Pros:** Balances initial development speed and stability with long-term goals for data richness. Allows deferring the complexity and risks of plugin development.
        *   **Cons:** Requires managing two integration points and potentially correlating data from them.

*   **Decision:**
    The Exocortex will adopt a **Hybrid Approach (Option 3)** for Hyprland integration.
    1.  The **primary and initial implementation path** will be to leverage the **Hyprland IPC Sockets** (specifically `socket2` for events, augmented by `socket1` for state queries) using an external Rust-based ingestor. This ingestor will aim for comprehensive capture of all events and states exposed via IPC.
    2.  Development of a **Native C++ Hyprland Plugin** is a strategic, longer-term goal. It will be pursued as a separate workstream to unlock deeper telemetry (e.g., render timings, precise input latencies, direct frame/texture access for advanced visual capture or analysis) that is not accessible via IPC. The plugin will be designed with extreme attention to stability, security, and performance best practices.
    3.  The C++ plugin, when developed, will either replace parts of the IPC ingestor (if it provides a superset of data more efficiently) or, more likely, act as a supplementary data source. The Exocortex backend will be responsible for fusing and correlating data from both the IPC ingestor and the native plugin.

*   **Rationale for Decision:**
    1.  **Time-to-Value & Lower Initial Risk:** The IPC socket approach allows for faster initial development of a Hyprland ingestor, providing significant data capture capabilities with lower risk to compositor stability compared to immediate plugin development. This aligns with iterative development principles.
    2.  **Broad Event Coverage via IPC:** The existing IPC interface already provides a rich set of events sufficient for many core Exocortex use cases related to desktop context (window management, focus, application usage).
    3.  **Mitigates Plugin Development Challenges:** Defers the significant complexities, stability risks, and maintenance burden associated with C++ plugin development until the core Exocortex infrastructure is more mature and the specific needs for plugin-only data are clearly defined and prioritized.
    4.  **Strategic Path to Maximum Fidelity:** Acknowledges that a native plugin offers the ultimate level of data fidelity and control, keeping this option open for future enhancement when the benefits outweigh the development costs and risks.
    5.  **Layered Data Capture:** The hybrid approach aligns with the Exocortex principle of layered fidelity, potentially allowing data from IPC and plugin sources to be cross-validated or fused.

*   **Consequences:**
    *   Initial Hyprland data capture will be subject to the limitations of the IPC interface (potential for missed events, latency for detailed state queries, no direct frame access).
    *   The Rust-based IPC ingestor must be designed to be robust against `socket2` unreliability (e.g., handling `EAGAIN`, potentially periodic state reconciliation via `socket1`).
    *   A separate, focused development effort will be required for the C++ plugin, including setting up a dedicated build environment, rigorous testing (e.g., in nested Hyprland sessions), and adherence to Hyprland plugin API best practices.
    *   Mechanisms for correlating events from the IPC ingestor and the C++ plugin (e.g., using common window identifiers and precise timestamps) will need to be considered if both run concurrently.

