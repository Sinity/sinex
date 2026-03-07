# TIM-HyprlandNativePluginDev: Hyprland Native C++ Plugin Development

*   **Relevant ADR:** ADR‑003 Hyprland Compositor Integration Path
*   **Original UG Context:** Section 4.2

This TIM outlines the architecture, capabilities, risks, and best practices for developing a native C++ plugin for Hyprland. This is an advanced integration path for achieving deeper telemetry not available via IPC sockets.

## 1. Rationale Summary

A C++ plugin running within Hyprland offers direct access to internal compositor data structures and event hooks, enabling maximum data fidelity (e.g., render timings, precise input latencies, frame capture) at the cost of higher development complexity and stability risk. See ADR-003 for the strategic decision context.

## 2. Plugin Architecture and Setup

*   **Mechanism [SR1, SA1, CR2]:** Plugins are shared object (`.so`) files compiled against Hyprland's headers (e.g., `PluginAPI.hpp`, `Window.hpp`, `Compositor.hpp`).
*   **Loading:** Specified in `hyprland.conf` (e.g., `plugin = /path/to/sinex_hyprland_plugin.so`). Loaded by Hyprland at startup. Executes within the compositor's process space.
*   **Core Hyprland Plugin Entry Points (Exported C Functions):**
    ```cpp
    // Required:
    // EXPORTC APICALL VOID PLUGIN_INIT(HANDLE handle); // Called when plugin is loaded
    // EXPORTC APICALL std::string PLUGIN_API_VERSION(); // Must return HYPRLAND_API_VERSION macro
    // EXPORTC APICALL PLUGIN_DESCRIPTION_INFO PLUGIN_DESCRIPTION(); // Provides metadata

    // Optional:
    // EXPORTC APICALL VOID PLUGIN_EXIT(); // Called when Hyprland is exiting or plugin is unloaded
    ```
    `HANDLE PHANDLE` (global in plugin) stores the handle passed to `PLUGIN_INIT`.

## 3. API Access and Key Data Structures [UG Sec 4.2.1]

Plugins directly access Hyprland's internal C++ objects.

*   **Key Classes (from Hyprland source/wiki, SR1, CR2):**
    *   `CWindow`: Represents a client window.
        *   Geometry: `m_vRealPosition`, `m_vRealSize`.
        *   Identifiers: `m_szTitle`, `m_szClass`, `m_szInitialClass`, `m_szAppID`.
        *   State: `m_bIsFloating`, `m_bIsFullscreen`, `m_bIsMapped`, `m_bIsUrgent`, `m_iWorkspaceID`.
        *   Process: `m_iPID`.
        *   XWayland: `m_bIsX11`, `m_iX11WindowID`.
        *   Surface: `m_pSurface` (e.g., `wlr_surface`, `wlr_xdg_surface`).
    *   `CMonitor`: Represents a display output.
        *   Identifiers: `ID`, `szName`. Geometry: `vecPosition`, `vecSize`. State: `activeWorkspace`.
    *   `CWorkspace`: Represents a workspace.
        *   Identifiers: `m_ID`, `m_szName`. State: `m_pMonitor`, `m_bHasFullscreenWindow`.
*   **Accessing Globals:** `g_pCompositor`, `g_pConfigManager`, `g_pInputManager`, `g_pLayoutManager`, `g_pRenderer`, `g_pKeybindManager`, `g_pHookSystem`.
*   **API Hooks (via `HyprlandAPI` or `g_pHookSystem`):**
    *   `HyprlandAPI::registerCallbackDynamic(PHANDLE, eventNameString, callbackFunction)`: Registers a callback for a named Hyprland event.
        *   Example Events: `"activeWindow"`, `"openWindow"`, `"closeWindow"`, `"moveWindow"`, `"workspace"`, `"monitorAdded"`, `"preRender"`, `"postFrame"`, `"mouseMove"`, `"keyboardKey"`, `"configReloaded"`.
    *   `HyprlandAPI::addDispatcher(PHANDLE, "myplugin:mycommand", callbackFunction)`: Adds a custom command invokable via `hyprctl dispatch myplugin:mycommand args`.
*   **Example Event Hook Registration (from UG Sec 4.2.5):**
    ```cpp
    // #include <hyprland/src/plugins/PluginAPI.hpp>
    // #include <hyprland/src/Window.hpp>
    // #include <hyprland/src/debug/Log.hpp> // For Debug::log

    // inline HANDLE PHANDLE = nullptr;

    // // Callback for "activeWindow" event
    // // Data for "activeWindow" is typically std::any_cast<CWindow*>(data) for the new active window.
    // void onActiveWindowChanged(void* /*thisptr*/, SCallbackInfo& /*info*/, std::any data) {
    //     try {
    //         auto* pNewActiveWindow = std::any_cast<CWindow*>(data);
    //         if (pNewActiveWindow) {
    //             std::string logMsg = "[SinexPlugin] Active window: " + pNewActiveWindow->m_szClass + " - " + pNewActiveWindow->m_szTitle;
    //             Debug::log(LOG, logMsg);
    //             // Construct JSON payload with window details from pNewActiveWindow
    //             // Send payload to Exocortex backend (e.g., via UNIX domain socket client in a separate thread)
    //         }
    //     } catch (const std::bad_any_cast& e) {
    //         Debug::log(ERR, "[SinexPlugin] Bad any_cast in onActiveWindowChanged: " + std::string(e.what()));
    //     }
    // }

    // EXPORTC APICALL VOID PLUGIN_INIT(HANDLE handle) {
    //     PHANDLE = handle;
    //     // Note: Event name string must exactly match what Hyprland expects.
    //     // Check Hyprland source (e.g., HookSystemManager.hpp) for canonical event names.
    //     // "activeWindow" is a common one.
    //     bool registered = HyprlandAPI::registerCallbackDynamic(PHANDLE, "activeWindow", ::onActiveWindowChanged);
    //     if (!registered) { Debug::log(ERR, "[SinexPlugin] Failed to register activeWindow callback!"); }
    //     HyprlandAPI::addNotification(PHANDLE, "[SinexPlugin] Initialized.", CColor{0.2f, 1.0f, 0.2f, 1.0f}, 5000);
    // }
    // // ... other required plugin exports (PLUGIN_API_VERSION, PLUGIN_DESCRIPTION) ...
    ```

## 4. Frame Capture via Plugin [UG Sec 4.2.5, `openai_sinex_6.md` Sec 4]

A key capability enabled by plugins is efficient screen/window frame capture.

*   **Mechanism:** Hook into Hyprland's rendering pipeline events (e.g., a "postFrame" or "monitorFrameCommitted" type event, actual hook name may vary).
*   **Accessing Pixel Data:**
    1.  **DMA-BUF Export (Preferred for Zero-Copy):**
        *   Within the frame event callback, get the `wlr_texture` for the target output (monitor) or window.
        *   Use `wlr_texture_export_dmabuf(pTexture, &dmabuf_attrs)` to get DMA-BUF file descriptors and metadata (format, strides, modifier).
        *   Send these FDs and metadata via IPC (e.g., UNIX domain socket with `sendmsg` and `SCM_RIGHTS`) to an external capture agent (e.g., `agent_video_encoder` or `agent_ocr_node_gpu`).
        *   The external agent imports the DMA-BUF for zero-copy access (e.g., direct to VAAPI/NVENC hardware encoder or GPU-based OCR).
    2.  **`wlr_renderer_read_pixels()` (Fallback, GPU-to-CPU Copy):**
        *   If DMA-BUF export fails or is not suitable, use `wlr_renderer_read_pixels()` to copy pixel data from a texture or output region into a CPU-accessible buffer.
        *   This data can then be sent over IPC as a raw pixel buffer or encoded (e.g., as PNG) within the plugin's worker thread before sending. Slower due to GPU-CPU transfer.
*   **Triggering Capture:** Can be continuous (for video recording), on damage events (for efficient screen updates), or on demand (e.g., triggered by an `exo` command via a custom plugin bridge).

## 5. Security Risks and Mitigation Strategies [UG Sec 4.2.2]

Plugins execute within the compositor process space with user privileges.

*   **Risks [SR1, SA1]:**
    *   **Compositor Crash:** Bugs in plugin (null pointers, exceptions, memory corruption) will crash Hyprland.
    *   **System Compromise:** Malicious plugin can keylog, screen scrape, execute arbitrary code, exfiltrate data.
*   **Mitigations:**
    1.  **Develop in Nested Hyprland Sessions:** Test plugin in a separate, disposable Hyprland instance.
    2.  **Strict Version Pinning:** Build plugin against a specific Hyprland commit/tag. Check `PLUGIN_API_VERSION()`.
    3.  **Defensive Programming:** Rigorous error handling (`try-catch`), null checks, validation. Use Hyprland's `Debug::log`.
    4.  **Code Audits:** Thorough security reviews for any distributed plugin.
    5.  **Minimize Plugin Scope:** Plugin acts as a lean sensor. Offload complex logic, data processing, and network I/O to a separate, less privileged Exocortex agent process, communicating via a secure local IPC (e.g., UNIX domain socket with restricted permissions).
    6.  **User Opt-In & Transparency:** Plugin enabled explicitly by user. Clear documentation of data access.

## 6. Performance Best Practices [UG Sec 4.2.3]

*   **Minimize Work in Callbacks [SA1]:** Callbacks run in critical compositor paths. Perform only essential data extraction. Enqueue data/tasks to a separate worker thread within the plugin for heavier processing or IPC.
*   **Efficient C++:** Use performant data structures. Profile plugin code. Avoid memory leaks.
*   **Asynchronous IPC:** Use non-blocking I/O from plugin's worker thread for sending data to external Exocortex agent.
*   **Conditional Capture:** Only enable expensive operations (like per-frame texture access) if specifically configured/requested.

## 7. API Stability Challenges [UG Sec 4.2.4]

*   **Hyprland Internal API Instability [SA1]:** Internal C++ APIs and data structures can change between Hyprland releases or even commits, breaking plugins.
*   **Maintenance Strategy [SA1]:**
    *   Pin plugin builds to specific Hyprland source versions.
    *   Regularly re-evaluate, recompile, and test against new Hyprland versions.
    *   Monitor Hyprland development for breaking changes.
    *   Prioritize using officially exported `PluginAPI.hpp` functions where possible.
