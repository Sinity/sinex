# TIM-SemanticDesktopStream: Synthesizing Context for Advanced Agency

*   **Purpose:** Details the architecture of the Semantic Desktop Stream, a synthesized, real-time model of the user's current desktop context and available actions, designed to enable advanced AI agency.
*   **Source:** Based on conceptual descriptions in Vision Part III.5.
*   **Dependencies:** Relies on data from various ingestors (Hyprland, AT-SPI2, application-specific), LLM capabilities.

## 1. Core Concept & Architectural Role

The Semantic Desktop Stream aims to provide a continuously updated, structured representation of the user's immediate digital environment beyond raw events. This understanding is crucial input for sophisticated LLM-driven agents intended for deep contextual understanding, task automation, and proactive assistance.

## 2. `SemanticDesktopContextManager` Agent

A dedicated, high-priority agent responsible for constructing and maintaining this stream.
*   **Input Event Subscriptions:**
    *   Hyprland Ingestor: Focus changes, window state/titles, layout changes.
    *   AT-SPI2 Ingestor: Focused widget details (path, role, name, value, text content), widget tree snapshots.
    *   Application-Specific Ingestors: Neovim (buffer content, mode, cursor), Browser (active tab URL, DOM summary), Kitty (CWD, current command).
    *   Living Document: Recent entries or active sections for user's current thoughts/plans.
*   **Internal State Model:**
    *   The agent maintains an in-memory model (or a rapidly updated cache in a dedicated DB table/view like `derived.current_desktop_semantic_context`) of the current semantic state.
    *   This model includes:
        *   `focused_application`: Name, PID, window ID, class.
        *   `focused_element`: (From AT-SPI2/app ingestor) Widget path/ID, role, name, current text value/content, associated actions.
        *   `visible_text_summary`: Key text content visible (e.g., editor buffer preview, webpage main content summary, terminal visible lines).
        *   `available_actions`: List of permissible actions (from AT-SPI2 actions, app-specific commands, common OS actions related to context).
        *   `broader_desktop_context`: Other significant open windows (titles, apps), recent notifications, ongoing Exocortex background tasks.
        *   `inferred_short_term_intent`: (LLM-derived) Based on recent activity sequence and LD.

## 3. Dynamic UI Widget Tree Analysis (with LLM Assistance)

For interpreting GUIs via AT-SPI2 data.
1.  **Pattern Learning:** Agent observes/hashes AT-SPI2 widget tree structures for different applications to learn common layouts.
2.  **LLM for Novel Layouts:** When a new/unrecognized widget tree for an app is encountered, the agent queries an LLM with the tree structure. Prompt: "Analyze this widget tree for app X. Identify main content areas, key input fields (e.g., search bars, message composers), primary action buttons (save, send, submit), and navigation elements. Provide JSONPaths or stable selectors for these."
3.  **Cached Parsers/Rules:** LLM-generated (or manually refined) extraction rules for known app layouts are cached by the agent.
4.  **Real-time Application:** For known layouts, cached rules extract semantic info from AT-SPI2 data. For new layouts, LLM is consulted. This balances efficiency with adaptability.

## 4. Output: The Structured Semantic Contextual Stream

The `SemanticDesktopContextManager` makes its synthesized context available via:
1.  **A `raw.events` Stream (Periodic or On-Change):**
    *   `source`: `"agent.semantic_desktop_manager"`
    *   `event_type`: `"desktop_semantic_context_updated"`
    *   `payload`: A rich JSON object representing the current synthesized understanding (focused app/element details, visible text summary, available actions, broader context, inferred intent). This payload should have a well-defined JSON Schema.
    *   Frequency: Emitted when significant state changes occur (e.g., focus shift to new app, major UI change in focused app) or periodically (e.g., every few seconds).
2.  **A Queryable API or Database View (Future):**
    *   Other agents or UI components could directly query this agent (via an internal RPC/HTTP API) or a dedicated, rapidly updated database view (e.g., `derived.current_desktop_semantic_context`) to get the latest full context on demand.

## 5. Enabling Advanced AI Agency (Read and "Write")

This stream is the input for LLM agents performing:
*   **Deep Contextual Understanding:** LLM "sees" what user sees, understands available interactions.
*   **Task Automation:** Plans and executes multi-step tasks across applications. Requires "write" capabilities.
*   **Proactive Assistance:** Offers relevant suggestions based on deep context.
*   **"Write" Capabilities (Agentic Control - Highly Sensitive, User-Opt-In, Sandboxed):**
    *   The same hooks used for capture become output channels for agents:
        *   Hyprland IPC/Plugin: Synthetic input events (keypresses, mouse clicks).
        *   AT-SPI2: Invoke actions on widgets (`do_action`), set text in fields.
        *   Browser Extension: Execute JavaScript via `scripting` API or messages to content scripts.
        *   `exo` CLI: Invoke shell commands.
    *   All "write" actions by agents must be:
        *   Explicitly permitted by user (global and per-agent/per-action granularity).
        *   Clearly auditable (logged as `sinex.agent.action_executed_on_desktop` events with full details).
        *   Sandboxed if possible to limit scope of action.
        *   Have undo/revert mechanisms where feasible.

