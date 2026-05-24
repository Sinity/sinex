# Desktop Capture Substrate

Status: design record for the desktop-side capture surfaces. The Hyprland IPC
coverage gap (#1019) is closed; this record consolidates the contracts that
desktop capture exposes to the rest of the system. Source modeling and runtime
placement of these surfaces stay aligned with
`docs/architecture/staged-source-parser-substrate.md` and
`docs/architecture/runtime-boundaries.md`.

## What This Owns

Desktop capture covers the live, target-session signals that a Wayland/Hyprland
workstation exposes to a privileged user process:

- Hyprland compositor IPC (`.socket2.sock`): window, workspace, monitor,
  layer, submap, urgency, fullscreen, screencast, and title events.
- Terminal application context for known terminal classes (kitty, foot,
  alacritty, wezterm): CWD, git repo/branch, remote/SSH state.
- Browser application context for qutebrowser via window-title parsing, with
  an explicit forward path to native-messaging via the WebExtension surface in
  #847/#808.
- Neovim buffer/session/diagnostic lifecycle via an editor-resident plugin
  delivering events to the gateway, not via title parsing.

## What This Does Not Own

- Accessibility-tree reads, OCR, and visual context. Those live in
  `docs/architecture/accessibility-and-ocr-capture.md`.
- Clipboard content storage and CAS provenance. Capture exposes clipboard
  events; storage/retention is the privacy/storage layer's concern.
- Audio capture and transcription. Those live in
  `docs/architecture/audio-processing-domain.md`.
- Aggregation into a single "what am I doing now" context object. That is the
  desktop-context automaton (still tracked in #392) and is a synthesis surface,
  not a capture contract.

## Event Surfaces

| Source | Event types | Mechanism |
| --- | --- | --- |
| `wm.hyprland` | `window.opened`, `window.closed`, `window.focused`, `window.moved`, `window.title_changed`, `window.urgent`, `window.fullscreen_changed`, `window.floating_changed`, `window.minimize_changed`, `workspace.switched`, `workspace.context_changed`, `monitor.focused`, `layer.opened`, `layer.closed`, `submap.changed`, `screencast.state_changed`, `state.captured`, `wm.unhandled` | `.socket2.sock` line stream parser, periodic snapshot, in-memory window/workspace state. |
| `desktop.context` | `terminal.context`, `focus_session.started`, `focus_session.ended`, `deep_work.detected`, `context_fragmentation.detected` | Triggered from Hyprland title/focus events and a per-terminal async enrichment task. |
| `browser.context` | `page.focused` (source `wm.title`), `page.focused` (source `native_messaging` once #847 lands) | Title automaton today; WebExtension native-messaging is the upgrade path. |
| `editor.neovim` | `buffer.entered`, `buffer.saved`, `session.started`, `session.ended`, `lsp.diagnostic_changed` | Lua plugin posts JSON-lines to the gateway HTTP ingest endpoint. |

Live browser capture remains part of the single `webhistory` source per
`staged-source-parser-substrate.md`: historical exports and live native-messaging
share the same source namespace and checkpoint family, not a parallel ingestor.

## Workspace Context

Hyprland workspace IDs carry no semantics. The capture process loads a
declarative workspace-to-context mapping at startup (NixOS module → env var or
`$SINEX_ROOT/.config/workspace-contexts.json`) and, on every `workspace.switched`,
emits a paired `workspace.context_changed` event with:

- from/to workspace id
- from/to context label (e.g. `coding`, `communication`, `unclassified`)
- `context_changed: bool` (false when the move stays in one context)
- `time_in_previous_context_secs` derived from the last switch

The capture process does not infer richer "active context" itself — it emits
raw signals and the label lookup. Synthesis into a single context state is the
desktop-context automaton's job, fed by these signals plus terminal/browser/
editor context, notifications, and accessibility (where enabled).

## State Snapshots

`state.captured` is emitted on three triggers, not just on a 5-minute timer:

1. Periodic timer (baseline reconstruction, covers missed events).
2. On workspace switch (a natural context boundary).
3. On monitor add/remove (physical layout change invalidates prior snapshot).
4. On explicit `sinexctl desktop snapshot` request (debugging).

Snapshot payloads stay as opaque `Vec<serde_json::Value>` so downstream diffing
needs no schema migration when Hyprland's snapshot shape evolves.

## Terminal And Editor Contracts

Terminal context (`desktop.context/terminal.context`) is emitted when a
`window.title_changed` fires and the focused window class is in the known
terminal set. Enrichment runs as a short-lived async task off the event loop:

- PID via `hyprctl activewindow -j`, `PWD` via `/proc/{pid}/environ` or title
  (Kitty shell integration writes CWD into the title).
- `git -C "$CWD" rev-parse --show-toplevel` and `branch --show-current` with a
  tight (~200ms) timeout; missing git output is a normal outcome.
- SSH detection from terminal-ingestor command history when available.

CWD goes through `ProcessingContext::Command` before storage — path redaction
is shared with shell-command ingestion.

Neovim is intentionally not polled from the desktop capture process. The
correct surface is an in-editor plugin emitting structured events directly to
the gateway HTTP ingest endpoint. Buffer/save/session/diagnostic events are in
scope; cursor and mode changes are not. The Lua plugin uses non-blocking
`jobstart` of `curl`; auth follows the gateway's bearer-token model. The
gateway endpoint itself is part of `docs/architecture/runtime-boundaries.md`.

## Relation To Source Parser Substrate

Desktop capture is the canonical example of a live capture surface that
`staged-source-parser-substrate.md` keeps as a separate runtime process: it
needs the user's session bus, Hyprland IPC socket, and target-session
privilege. It is not a staged source-material parser.

Capture-side artefacts may still be staged downstream. For example: an
OCR-derived blob and its text become two `raw.source_material_registry` rows
with synthesis-provenance linking them, parsed by the OCR domain rather than
by desktop capture itself.

## Privacy

Every text-bearing event runs through the privacy engine with an explicit
context before leaving the capture process:

| Field | Context |
| --- | --- |
| Window titles | `ProcessingContext::WindowTitle` |
| Terminal CWD / command tail | `ProcessingContext::Command` |
| Clipboard text | `ProcessingContext::Clipboard` |
| Notification body | `ProcessingContext::Notification` (planned, see #1042) |
| Accessibility text | covered in `accessibility-and-ocr-capture.md` |

Capture must not bypass the privacy engine even for "system" titles —
classification of which titles are system vs. user is itself a privacy decision.

## Open Questions

- Whether the desktop capture process keeps its current per-domain ingestor
  shape, or merges into a single capture node once #1054 resolves runtime
  topology. The contracts above are stable regardless of which binary owns
  them.
- How `screencast.state_changed` should gate other capture surfaces (OCR,
  accessibility) when the user is screen-sharing. Default expectation:
  capture pauses or downgrades sensitivity; concrete policy belongs to the
  privacy admission work in #1042.
- Whether a future single `wm.captured` snapshot supersedes the per-event
  stream. Today both exist deliberately: per-event for low-latency synthesis,
  snapshot for reconstruction.

## Boundaries

- Do not capture mouse motion, button events, or per-keystroke input from the
  compositor. Scribe-tap is the keystroke surface and runs separately.
- Do not infer "active context" inside the capture process. Emit raw signals
  plus declared labels; synthesize elsewhere.
- Do not create a second `browser` source for live capture. Webhistory is one
  source with multiple acquisition modes.
- Do not pull editor state via window-title parsing. Editors emit their own
  events.

**Related:** `docs/architecture/staged-source-parser-substrate.md`,
`docs/architecture/runtime-boundaries.md`,
`docs/architecture/accessibility-and-ocr-capture.md`,
issues #393, #394, #395, #389, #847.
