# Neovim Plugin Integration

## Overview
Provide a Neovim plugin to emit structured editing events (buffers, windows, commands) into Sinex for deeper developer workflow context and cross‑tool correlation.

## Goals
- Capture editor context with low overhead and strong privacy controls
- Correlate edits, commands, and terminal activity with broader system events
- Remain optional and user‑controlled

## Architecture
- Plugin uses Neovim RPC (Lua) to subscribe to buffer and command events.
- Emits structured events via the gateway (HTTP/JSON‑RPC) or JetStream bridge.
- Events normalized in `core.events` with `source: sinex-neovim-plugin`.

## Event Types (examples)
- `neovim.buffer_opened`: file_path_hash, language, project_hash
- `neovim.buffer_saved`: file_path_hash, bytes, symbols_changed?
- `neovim.command_executed`: cmd, args?, duration_ms?
- `neovim.window_focus`: from_buffer_hash, to_buffer_hash

## Privacy & Performance
- Hash file paths; never send content by default.
- Allow per‑project allowlists; exclude sensitive directories.
- Batch and debounce events to minimize overhead.

## Implementation Notes
- Lua plugin with async HTTP client; fallback to local queue if offline.
- Optional content hooks for explicit operations like rename refactors.
- Map Neovim autocmds (BufEnter, BufWritePost, CmdlineLeave) to event emissions.

## Roadmap
- P1: Buffer open/save + command executed
- P2: Project/workspace correlation with terminal and VCS events
- P3: Selective content diffs and symbol‑level insights
