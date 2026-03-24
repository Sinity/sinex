# Unified Terminal node

This node currently captures shell-history data through the Stage-as-you-go pipeline:

- Bash history files
- Zsh history files
- Fish history, including the SQLite-backed fish store when present

## Architecture: Stage-as-you-go Capture

The live terminal node path flows through the Stage-as-you-go material pipeline:

1. **Source Material Capture** - raw history bytes are staged via the SDK
2. **Temporal Ledger** - ordered material slices preserve provenance
3. **Event Generation** - structured shell-history events are emitted from staged input
4. **Provenance Chain** - every event references its source material origin

### Capture Integration

- **AppendStream-style staging** for:
  - Shell history files (`.bash_history`, `.zsh_history`, `fish_history`)
  - Fish SQLite history when the configured `fish_history` path is actually a SQLite store

### Event Types Generated

All events have `Provenance::Material` with references to Source Material:

- `terminal.bash_historical_command` - commands from bash history
- `terminal.zsh_historical_command` - commands from zsh history
- `terminal.fish_historical_command` - commands from fish history

## Scope Notes

Earlier direct-emission modules have been removed. The live terminal node now
captures shell-history materials first and emits events from that staged input.

Atuin historical import now belongs to the terminal node path itself rather than a
separate direct-write CLI. Kitty, recording, and richer terminal-session capture
are not wired through this node yet.
