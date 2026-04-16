# Unified Terminal node

This node currently captures shell-history data through the Stage-as-you-go pipeline:

- Bash history files
- Zsh history files
- Atuin `SQLite` history
- Explicitly configured SQLite-backed Fish history

Native Fish YAML history and Elvish's native database are not ingested.

## Architecture: Stage-as-you-go Capture

The live terminal node path flows through the Stage-as-you-go material pipeline:

1. **Source Material Capture** - raw history bytes are staged via the SDK
2. **Temporal Ledger** - ordered material slices preserve provenance
3. **Event Generation** - structured shell-history events are emitted from staged input
4. **Provenance Chain** - every event references its source material origin

### Capture Integration

- **AppendStream-style staging** for:
  - Shell history files (`.bash_history`, `.zsh_history`)
  - Atuin `SQLite` (`~/.local/share/atuin/history.db`)
  - SQLite-backed Fish history when a configured `fish_history` path is actually a `SQLite` store

### Event Types Generated

All events have `Provenance::Material` with references to Source Material:

- `source = shell.history`, `event_type = command.imported` for line-oriented Bash/Zsh/Fish-SQLite history rows
- `source = shell.atuin`, `event_type = command.executed` for Atuin command rows with working directory, exit code, and timing

## Scope Notes

Earlier direct-emission modules have been removed. The live terminal node now
captures shell-history materials first and emits events from that staged input.

Atuin historical import now belongs to the terminal node path itself rather than a
separate direct-write CLI. Kitty, recording, and richer terminal-session capture
are not wired through this node yet.
