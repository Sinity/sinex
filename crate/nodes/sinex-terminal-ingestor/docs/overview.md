# Unified Terminal node

This node captures terminal-related data through the Stage-as-you-go pipeline:

- Atuin database monitoring with `AcquisitionManager` + `StageAsYouGoContext`
- Shell history file monitoring with streaming material capture
- Terminal recordings with staged blobs and temporal ledger slices
- Kitty terminal integration with staged material + event emission

## Architecture: Stage-as-you-go Capture

All terminal data flows through the Stage-as-you-go material pipeline:

1. **Source Material Capture** – raw terminal data staged via the SDK
2. **Temporal Ledger** – ordered material slices with provenance tracking
3. **Event Generation** – events created from Source Material with provenance
4. **Provenance Chain** – each event references its Source Material origin

### Capture Integration

- **AppendStream-style staging** for:
  - Atuin SQLite database monitoring
  - Shell history files (`.bash_history`, `.zsh_history`, `fish_history`)
  - Kitty remote control socket monitoring

- **TreeWatch-style staging** for:
  - Terminal recording directories (asciinema `.cast` files)

### Event Types Generated

All events have `Provenance::Material` with references to Source Material:

- `terminal.atuin_command_executed` – commands from the Atuin database
- `terminal.bash_historical_command` – commands from bash history
- `terminal.zsh_historical_command` – commands from zsh history
- `terminal.fish_historical_command` – commands from fish history
- `terminal.recording_started` – Asciinema recording begins
- `terminal.recording_ended` – Asciinema recording completes
- `terminal.kitty_window_state` – Kitty window/tab state
- `terminal.kitty_content_captured` – Kitty scrollback content

## Direct Event Creation Removed

Previous modules (`atuin.rs`, `kitty.rs`, `recording.rs`, `scrollback.rs`, `history.rs`)
that created events directly have been removed. All terminal data capture now flows
through Stage-as-you-go material capture first.
