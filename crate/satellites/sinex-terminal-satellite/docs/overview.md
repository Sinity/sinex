# Unified Terminal Satellite

This satellite captures terminal-related data through the sensd Source Material system:

- Atuin database monitoring via sensd `AppendStream`
- Shell history file monitoring via sensd `AppendStream`
- Terminal recordings via sensd `TreeWatch`
- Kitty terminal integration via sensd `AppendStream`

## Architecture: sensd-First Data Capture

All terminal data flows through the sensd Source Material system:

1. **Source Material Capture** – raw terminal data captured by sensd sensors
2. **Temporal Ledger** – ordered material slices with provenance tracking
3. **Event Generation** – events created from Source Material with proper provenance
4. **Provenance Chain** – each event references its Source Material origin

### sensd Sensor Integration

- **AppendStream sensors** for:
  - Atuin SQLite database monitoring
  - Shell history files (`.bash_history`, `.zsh_history`, `fish_history`)
  - Kitty remote control socket monitoring

- **TreeWatch sensors** for:
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

## Eliminated Direct Event Creation

Previous modules (`atuin.rs`, `kitty.rs`, `recording.rs`, `scrollback.rs`, `history.rs`)
that created events directly have been removed. All terminal data capture now flows
through the sensd Source Material system first.
