# TIM-GenericTerminalLogging: Asciinema and Atuin Integration

## Status Dashboard
**Maturity Level**: L4 - Implemented
**Implementation**: 95% (Asciinema and Atuin integration fully working)
**Dependencies**: asciinema binary, atuin binary, EventSource trait, shell history access
**Blocks**: Terminal session analysis, command pattern recognition, productivity insights

## MVP Specification
- Asciinema session recording integration
- Atuin command history ingestion
- Shell-agnostic command capture
- Session replay capability
- Structured command metadata

## Enhanced Features
- Real-time session streaming
- Advanced command categorization
- Shell prompt context extraction
- Cross-session correlation
- Privacy-aware filtering

## Implementation Checklist
- [x] Asciinema binary integration
- [x] Atuin command history ingestion
- [x] Shell history file monitoring
- [x] Session metadata capture
- [x] Command structure parsing
- [x] Real-time event generation
- [ ] Advanced session analysis
- [ ] Command categorization
- [ ] Privacy filtering rules

* **Relevant ADR:** `[ADR-008-TerminalActivityCaptureStrategy.md](docs/adr/ADR-008-TerminalActivityCaptureStrategy.md)` (Atuin & Asciinema are core layers)
* **Original UG Context:** Section 8.2

This TIM details the setup and integration of Asciinema (for full PTY session replay) and Atuin (for structured command history) as terminal-agnostic logging layers.

## 1. Rationale Summary

As per ADR-008, Asciinema provides complete textual replayability of terminal sessions, while Atuin offers rich, structured, queryable command history across all shells. They complement emulator-specific ingestors like the Kitty RC one.

## 2. Asciinema: PTY Session Recording [UG Sec 8.2.1, SA4]

### 2.1. Mechanism and Setup

* `asciinema rec [options] [filename]` records all terminal I/O (PTY master output byte stream) with timing.
* **Setup for Exocortex (Shell Profile Integration - e.g., `~/.zshrc` or `~/.bashrc`):**
    The goal is to automatically start `asciinema rec` for every interactive shell session.

    ```bash
    # In ~/.zshrc or ~/.bashrc
    # Ensure this block is sourced only for interactive shells with a TTY.
    if [[ -z "$SINEX_ASC_SESSION_ID" && -z "$ASCIINEMA_REC" && "$-" == *i* && -t 0 && -t 1 ]]; then
        # -z "$SINEX_ASC_SESSION_ID": Custom var to prevent re-invocation if script already set it.
        # -z "$ASCIINEMA_REC": Prevents re-invocation if already inside asciinema.
        # "$-" == *i*: Checks if shell is interactive.
        # -t 0 && -t 1: Check if stdin and stdout are TTYs.

        # Generate a ULID for this session using a helper CLI tool (assumed available)
        # Replace with actual path to a ULID generator (e.g., from a Rust crate CLI)
        export SINEX_TERMINAL_SESSION_ULID=$(/opt/sinex/bin/sinex_ulid_generator_cli || echo "dummy_ulid_$(date +%s%N)")

        # Define log directory and filename
        SINEX_SESSION_LOG_DIR="$HOME/.local/share/sinex/terminal_logs/asciinema_casts"
        mkdir -p "$SINEX_SESSION_LOG_DIR"
        SINEX_CAST_FILENAME="$SINEX_SESSION_LOG_DIR/${SINEX_TERMINAL_SESSION_ULID}.cast"

        # Get terminal emulator info if possible
        TERMINAL_EMULATOR="${TERM_PROGRAM:-unknown}"
        if [[ -n "$KITTY_PID" ]]; then # Kitty specific
            TERMINAL_EMULATOR="kitty"
        fi

        # Log session start event to Exocortex (e.g., via exo CLI)
        # This requires 'exo' to be in PATH and configured.
        # The payload should ideally be structured JSON.
        # Ensure this logging call doesn't itself trigger another wrapper instance (e.g., if 'exo' spawns a shell).
        # A safer way is for a dedicated agent to monitor the LOG_DIR for new files later.
        # For now, conceptual logging:
        # ( /opt/sinex/bin/exo log terminal.session started \
        #    --payload-json "{\"session_id_ulid\":\"$SINEX_TERMINAL_SESSION_ULID\", \"type\":\"asciinema\", \"terminal_emulator\":\"$TERMINAL_EMULATOR\", \"shell\":\"$SHELL\", \"pty_device\":\"$(tty)\"}" \
        #    --ts_orig "$(date --iso-8601=seconds)" & ) > /dev/null 2>&1 # Run in background, suppress output


        # Start asciinema recording, replacing the current shell process
        # Ensure 'asciinema' binary is in PATH (e.g., via NixOS environment)
        if command -v asciinema >/dev/null; then
            # --quiet: Suppress asciinema's own start/stop messages
            # --title: Add metadata to the cast header
            # The final 'exit $?' ensures the exit code of the recorded shell session is propagated.
            # exec prevents the rest of the .zshrc/.bashrc from running again in the sub-shell.
            echo "Starting Asciinema recording for session $SINEX_TERMINAL_SESSION_ULID to $SINEX_CAST_FILENAME..." >&2
            exec asciinema rec --quiet --title "Sinex Session $SINEX_TERMINAL_SESSION_ULID" "$SINEX_CAST_FILENAME"
        else
            echo "Warning: asciinema command not found. Terminal session will not be recorded by asciinema." >&2
            # Export the variable anyway so subsequent checks in this script know it was attempted
            export SINEX_ASC_SESSION_ID="$SINEX_TERMINAL_SESSION_ULID"
        fi
    fi
    ```

  * **Robustness:** A more robust setup might involve a dedicated `login_shell_wrapper` script that handles ULID generation, logging the `terminal.session.started` event (e.g., via a small utility that directly inserts into DB or calls an ingest API, to avoid complex shell scripting for JSON payloads), and then `exec`s `asciinema rec`. This wrapper would be set as the user's login shell or invoked by the terminal emulator.
  * **Alternative Tool:** `script` command with `scriptreplay` can be used if Asciinema is not desired. `script -t 2> timing_file.time -a output_typescript_file.session`. The `timing_file.time` and `output_typescript_file.session` are then stored.

### 2.2. Asciinema `.cast` File Format (Version 2) [SA4]

* JSONL (JSON Lines) format.
* **Header Line (First Line):** JSON object.

    ```json
    // {
    //   "version": 2,
    //   "width": 120, // Terminal columns
    //   "height": 30, // Terminal rows
    //   "timestamp": 1678886400, // Unix epoch (integer seconds) of session start
    //   "title": "Sinex Session ULID_XYZ", // Optional
    //   "env": {
    //     "SHELL": "/bin/zsh",
    //     "TERM": "xterm-kitty"
    //   }
    //   // "theme": { ... } // Optional color theme
    // }
    ```

* **Event Lines (Subsequent Lines):** Arrays `[time_delta_sec, event_type_char, event_data_string]`
  * `time_delta_sec`: Float, time since the *previous event line's timestamp* (not since session start).
  * `event_type_char`: `"o"` for output to PTY, `"i"` for input to PTY (if `--stdin` recording enabled, rare).
  * `event_data_string`: UTF-8 encoded text chunk that was output/input.
  * Example: `[0.015678, "o", "hello world\r\n"]`

### 2.3. Eventification and Storage

An Exocortex agent (`agent/terminal_session_logger` or `ingestor/asciinema_log_processor`) monitors the Asciinema log directory (e.g., `~/.local/share/sinex/terminal_logs/asciinema_casts`).

1. **On New `.cast` File Creation (or Initial Detection):**
    * The agent detects a new `.cast` file (e.g., via `inotify` on the directory, or periodic scan). The filename is the `SINEX_TERMINAL_SESSION_ULID`.
    * It parses the header line of the `.cast` file.
    * Emits `terminal.session.started` event to `raw.events`.
        * `source`: `"agent.terminal_session_logger"`
        * `event_type`: `"session_started"`
        * `payload`: `{ "session_id_ulid": "ULID_from_filename", "recording_tool": "asciinema", "terminal_emulator_name": "kitty" (from header or env), "shell_path": "/bin/zsh" (from header or env), "initial_width": 120, "initial_height": 30, "start_ts_iso": "ISO8601_from_header_timestamp", "pty_device": "/dev/pts/X" (if available from shell wrapper script env) }`
2. **On `.cast` File Finalization (Session Ends):**
    * The agent detects the session has ended (e.g., `asciinema rec` process exits, or by timeout if file not modified and header indicated active session).
    * The complete `.cast` file is added to `git-annex` via `core_blobs`.
        * `core_blobs.content_annex_key` stores the annex key.
        * `core_blobs.content_blake3_hash` stores BLAKE3 of the `.cast` file.
        * `core_blobs.mime_type` set to `application/x-asciicast` or `application/jsonl`.
    * Emits `terminal.session.ended` event to `raw.events`.
        * `source`: `"agent.terminal_session_logger"`
        * `event_type`: `"session_ended"`
        * `payload`: `{ "session_id_ulid": "ULID_from_filename", "duration_seconds": N (calculated from event lines or start/end events), "end_ts_iso": "...", "recording_blob_annex_key": "key_for_cast_file_in_annex", "recording_content_hash_blake3": "hash_of_cast_file" }`
3. **Downstream Processing:** Other agents can later retrieve the `.cast` blob and parse its event lines to extract full command outputs, correlate with Atuin command entries, or build TUIs interaction models.

## 3. Atuin: Structured Command History [UG Sec 8.2.2, SA4]

### 3.1. Mechanism and Setup

* Atuin replaces default shell history with a local SQLite DB (default: `~/.local/share/atuin/history.db`).
* Logs: command string, timestamp, CWD, exit status, duration, host, Atuin session ID.
* **Setup (Shell Profile - e.g., `~/.zshrc`, `~/.bashrc`):**
    Add to the end of the shell config file:

    ```bash
    # Ensure 'atuin' binary is in PATH
    if command -v atuin >/dev/null; then
        # The specific command depends on your shell (zsh, bash, fish)
        # For Zsh:
        eval "$(atuin init zsh --disable_up_arrow)" # Optionally disable up-arrow to use Atuin's UI exclusively
        # For Bash:
        # eval "$(atuin init bash --disable_up_arrow)"
        # For Fish:
        # atuin init fish | source

        # Optional: Import existing shell history on first setup
        # Consider running this manually once, not in every shell init.
        # atuin import auto
    else
        echo "Warning: atuin command not found. Enhanced command history will not be available." >&2
    fi
    ```

### 3.2. Ingestion into Exocortex

An agent (`ingestor/atuin_db_reader`, e.g., Rust or Python with SQLite bindings) periodically syncs new command history from Atuin's SQLite DB.

1. **Watermarking:** The agent maintains a watermark of the last processed `id` (Atuin's auto-incrementing PK in its `history` table) or `timestamp` from the Atuin DB.
2. **Query Atuin DB:** Connects to `~/.local/share/atuin/history.db` (read-only).

    ```sql
    -- Example query for Atuin DB (schema may vary slightly with Atuin versions)
    SELECT
        id,         -- Atuin's internal history ID
        timestamp,  -- Nanosecond precision Unix timestamp (integer)
        duration,   -- Command duration in nanoseconds (integer)
        exit_code,  -- Exit status
        command,    -- Full command string
        cwd,        -- Current working directory
        session,    -- Atuin's session ID (string)
        hostname    -- Hostname where command was run
    FROM history
    WHERE id > ? -- ? is the last_processed_atuin_id
    ORDER BY id ASC
    LIMIT 100; -- Process in batches
    ```

3. **Eventification:** For each new Atuin history entry:
    * Emit `shell.command.executed_atuin` event to `raw.events`.
    * `source`: `"ingestor.atuin_db_reader"`
    * `event_type`: `"command_executed"` (or `shell.command.executed_atuin` for clarity)
    * `ts_orig`: Convert Atuin's nanosecond `timestamp` to `TIMESTAMPTZ`.
    * `host`: Use `hostname` from Atuin DB (should match Exocortex `host`).
    * `payload`:

        ```json
        // {
        //   "command_string": "full command text",
        //   "cwd": "/current/working/directory",
        //   "exit_code": 0,
        //   "duration_ns": 1234567890, // Nanoseconds
        //   "atuin_history_id": 12345, // Atuin's own DB ID for this entry
        //   "atuin_session_id": "atuin_session_uuid_or_string",
        //   "terminal_session_ulid": "ULID_from_SINEX_TERMINAL_SESSION_ULID_env_var" // If available and logged by Atuin
        // }
        ```

        * **Correlation with Asciinema Session:** If the `SINEX_TERMINAL_SESSION_ULID` environment variable (set by the Asciinema wrapper script) can be captured by Atuin (e.g., Atuin might have features to log specific environment variables or its session ID can be mapped), this ULID should be included in the payload for direct correlation. Otherwise, temporal proximity and `host`/`cwd` matching will be used.
4. **Update Watermark:** After successful batch processing, update the agent's `last_processed_atuin_id`.

## 4. Recommended Combined Approach [UG Sec 8.2.3, SA4, ADR-008]

Layered capture:

1. **Atuin:** Primary source for structured command history.
2. **Asciinema (or `script`):** Primary source for full session textual I/O and replayability.
3. **Kitty RC Ingestor (if Kitty used):** For Kitty-specific semantic events (see `TIM-KittyTerminalIntegration.md`).

Data from these sources is correlated in the Exocortex backend using timestamps, `host`, `cwd`, `session_id_ulid` (if linkable), and content analysis to build a comprehensive picture of terminal activity.
