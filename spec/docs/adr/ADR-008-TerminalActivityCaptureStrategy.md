# ADR-008: Terminal Activity Capture Strategy

*   **Status:** Accepted
*   **Date:** 2024-03-11
*   **Context & Problem Statement:**
    Capturing user activity within terminal emulators is crucial for the Exocortex, as much developer and power-user work occurs in the command line. The goal is to achieve comprehensive capture that includes not just executed commands but also their output, the surrounding terminal session context, and semantic information about the terminal environment itself. Different tools offer varying levels of fidelity and types of data.

*   **Discussed Options:**

    1.  **Shell History Files Only (e.g., `.bash_history`, `.zsh_history`):**
        *   **Description:** Rely on standard shell history mechanisms. An ingestor periodically parses these files.
        *   **Pros:** Simple, built-in to shells.
        *   **Cons:** Captures only command strings. Misses timestamps (often only coarsely logged), CWD, exit status, duration, command output, and any TUI interactions. Prone to data loss if history files are corrupted or not written (e.g., abnormal shell termination).

    2.  **Specific Terminal Emulator Protocol (e.g., Kitty Remote Control):**
        *   **Description:** Use an ingestor that interacts with a specific terminal emulator's advanced features (e.g., Kitty's remote control protocol via sockets or escape sequences).
        *   **Pros:** Can provide rich semantic information specific to that emulator: tab/window/split management, titles, CWD of active pane, potentially access to scrollback buffer, internal clipboard. Low latency for these specific events.
        *   **Cons:** Emulator-specific; does not work if the user uses a different terminal. Capturing shell commands executed *within* the emulator is often indirect (e.g., inferring from prompt changes or scrollback analysis) rather than a direct "command executed" event from the shell itself.

    3.  **PTY Session Recording (e.g., `script`, Asciinema):**
        *   **Description:** Wrap interactive shell sessions with a PTY logging tool (`script` command, `asciinema rec`). This captures the entire byte stream of terminal I/O (input and output) along with timing information.
        *   **Pros:** **Highest fidelity for replayability.** Captures everything that appears on the terminal, including command output, TUI application interactions, and exact visual rendering (text-based). Terminal-agnostic (wraps any shell).
        *   **Cons:** Output is a raw byte stream (or structured format like Asciinema's `.cast` JSONL). Requires parsing to extract structured commands or semantic information. Generated log files can become very large for long sessions. Does not inherently provide structured metadata like command exit status or precise CWD per command (though CWD might be inferred from output).

    4.  **Enhanced Shell History Tools (e.g., Atuin):**
        *   **Description:** Replace default shell history with a tool like Atuin, which stores command history in a structured local database (SQLite for Atuin).
        *   **Pros:** Captures rich, structured metadata for each command: full command string, timestamp, CWD, exit status, duration, host, session ID. Queryable history. Optional encrypted sync across devices. Shell-agnostic (supports Bash, Zsh, Fish).
        *   **Cons:** Only captures the commands themselves and their direct metadata, not their output or surrounding terminal interaction.

    5.  **eBPF for Low-Level Syscall Tracing:**
        *   **Description:** Use eBPF programs attached to kernel tracepoints or kprobes for syscalls like `execve`, `read`/`write` on TTY FDs, and `ioctl` for PTY control.
        *   **Pros:** Very low-level, can capture command executions (and arguments), TTY I/O, and terminal state changes (like window size) directly from the kernel. Potentially very comprehensive if all relevant syscalls are monitored.
        *   **Cons:** Highly complex to implement and maintain. Requires `CAP_SYS_ADMIN` or significant privileges. eBPF programs are kernel-version sensitive (though CO-RE mitigates this). Data correlation (e.g., linking specific `write` calls to a command's output) can be challenging. Generates a high volume of low-level data that needs significant processing. Overkill for many common terminal capture needs.

    6.  **Layered/Hybrid Approach (Combining Multiple Methods):**
        *   **Description:** Use a combination of the above methods to leverage their respective strengths and cover different aspects of terminal activity.
        *   **Example Combination:**
            *   Kitty Remote Control (if Kitty is used) for emulator-specific semantics.
            *   Atuin for structured command history across all shells.
            *   Asciinema (or `script`) for full PTY session replayability.
        *   **Pros:** Provides the most comprehensive and multi-faceted capture. Data from different sources can be correlated to build a richer understanding.
        *   **Cons:** Increases complexity of the ingestion layer (multiple ingestors/agents). Requires clear strategies for data correlation and deduplication (e.g., avoid storing command string twice if captured by both Atuin and Kitty/Asciinema analysis, but link them).

*   **Decision:**
    The Exocortex will adopt a **Layered/Hybrid Approach (Option 6)** for terminal activity capture to achieve maximum data richness, fidelity, and utility. The specific layers are:
    1.  **Atuin (Primary for Structured Command History):** Atuin will be configured for all supported shells (Bash, Zsh, Fish) to capture structured command history (command, timestamp, CWD, exit status, duration) into its local SQLite database. A dedicated Exocortex agent (`ingestor/atuin_db_reader`) will periodically ingest new command entries from this database into `raw.events` (e.g., as `shell.command.executed_atuin` events).
    2.  **PTY Session Recording (Primary for Full Session Replay - Asciinema or `script`):** All interactive shell sessions will be wrapped by a PTY recording tool (e.g., `asciinema rec`, or `script` with subsequent processing). This will capture the full textual I/O stream with timings. The resulting session recording files (`.cast` for Asciinema, or typescript files) will be stored as `core_blobs` (managed by `git-annex`). `terminal.session.started` and `terminal.session.ended` events will be logged, with the latter linking to the recording blob.
    3.  **Kitty Terminal Emulator Integration (Conditional, for Enhanced Semantics):** If the Kitty terminal emulator is detected as being used by the user, the `ingestor/kitty` (using Kitty's remote control protocol) will be activated. This ingestor will capture Kitty-specific semantic events: OS window/tab/Kitty window (pane) lifecycle and focus changes, title changes, CWD changes within Kitty panes, and potentially periodic scrollback buffer snapshots (stored as `core_blobs`). These events (e.g., `app.terminal.kitty.window_focused`, `app.terminal.kitty.scrollback_captured`) will supplement the Atuin and PTY recording data.
    4.  **(Future Enhancement/Specialized Option) eBPF Shell Monitoring:** eBPF-based capture of `execve` and TTY I/O remains a specialized, advanced option for very low-level auditing or specific diagnostic scenarios, but it is not part of the primary terminal capture strategy due to its complexity and privilege requirements.
    5.  **(Future Enhancement) Visual Screen Capture of Terminal:** As a supplementary layer for specific use cases (e.g., capturing TUIs that Asciinema might not render perfectly, or for user preference), visual screen capture (screenshots or video) of terminal windows can be considered. An intelligent pruning agent would aim to remove visual captures that are fully redundant with high-fidelity Asciinema/script logs for the same session to manage storage.

*   **Rationale for Decision:**
    1.  **Comprehensiveness:** This layered approach aims to capture the "what, when, where, how, and with what result" of terminal activity from multiple perspectives, providing both structured semantic data (Atuin, Kitty RC) and full textual replayability (Asciinema/`script`).
    2.  **Leveraging Best-of-Breed Tools:** Uses specialized tools (Atuin for command history, Asciinema for session recording, Kitty RC for emulator specifics) that are already robust and feature-rich in their respective domains.
    3.  **User Workflow Flexibility:** Supports users who may use different shells (via Atuin) or prefer Kitty for its specific features.
    4.  **Rich Data for Analysis & Agents:** The combined dataset enables powerful analysis (e.g., correlating command success/failure with surrounding output, understanding workflows within TUIs) and provides rich context for LLM agents.
    5.  **Redundancy as Strength:** Some overlap in captured data (e.g., command strings might appear in Atuin, Kitty scrollback, and Asciinema logs) can be used for cross-validation or to reconstruct information if one capture method fails. The primary storage cost for text is relatively low.

*   **Consequences:**
    *   Requires multiple ingestor components/agents: `ingestor/atuin_db_reader`, an agent to manage PTY session recording lifecycle and blob storage (e.g., `agent/terminal_session_logger`), and `ingestor/kitty`.
    *   User setup for Atuin hooks and PTY session wrapping scripts in shell profiles (`.zshrc`, `.bashrc`) is necessary.
    *   Strategies for correlating data from these different sources will be important (e.g., using session IDs, precise timestamps, CWD matching). The `payload._provenance.correlation_id` should ideally be propagated into terminal sessions if initiated by an Exocortex-aware action.
    *   Storage for PTY session recordings (`core_blobs` in `git-annex`) needs to be considered, though text compresses well. Policies for retention or summarization of very old session recordings might be needed in the long term.
    *   Parsing of Asciinema `.cast` files or `script` typescript files by downstream agents will be necessary if structured data (beyond just replay) is needed from them (e.g., extracting all output related to a specific command found in Atuin).

