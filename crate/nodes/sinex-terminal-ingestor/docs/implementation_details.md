# Terminal Ingestor Implementation Details

The Terminal Ingestor captures shell command history from multiple sources, providing a high-fidelity audit trail of terminal activity with built-in privacy protections.

## History Tailing

- **Incremental Parsing**: Monitors text-based history files (e.g., `.bash_history`, `.zsh_history`) and tracks the byte offset and line count to ensure only new commands are processed.
- **Fish SQLite Support**: includes a specialized parser for the Fish shell's SQLite-based history format, using ROWID for reliable incremental reading.
- **Checkpoint Persistence**: Last-read positions are persisted to disk using an atomic write pattern (temp file + rename) to ensure resumption after restarts.

## Secret Redaction

To prevent sensitive credentials from being stored in the Sinex database, the ingestor applies a suite of regex-based redaction patterns before content is persisted:
- **AWS Access Keys**: Identifies and masks standard AWS credential formats.
- **URL Credentials**: Redacts passwords from URI schemes (e.g., `https://user:pass@host`) while preserving the host and username context.
- **Private Key Headers**: Detects PEM-encoded private key starts.
- **CLI Flags**: Specifically targets common command-line secrets like `--password` or `--token`.

## Performance & Safety

- **Zero-Copy Redaction**: Uses `Cow<'a, str>` to avoid allocations when a command doesn't match any sensitive patterns.
- **Binary Data Rejection**: Commands containing null bytes or excessive control characters are rejected to prevent corrupted data from entering the pipeline.
- **Size Limits**: Enforces a per-command size limit (default 32KB) to protect against resource exhaustion attacks via malformed history entries.
