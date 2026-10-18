# Shell command grouping

Sinex groups Atuin `command.executed` and zsh `command.imported` events that describe the same command into one canonical `command.canonical` event. Indexed search, `query 'commands'`, sessions and the MCP tools read these canonical events. Grouping runs only when `read_model_connection_file` is configured ([search.md](search.md)).

## zsh source

```toml
sources = ["terminal.atuin-history", "terminal.zsh-history"]
read_model_connection_file = "/abs/read-model-connection"

[zsh]
path = "/abs/extended-history"
execution_host = "declared-execution-host"
protocol = "append_only_extended_v1"
poll_interval_ms = 30000   # 100..=3600000, default 30000
```

`protocol` must be `append_only_extended_v1`: the operator declares that the file is only ever appended to. Sinex checks a digest of the previously stored prefix on each read; a mismatch or truncation fences the input permanently, even if the old bytes come back. Reading the file again requires declaring a new input. `execution_host` is required (1-255 bytes). A missing file is reported unavailable and never created.

Only extended records (`: start:elapsed;command`) are read. Zsh metafication, escaped newlines and the trailing-backslash sentinel space are decoded after the original bytes are stored; the `zsh-extended-v2` interpretation removes exactly that one sentinel space. Elapsed zero never yields a duration or exit status. Invalid records are stored as record failures and keep zsh coverage incomplete, also after later clean polls. A trailing partial record waits for the next poll.

Each record is keyed by input and `(start_ns, command, n)`, where `n` counts repeated identical records. Its native label is `chain-record:SOURCE_OFFSET`.

Privacy rules cannot be configured together with the zsh source. Grouping also requires every parent to have been admitted without privacy rules; otherwise grouping reports unavailable and answers are partial. Replaying the Atuin material without rules restores comparability.

## Matching

The rule is [shell-rule.json](../crate/sinex-store/src/shell-rule.json). Two records match when their outer-trimmed command text and execution host are equal; inner whitespace and case matter. A group holds at most one record from each source.

1. Strong pass: pick globally shortest compatible pairs within 2 seconds (2,000,000,000 ns). Ties break on source order, then stored material digest and anchor.
2. Fallback pass: among records still unmatched, accept pairs within 60 seconds only when each side has exactly one candidate.
3. Every remaining record becomes a singleton.

Unknown times never match. The canonical event lists its exact parents, which parent supplied each field, conflicting claims and candidate counts. Atuin supplies cwd, exit status and duration when present. The time comes from the best-quality parent (then source order, then digest and anchor).

`show` and `trace` follow every parent to its stored record. `show.current_eligible` and `trace.current_eligible` say whether the canonical event is still current, which matters while a replacement is pending. Excluding an Atuin parent archives its canonical children right away; the zsh record remains as its own observation. Zsh replay and exclusion are not implemented.

## Maintenance

The writer records dirty work in the same transaction as admission, replay, removal and exclusion. Reconciliation loads the complete eligible population at a pinned commit position into PostgreSQL temporary tables and publishes results in turns of at most 64 groups, each turn in one transaction; an error or crash rolls back the whole turn. Restart rebuilds the temporary tables and reuses already committed identical outputs, keeping their IDs.

The writer runs at most two queued source or control commands, then one background turn, even when more commands are waiting; replay and grouping alternate within background turns. Shutdown stops admission and drains accepted commands without starting new background work.

While grouping work is pending or failed, reads that depend on it return the eligible rows with `complete: false` and the current phase or failure; `status` reports progress.
