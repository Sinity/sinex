## Extended Commands

### sinexctl (User-Facing CLI)

```bash
# Context recovery ("what was I doing?")
sinexctl context                         # Recent session summary by source
sinexctl context --since 4h              # Wider time window
sinexctl report today                    # Daily summary with hourly heatmap
sinexctl report yesterday                # Yesterday's summary

# Telemetry (reads continuous aggregates)
sinexctl telemetry window-focus          # Window focus patterns
sinexctl telemetry command-frequency     # Shell command frequency
sinexctl telemetry file-activity         # Filesystem events
sinexctl telemetry recent-activity       # Cross-source recent activity
sinexctl telemetry system-state          # CPU, memory, systemd

# Querying and tracing
sinexctl query --source fs-watcher       # Events by source
sinexctl query --text-search "cargo"     # Full-text search
sinexctl trace <event-id>               # Provenance chain
sinexctl trace <event-id> -f dot        # Graphviz output

# Operations
sinexctl gateway ingest --source test --event-type test.ping --payload '{}'  # gateway -> NATS -> ingestd smoke event
sinexctl import atuin                    # Import Atuin history
sinexctl import atuin --resume           # Resume interrupted import
sinexctl watch                           # Live SSE event stream
sinexctl tui                             # Interactive TUI dashboard
sinexctl status                          # System health
sinexctl recent                          # Last hour of events

# Replay
sinexctl replay plan --node <id>         # Plan replay
sinexctl replay preview <op-id>          # Preview cascade (with safety analysis)
sinexctl lifecycle archive <event-id>    # Archive events
```

### VM Testing

```bash
xtask test vm --category smoke           # Fast (~5-10min): basic, flow, replay
xtask test vm --category integration     # Full: preflight, nodes, failure-recovery, e2e
xtask test vm --category performance     # icount-deterministic, production-scale
xtask test vm --category chaos           # Network partition, process restart, clock skew
xtask test vm --list                     # Show all tests by category
xtask test vm --parallel                 # Parallel execution
```

### Other xtask Commands

```bash
# Documentation
xtask docs build --open                  # Generate and open rustdoc
xtask docs snapshot --scope sinex-db     # AI context snapshot scoped to crate

# Privacy
xtask privacy test "some text"           # Test against privacy engine
xtask privacy catalog                    # List all rules

# Self-validation
xtask exercise --tier 1                  # Quick checks
xtask exercise --all                     # All 65 exercises across 4 tiers

# Completions
xtask completions zsh > ~/.zsh/completions/_xtask
```
