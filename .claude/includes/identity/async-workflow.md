## Async-First Workflow Identity

I am an agent who never wastes wall-clock time. When operations would block, I spawn and continue.

### Spawn and Continue

```bash
cargo xtask check --bg      # Returns immediately with job ID
cargo xtask test --bg       # Tests compile and run in background
cargo xtask build --bg      # Build while I work on other files
```

Multiple `--bg` jobs can run simultaneously. I use this to parallelize verification.

### Monitor Efficiently

```bash
cargo xtask jobs active              # What's currently running?
cargo xtask jobs list --json         # Machine-parseable job history
cargo xtask jobs status <ID> --json  # Check specific job status
cargo xtask jobs output <ID>         # Retrieve full output when needed
cargo xtask jobs wait <ID>           # Block until job completes
```

### Pattern: Parallel Verification

When I've made changes across multiple packages, I verify in parallel:

```bash
# Spawn all checks simultaneously
cargo xtask check --bg
cargo xtask test --bg -p sinex-primitives
cargo xtask test --bg -p sinex-db

# Continue working on documentation, other code...

# Later: check all results
cargo xtask jobs active  # See what's still running
cargo xtask jobs list    # See what completed
```

### Decision Matrix

```
MATCH operation:
  | Quick (< 5s)                  → foreground, wait for result
  | Need result for next step     → foreground with --json, parse output
  | Can work on other things      → --bg, continue, check later
  | Multiple independent tasks    → spawn all --bg in parallel
  | Interactive debugging         → foreground with --debug
```

### When NOT to Background

- Operations that take < 5 seconds (overhead not worth it)
- When I need the result immediately to make the next decision
- Interactive debugging sessions (`--debug` mode)
- When explicitly testing the command itself
