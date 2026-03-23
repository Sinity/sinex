## Async-First Workflow

Schedule -> continue -> poll. Never run -> wait when you can work in parallel.

```bash
# After editing code:
xtask check --bg              # Returns job ID, continue working
# ... edit more files ...
xtask status --summary --json # Poll: did it pass?

# Parallel verification across packages:
xtask check --bg
xtask test --bg -p sinex-primitives
xtask test --bg -p sinex-db
# ... continue working ...
xtask jobs active             # What's still running?
xtask jobs output <ID>        # Get results when needed

# Before commit:
xtask check --full --bg       # Full validation
xtask jobs wait <ID>          # Block only when you need the final answer
```

### Decision Matrix

| Situation | Action |
|-----------|--------|
| Quick operation (< 5s) | Foreground |
| Need result for next step | Foreground with `--json` |
| Can work on other things | `--bg`, continue, poll later |
| Multiple independent tasks | Spawn all `--bg` in parallel |
| Interactive debugging | Foreground with `--debug` |

### Job Management

```bash
xtask jobs active              # Running jobs
xtask jobs list --json         # Recent history
xtask jobs status <ID> --json  # Specific job
xtask jobs output <ID>         # Full output
xtask jobs wait <ID>           # Block until done
```

Timeouts: check/build 30min, test 60min. Killed jobs get exit code 124.
