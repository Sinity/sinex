## Async-First Workflow

Schedule -> continue -> poll. Never run -> wait when you can work in parallel.

### History Is Evidence, Not Cache

`$SINEX_STATE_DIR/xtask-history.db` is the accumulated development-loop
observability dataset for this checkout. Treat it as durable project evidence:
use it to analyze wall-clock time, test failures, diagnostics, and resource
patterns, but do not classify it as disposable cache or delete it during
performance cleanup. If history itself appears to slow a workflow down,
investigate query shape, indexes, WAL behavior, compaction, or archival strategy
with measurements; preserve the dataset first.

Disposable development cache lives under `$SINEX_CACHE_DIR` / `CARGO_TARGET_DIR`,
not under `$SINEX_STATE_DIR`.

**Where these actually resolve matters.** `$SINEX_STATE_DIR` is durable
checkout-local state and should resolve to `<checkout>/.sinex/state` on
`sinnix-prime`; `xtask-history.db` lives there. The sinnix devshell hook
relocates build cache and runtime scratch to
`/var/cache/sinex/<user>/<checkout-hash>/` (`SINEX_CACHE_DIR`,
`CARGO_TARGET_DIR`, `SINEX_DEV_STATE_DIR`, Postgres socket, NATS state), but it
must not relocate the history DB into that cache-shaped tree. Resolve the real
path with `echo $SINEX_STATE_DIR` before doing history work; if it points under
`/var/cache/sinex/.../dev-state/state`, reload the devshell before running
xtask.

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
