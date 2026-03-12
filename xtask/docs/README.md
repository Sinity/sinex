# xtask - Development Task Automation

> **For AI Agents**: Use `--json` on any command for structured, machine-parseable output.
> This eliminates the need to parse human-readable text.

## Quick Reference

```bash
# Essential commands
xtask check                    # Fast compile check
xtask check --lint             # Compile + clippy
xtask check --full             # fmt + clippy + forbidden
xtask test                     # Run tests (retries enabled)
xtask fix                      # Auto-fix formatting + clippy

# With JSON output (recommended for agents)
xtask check --json             # {"command":"check","status":"success",...}
xtask test --json | jq '.status'
```

---

## Machine-Readable Output

### Global Flags

| Flag | Description |
|------|-------------|
| `--json` | Output structured JSON (shorthand for `--format json`) |
| `--format <FORMAT>` | Output format: `human` (default), `json`, `compact`, `silent` |
| `--bg` | Run in background; returns immediately with a job ID |

### JSON Schema

**Successful command:**

```json
{
  "command": "check",
  "status": "success",
  "duration_secs": 0.52,
  "timestamp": "2026-01-20T23:46:03Z"
}
```

**Failed command:**

```json
{
  "command": "check",
  "status": "failed",
  "duration_secs": 1.21,
  "timestamp": "2026-01-20T23:46:03Z",
  "errors": [
    {
      "code": "CMD_FAILED",
      "message": "cargo fmt --check failed with status exit status: 1"
    }
  ]
}
```

### Status Values

| Status | Meaning |
|--------|---------|
| `success` | Command completed without errors |
| `failed` | Command failed |
| `partial` | Some subtasks failed |
| `running` | Async operation in progress |
| `cancelled` | Operation was cancelled |

### Agent Integration Examples

```bash
# Check if build passes
if xtask check --json | jq -e '.status == "success"' > /dev/null; then
    echo "Build OK"
fi

# Extract duration
xtask test --json | jq '.duration_secs'

# Get error message on failure
xtask test --json | jq -r '.errors[0].message // "No errors"'
```

---

## Commands Reference

### Build & Check

| Command | Purpose | Exit Code |
|---------|---------|-----------|
| `xtask check` | cargo check only (~3s warm) | 0/1 |
| `xtask check --lint` | cargo check + clippy (~20s warm) | 0/1 |
| `xtask check --full` | fmt + clippy + forbidden (~25s warm) | 0/1 |
| `xtask check --fix` | auto-fix then full check | 0/1 |
| `xtask fix` | Auto-fix fmt + clippy | 0/1 |
| `xtask build` | Build packages | 0/1 |
| `xtask build --affected` | Build only changed packages | 0/1 |

### Testing

| Command | Purpose |
|---------|---------|
| `xtask test` | Run tests (retries enabled) |
| `xtask test --debug` | Single-threaded, full output |
| `xtask test --prime` | Prime database pool before tests |
| `xtask test --heavy` | Include `#[ignore]` tests |
| `xtask test -p <pkg>` | Only the named package |
| `xtask test -E 'test(name)'` | Filter by nextest expression |
| `xtask test bench` | Run benchmark suite |
| `xtask test coverage` | Run with coverage collection |
| `xtask test fuzz` | Discover / run fuzz targets |
| `xtask test mutants` | Mutation testing |
| `xtask test vm` | NixOS VM smoke tests |

### Environment & Status

| Command | Purpose |
|---------|---------|
| `xtask status` | Compact workspace status |
| `xtask status --summary` | One-line MOTD summary |
| `xtask status --watch` | Continuous watch mode |
| `xtask doctor` | Full environment health check |
| `xtask doctor --fix` | Auto-remediate missing infra |
| `xtask doctor --runtime` | Runtime health (ingestd heartbeat, lag, latency) |

### Runtime

| Command | Purpose |
|---------|---------|
| `xtask run ingestd` | Run sinex-ingestd |
| `xtask run gateway` | Run sinex-gateway |
| `xtask run node <name>` | Run a specific node by name |
| `xtask run core` | Run ingestd + gateway bundle |
| `xtask run all-ingestors` | Run all ingestor nodes |
| `xtask run all-automatons` | Run all automaton nodes |
| `xtask run list` | List all available binaries |
| `xtask run <cmd> --watch` | Hot-reload on source changes |
| `xtask run <cmd> --bg` | Run in background via job manager |
| `xtask run <cmd> --release` | Build in release mode |

### Jobs

| Command | Purpose |
|---------|---------|
| `xtask jobs list` | List all jobs |
| `xtask jobs active` | Show running jobs |
| `xtask jobs output <id>` | Show job output |
| `xtask jobs status <id>` | Show job status |
| `xtask jobs wait <id>` | Block until job completes |

### History

| Command | Purpose |
|---------|---------|
| `xtask history list` | Recent invocations |
| `xtask history diagnostics` | Current diagnostics (package-scoped) |
| `xtask history tests failures` | Failing tests from last run |
| `xtask history tests analyze` | Comprehensive test analysis |
| `xtask history tests slowest` | Slowest tests |
| `xtask history stats --command check` | Command statistics |

### Documentation

All documentation generation lives under the `docs` family:

| Command | Purpose |
|---------|---------|
| `xtask docs build` | Build workspace docs with cargo doc |
| `xtask docs build --open` | Build and open in browser |
| `xtask docs build -p <crate>` | Build single crate docs |
| `xtask docs serve` | Serve docs locally (port 8080) |
| `xtask docs serve --port 9090` | Serve on custom port |
| `xtask docs agents` | Generate AGENTS.md from CLAUDE.md |
| `xtask docs snapshot` | Codebase snapshot for AI context (repomix) |
| `xtask docs snapshot --compress` | Tree-sitter structure extraction |
| `xtask docs snapshot --changed` | Include git-changed files |
| `xtask docs snapshot --context` | Inject xtask state block |
| `xtask docs snapshot --scope <crate>` | Scope to a crate + its deps |

### Infrastructure

| Command | Purpose |
|---------|---------|
| `xtask infra start` | Start Postgres + NATS |
| `xtask infra stop` | Stop infrastructure |
| `xtask infra status` | Show infrastructure status |
| `xtask reset --yes` | Full developer state wipe |
| `xtask reset --yes --db` | Drop + recreate database only |
| `xtask reset --yes --nats` | Wipe NATS JetStream data only |

### Analytics

| Command | Purpose |
|---------|---------|
| `xtask analytics workspace-health` | Composite health score (0–100) |
| `xtask analytics hotspots` | Most active recurring diagnostics |
| `xtask analytics reliability` | Test pass rates / flakiness |
| `xtask analytics velocity` | Build + test time trends |
| `xtask analytics recommend` | Actionable heuristic recommendations |

### Deps

| Command | Purpose |
|---------|---------|
| `xtask deps tree [pkg]` | Dependency tree |
| `xtask deps duplicates` | Duplicate versions |
| `xtask deps unused` | Unused dependencies |
| `xtask deps timings` | Build time analysis |
| `xtask deps impact [pkg]` | Rebuild impact analysis |

### Privacy

| Command | Purpose |
|---------|---------|
| `xtask privacy catalog` | List all privacy rules |
| `xtask privacy test "text"` | Test text against privacy engine |
| `xtask privacy decrypt <TOKEN>` | Decrypt a privacy token |

---

## Test Flags

Use xtask flags; `-p` and `-E` are first-class (never `-- -p`):

| Flag | Purpose |
|------|---------|
| (none) | Standard run with retries |
| `--debug` | Single-threaded, full output |
| `--prime` | Prime database template before testing |
| `--heavy` | Include `#[ignore]` tests |
| `--all` | All packages (not just affected) |
| `-p <pkg>` | Single package |
| `-E 'expr'` | Nextest filter expression |
| `--update-snapshots` | Sets INSTA_UPDATE=always |

---

## Environment Variables

xtask reads configuration from environment (typically set by devenv):

| Variable | Purpose | Default |
|----------|---------|---------|
| `DATABASE_URL` | PostgreSQL connection | - |
| `SINEX_NATS_URL` | NATS server URL | - |
| `SINEX_STATE_DIR` | State directory | `<repo>/.sinex/state` |
| `SINEX_CACHE_DIR` | Cache directory | `<repo>/.sinex/cache` |
| `SINEX_TEST_RESULTS_DIR` | Test results directory | `<repo>/.sinex/cache/test-results` |
| `SINEX_DEVENV_TOOLCHAIN` | Toolchain identifier | - |
| `SINEX_PG_BIN` | PostgreSQL binary prefix | - |
| `NATS_SERVER_BIN` | NATS server binary path | - |

---

## Common Workflows

### Pre-commit Check

```bash
xtask check --full && xtask test
```

### Full Validation

```bash
xtask ci workspace
```

### Debug a Failing Test

```bash
xtask test --debug -E 'test(failing_test_name)'
```

### Check Environment Health

```bash
xtask doctor
```

### Run with Hot Reload

```bash
xtask run node terminal-ingestor --watch
```

### Background Job

```bash
xtask check --bg
xtask jobs active
xtask jobs output <id>
```

### AI Context Snapshot

```bash
xtask docs snapshot                        # Full workspace snapshot
xtask docs snapshot --changed --context    # Changed files + xtask state
xtask docs snapshot --scope sinex-db       # Scoped to crate + deps
```
