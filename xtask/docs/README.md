# xtask - Development Task Automation

> **For AI Agents**: Use `--json` on any command for structured, machine-parseable output.
> This eliminates the need to parse human-readable text.

## Quick Reference

```bash
# Essential commands
xtask check                    # Fast: fmt + cargo check
xtask test                     # Run tests (retries enabled)
xtask test --debug             # Debug mode (single-threaded)
xtask db setup                 # Create database + migrate

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

# Combine with other tools
xtask check --json 2>&1 | tee result.json | jq -r '.status'
```

---

## Commands Reference

### Build & Check

| Command | Purpose | Exit Code |
|---------|---------|-----------|
| `xtask check` | cargo check only (~3s warm) | 0/1 |
| `xtask check --lint` | cargo check + clippy (~20s warm) | 0/1 |
| `xtask check --fmt` | cargo check + fmt --check | 0/1 |
| `xtask check --full` | fmt + clippy + forbidden (~25s warm) | 0/1 |
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
| `xtask test --affected` | Only changed packages |
| `xtask test --bench` | Run benchmark suite |
| `xtask test --coverage` | Run with coverage collection |

**Passing args to nextest:**

```bash
# Filter by package
xtask test -- -p sinex-primitives

# Filter by test name
xtask test -- -E 'test(my_test_name)'

# Combine filters
xtask test -- -p sinex-node-sdk -E 'test(unit::)'
```

### Coverage

| Command | Purpose |
|---------|---------|
| `xtask coverage html` | Generate HTML report |
| `xtask coverage html --open` | Generate and open in browser |
| `xtask coverage lcov` | Generate LCOV for CI |
| `xtask coverage summary` | Print summary to stdout |
| `xtask coverage enforce --threshold 80` | Assert minimum coverage % |
| `xtask coverage clean` | Remove coverage artifacts |

### Database

| Command | Purpose |
|---------|---------|
| `xtask db status` | Check Postgres connectivity |
| `xtask db setup` | Create database + migrate |
| `xtask db apply` | Apply declarative schema only |
| `xtask db reset --yes` | Drop + recreate (dangerous) |

### Contracts (Schema Management)

| Command | Purpose |
|---------|---------|
| `xtask contracts generate` | Generate JSON schemas from EventPayload types |
| `xtask contracts check-ready` | Verify core tables exist |
| `xtask contracts deploy` | Deploy schemas to database |
| `xtask contracts compat` | Validate schema contract changes |

### Environment & Status

| Command | Purpose |
|---------|---------|
| `xtask status` | Compact workspace status |
| `xtask status --summary` | One-line MOTD summary |
| `xtask status --doctor` | Full environment health check |
| `xtask status --pipelines` | Trigger pipeline smoke test |
| `xtask status --watch` | Continuous watch mode |

### Runtime

| Command | Purpose |
|---------|---------|
| `xtask run ingestd` | Run sinex-ingestd |
| `xtask run gateway` | Run sinex-gateway |
| `xtask run node <name>` | Run a specific node by name |
| `xtask run stack` | Run ingestd + gateway bundle |
| `xtask run all-ingestors` | Run all ingestor nodes |
| `xtask run all-automatons` | Run all automaton nodes |
| `xtask run list` | List all available binaries |
| `xtask run <cmd> --watch` | Hot-reload on source changes |
| `xtask run <cmd> --bg` | Run in background via job manager |
| `xtask run <cmd> --release` | Build in release mode |
| `xtask run <cmd> --dry-run` | Print command without executing |

### Jobs

| Command | Purpose |
|---------|---------|
| `xtask jobs list` | List all jobs |
| `xtask jobs active` | Show running jobs |
| `xtask jobs logs <id>` | Show job output |
| `xtask jobs kill <id>` | Terminate a job |

### Documentation

| Command | Purpose |
|---------|---------|
| `xtask docs build` | Build workspace docs with cargo doc |
| `xtask docs build --open` | Build and open in browser |
| `xtask docs build -p <crate>` | Build single crate docs |
| `xtask docs serve` | Serve docs locally (port 8080) |
| `xtask docs serve --port 9090` | Serve on custom port |

### Rarely-Used (xtr umbrella)

| Command | Purpose |
|---------|---------|
| `xtask xtr patterns -p '$X.unwrap()'` | AST-grep pattern search |
| `xtask xtr ci workspace` | Full CI pipeline |
| `xtask xtr ci postgres -- xtask test` | CI in Postgres container |
| `xtask xtr completions zsh` | Generate zsh completions |
| `xtask xtr completions bash` | Generate bash completions |
| `xtask xtr tls generate-dev-certs` | Generate dev TLS certificates |
| `xtask xtr tls check` | Verify TLS certificate validity |

### GitOps

| Command | Purpose |
|---------|---------|
| `xtask gitops status` | Show GitOps schema source status |
| `xtask gitops sync` | Trigger schema sync |

---

## Test Flags

Use xtask flags instead of nextest profiles:

| Flag | Purpose |
|------|---------|
| (none) | Standard runs with retries |
| `--debug` | Single-threaded, full output |
| `--prime` | Prime database template before testing |
| `--heavy` | Include `#[ignore]` tests |
| `--affected` | Only test changed packages |
| `--bench` | Run benchmark suite |
| `--coverage` | Collect coverage with llvm-cov |

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
xtask check && xtask test
```

### Full Validation

```bash
xtask xtr ci workspace
```

### Debug a Failing Test

```bash
xtask test --debug -- -E 'test(failing_test_name)'
```

### Check Environment Health

```bash
xtask status --doctor
```

### Run with Hot Reload

```bash
xtask run node terminal-ingestor --watch
```

### Background Job

```bash
xtask run ingestd --bg
xtask jobs active
```
