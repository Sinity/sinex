# xtask - Development Task Automation

> **For AI Agents**: Use `--json` on any command for structured, machine-parseable output.
> This eliminates the need to parse human-readable text.

## Quick Reference

```bash
# Essential commands
xtask check                    # Fast: fmt + cargo check
xtask lint                     # Clippy with -D warnings
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
| `partial` | Some subtasks failed (future) |
| `running` | Async operation in progress (future) |
| `cancelled` | Operation was cancelled (future) |

### Agent Integration Examples

```bash
# Check if build passes
if xtask check --json | jq -e '.status == "success"' > /dev/null; then
    echo "Build OK"
fi

# Extract duration
xtask test --profile fast --json | jq '.duration_secs'

# Get error message on failure
xtask lint --json | jq -r '.errors[0].message // "No errors"'

# Combine with other tools
xtask check --json 2>&1 | tee result.json | jq -r '.status'
```

---

## Commands Reference

### Build & Check

| Command | Purpose | Exit Code |
|---------|---------|-----------|
| `xtask check` | fmt --check + cargo check | 0/1 |
| `xtask check --skip-fmt` | Skip format check | 0/1 |
| `xtask check --skip-check` | Skip cargo check | 0/1 |
| `xtask lint` | Clippy with -D warnings | 0/1 |
| `xtask lint-forbidden` | Scan for forbidden patterns | 0/1 |

### Testing

| Command | Purpose |
|---------|---------|
| `xtask test` | Run tests (retries enabled) |
| `xtask test --debug` | Single-threaded, full output |
| `xtask test --prime` | Prime database pool before tests |
| `xtask test --heavy` | Include `#[ignore]` tests |
| `xtask test --affected` | Only changed packages |

**Passing args to nextest:**

```bash
# Filter by package
xtask test -- -p sinex-primitives

# Filter by test name
xtask test -- -E 'test(my_test_name)'

# Combine filters
xtask test -- -p sinex-node-sdk -E 'test(unit::)'
```

### Database

| Command | Purpose |
|---------|---------|
| `xtask db status` | Check Postgres connectivity |
| `xtask db setup` | Create database + migrate |
| `xtask db migrate` | Apply migrations only |
| `xtask db reset --yes` | Drop + recreate (dangerous) |

### Schema Management

| Command | Purpose |
|---------|---------|
| `xtask schema generate` | Generate JSON schemas from EventPayload types |
| `xtask schema check-ready` | Verify core tables exist |
| `xtask schema deploy` | Deploy schemas to database |
| `xtask schema compat` | Check backward compatibility |

### Environment & CI

| Command | Purpose |
|---------|---------|
| `xtask doctor` | Environment health check |
| `xtask doctor --pipelines` | Include pipeline smoke test |
| `xtask ci-preflight` | Full pre-merge validation |
| `xtask ci workspace` | Full CI pipeline |

### Coverage

| Command | Purpose |
|---------|---------|
| `xtask coverage html` | Generate HTML report |
| `xtask coverage html --open` | Generate and open in browser |
| `xtask coverage lcov` | Generate LCOV for CI |
| `xtask coverage summary` | Print summary to stdout |
| `xtask coverage clean` | Remove coverage artifacts |

### Benchmarking

Use `xtask test --bench` to run benchmarks.

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

---

## Environment Variables

xtask reads configuration from environment (typically set by devenv):

| Variable | Purpose | Default |
|----------|---------|---------|
| `DATABASE_URL` | PostgreSQL connection | - |
| `SINEX_NATS_URL` | NATS server URL | - |
| `SINEX_STATE_DIR` | State directory | `~/.local/state/sinex` |
| `SINEX_CACHE_DIR` | Cache directory | `~/.cache/sinex` |
| `SINEX_TEST_RESULTS_DIR` | Test results directory | - |
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
xtask ci-preflight
```

### Debug a Failing Test

```bash
xtask test --debug -- -E 'test(failing_test_name)'
```

### Check Environment Health

```bash
xtask doctor --pipelines
```
