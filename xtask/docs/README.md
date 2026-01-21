# xtask - Development Task Automation

> **For AI Agents**: Use `--json` on any command for structured, machine-parseable output.
> This eliminates the need to parse human-readable text.

## Quick Reference

```bash
# Essential commands
cargo xtask check                    # Fast: fmt + cargo check
cargo xtask lint                     # Clippy with -D warnings
cargo xtask test --profile default   # Run tests (default profile)
cargo xtask db setup                 # Create database + migrate

# With JSON output (recommended for agents)
cargo xtask check --json             # {"command":"check","status":"success",...}
cargo xtask test --profile fast --json | jq '.status'
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
if cargo xtask check --json | jq -e '.status == "success"' > /dev/null; then
    echo "Build OK"
fi

# Extract duration
cargo xtask test --profile fast --json | jq '.duration_secs'

# Get error message on failure
cargo xtask lint --json | jq -r '.errors[0].message // "No errors"'

# Combine with other tools
cargo xtask check --json 2>&1 | tee result.json | jq -r '.status'
```

---

## Commands Reference

### Build & Check

| Command | Purpose | Exit Code |
|---------|---------|-----------|
| `cargo xtask check` | fmt --check + cargo check | 0/1 |
| `cargo xtask check --skip-fmt` | Skip format check | 0/1 |
| `cargo xtask check --skip-check` | Skip cargo check | 0/1 |
| `cargo xtask lint` | Clippy with -D warnings | 0/1 |
| `cargo xtask lint-forbidden` | Scan for forbidden patterns | 0/1 |

### Testing

| Command | Purpose |
|---------|---------|
| `cargo xtask test --profile <PROFILE>` | Run nextest with profile |
| `cargo xtask test --profile fast` | Quick iteration (no retries) |
| `cargo xtask test --profile default` | CI profile (balanced) |
| `cargo xtask test --profile debug` | Single-threaded, full output |
| `cargo xtask test --profile perf` | Performance tests only |
| `cargo xtask test --prime` | Prime database pool before tests |

**Passing args to nextest:**
```bash
# Filter by package
cargo xtask test --profile fast -- -p sinex-core

# Filter by test name
cargo xtask test -- -E 'test(my_test_name)'

# Combine filters
cargo xtask test --profile fast -- -p sinex-node-sdk -E 'test(unit::)'
```

### Database

| Command | Purpose |
|---------|---------|
| `cargo xtask db status` | Check Postgres connectivity |
| `cargo xtask db setup` | Create database + migrate |
| `cargo xtask db migrate` | Apply migrations only |
| `cargo xtask db reset --yes` | Drop + recreate (dangerous) |

### Schema Management

| Command | Purpose |
|---------|---------|
| `cargo xtask schema generate` | Generate JSON schemas from EventPayload types |
| `cargo xtask schema check-ready` | Verify core tables exist |
| `cargo xtask schema deploy` | Deploy schemas to database |
| `cargo xtask schema compat` | Check backward compatibility |

### Environment & CI

| Command | Purpose |
|---------|---------|
| `cargo xtask doctor` | Environment health check |
| `cargo xtask doctor --pipelines` | Include pipeline smoke test |
| `cargo xtask ci-preflight` | Full pre-merge validation |
| `cargo xtask ci workspace` | Full CI pipeline |

### Coverage

| Command | Purpose |
|---------|---------|
| `cargo xtask coverage html` | Generate HTML report |
| `cargo xtask coverage html --open` | Generate and open in browser |
| `cargo xtask coverage lcov` | Generate LCOV for CI |
| `cargo xtask coverage summary` | Print summary to stdout |
| `cargo xtask coverage clean` | Remove coverage artifacts |

### Benchmarking

| Command | Purpose |
|---------|---------|
| `cargo xtask bench --mode sweeps` | Thread count sweeps |
| `cargo xtask bench --mode refine` | Refine specific tests |
| `cargo xtask bench --threads 4,8,16` | Custom thread counts |

---

## Nextest Profiles

Profiles are defined in `.config/nextest.toml`:

| Profile | Purpose | Retries | Parallelism |
|---------|---------|---------|-------------|
| `default` | CI + pre-commit | 1 | Auto |
| `fast` | Quick local iteration | 0 | High |
| `debug` | Debugging failures | 0 | 1 thread |
| `perf` | Performance tests | 0 | Controlled |
| `ci` | Full CI validation | 2 | Auto |

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
cargo xtask check && cargo xtask test --profile fast
```

### Full Validation
```bash
cargo xtask ci-preflight
```

### Debug a Failing Test
```bash
cargo xtask test --profile debug -- -E 'test(failing_test_name)'
```

### Check Environment Health
```bash
cargo xtask doctor --pipelines
```
