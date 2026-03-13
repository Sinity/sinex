## Testing Configuration

**xtask test flags:**

| Flag | Effect |
|------|--------|
| (default) | Multi-threaded with retries |
| `--debug` | Single-threaded, full output |
| `--heavy` | Include `#[ignore]` tests |
| `--prime` | Prime database before testing |
| `--affected` | Only changed packages |
| `--bg` | Run in background, return job ID |

**Note:** Performance/stress/external tests are marked `#[ignore]` and skipped by default. Run them with `xtask test --heavy`.

---

## Agent Decision Patterns

### When to Use --json

| Situation | Use --json? | Reason |
|-----------|-------------|--------|
| Checking success/failure | Yes | Parse `.status` field programmatically |
| Extracting specific data | Yes | Structured access to fields |
| Debugging interactively | No | Human output more readable |
| Logging for later review | Either | JSON for parsing, human for reading |

### When to Use --bg

| Situation | Use --bg? | Reason |
|-----------|-----------|--------|
| Operation > 10 seconds | Yes | Continue working while it runs |
| Need result immediately | No | Blocking is simpler |
| Multiple independent tasks | Yes | Spawn all in parallel |
| Interactive debugging | No | Need real-time output |

### Chaining Pattern

```bash
# Spawn, extract ID, continue working, later check result
JOB=$(xtask test --bg --json -p PKG | jq -r '.data.job_id')
# ... do other work ...
xtask jobs status "$JOB" --json | jq '.data.status'
```

### Conditional Execution

```bash
# Check if tests pass before proceeding
if xtask test --json 2>&1 | jq -e '.status == "success"' > /dev/null; then
    echo "Tests passed, proceeding..."
else
    echo "Tests failed, investigating..."
    xtask test --json 2>&1 | jq -r '.errors[].message'
fi
```

---

## xtask JSON Output

> **AI AGENTS: Always use `--json` for machine-readable output.** This eliminates parsing human text.

```bash
# Get structured output from any command
xtask check --json
xtask test --json
xtask doctor --json

# Parse with jq
xtask check --json | jq '.status'           # "success" or "failed"
xtask test --json | jq '.duration_secs'     # timing info
xtask deps unused --json | jq '.data.unused' # unused deps
```

---

## Test Filtering (First-Class Flags)

`-p` and `-E` are first-class flags — do NOT use `--` passthrough for them.

```bash
# Run specific package
xtask test -p sinex-primitives

# Run specific test by name (debug mode for full output)
xtask test --debug -E 'test(my_test_name)'

# Run tests matching filter expression
xtask test -E 'package(sinex-primitives) & test(unit::)'

# Run single package with debug
xtask test --debug -p sinex-node-sdk -E 'test(unit::)'
```

---

## Advanced Commands

```bash
# Benchmark test performance
xtask test bench
xtask test bench --contracts    # Enforce perf budgets

# CI ephemeral Postgres (requires sandbox feature)
xtask ci postgres -- xtask test

# Codebase snapshot for AI context
xtask docs snapshot --output context.md
```

**Full Documentation:** `xtask/docs/README.md`

---

## Runtime Configuration

```rust
// NixOS modules are the canonical deployment surface.
// Runtime binaries then read env/CLI into typed config objects.
let ingestd = IngestdConfig::from_args(...);
let node = NodeConfig::load_from_env("my-node");
let gateway = GatewayConfig::load();
```

Notes:
- `sinex-ingestd` uses CLI/env construction (`IngestdConfig::from_args`).
- `sinex-node-sdk` uses env-first typed config (`NodeConfig::load_from_env`, `EventSourceConfig::load_from_env`, `AutomatonConfig::load_from_env`).
- `sinex-gateway` now follows the same env-first typed-config model; NixOS remains the canonical deployment surface and env is the process-boundary transport.

Full environment variable reference: `docs/current/configuration/environment-variables.md`
