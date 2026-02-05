## Testing Configuration

**xtask test flags:**

| Flag | Effect |
|------|--------|
| (default) | Multi-threaded with retries |
| `--debug` | Single-threaded, full output |
| `--heavy` | Include `#[ignore]` tests |
| `--prime` | Prime database before testing |
| `--affected` | Only changed packages |

**Note:** Performance/stress/external tests are marked `#[ignore]` and skipped by default. Run them with `cargo xtask test --heavy`.

---

## xtask JSON Output

> **AI AGENTS: Always use `--json` for machine-readable output.** This eliminates parsing human text.

```bash
# Get structured output from any command
cargo xtask check --json
cargo xtask test --json
cargo xtask status --doctor --json

# Parse with jq
cargo xtask check --json | jq '.status'           # "success" or "failed"
cargo xtask test --json | jq '.duration_secs'     # timing info
cargo xtask deps unused --json | jq '.data.unused' # unused deps
```

**Agent Decision Pattern:**

```bash
# Check if tests pass before proceeding
if cargo xtask test --json 2>&1 | jq -e '.status == "success"' > /dev/null; then
    echo "Tests passed, proceeding..."
else
    echo "Tests failed, investigating..."
    cargo xtask test --json 2>&1 | jq -r '.errors[].message'
fi
```

---

## Passing Args to Nextest

```bash
# Run specific package
cargo xtask test -- -p sinex-primitives

# Run specific test by name (debug mode for full output)
cargo xtask test --debug -- -E 'test(my_test_name)'

# Run tests matching filter expression
cargo xtask test -- -E 'package(sinex-primitives) & test(unit::)'

# Run single package with debug
cargo xtask test --debug -- -p sinex-node-sdk -E 'test(unit::)'
```

---

## Advanced Commands

```bash
# Benchmark test performance
cargo xtask bench --mode sweeps --threads 8,12,16
cargo xtask bench --mode refine --runs 5

# CI ephemeral Postgres (requires sandbox feature)
cargo xtask xtr ci postgres -- cargo xtask test

# Code pattern search (ast-grep)
cargo xtask patterns -p '$X.unwrap()' --limit 10

# Codebase snapshot for AI context
cargo xtask snapshot --output context.md
```

**Full Documentation:** `xtask/docs/README.md`

---

## Figment Configuration (used by ingestd, gateway)

```rust
use figment::{Figment, providers::{Env, Toml, Format}};

let config: Config = Figment::new()
    .merge(Toml::file("config.toml"))
    .merge(Env::prefixed("SINEX_"))
    .extract()?;
```

Full environment variable reference: `docs/current/configuration/environment-variables.md`
