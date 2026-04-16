# Dependency Analysis Commands

The `deps` subcommand provides workspace dependency analysis and health checking tools.

## Quick Start

```bash
# List all workspace packages
xtask deps list

# Find duplicate dependency versions
xtask deps duplicates

# Show dependency tree for a package
xtask deps tree --package sinex-gateway
```

## Commands

### `deps list`

List all workspace dependencies.

**Usage**:
```bash
xtask deps list [--format <format>]
```

**Options**:
- `--format <format>` - Output format: `human` (default) or `json`

**Examples**:
```bash
# Human-readable list
xtask deps list

# JSON output
xtask deps list --format json
```

**Output**:
```
Workspace packages (15 total):

  xtask v0.1.0
  sinex-gateway v0.1.0
  sinex-gateway v0.1.0
  sinex-ingestd v0.1.0
  ...
```

---

### `deps tree`

Show dependency tree for a package.

**Usage**:
```bash
xtask deps tree [--package <name>] [--depth <n>]
```

**Options**:
- `--package <name>` - Package name to analyze (default: workspace overview)
- `--depth <n>` - Maximum depth to display (default: 10)

**Examples**:
```bash
# Workspace overview
xtask deps tree

# Specific package
xtask deps tree --package sinex-gateway

# Limited depth
xtask deps tree --package sinex-gateway --depth 3
```

**Output**:
```
Dependency tree for 'sinex-gateway' (depth: 10):
sinex-gateway
├── sinex-primitives
├── sinex-db
└── ...
```

---

### `deps duplicates`

Find duplicate dependency versions.

**Usage**:
```bash
xtask deps duplicates [--threshold <n>]
```

**Options**:
- `--threshold <n>` - Minimum number of versions to report (default: 2)

**Examples**:
```bash
# Find all duplicates
xtask deps duplicates

# Only show packages with 3+ versions
xtask deps duplicates --threshold 3
```

**Output**:
```
Duplicate dependencies (2 total):

  syn has 2 versions:
    - 1.0.109
    - 2.0.48

  tokio has 2 versions:
    - 1.35.1
    - 1.36.0

Total: 2 packages with duplicates
```

---

## Common Use Cases

### Dependency audit

```bash
# Check for duplicates
xtask deps duplicates

# Review all dependencies
xtask deps list --format json | jq .
```

### Package analysis

```bash
# Verify package exists
xtask deps tree --package my-crate

# List workspace members
xtask deps list
```

### CI integration

```bash
# Fail if duplicates found (CI script)
if xtask deps duplicates | grep -q "Total: [1-9]"; then
  echo "Duplicate dependencies found!"
  exit 1
fi
```

---

## JSON Output Format

### `deps list --format json`

```json
{
  "packages": [
    {
      "name": "xtask",
      "version": "0.1.0",
      "is_workspace": true
    }
  ],
  "count": 15
}
```

---

### `deps unused`

Detect dependencies declared in Cargo.toml but not used in code.

**Usage**:
```bash
xtask deps unused [--format <format>] [--ci]
```

**Options**:
- `--format <format>` - Output format: `human` (default) or `json`
- `--ci` - CI mode (fails if unused deps found)

**Examples**:
```bash
# Detect unused dependencies (requires cargo-machete or cargo-udeps)
xtask deps unused

# JSON output
xtask deps unused --format json

# CI mode (fails if unused deps found)
xtask deps unused --ci
```

**Output Formats**:

Human Format:
```
Found 3 unused dependencies (tool: cargo-machete):

  sinex-gateway:
    - serde_json
    - tokio-util

  sinex-gateway:
    - anyhow
```

JSON Format:
```json
{
  "unused": [
    { "package": "sinex-gateway", "dependency": "serde_json" },
    { "package": "sinex-gateway", "dependency": "tokio-util" },
    { "package": "sinex-gateway", "dependency": "anyhow" }
  ],
  "tool": "cargo-machete"
}
```

**Prerequisites**:

Install one of the detection tools:

```bash
# NixOS (recommended - add to home.packages)
pkgs.cargo-machete
pkgs.cargo-udeps

# Or via cargo (fallback)
cargo install cargo-machete
cargo +nightly install cargo-udeps
```

**CI Integration**:

```yaml
# .github/workflows/ci.yml
- name: Check for unused dependencies
  run: xtask deps unused --ci
```

---

### `deps timings`

Analyze build times to identify slow compilation units.

**Usage**:
```bash
xtask deps timings [--top <n>]
```

**Options**:
- `--top <n>` - Show top N slowest crates (default: 10)

**Examples**:
```bash
# Analyze build timings
xtask deps timings

# Show top 5 slowest crates
xtask deps timings --top 5
```

**Output**:

```
Build Timing Analysis
Total build time: 127.45s

Top 10 slowest crates:
  1. sinex-gateway - 45.23s (35.5%)
  2. sinex-gateway - 23.12s (18.1%)
  3. sinex-ingestd - 18.34s (14.4%)
  4. sinex-node-sdk - 12.45s (9.8%)
  5. sinex-services - 9.87s (7.7%)
  6. sinex-schema - 7.34s (5.8%)
  7. sinex-macros - 6.12s (4.8%)
  8. sinex-test-utils - 2.89s (2.3%)
  9. xtask - 0.56s (0.4%)

HTML report: /realm/project/sinex/.sinex/target/cargo-timings/cargo-timing.html
```

**Notes**:

- First run executes `cargo build --release --timings` which may take time
- Generates an HTML report with detailed timing breakdown at `.sinex/target/cargo-timings/cargo-timing.html`
- Compare parameter (--compare) reserved for future enhancement
- Run periodically to track build performance trends

---

## Common Workflows

### Pre-commit Cleanup
```bash
# Check for issues before committing
xtask deps duplicates
xtask deps unused --ci
```

### Performance Investigation
```bash
# Identify slow build targets
xtask deps timings --top 15

# Check rebuild impact of changes
xtask deps impact sinex-gateway
```

### Dependency Audit
```bash
# Full dependency health check
xtask deps list --format json > deps.json
xtask deps duplicates --threshold 2
xtask deps unused
```

---

## Implementation Status

| Command | Status | Tools Required | Notes |
|---------|--------|----------------|-------|
| `deps list` | ✅ Complete | None | Lists workspace packages |
| `deps tree` | ✅ Complete | None | Shows dependency tree |
| `deps duplicates` | ✅ Complete | None | Finds version conflicts |
| `deps unused` | ✅ Complete | cargo-machete OR cargo-udeps | Detects unused deps |
| `deps timings` | ✅ Complete | None (uses cargo --timings) | Build performance |

---

## See Also

- [Testing Guide](../../testing/) - How to test deps commands
- [Development Workflow](../../development-workflow.md) - Using deps in development
