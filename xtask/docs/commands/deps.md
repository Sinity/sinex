# Dependency Analysis Commands

The `deps` subcommand provides workspace dependency analysis and health checking tools.

## Quick Start

```bash
# List all workspace packages
xtask deps list

# Find duplicate dependency versions
xtask deps duplicates

# Show dependency tree for a package
xtask deps tree --package sinexd
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
  sinexd v0.1.0
  sinexctl v0.1.0
  sinex-db v0.1.0
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
xtask deps tree --package sinexd

# Limited depth
xtask deps tree --package sinexd --depth 3
```

**Output**:
```
Dependency tree for 'sinexd' (depth: 10):
sinexd
├── sinex-primitives
├── sinex-db
└── ...
```

---

### `deps duplicates`

Find duplicate dependency versions.

**Usage**:
```bash
xtask deps duplicates [--threshold <n>] [--direct-only | --transitive-only]
```

**Options**:
- `--threshold <n>` - Minimum number of versions to report (default: 2)
- `--direct-only` - Only report duplicate versions directly requested by workspace manifests
- `--transitive-only` - Only report duplicate versions introduced solely through upstream dependencies

**Examples**:
```bash
# Find all duplicates
xtask deps duplicates

# Only show packages with 3+ versions
xtask deps duplicates --threshold 3

# Find duplicates that can be acted on in first-party manifests
xtask deps duplicates --direct-only

# Confirm which duplicate families are upstream-only
xtask deps duplicates --transitive-only --json
```

**Output**:
```
Duplicate dependencies (2 total):
  1 direct workspace debt, 1 transitive upstream

  syn has 2 versions (direct workspace, 1 direct workspace roots):
    - 1.0.109
      roots: sinexctl
      direct: sinexctl
    - 2.0.48
      roots: sinex-macros

  tokio has 2 versions (transitive upstream, 0 direct workspace roots):
    - 1.35.1
      roots: <none>
    - 1.36.0
      roots: <none>

Total: 2 packages with duplicates (1 direct, 1 transitive)
```

---

## Common Use Cases

### Dependency audit

```bash
# Check for duplicates
xtask deps duplicates

# Check only duplicates actionable from workspace manifests
xtask deps duplicates --direct-only

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
# Fail only on duplicates directly requested by workspace manifests
if xtask deps duplicates --direct-only --json | jq -e '.data.count > 0'; then
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
xtask deps unused [--format <format>]
```

**Options**:
- `--format <format>` - Output format: `human` (default) or `json`

**Examples**:
```bash
# Detect unused dependencies through the xtask wrapper
xtask deps unused

# JSON output
xtask deps unused --format json
```

**Output Formats**:

Human Format:
```
Found 3 unused dependencies (tool: cargo-machete):

  sinexd:
    - serde_json
    - tokio-util

  sinexctl:
    - anyhow
```

JSON Format:
```json
{
  "unused": [
    { "package": "sinexd", "dependency": "serde_json" },
    { "package": "sinexd", "dependency": "tokio-util" },
    { "package": "sinexd", "dependency": "anyhow" }
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

# Or enter a temporary shell while investigating locally
nix shell nixpkgs#cargo-machete
```

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
  1. sinexd - 45.23s (35.5%)
  2. sinexd - 23.12s (18.1%)
  3. sinexd - 18.34s (14.4%)
  4. sinexd - 12.45s (9.8%)
  5. sinex-schema - 7.34s (5.8%)
  6. sinex-macros - 6.12s (4.8%)
  7. sinex-test-utils - 2.89s (2.3%)
  8. xtask - 0.56s (0.4%)

HTML report: /realm/project/sinex/.sinex/cache/target/cargo-timings/cargo-timing.html
```

**Notes**:

- First run executes `cargo build --release --timings` which may take time
- Generates an HTML report with detailed timing breakdown under the resolved Cargo target directory, for example `.sinex/cache/target/cargo-timings/cargo-timing.html`
- Compare parameter (--compare) reserved for future enhancement
- Run periodically to track build performance trends

---

## Common Workflows

### Pre-commit Cleanup
```bash
# Check for issues before committing
xtask deps duplicates
xtask deps unused
```

### Performance Investigation
```bash
# Identify slow build targets
xtask deps timings --top 15

# Check rebuild impact of changes
xtask deps impact --package sinexd
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
