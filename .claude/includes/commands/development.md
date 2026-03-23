## Development Commands

### !!! CARGO IS NEVER USED DIRECTLY !!!

`xtask` wraps every cargo command with: history tracking, diagnostics capture, preflight (auto-start DB/NATS), and JSON output. Bare `cargo` bypasses all of this.

### Check (Compile Verification)

```bash
xtask check                    # Fastest: compile-only (~3s warm)
xtask check --lint             # Compile + clippy (~20s warm)
xtask check --full             # fmt + clippy + forbidden (~25s warm)
xtask check --fix              # Auto-fix then full validation
xtask check -p PKG             # Single package
xtask check --bg               # Background (continue working)
```

Speed: `check` (fastest) -> `check --lint` -> `check --full` (slowest). `--affected` is default ON.

### Fix

```bash
xtask fix                      # Fix affected packages
xtask fix --smart              # Only packages with stored fixable diagnostics
xtask fix --thorough           # Per-package iteration (catches more)
```

### Build

```bash
xtask build -p PKG             # Specific package
xtask build --release          # Release mode
```

### Test

```bash
xtask test                     # Affected packages, auto-starts infra
xtask test -p PKG              # Single package
xtask test --debug -E 'test(name)'  # Debug: 1 thread, full output
xtask test --heavy             # Include #[ignore] tests
xtask test --update-snapshots  # Insta snapshot updates
xtask test --bg -p PKG         # Background

# Subcommands (specialized modes):
xtask test bench               # Benchmarks
xtask test bench --contracts   # Enforce perf budgets
xtask test fuzz                # List fuzz targets
xtask test coverage            # HTML coverage
xtask test mutants -p PKG      # Mutation testing
xtask test vm --category smoke # NixOS VM tests
```

`-p` and `-E` are first-class flags. Do NOT use `-- -p` or `-- -E`.

### Run

```bash
xtask run list                 # Available binaries
xtask run core                 # ingestd + gateway
xtask run ingestd --watch      # Hot reload
xtask run node <NAME>          # Specific node
xtask run --bg stack           # Background stack
```

### Workflow Shortcuts

```bash
xtask work test                # check then test (skip if fresh)
xtask check --full && xtask test  # Before commit
xtask ci workspace             # Full validation: schema + lint + all tests
```
