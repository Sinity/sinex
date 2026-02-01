# CI Command

Comprehensive continuous integration workflows for the Sinex workspace.

## Overview

The `ci` command provides pre-configured CI workflows that combine multiple validation steps into a single command. These are designed for use in CI/CD pipelines and pre-merge validation.

## Subcommands

### `cargo xtask ci workspace`

Full workspace CI validation.

**What it does:**
1. Runs `cargo fmt --check` to verify formatting
2. Runs `cargo clippy --all-targets --all-features -- -D warnings` for lint errors
3. Runs tests with the `default` profile (balanced parallelism, retries enabled)
4. Generates test result artifacts

**Usage:**
```bash
# Basic usage
cargo xtask ci workspace

# With custom target directory (for CI isolation)
cargo xtask ci workspace --target-dir /tmp/ci-build

# JSON output for CI integration
cargo xtask ci workspace --json
```

**Parameters:**
- `--target-dir <PATH>` - Custom target directory for build artifacts (optional)

**Exit codes:**
- `0` - All checks passed
- `1` - One or more checks failed

**JSON Output:**
```json
{
  "command": "ci",
  "status": "success",
  "duration_secs": 125.3,
  "timestamp": "2026-01-23T15:00:00Z",
  "details": [
    "Format check passed",
    "Clippy check passed",
    "Tests passed: 456 total"
  ]
}
```

**When to use:**
- **CI/CD pipelines** - Use as the primary validation step
- **Pre-merge checks** - Ensure code quality before merging
- **Release validation** - Verify release candidates

**When NOT to use:**
- **Quick local iteration** - Use `cargo xtask check` instead (much faster)
- **Debugging tests** - Use `cargo xtask test --debug` for single-threaded execution
- **Incremental testing** - Use `cargo xtask test` for quick feedback

## CI Integration Examples

### GitHub Actions

```yaml
name: CI

on: [push, pull_request]

jobs:
  validate:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3

      - name: Setup Rust
        uses: actions-rs/toolchain@v1
        with:
          profile: minimal
          toolchain: stable

      - name: Run CI Validation
        run: cargo xtask ci workspace --json | tee ci-results.json

      - name: Check Results
        run: |
          if jq -e '.status == "success"' ci-results.json > /dev/null; then
            echo "✅ CI passed"
          else
            echo "❌ CI failed"
            exit 1
          fi
```

### GitLab CI

```yaml
test:
  stage: test
  script:
    - cargo xtask ci workspace --json > ci-results.json
  artifacts:
    reports:
      junit: target/nextest/ci/junit.xml
    paths:
      - ci-results.json
```

## Performance Notes

**Expected duration:**
- **Clean build**: 5-8 minutes (full compilation + tests)
- **Incremental build**: 1-3 minutes (only changed code)

**Factors affecting duration:**
- Number of changed files
- Test parallelism (auto-detected from CPU count)
- Whether build cache is warm
- Network latency (for dependencies)

**Optimization tips:**
1. **Use build caching** - Cache `target/` directory between CI runs
2. **Dependency caching** - Cache `~/.cargo/registry/` and `~/.cargo/git/`
3. **Incremental compilation** - Enable in CI for faster rebuilds
4. **Parallel test execution** - Default profile auto-detects optimal thread count

## Comparison with Other Commands

| Command | Purpose | Duration | Use Case |
|---------|---------|----------|----------|
| `cargo xtask check` | Fast format + compile check | ~10s | Local iteration |
| `cargo xtask test` | Quick test run | ~30-60s | Pre-commit check |
| `cargo xtask ci workspace` | Full CI validation | ~2-5min | Pre-merge/CI pipeline |
| `cargo xtask status --doctor` | Environment diagnostics | ~5s | Troubleshooting |

## Troubleshooting

### CI fails but local tests pass

**Cause:** Environment differences between local and CI

**Solutions:**
1. **Check environment variables:**
   ```bash
   cargo xtask doctor --pipelines
   ```

2. **Run CI command locally:**
   ```bash
   cargo xtask ci workspace
   ```

3. **Compare configurations:**
   - Database connectivity (CI may have different DB)
   - NATS configuration
   - TLS certificate paths

### CI timeout

**Cause:** Tests taking too long

**Solutions:**
1. **Increase timeout** in CI configuration (default: 30min)
2. **Check for slow tests:**
   ```bash
   cargo xtask history tests slowest
   ```
3. **Review test profile** - May need to reduce parallelism

### Intermittent CI failures

**Cause:** Flaky tests or timing issues

**Solutions:**
1. **Check retry configuration** in `.config/nextest.toml`
2. **Identify flaky tests:**
   ```bash
   cargo xtask history tests getting-slower
   ```
3. **Review timing dependencies** - Tests may have race conditions

## See Also

- **Test profiles** - `.config/nextest.toml` - Profile configuration
- **CI preflight** - `cargo xtask ci-preflight` - More comprehensive validation
- **Doctor command** - `cargo xtask doctor` - Environment diagnostics
- **Testing guide** - `../docs/current/testing/` - Comprehensive testing documentation
