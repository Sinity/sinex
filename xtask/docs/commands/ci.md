# CI Command

Continuous-integration helpers for the Sinex workspace.

## Overview

The `ci` command family provides reusable building blocks for CI/CD pipelines and
pre-merge validation. The repository's main GitHub Actions gate currently runs the
Postgres-backed workspace lane:

`xtask ci postgres -- xtask ci workspace`

This page documents that public `xtask ci` surface. Older references to
`xtask verify` are stale; the public perf-verification entrypoint moved under
`xtask test bench`. For the authoritative option surface, use
`xtask ci --help` and `xtask ci <subcommand> --help`.

## Subcommands

### `xtask ci workspace`

Postgres-backed workspace validation.

**What it does:**
1. Applies declarative schema to the target database
2. Verifies required contract tables are present
3. Runs `cargo deny check`
4. Runs `xtask check` plus the internal forbidden-pattern lane in parallel
5. Fails if the workspace is left dirty by generated output
6. Runs `xtask test --fail-fast -p sinex-e2e-tests`
7. Runs `xtask test --all --prime --exclude sinex-e2e-tests`

This is the broadest Rust/package gate currently wired into GitHub Actions, but it
still does **not** cover the NixOS VM suite under `tests/e2e/nixos-vm/`.

The closest public local equivalent for that lane is `xtask check --forbidden`
or `xtask check --full`. Internally, the CI workspace lane runs the same
forbidden-pattern logic in parallel with `xtask check`. That logic also
executes the repo `ast-grep` rule catalog, but only `error`-severity ast-grep
findings are currently blocking; warning/hint findings remain advisory until
the catalog is clean enough to tighten further.

**Usage:**
```bash
# Typical local reproduction of the workspace gate
xtask ci postgres -- xtask ci workspace

# Override the target directory when isolating build artifacts
xtask ci workspace --target-dir /tmp/ci-build
```

**Parameters:**
- `--target-dir <PATH>` - Custom target directory for build artifacts (optional)

**Exit codes:**
- `0` - All checks passed
- `1` - One or more checks failed

**When to use:**
- **Postgres-backed pre-merge validation** - Reproduce the main Rust/package gate
- **Schema + test integration checks** - Validate schema apply and the package-level
  test surfaces together
- **Before touching DB/test/lint surfaces** - Catch the broad workspace failures in one run

**When NOT to use:**
- **Quick local iteration** - Use `xtask check` instead (much faster)
- **Schema-only checks** - Use `xtask ci schema-only`
- **Refreshing the checked-in `schemas/` bundle** - Use `xtask docs schema-bundle`
- **VM deployment-path coverage** - Use `tests/e2e/nixos-vm` separately

### `xtask ci postgres`

Starts an ephemeral local Postgres instance, sets the expected CI environment
variables, and runs the command that follows `--`.

```bash
xtask ci postgres -- xtask ci workspace
xtask ci postgres -- xtask ci schema-only
```

### `xtask ci schema-only`

Runs only the declarative schema apply + readiness check path:

```bash
xtask ci postgres -- xtask ci schema-only
```

Use this when you need confidence in the schema bootstrap path without paying for the
full workspace test suite.

This does **not** regenerate the checked-in JSON schema bundle under `schemas/`.
For that surface, use:

```bash
xtask docs schema-bundle
xtask docs schema-bundle --check
```

### `xtask ci check-ready`

Checks that the required contract tables exist in the target database.

```bash
xtask ci check-ready
```

This is useful for debugging schema bootstrap failures or verifying a database after
apply/setup.

## CI Integration Examples

### GitHub Actions

```yaml
jobs:
  validate:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v5
      - name: Postgres-backed workspace gate
        run: |
          xtask ci postgres -- \
          xtask ci workspace
```

## Performance Notes

**Expected duration:**
- **Clean build**: variable; schema apply, dependency audit, and test execution all contribute
- **Incremental build**: usually much faster once the Cargo/Nix caches are warm

**Factors affecting duration:**
- Schema/bootstrap cost
- Cargo/Nix cache warmth
- Test parallelism and E2E package runtime
- Dependency audit and lint time

**Optimization tips:**
1. **Use build caching** - Cache Cargo/Nix artifacts between CI runs
2. **Run `xtask ci schema-only` first** - It isolates DB/bootstrap failures before the full gate
3. **Use `schema-only` when narrowing DB bootstrap failures**
4. **Remember nextest retries are disabled** - Intermittent failures need diagnosis, not hidden reruns

## Comparison with Other Commands

| Command | Purpose | Duration | Use Case |
|---------|---------|----------|----------|
| `xtask check` | Fast compile check | ~10s | Local iteration |
| `xtask test` | Quick test run | ~30-60s | Pre-commit check |
| `xtask ci schema-only` | Schema apply + readiness | Varies | DB/bootstrap validation |
| `xtask ci workspace` | Broad Postgres-backed package gate | Varies | Pre-merge/CI pipeline |
| `xtask doctor` | Environment diagnostics | ~5s | Troubleshooting |

## Troubleshooting

### CI fails but local tests pass

**Cause:** Environment differences between local and CI

**Solutions:**
1. **Check environment variables:**
   ```bash
   xtask doctor --pipelines
   ```

2. **Run the same command locally:**
   ```bash
   xtask ci postgres -- xtask ci workspace
   ```

3. **Compare configurations:**
   - Database connectivity (CI may have different DB)
   - NATS configuration
   - TLS certificate paths

### CI timeout or unexpectedly long runs

**Cause:** Schema/bootstrap cost, cache misses, or slow tests

**Solutions:**
1. **Increase timeout** in CI configuration (default: 30min)
2. **Check for slow tests:**
   ```bash
   xtask history tests slowest
   ```
3. **Narrow the failing lane first** - Use `schema-only` before rerunning the full workspace gate
4. **Review test profile** - The default nextest profile uses no retries

### Intermittent CI failures

**Cause:** Flaky tests, timing issues, or brittle environment assumptions

**Solutions:**
1. **Check the failing lane** - schema/bootstrap, e2e package, or general tests
2. **Identify flaky tests:**
   ```bash
   xtask history tests getting-slower
   ```
3. **Review timing dependencies** - Tests may have race conditions

## See Also

- **Test profiles** - `.config/nextest.toml` - Profile configuration
- **Verification overview** - `xtask/docs/verification.md`
- **Doctor command** - `xtask doctor` - Environment diagnostics
- **Testing guide** - `xtask/docs/sandbox/README.md` - Comprehensive testing documentation
