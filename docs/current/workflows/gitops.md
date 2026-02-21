# GitOps Workflow

> **Purpose:** Development, CI/CD, and deployment workflows for the Sinex project.
> **Last Verified:** 2025-01-23

This document describes the complete lifecycle from local development through CI validation to NixOS deployment.

---

## Development Workflow

### Environment Setup

The project uses Nix for reproducible development environments:

```bash
# Method 1: direnv (automatic activation on cd)
direnv allow

# Method 2: Manual activation
nix develop --accept-flake-config

# Verify environment
xtask status --doctor
```

**What you get:**

- Rust toolchain (stable)
- PostgreSQL with TimescaleDB
- NATS JetStream server
- Development utilities (nextest, cargo-deny, sccache)

### Fast Iteration Loop

For quick feedback during development:

```bash
# 1. Format and type-check (5-10 seconds)
xtask check

# 2. Quick test pass (30-60 seconds)
xtask test

# 3. Iterate on failing tests
xtask test --debug -- -E 'test(my_test_name)'
```

**Optimization tips:**

- `xtask check --skip-fmt` if you know formatting is fine
- `xtask test -- -p <package>` to test a single crate
- Use `--json` flag for machine-parseable output

### Pre-Commit Checklist

Before committing changes, run:

```bash
# Essential checks (required)
xtask check
xtask test

# If you modified schemas
xtask contracts generate
git add schemas/

# If you added forbidden patterns (should fail)
xtask lint-forbidden
```

**Time budget:** ~2 minutes for essential checks

### Pre-PR Checklist

Before opening a pull request:

```bash
# Full validation suite
xtask xtr ci workspace
```

This runs:

1. Format check (`cargo fmt --check`)
2. Compilation check (`cargo check --workspace`)
3. Clippy lints (`-D warnings`)
4. Forbidden pattern scan (no `#[tokio::test]`, raw SQL, etc.)
5. Schema validation and drift detection
6. Full test suite with retries

**Time budget:** ~5-10 minutes depending on hardware

---

## Branch Strategy

### Branch Protection

| Branch | Protection Rules |
|--------|-----------------|
| `master` | - Require PR approval<br>- Require CI pass<br>- Require schema compatibility check<br>- No force push |

### Branch Naming Conventions

Use descriptive branch names with prefixes:

```bash
feat/add-health-monitoring          # New features
fix/checkpoint-race-condition       # Bug fixes
docs/update-gitops-workflow         # Documentation
refactor/simplify-error-handling    # Code refactoring
perf/optimize-batch-processing      # Performance improvements
chore/update-dependencies           # Maintenance tasks
```

### Feature Development Flow

```bash
# 1. Create feature branch
git checkout -b feat/my-feature

# 2. Make changes and commit
git add .
git commit -m "feat(component): add feature description"

# 3. Push and open PR
git push -u origin feat/my-feature
gh pr create --title "feat: Add feature description"
```

---

## Commit Message Convention

Follow conventional commit format:

```
<type>(<scope>): <description>

[optional body]

[optional footer]
```

### Types

| Type | Use Case |
|------|----------|
| `feat` | New feature or enhancement |
| `fix` | Bug fix |
| `docs` | Documentation changes |
| `refactor` | Code restructuring without behavior change |
| `perf` | Performance improvement |
| `test` | Test additions or modifications |
| `chore` | Build process, dependencies, tooling |
| `style` | Formatting, whitespace (rare, prefer `cargo fmt`) |

### Scopes

Common scopes matching the crate structure:

- `sdk` - sinex-node-sdk
- `core` - sinex-primitives
- `ingestd` - sinex-ingestd
- `gateway` - sinex-gateway
- `schema` - sinex-schema
- `tests` - Test infrastructure
- `ci` - CI/CD configuration

### Examples

```bash
# Good commit messages
feat(sdk): auto-enable health monitoring for SimpleNode
fix(gateway): handle graceful shutdown with timeout
docs(AGENTS.md): update with current state tracking
refactor(core): simplify error context propagation

# Avoid
Update stuff
Fix bug
WIP
```

---

## CI Pipeline

### Pipeline Stages

The CI pipeline runs on every push to `master` and on all pull requests:

```
┌─────────────────────────────────────────────────────────────┐
│                    CI Pipeline (ci.yml)                      │
├─────────────────────────────────────────────────────────────┤
│ 1. Cache Setup                                               │
│    - sccache (compiled artifacts)                            │
│    - cargo registry                                          │
│    - cargo git                                               │
│                                                              │
│ 2. Nix Bootstrap                                             │
│    - Install Nix with flakes                                 │
│    - Restore Cachix binary cache                             │
│    - Build devenv shell                                      │
│                                                              │
│ 3. Database Bootstrap                                        │
│    - xtask ci postgres                                 │
│    - Start PostgreSQL + TimescaleDB                          │
│    - Apply migrations                                        │
│                                                              │
│ 4. Workspace Validation                                      │
│    - xtask ci workspace                                │
│      ├── Format check (cargo fmt --check)                    │
│      ├── Lint (cargo clippy -D warnings)                     │
│      ├── Forbidden pattern scan                              │
│      ├── Schema validation                                   │
│      │   ├── xtask contracts check-ready               │
│      │   └── Schema drift detection                          │
│      ├── Security audit (cargo deny check)                   │
│      └── Tests                                               │
│          ├── E2E smoke tests (--profile fast)                │
│          └── Full suite (--profile ci --prime)               │
└─────────────────────────────────────────────────────────────┘
```

### Parallel Workflows

Additional workflows run concurrently:

**Database Checks (`db-checks.yml`)**

- Triggered by: Changes to `crate/lib/sinex-schema/migrations/**`
- Validates: Schema readiness and migration integrity

**Schema Compatibility (`schema-compatibility.yml`)**

- Triggered by: Pull requests with schema changes
- Validates: Backward compatibility with base branch
- Posts: PR comment with compatibility report

### Local CI Reproduction

Run the exact CI pipeline locally:

```bash
# Full CI pipeline (matches ci.yml)
nix develop --accept-flake-config --no-pure-eval --command \
  xtask ci postgres -- \
  xtask ci workspace

# Individual stages
xtask ci postgres        # Bootstrap database
xtask ci workspace       # Run validation suite

# Schema compatibility check (matches PR check)
CI_BASE_BRANCH=master xtask contracts compat
```

### CI Performance

Typical CI run times:

- Cache hit: ~8-12 minutes
- Cache miss: ~15-20 minutes
- Database bootstrap: ~30 seconds
- Test suite: ~5-8 minutes

---

## Pull Request Workflow

### Creating a PR

1. **Push your branch:**

    ```bash
    git push -u origin feat/my-feature
    ```

2. **Open PR via GitHub CLI:**

    ```bash
    gh pr create \
      --title "feat: Add feature description" \
      --body "$(cat <<'EOF'
    ## Description
    Brief summary of changes

    ## Type of Change
    - [x] New feature

    ## Testing
    - [x] Added tests for new functionality
    - [x] Ran `xtask test`

    ## Code Quality
    - [x] Ran `cargo fmt`
    - [x] Ran `cargo clippy`
    - [x] Updated documentation
    EOF
    )"
    ```

3. **Monitor CI checks:**

    ```bash
    gh pr checks        # Watch CI status
    gh pr view          # View PR in browser
    ```

### PR Requirements

All PRs must satisfy:

- [ ] CI pipeline passes (green checkmark)
- [ ] Schema compatibility check passes (if schemas changed)
- [ ] All template checklist items addressed
- [ ] Abstraction compliance (database ops, error handling, validation)
- [ ] Code review approval from maintainer

### PR Template

The project uses a comprehensive PR template covering:

- Change type classification
- Testing checklist
- Abstraction compliance (database, errors, validation)
- Code quality gates
- Related issues

See `.github/pull_request_template.md` for the full template.

---

## Security & Dependency Management

### cargo-deny Integration

The project uses `cargo-deny` for security and licensing compliance:

```bash
# Run all checks
cargo deny -c .config/deny.toml check

# Individual checks
cargo deny -c .config/deny.toml check advisories    # Security vulnerabilities
cargo deny -c .config/deny.toml check licenses      # License compliance
cargo deny -c .config/deny.toml check bans          # Banned dependencies
cargo deny -c .config/deny.toml check sources       # Source verification
```

**CI enforcement:** `cargo deny check advisories` runs in CI pipeline

**Configuration:** See `.config/deny.toml` for policy definitions

### Security Scanning

- **Dependency audits:** Via `cargo-deny` on every CI run
- **RUSTSEC database:** Automatically checked against known vulnerabilities
- **License compliance:** Ensures all dependencies use approved licenses

---

## Schema Management

### Schema Generation Workflow

When you modify event payloads:

```bash
# 1. Modify EventPayload struct in crate/lib/sinex-schema/src/payloads/
# 2. Regenerate schemas
xtask contracts generate

# 3. Verify changes
git diff schemas/

# 4. Commit both code and schemas
git add crate/lib/sinex-schema/src/payloads/ schemas/
git commit -m "feat(schema): add new event type"
```

**CI enforcement:** CI fails if generated schemas are out of sync with code.

### Schema Compatibility Checks

For backward compatibility validation:

```bash
# Check compatibility with master
xtask contracts compat --base master

# Check specific version compatibility
xtask contracts compat --base v0.4.1
```

**Breaking changes trigger:**

- Red "failed" status on PR
- Comment explaining incompatibility
- Manual approval required from maintainer

### Schema Deployment

Schemas are automatically deployed to the database:

1. **On PR merge to master:** Schema management workflow triggers
2. **If production credentials exist:** `xtask contracts deploy` runs
3. **Schemas registered:** EventPayload schemas inserted into `sinex_schemas.event_payload_schemas`

**Manual deployment:**

```bash
xtask contracts deploy --env production
```

---

## Testing Strategy

### Test Profiles

| Profile | Use Case | Characteristics |
|---------|----------|-----------------|
| `fast` | Local iteration | No retries, 60s timeout, 12 threads |
| `default` | Pre-commit | 1 retry, 180s timeout, 12 threads |
| `ci` | CI pipeline | 2 retries, 300s timeout, auto threads |
| `debug` | Debugging failures | 0 retries, 1 thread, full output |
| `perf` | Performance tests | Controlled parallelism, stress/soak |

### Running Tests

```bash
# Local development
xtask test --profile fast

# Pre-commit validation
xtask test --profile default

# Debug specific test
xtask test --profile debug -- -E 'test(my_test_name)'

# Test single package
xtask test --profile fast -- -p sinex-primitives

# Performance tests
xtask test --profile perf
```

### Test Infrastructure

- **Database isolation:** Each test gets a dedicated database via template cloning
- **NATS sharing:** Process-wide singleton with namespace isolation
- **Parallel execution:** Up to 12 tests simultaneously
- **Automatic cleanup:** Multi-layer timeout and panic handling

See `docs/current/testing/README.md` for comprehensive testing guide.

---

## Release Workflow

### Version Management

Project version is managed in workspace `Cargo.toml`:

```toml
[workspace.package]
version = "0.4.2"
```

### Release Process

```bash
# 1. Update version
# Edit Cargo.toml [workspace.package] version
vim Cargo.toml

# 2. Update CHANGELOG.md
# Add new section for version with changes
vim CHANGELOG.md

# 3. Commit version bump
git add Cargo.toml CHANGELOG.md
git commit -m "chore: bump version to 0.5.0"

# 4. Tag release
git tag -a v0.5.0 -m "Release v0.5.0"

# 5. Push with tags
git push origin master --tags
```

### Tag Conventions

- Format: `v{MAJOR}.{MINOR}.{PATCH}`
- Examples: `v0.4.2`, `v0.5.0`, `v1.0.0`
- Follows [Semantic Versioning](https://semver.org/)

### Release Notes

Generate release notes from git history:

```bash
# Get commits since last tag
git log v0.4.2..HEAD --oneline --no-decorate

# Categorize by type
git log v0.4.2..HEAD --oneline --grep="^feat"
git log v0.4.2..HEAD --oneline --grep="^fix"
```

---

## NixOS Deployment

### Overview

Sinex components are deployed as NixOS services via the `sinnix` configuration flake.

```
sinex (codebase) → sinnix (NixOS config) → System services
```

### Service Modules

Location: `sinnix/modules/services/`

Available services:

- `sinnix.services.sinex.ingestd` - Event ingestion daemon
- `sinnix.services.sinex.gateway` - JSON-RPC gateway
- `sinnix.services.sinex.nodes.*` - Event capture nodes

### Deployment Flow

```bash
# 1. Update Sinex in sinnix flake input
cd /realm/project/sinnix
nix flake lock --update-input sinex

# 2. Build configuration
sudo nixos-rebuild build --flake .#sinnix-prime

# 3. Verify changes
nix store diff-closures /run/current-system ./result

# 4. Activate new configuration
sudo nixos-rebuild switch --flake .#sinnix-prime
```

### Service Management

```bash
# Check service status
systemctl status sinex-ingestd
systemctl status sinex-gateway

# View logs
journalctl -u sinex-ingestd -f
journalctl -u sinex-gateway -f

# Restart services
sudo systemctl restart sinex-ingestd
sudo systemctl restart sinex-gateway
```

### Configuration Updates

Service configuration is managed via NixOS modules:

```nix
# Example: Enable and configure ingestd
sinnix.services.sinex.ingestd = {
  enable = true;
  batch_size = 1000;
  strict_validation = true;
};
```

**After configuration changes:**

```bash
sudo nixos-rebuild switch --flake .#sinnix-prime
```

### Health Monitoring

Services emit health events and heartbeats:

```bash
# Check recent health events
nats sub 'events.confirmed.health.>'

# Query health status via gateway
python3 /realm/project/sinex/cli/exo.py query \
  --rpc-token "$SINEX_RPC_TOKEN" \
  --type health.status
```

### Rollback Procedure

If deployment causes issues:

```bash
# 1. List previous generations
sudo nix-env --list-generations --profile /nix/var/nix/profiles/system

# 2. Rollback to previous generation
sudo nixos-rebuild switch --rollback

# 3. Or switch to specific generation
sudo nix-env --switch-generation 42 --profile /nix/var/nix/profiles/system
sudo /nix/var/nix/profiles/system/bin/switch-to-configuration switch
```

### Database Migrations

Migrations are applied automatically via `sinex-schema`:

```nix
# Migration handling in NixOS module
systemd.services.sinex-ingestd = {
  preStart = ''
    ${sinex-schema}/bin/sinex-schema migrate
  '';
};
```

**Manual migration:**

```bash
cd /realm/project/sinex
cargo run --bin sinex-schema -- migrate
```

---

## Troubleshooting

### CI Failures

**Format check failed:**

```bash
cargo fmt
git add -u
git commit --amend --no-edit
git push --force-with-lease
```

**Clippy warnings:**

```bash
cargo clippy --workspace --all-targets -- -D warnings
# Fix warnings, then commit
```

**Test failures:**

```bash
# Reproduce locally
xtask test --profile ci

# Debug specific failure
xtask test --profile debug -- -E 'test(failing_test)'
```

**Schema drift:**

```bash
xtask contracts generate
git add schemas/
git commit -m "chore(schema): regenerate after code changes"
```

### Local Development Issues

**Database connection failures:**

```bash
xtask db status        # Check connectivity
xtask db setup         # Recreate database
xtask status --doctor  # Full environment check
```

**NATS connection failures:**

```bash
# Check if NATS is running
devenv up nats

# Verify URL
echo $SINEX_NATS_URL
```

**Test timeouts:**

```bash
# Increase timeout for slow environments
SINEX_TEST_TIMEOUT_MULTIPLIER=2 xtask test
```

---

## Best Practices

### Do's

✅ Run `xtask check` before every commit
✅ Use `xtask xtr ci workspace` before opening PR
✅ Write tests for all new functionality
✅ Update documentation when changing behavior
✅ Regenerate schemas after EventPayload changes (`xtask contracts generate`)
✅ Use conventional commit messages
✅ Keep PRs focused and reviewable
✅ Respond to CI feedback promptly

### Don'ts

❌ Don't commit without running `cargo fmt`
❌ Don't ignore Clippy warnings
❌ Don't use `#[tokio::test]` (use `#[sinex_test]`)
❌ Don't write raw SQL (use sqlx macros)
❌ Don't bypass repository abstractions
❌ Don't push to `master` directly
❌ Don't force-push to `master`
❌ Don't commit `.unwrap()` in production code

---

## Quick Reference

### Essential Commands

| Command | Purpose |
|---------|---------|
| `xtask check` | Fast format + type check |
| `xtask test` | Quick test pass |
| `xtask xtr ci workspace` | Full pre-merge validation |
| `xtask contracts generate` | Regenerate event schemas |
| `xtask status --doctor` | Environment health check |
| `gh pr create` | Create pull request |
| `gh pr checks` | Monitor CI status |

### Time Budgets

| Activity | Expected Duration |
|----------|-------------------|
| `xtask check` | 5-10 seconds |
| `xtask test` | 30-60 seconds |
| `xtask xtr ci workspace` | 5-10 minutes |
| Full CI pipeline | 8-20 minutes |
| NixOS deployment | 2-5 minutes |

### Support Resources

| Resource | Location |
|----------|----------|
| Architecture docs | `docs/current/architecture/` |
| Testing guide | `docs/current/testing/` |
| xtask reference | `xtask/docs/README.md` |
| CI workflows | `.github/workflows/README.md` |
| Agent memory | `CLAUDE.md` |
| Getting started | `docs/current/getting-started.md` |

---

## Related Documentation

- [Core Architecture](../architecture/Core_Architecture.md) - System design overview
- [Testing Guide](../testing/README.md) - Comprehensive testing patterns
- [Getting Started](../getting-started.md) - Developer onboarding
- [xtask Reference](../../xtask/docs/README.md) - Complete xtask command reference
- [CI Workflows](.github/workflows/README.md) - GitHub Actions documentation
