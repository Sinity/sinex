# GitHub Actions Workflows

This directory contains CI/CD workflows for the Sinex project.

## Workflows

### `ci.yml` - Main CI Pipeline
- Runs on all pushes and pull requests
- Builds all packages using Nix flakes
- Runs test suite
- Validates code formatting

### `sqlx-check.yml` - SQLX Offline Check
- Validates SQLX queries compile correctly
- Ensures `.sqlx/` cache is up to date
- Prevents runtime SQL errors

### `sqlx-cache.yml` - Update SQLX Cache
- Automatically updates `.sqlx/` cache when migrations change
- Creates PR with updated cache files

### `schema-validation.yml` - JSON Schema Validation
- Validates all JSON schemas in `/schemas/`
- Checks schema compatibility
- Ensures event schemas are well-formed

### `schema-management.yml` - Schema Deployment
- Deploys validated schemas to production
- Updates schema registry in database
- Maintains schema version history

## Local Testing

Workflows are designed to run inside the `nix develop` environment. If you want to execute
them locally with [`act`](https://github.com/nektos/act), install `act` separately and point
it at the desired workflow (for example: `act -W .github/workflows/ci.yml`). There is no
first-class wrapper in the dev shell today.

## Nix Integration

All workflows use Nix for reproducible builds:
- `cachix/install-nix-action` sets up Nix
- `cachix/cachix-action` provides build caching
- All builds use `nix build` with flakes

## Best Practices

1. **Use Nix for all builds** - Ensures reproducibility
2. **Cache aggressively** - Use Cachix to speed up builds
3. **Test in CI what runs in production** - Use same Nix derivations
4. **Keep workflows simple** - Complex logic belongs in Nix
5. **Validate early** - Run quick checks before expensive builds

## Future Enhancements (Not Yet Implemented)

### Security Scanning Pipeline
- **SAST Tools**: Integrate Semgrep, SonarQube for static analysis
- **Dependency Scanning**: 
  - `cargo audit` for Rust vulnerabilities
  - `vulnix` for Nix package CVEs
  - Trivy/Grype for container scanning
- **License Compliance**: Automated license checking
- **Secret Detection**: Prevent accidental credential commits

### Release Engineering
- **Semantic Versioning**: Automated version bumping based on commit messages
- **Changelog Generation**: Automatic CHANGELOG.md from conventional commits
- **GitHub Releases**: Automated release creation with artifacts
- **Binary Distribution**: Pre-built binaries for common platforms
- **Docker Images**: Build and push container images to GHCR

### Advanced Testing
- **Performance Benchmarks**: Track performance regressions
- **Fuzzing**: Automated fuzz testing for parsers
- **Property-Based Tests**: Run in CI with more iterations
- **Load Testing**: Stress test event processing pipeline
- **NixOS VM Tests**: Full integration tests with service orchestration

### Deployment Automation
- **Staging Environment**: Deploy PRs to preview environments
- **Blue-Green Deployments**: Zero-downtime production updates
- **Rollback Automation**: Automatic rollback on health check failures
- **Monitoring Integration**: Alert on deployment issues

### Developer Experience
- **PR Previews**: Live preview of changes
- **Test Coverage Reports**: Automated coverage tracking
- **Performance Reports**: Benchmark comparisons in PRs
- **Documentation Preview**: Build and preview docs changes
