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

### `abstractions.yml` - Code Quality Checks
- Runs additional linting and analysis
- Checks for code abstractions and patterns
- Enforces architectural guidelines

## Local Testing

Run workflows locally using `act`:

```bash
# Install act
nix develop  # Includes act

# Run CI workflow
act -W .github/workflows/ci.yml

# Run specific job
act -j build_and_test_linux
```

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