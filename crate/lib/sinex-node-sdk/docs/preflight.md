# Preflight Verification System

The Preflight system implements a **Fail-Fast Deployment Model**. It validates the operational readiness of a node before it begins processing real data, preventing cascading failures in production.

## 🚦 Verification Categories

### 1. 🗄️ Database Readiness
- **Connectivity**: Validates the same effective runtime database the node will use. `DATABASE_URL`
  is resolved through the current Sinex environment, so a base URL like `.../sinex` becomes the
  environment-scoped database (for example `sinex_dev`) before preflight connects.
- **Extensions**: Verifies required Postgres extensions are loaded (`timescaledb`, `pg_jsonschema`, `vector`, `pg_trgm`).
- **Schema Readiness**: Performs declarative schema dry-run checks (required core tables/columns
  and schema source accessibility). Preflight is read-only: it does **not** apply schema or create
  the database for you.

### 2. 🛰️ Service Dependencies
- **NATS `JetStream`**: Verifies connectivity and ensures required streams (`SINEX_RAW_EVENTS`) exist.
- **Binary PATH**: Checks for essential tools (`git-annex`, `psql`, `systemctl`).
- **Orchestration**: Validates `SystemD` service status where applicable.

### 3. 📦 Resource Capacity
- **Disk Space**: Verifies sufficient headroom in configured `SINEX_DATA_DIR`/`SINEX_STATE_DIR`, `SINEX_LOG_DIR`, and `TMPDIR`.
- **Memory**: Checks available RSS memory against configured minimums.
- **Permissions**: Ensures the `work_dir` is writable by the service user.

### 4. ⚙️ Configuration Validation
- **Effective runtime config**: Validates the configuration the node will actually start with,
  typically environment supplied by the NixOS module plus any explicit CLI overrides.
- **Environment**: Checks for required `SINEX_*` environment variables and flags stale optional
  config-file references when present.

## 🆔 Identifier Convention
- Persisted identifiers are `UUIDv7`.
- Rust code should use typed `Id<T>` wrappers and convert at boundaries (`to_uuid()`).

## 🛠️ Usage

### Automatic Execution
The SDK runs preflight checks automatically during `NodeRunner::initialize_with_transport`. A failure here will result in a clean exit with a non-zero status code.

### Manual Execution
You can run the preflight tool independently using the `sinex-preflight` binary:

```bash
# Run all checks
sinex-preflight verify

# Run only database checks
sinex-preflight verify --skip resources --skip configuration --skip services
```

## 📊 Status Levels

- **PASS**: All critical and optional checks succeeded against the effective runtime database and services.
- **WARNING**: Critical checks passed, but optional dependencies are missing (e.g., `git-annex` not installed).
- **FAIL**: Critical dependencies are missing. Startup is blocked.

## Notes On First Boot

- Preflight proves the target database and dependencies are readable and coherent; it does not
  bootstrap them.
- A fresh database without schema will fail schema/integration checks until schema apply has run.
- If preflight fails on missing `core.events`, fix the deployment/bootstrap path rather than
  expecting preflight itself to create schema.
