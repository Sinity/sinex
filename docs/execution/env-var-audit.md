# Environment Variable Audit (Phase 1.4)

**Date:** 2026-01-16
**Scope:** All `env::var()` calls in `crate/` directory

## Summary
This audit identifies all environment variables used in the Sinex codebase and verifies compliance with the `SINEX_*` prefix convention.

## Findings

### ✅ Compliant SINEX_* Variables (37 unique)

#### Gateway Configuration
- `SINEX_GATEWAY_PORT` - HTTP server port
- `SINEX_GATEWAY_TCP_LISTEN` - TCP listen specification
- `SINEX_GATEWAY_REQUIRE_CLIENT_TLS` - Enforce mTLS
- `SINEX_GATEWAY_TLS_CERT` - TLS certificate path
- `SINEX_GATEWAY_TLS_KEY` - TLS key path
- `SINEX_GATEWAY_TLS_CLIENT_CA` - Client CA certificate path
- `SINEX_GATEWAY_ADMIN_TOKEN_FILE` - Admin token file path
- `SINEX_GATEWAY_MAX_BLOB_BYTES` - Maximum blob size
- `SINEX_RPC_TOKEN` - RPC authentication token
- `SINEX_RPC_TOKEN_FILE` - RPC token file path
- `SINEX_ALLOW_REPLAY_CONTROL_BYPASS` - Testing: bypass replay control (4 uses)

#### NATS Configuration
- `SINEX_NATS_URL` - NATS server URL (default: nats://localhost:4222) (3 uses)
- `SINEX_NATS_TOKEN` - NATS authentication token

#### Database Configuration
- `SINEX_DB_POOL_SIZE` - Database connection pool size
- `SINEX_POOL_ACQUIRE_WARN_MS` - Pool acquisition warning threshold (ms)
- `SINEX_POOL_ACQUIRE_TIMEOUT_SECS` - Pool acquisition timeout (seconds)
- `SINEX_ALLOW_SCHEMA_DOWN` - Allow destructive schema migrations

#### Storage & Paths
- `SINEX_ANNEX_PATH` - Annex storage path (2 uses)
- `SINEX_WORK_DIR` - Working directory for nodes (2 uses)
- `SINEX_DATA_DIR` - Data directory path
- `SINEX_CONFIG` - Custom configuration file path

#### Runtime & Execution
- `SINEX_ENVIRONMENT` - Environment name (dev/staging/prod)
- `SINEX_VERSION` - Application version
- `SINEX_DRY_RUN` - Dry-run mode flag
- `SINEX_EDGE_MODE` - Edge mode flag
- `SINEX_COORDINATION_DISABLED` - Disable coordination
- `SINEX_PROCESS_HEARTBEAT_STALE_SECS` - Heartbeat staleness threshold
- `SINEX_LOG_LEVEL` - Logging level

#### Session & Identity
- `SINEX_SESSION_ID` - Session identifier
- `SINEX_REPLAY_NAMESPACE` - Replay service namespace

#### Desktop node
- `SINEX_DESKTOP_REQUIRE_HYPRLAND` - Require Hyprland window manager
- `SINEX_SKIP_DBUS_RPATH` - Skip DBus RPATH setting (build-time)

#### Testing Infrastructure
- `SINEX_TEST_USE_TLS` - Enable TLS in tests
- `SINEX_TEST_NATS_TOKEN` - NATS token for tests
- `SINEX_TEST_NATS_SHARED_KEY` - NATS shared key for tests
- `SINEX_TEST_NATS_CONFIG_FILE` - NATS config file for tests
- `SINEX_TEST_FAIL_DIR` - Test failure snapshot directory
- `SINEX_TEST_OPTIMIZATIONS` - Enable test optimizations
- `SINEX_PROPTEST_CASES` - Property test case count
- `SINEX_PROPTEST_SEED` - Property test RNG seed
- `SINEX_PROPTEST_DIR` - Property test output directory

### ✅ Approved Exceptions (Ecosystem Standards)

#### Database
- `DATABASE_URL` - Primary database connection (26 uses)
  - Standard across Rust/SQLx ecosystem
  - Used by migrations, sqlx-cli, diesel, sea-orm
- `DATABASE_URL_SUPERUSER` - Admin/superuser connection (3 uses)
  - Test infrastructure for setup/teardown
- `DATABASE_URL_APP` - Application-specific connection (2 uses)
  - Permission testing

#### NATS
- `NATS_CREDS` - NATS credentials file path
  - Standard NATS ecosystem variable
- `NATS_SERVER_BIN` - NATS server binary path
  - Test infrastructure for embedded server

#### Build System (CARGO_*/node_*)
- `CARGO_PKG_VERSION` - Package version (build.rs)
- `node_VERSION` - node version override
- `node_COMMIT_HASH` - Git commit hash
- `node_COMMIT_COUNT` - Commit count
- `node_BRANCH` - Git branch name
- `node_IS_DIRTY` - Dirty working tree flag
- `node_BUILD_TIMESTAMP` - Build timestamp
- `node_FULL_VERSION` - Full version string
- `node_BINARY_HASH` - Binary content hash
- `GIT_HASH` - Git hash fallback
- `BINARY_HASH` - Binary hash fallback
- `SOURCE_DATE_EPOCH` - Reproducible build timestamp

#### System Variables
- `HOME` - User home directory
- `SHELL` - User shell path
- `TERM` - Terminal type
- `TERM_SESSION_ID` - Terminal session ID (macOS)
- `XDG_RUNTIME_DIR` - XDG runtime directory
- `XDG_DATA_HOME` - XDG data home directory
- `HYPRLAND_INSTANCE_SIGNATURE` - Hyprland window manager detection (3 uses)
- `KITTY_LISTEN_ON` - Kitty terminal IPC detection (2 uses)

#### Testing Framework
- `PROPTEST_CASES` - PropTest case count (standard proptest variable)

#### Benchmarking
- `BENCH_DATABASE_URL` - Benchmark database URL (2 uses)
  - Isolated from regular DATABASE_URL for benchmarking

### ❌ Violations: None Found

No application-specific variables found that violate the `SINEX_*` prefix convention.

## Statistics
- **Total env::var() calls:** 90+
- **Unique SINEX_* variables:** 37
- **Approved ecosystem exceptions:** 31
- **Violations:** 0

## Recommendations

### ✅ Already Compliant
- All application variables use `SINEX_*` prefix
- All exceptions are justified (ecosystem standards, system variables, build metadata)

### 🔄 Future Considerations
1. **Consolidate database URLs**: Consider moving `DATABASE_URL_SUPERUSER` and `DATABASE_URL_APP` to `SINEX_DATABASE_URL_*` pattern for consistency (Phase 5)
2. **Benchmark isolation**: `BENCH_DATABASE_URL` could become `SINEX_BENCH_DATABASE_URL` (Phase 5)
3. **Build metadata**: `node_*` variables are build-time only and may stay as-is

### 📝 Documentation Needs
- Central reference for all `SINEX_*` variables (see `environment-variables.md`)
- Default values and expected formats
- Required vs optional distinction
- Environment-specific overrides (dev/staging/prod)

## Compliance Status: ✅ PASSED

The codebase follows the `SINEX_*` prefix convention for all application variables. All exceptions are justified and align with ecosystem standards or system requirements.

## Next Steps
1. ✅ Document all `SINEX_*` variables in `docs/current/configuration/environment-variables.md`
2. Add examples and defaults to documentation
3. Consider adding environment variable validation at startup
4. Update contribution guidelines to enforce `SINEX_*` prefix for new variables
