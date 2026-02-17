# GitOps Schema Sources: Status and Implementation Roadmap

## Status: ASPIRATIONAL (Table Defined, No Implementation)

The `sinex_schemas.gitops_schema_sources` table represents an **intentional but unimplemented architectural pattern** for managing event schemas through infrastructure-as-code (GitOps) workflows.

**Current State:**
- ✅ Table schema defined in `/crate/lib/sinex-schema/src/schema/sinex_schemas.rs`
- ✅ Table created in canonical migration (`m20241028_000001_create_canonical_schema.rs`)
- ✅ Indexes and triggers provisioned
- ❌ No sync service implementation
- ❌ No repository polling logic
- ❌ No seed data or onboarding

## Table Definition

**Location:** `sinex_schemas.gitops_schema_sources`

**Purpose:** Define Git repositories as sources of truth for event payload schemas, enabling fully automated CI/CD-driven schema management.

**Schema:**
```sql
CREATE TABLE sinex_schemas.gitops_schema_sources (
    id ULID PRIMARY KEY DEFAULT gen_ulid(),
    repository_url TEXT NOT NULL,           -- Git repo URL (e.g., https://github.com/org/schemas)
    branch TEXT NOT NULL DEFAULT 'main',    -- Branch to monitor
    path_pattern TEXT NOT NULL DEFAULT 'schemas/**/*.json',  -- Glob pattern for schema files
    sync_enabled BOOLEAN NOT NULL DEFAULT TRUE,  -- Toggle on/off without dropping
    last_sync_at TIMESTAMPTZ,               -- Last successful sync timestamp
    last_sync_commit TEXT,                  -- Last commit hash processed
    sync_frequency_minutes INTEGER NOT NULL DEFAULT 60,  -- Polling interval
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()  -- Audit tracking
);

-- Indexes
uk_gitops_source: UNIQUE (repository_url, branch, path_pattern)
ix_gitops_sources_for_sync: (last_sync_at) WHERE sync_enabled = true
```

## Intended Workflow

```
[External Git Repo]
  │ Contains: schemas/events/*.json
  │
  ├─ fs-ingestor.events.v1.json
  ├─ terminal-ingestor.shell.command.v1.json
  └─ desktop-ingestor.window.focused.v1.json
  │
  v
[GitOps Schema Sync Service]  <-- NOT IMPLEMENTED
  │ (Background job in ingestd or separate service)
  │ - Polls every sync_frequency_minutes
  │ - Clones/pulls repo, matches path_pattern
  │ - Parses JSON schemas, computes content_hash
  │ - Inserts/updates event_payload_schemas
  │
  v
[sinex_schemas.event_payload_schemas]
  │ Registers each discovered schema
  │ Becomes validation ruleset for ingestd
  │
  v
[ingestd Event Validation]
  └─ Validates incoming events against auto-discovered schemas
```

## Why It Matters

**Without GitOps:** Schema management is manual or coupled to Rust code deployment:
- Schema changes require code changes + rebuild + deploy
- External systems can't control Sinex's schema evolution
- Registry drift is difficult to detect and reconcile

**With GitOps:** Schemas become independently version-controlled assets:
- Schema changes can be deployed independently of Sinex releases
- Ownership can live with data teams, not platform teams
- CI/CD can validate schemas before they're synced to production
- Multi-environment schema management (dev/staging/prod schemas from same repo)

## Implementation Roadmap

### Phase 1: Sync Service Skeleton (Foundation)
- [ ] Create `GitOpsSchemaSync` service in `sinex-services` or `sinex-ingestd`
- [ ] Implement schema repository cloning/pulling (using `git2` or `gix` crate)
- [ ] Implement file discovery with glob matching (`globset` crate)
- [ ] Implement JSON schema parsing and validation
- [ ] Track last sync state (commit hash, timestamp) for efficient polling
- [ ] Add error handling: invalid JSON, missing schemas, network failures

### Phase 2: Integration with ingestd (Runtime)
- [ ] Add `GitOpsSchemaSync` background task to `IngestService`
- [ ] Make sync frequency configurable via environment variable or config
- [ ] Emit metrics: sync duration, schemas discovered, sync errors
- [ ] Handle graceful shutdown: allow in-flight syncs to complete
- [ ] Respect `sync_enabled` flag for per-source on/off toggle

### Phase 3: Authentication & Security (Hardening)
- [ ] Support Git repository authentication (SSH keys, personal access tokens)
- [ ] Support private repositories with credential management via `agenix` (sinnix pattern)
- [ ] Validate repository URLs to prevent SSRF attacks
- [ ] Rate-limit repository access to avoid quota exhaustion
- [ ] Add schema signature verification (optional: GPG signing of schema files)

### Phase 4: Observability (Operations)
- [ ] Log sync attempts: repo URL, branch, schemas discovered, outcome
- [ ] Emit structured metrics: `gitops_sync_duration_ms`, `gitops_schemas_discovered`, `gitops_sync_errors_total`
- [ ] Create Grafana dashboard for sync health
- [ ] Alert on repeated sync failures (e.g., 3+ consecutive failures)
- [ ] Add query endpoints to check last sync status

### Phase 5: Testing & Documentation (Maintenance)
- [ ] Unit tests: schema parsing, glob matching, content hash computation
- [ ] Integration tests: mock Git server, verify sync workflow end-to-end
- [ ] Documentation: "Setting Up GitOps Schema Management" guide
- [ ] Example: seed `gitops_schema_sources` with working external repo
- [ ] Troubleshooting guide: common sync failure modes

## Current Workarounds

Until GitOps sync is implemented:

1. **Manual Schema Registration** (Current approach)
   - Define Rust `EventPayload` types in `sinex-core`
   - Use `xtask schema generate` to produce JSON schemas
   - Commit schemas to `schemas/` directory for documentation
   - `ingestd` automatically discovers and syncs schemas at startup

2. **Registry Tracking**
   - Query `sinex_schemas.event_payload_schemas` to see all registered schemas
   - Query `sinex_schemas.gitops_schema_sources` to verify (currently empty)

3. **External GitOps Tooling** (If needed now)
   - Could write custom webhook/polling service outside Sinex
   - Would manually populate `gitops_schema_sources` with sync timestamps
   - Would POST schema updates to a custom Sinex API endpoint (not yet implemented)

## Code References

- **Schema Definition:** `/crate/lib/sinex-schema/src/schema/sinex_schemas.rs` (lines 273-391)
- **Migration:** `/crate/lib/sinex-schema/src/migrations/m20241028_000001_create_canonical_schema.rs` (lines 399, 465, 551)
- **Current Schema Sync:** `/crate/core/sinex-ingestd/src/schema_sync.rs` (synchronizes from Rust code only)
- **Exploration Notes:** `/docs/exploration/loops/loop-020/` through `/loop-021/` (decision history)

## Decision Rationale

The table was created upfront because:
1. Schema evolution is a known bottleneck in event systems
2. GitOps patterns are industry best practice for configuration management
3. The table design is clean and extensible
4. No breaking changes would be needed later to add sync service
5. It signals intent: "We plan to support external schema sources"

The sync service was deferred because:
1. **MVP Focus:** Current system works well with code-driven schema management
2. **Complexity:** Git integration, credential management, error recovery are non-trivial
3. **Unclear Demand:** May not be needed if teams keep schemas in Rust
4. **Alternative Paths:** Could evaluate GraphQL API or message-based schema push

## Risks of Leaving Unimplemented

- **User Confusion:** Table exists but has no discoverable purpose
- **Tech Debt:** If GitOps becomes necessary later, code integration is more complex
- **Discoverability:** If users try to populate the table manually, sync won't happen (silent failure)

## Recommendations

### Short-term (Immediate)
- ✅ **Document:** Add this file to codebase (explains intent and roadmap)
- ✅ **Flag:** Add comment in schema code: "// TODO: GitOps sync service not yet implemented"
- ✅ **Documentation:** Link this roadmap from `/docs/current/architecture/schema-management.md`

### Medium-term (Next quarter)
- **Validation:** If GitOps becomes a stated requirement, begin Phase 1 work
- **Monitoring:** Add a simple query to detect if `gitops_schema_sources` is populated without sync occurring
- **Guidance:** Add onboarding docs for manual schema registration until GitOps is ready

### Long-term (Future)
- **Decide:** Commit to implementing GitOps or remove the table
- **Path:** If removed, document migration for any future attempts

---

## Related Documentation

- Event Payload Schemas: `crate/lib/sinex-schema/docs/schema_design.md`
- Type System Patterns: `docs/current/architecture/type-system-patterns.md`
- Schema Registry: `crate/lib/sinex-core/docs/schema_management.md`
- Exploration Analysis: `docs/exploration/loops/loop-020/02-analysis.md` (GitOps findings)
