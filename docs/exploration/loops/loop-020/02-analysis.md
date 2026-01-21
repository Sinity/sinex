# Loop 020 - GitOps Schema Sources and External Schema Provenance

Scope
- `sinex_schemas.gitops_schema_sources` table definition.
- Code paths for seeding or syncing GitOps schema sources.

Table Definition
- The table is defined in `crate/lib/sinex-schema/src/schema/sinex_schemas.rs` with fields:
  - `repository_url`, `branch`, `path_pattern`, `sync_enabled`.
  - `last_sync_at`, `last_sync_commit`, `sync_frequency_minutes`.
  - `updated_at` trigger for change tracking.
- Default `branch` is `main`, default `path_pattern` is `schemas/**/*.json`.

Seeding / Sync Logic
- No code paths were found that insert rows into `gitops_schema_sources`.
- No service or job is present in the codebase that polls Git repositories for schemas.
- The schema design documentation describes the GitOps pattern, but no implementation exists in the runtime.

Findings
- `gitops_schema_sources` is a defined table but has no seeding or sync implementation in this repo.
- Registry-only schemas are therefore not explained by GitOps sources in the current codebase.

Risks
- GitOps schema integration is currently aspirational; registry drift must be managed manually or via external tooling.
- Without a sync job, schema sources in this table (if added manually) will not be acted on.

Opportunities
- Implement or document a GitOps schema sync process if external schema sources are intended.
- Add explicit seed data or onboarding docs if this table is expected to be populated.
