# Loop 020 - GitOps Schema Sources and External Schema Provenance

Goal
- Identify how `sinex_schemas.gitops_schema_sources` is defined and whether it is seeded with external schema repos.
- Determine whether registry-only schemas are expected to come from GitOps sources.

Process
1) Locate the `gitops_schema_sources` schema/table definition.
2) Search for migrations or seed logic that insert GitOps sources.
3) Identify any code paths that read or sync from GitOps sources.
4) Summarize whether external schemas are expected and which repositories are configured.

Deliverables
- Summary of GitOps schema source structure.
- Evidence of seeding/defaults (or lack thereof).
- Implications for registry-only schemas.
