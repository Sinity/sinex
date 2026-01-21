# Loop 021 - Concrete Issues

1) GitOps schema sources are not seeded in migrations.
- The canonical migration creates `sinex_schemas.gitops_schema_sources` but does not insert any default sources.
- Without external tooling, the table remains empty and unused.
