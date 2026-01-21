# Loop 020 - Concrete Issues

1) GitOps schema sources are defined but unused.
- `sinex_schemas.gitops_schema_sources` has no seeding or sync implementation in this repo.
- External schema provenance is not handled by runtime code, leaving schema drift to manual processes.
