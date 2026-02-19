## Description

<!-- Provide a brief summary of your changes -->

## Type of Change

- [ ] Bug fix (non-breaking change which fixes an issue)
- [ ] New feature (non-breaking change which adds functionality)
- [ ] Breaking change (fix or feature that would cause existing functionality to not work as expected)
- [ ] Documentation update
- [ ] Performance improvement
- [ ] Code refactoring

## Testing

- [ ] I have added tests that prove my fix is effective or that my feature works
- [ ] All new and existing tests pass locally
- [ ] I have run `xtask test` to verify basic functionality

## Abstraction Compliance Checklist

### Database Operations

- [ ] Database access flows through `sinex-db` repositories/query helpers (no ad-hoc `PgPool` usage).
- [ ] SQL uses `sqlx::query!`/`query_as!` (compile-time checked) or `sqlx::QueryBuilder` for dynamic clauses—never raw string concatenation.
- [ ] ULIDs/UUIDs rely on the conversion helpers in `sinex-schema` (no manual `.to_uuid()` / `.parse()` chains).

### Error Handling

- [ ] Workspace crates return `SinexError` from `sinex-primitives` rather than `anyhow!` in production paths.
- [ ] `.with_context()` is used to enrich fallible operations.
- [ ] No `.unwrap()` / `.expect()` outside tests and intentional crash points.

### Validation & Constants

- [ ] Inputs go through the shared validation / sanitization helpers (`sinex_primitives::validation`, path sanitizers, etc.) instead of bespoke logic.
- [ ] Event/source/service identifiers reuse existing constants when available—avoid sprinkling string literals such as `"process.heartbeat"` throughout the codebase.
- [ ] Timestamps use `Timestamp` from `sinex-primitives` (not `time::OffsetDateTime`).

## Code Quality

- [ ] I have run `cargo fmt` to format my code
- [ ] I have run `cargo clippy` and addressed all warnings
- [ ] If schema definitions changed, I ran `xtask schema generate` and committed the updated `schemas/` artifacts
- [ ] I have added/updated documentation as needed
- [ ] My code follows the project's style guidelines

## Related Issues

<!-- Link to related issues: Fixes #123, Relates to #456 -->

## Additional Notes

<!-- Any additional information that reviewers should know -->

---

### Reviewer Checklist

- [ ] Code uses proper Sinex abstractions (QueryBuilder, CoreError, constants)
- [ ] No anti-patterns introduced (raw SQL, anyhow, hardcoded strings)
- [ ] Tests follow the same abstraction standards
- [ ] Documentation is clear and up-to-date
- [ ] Performance impact has been considered
