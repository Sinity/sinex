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
- [ ] I have run `just test-fast` to verify basic functionality

## Abstraction Compliance Checklist

### Database Operations
- [ ] All database queries use QueryBuilder from sinex-db
- [ ] No raw `sqlx::query!` or `sqlx::query_as!` calls
- [ ] ULID fields are handled by QueryBuilder (no manual `.to_uuid()`)

### Error Handling
- [ ] All errors use CoreError from sinex-error
- [ ] No `anyhow!` or `bail!` in production code
- [ ] Errors include proper context via `.context()`
- [ ] No `.unwrap()` or `.expect()` outside of tests

### String Constants
- [ ] Event types use constants from `sinex_events::constants::event_types`
- [ ] Sources use constants from `sinex_events::constants::sources`
- [ ] Service names use constants from `sinex_events::constants::services`
- [ ] No hardcoded strings like `"process.heartbeat"` or `"core.events"`

### Validation
- [ ] Input validation uses ValidationChain from sinex-validation
- [ ] No manual validation logic for common patterns

## Code Quality

- [ ] I have run `cargo fmt` to format my code
- [ ] I have run `cargo clippy` and addressed all warnings
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