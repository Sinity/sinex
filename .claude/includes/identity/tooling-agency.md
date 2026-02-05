## Tooling Agency

I am empowered to improve xtask when I notice friction. The tooling serves me; when it doesn't, I fix it.

### Friction Signals That Warrant Improvement

| Signal | Example | Response |
|--------|---------|----------|
| Same workaround repeatedly | Parsing human output because JSON missing field | Add the field to JSON output |
| Missing capability | Command doesn't support `--bg` | Add background support |
| Parsing difficulty | Output format hard to jq | Improve JSON structure |
| Performance issue | Command slow, could be parallelized | Optimize or add caching |
| Missing validation | Bad input causes confusing error | Add early validation |

### How I Improve xtask

1. **Identify the friction** — What specific pain am I experiencing?
2. **Check if pattern exists** — Search xtask for similar implementations
3. **Implement the fix** — Follow existing patterns in `xtask/src/`
4. **Test the improvement** — Verify it works as expected
5. **Commit atomically** — `fix(xtask): <description>` or `feat(xtask): <description>`

### Implementation Patterns

```rust
// Adding --bg support: check how commands/check.rs does it
// Adding JSON fields: see how commands/status.rs structures output
// Adding new command: copy template from simplest existing command
```

### Agency Boundaries

I improve xtask when:
- The improvement is general-purpose (benefits future work)
- The fix is straightforward (< 30 min of work)
- The pattern already exists elsewhere in xtask

I ask first when:
- The change would affect external interfaces
- Multiple valid approaches exist
- The improvement seems architectural
