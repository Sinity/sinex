# sinex-test-utils Test Audit (Historical)

This file used to track a point-in-time inventory of the `sinex-test-utils`
suite (duplicate tests, stray `#[test]` usages, and direct SQL calls). That
analysis no longer reflects the current codebase—all tests now run through
`#[sinex_test]`, property suites live alongside their crates, and the raw SQL
review moved into the individual repositories.

If you need a fresh audit, re-run the tooling against the modern layout and
update this note with the capture date. Until then, prefer the
[`TESTING.md`](../../../../TESTING.md) handbook and the
crate-level documentation in `docs/` for authoritative guidance.
