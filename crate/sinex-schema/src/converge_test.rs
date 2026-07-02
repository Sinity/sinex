// Exception to per-crate tests/: this exercises private registry lookup helpers
// without widening the convergence API.
use super::{convergible_tables, find_meta_in};
use crate::apply::ApplyError;
use crate::defs::TableMeta;
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn find_meta_surfaces_missing_registry_entries() -> TestResult<()> {
    let tables: &[TableMeta] = &[];
    let err = find_meta_in(tables, "core.missing").expect_err("missing table should fail");
    assert!(matches!(err, ApplyError::Internal(_)));
    assert_eq!(
        err.to_string(),
        "convergence registry references unknown table metadata: core.missing"
    );
    Ok(())
}

#[sinex_test]
async fn convergible_tables_resolve_known_metadata() -> TestResult<()> {
    let tables = convergible_tables()?;
    // The convergible registry holds every table needing column convergence,
    // named-constraint idempotency, and FK management. Assert structural
    // invariants instead of a magic count (the count legitimately grows as
    // schema lanes are added — a pinned literal just rots and fails on a clean
    // addition, as it did at 17 vs the current registry size).
    assert!(
        tables.len() >= 17,
        "convergible registry collapsed: {} tables",
        tables.len()
    );
    assert_eq!(tables[0].meta.qualified_name, "core.events");
    let mut names: Vec<_> = tables.iter().map(|t| t.meta.qualified_name).collect();
    let total = names.len();
    names.sort_unstable();
    names.dedup();
    assert_eq!(names.len(), total, "convergible registry has duplicate tables");
    Ok(())
}
