use super::*;
use crate::sandbox::sinex_test;

#[sinex_test]
async fn temporal_ledger_uses_truncate() -> ::xtask::sandbox::TestResult<()> {
    let config = CleanupConfig::default();
    let temporal_ledger = config
        .tables
        .iter()
        .find(|t| t.table_name == "raw.temporal_ledger")
        .expect("temporal_ledger should be in config");

    assert_eq!(
        temporal_ledger.method,
        CleanupMethod::Truncate,
        "temporal_ledger should use TRUNCATE (bypasses append-only trigger)"
    );
    Ok(())
}

#[sinex_test]
async fn core_events_uses_truncate() -> ::xtask::sandbox::TestResult<()> {
    let config = CleanupConfig::default();
    let events = config
        .tables
        .iter()
        .find(|t| t.table_name == "core.events")
        .expect("core.events should be in config");

    assert_eq!(
        events.method,
        CleanupMethod::Truncate,
        "core.events should use TRUNCATE (TimescaleDB 2.x+ supports it)"
    );
    Ok(())
}

#[sinex_test]
async fn no_duplicate_tables() -> ::xtask::sandbox::TestResult<()> {
    let config = CleanupConfig::default();
    let mut seen = std::collections::HashSet::new();

    for table in &config.tables {
        assert!(
            !table.protected || table.method == CleanupMethod::Skip,
            "Protected tables must be marked Skip: {}",
            table.table_name
        );
        assert!(
            seen.insert(table.table_name),
            "Duplicate table in config: {}",
            table.table_name
        );
    }
    Ok(())
}

#[sinex_test]
async fn all_tables_have_valid_names() -> ::xtask::sandbox::TestResult<()> {
    let config = CleanupConfig::default();

    for table in &config.tables {
        assert!(
            table.table_name.contains('.'),
            "Table name should be fully qualified (schema.table): {}",
            table.table_name
        );
    }
    Ok(())
}

#[sinex_test]
async fn ordered_tables_cover_all_entries() -> ::xtask::sandbox::TestResult<()> {
    let config = CleanupConfig::default();
    let ordered = config.ordered_tables();

    assert_eq!(
        ordered.len(),
        config.tables.len(),
        "ordered_tables should include every configured table"
    );

    let mut seen = std::collections::HashSet::new();
    for t in ordered {
        assert!(
            seen.insert(t.table_name),
            "ordered_tables contains duplicate: {}",
            t.table_name
        );
    }
    Ok(())
}
