use super::*;
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn archived_events_schema_omits_direct_supersession_column() -> TestResult<()> {
    let table_sql = ArchivedEvents::create_table_sql();
    assert!(!table_sql.contains("superseded_by_event_id"));

    let index_sql = ArchivedEvents::create_indexes_sql().join("\n");
    assert!(!index_sql.contains("superseded_by_event_id"));

    let trigger_sql = ArchivedEvents::create_archive_trigger_sql();
    assert!(!trigger_sql.contains("sinex.superseded_by_id"));
    assert!(!trigger_sql.contains("sup_id"));
    assert!(
        trigger_sql
            .contains("INSERT INTO audit.archived_events SELECT OLD.*, now(), who, why;")
    );

    Ok(())
}
