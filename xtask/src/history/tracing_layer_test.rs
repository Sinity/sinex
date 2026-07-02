use super::*;
use crate::sandbox::sinex_test;
use crate::sandbox::timing::WaitHelpers;
use color_eyre::eyre::Context;
use tempfile::tempdir;
use tracing_subscriber::prelude::*;

#[sinex_test]
async fn test_history_tracing_layer_is_lazy_until_first_persisted_event() -> TestResult<()> {
    let temp = tempdir().context("failed to create tempdir")?;
    let db_path = temp.path().join("history.db");

    let _layer = HistoryTracingLayer::new(db_path.clone());

    assert!(
        !db_path.exists(),
        "history trace DB should not exist before the first persisted event"
    );
    Ok(())
}

#[sinex_test]
async fn test_history_tracing_layer_persists_first_warn_event() -> TestResult<()> {
    let temp = tempdir().context("failed to create tempdir")?;
    let db_path = temp.path().join("history.db");
    let subscriber =
        tracing_subscriber::registry().with(HistoryTracingLayer::new(db_path.clone()));

    tracing::subscriber::with_default(subscriber, || {
        tracing::warn!(target: "xtask::history.tests", code = 17_i64, "persist trace event");
    });

    WaitHelpers::wait_for_condition(
        || {
            let db_path = db_path.clone();
            async move {
                if !db_path.exists() {
                    return Ok::<bool, color_eyre::Report>(false);
                }

                let conn = Connection::open(&db_path)
                    .with_context(|| format!("failed to open {}", db_path.display()))?;
                let table_exists = conn
                    .query_row(
                        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'trace_events'",
                        [],
                        |_| Ok(()),
                    )
                    .map(|()| true)
                    .or_else(|error| {
                        if matches!(error, rusqlite::Error::QueryReturnedNoRows) {
                            Ok(false)
                        } else {
                            Err(error)
                        }
                    })?;
                if !table_exists {
                    return Ok::<bool, color_eyre::Report>(false);
                }

                let count: i64 =
                    conn.query_row("SELECT COUNT(*) FROM trace_events", [], |row| row.get(0))?;
                Ok::<bool, color_eyre::Report>(count == 1)
            }
        },
        5,
    )
    .await?;

    Ok(())
}
