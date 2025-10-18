use std::collections::HashMap;

use serde_json::Value as JsonValue;
use sinex_core::db::replay::{DryRunExecutor, ReplayConfig};
use sinex_core::{Event, Id};
use sinex_test_utils::sinex_test;

#[sinex_test]
fn dry_run_executor_tracks_operations() -> color_eyre::eyre::Result<()> {
    let mut executor = DryRunExecutor::new(ReplayConfig {
        dry_run: true,
        dry_run_verbose: true,
        ..Default::default()
    });

    executor.simulate_archive(Id::<Event<JsonValue>>::new());
    executor.simulate_delete(Id::<Event<JsonValue>>::new());

    let mut changes = HashMap::new();
    changes.insert("status".to_string(), serde_json::json!("processed"));
    executor.simulate_modify(Id::<Event<JsonValue>>::new(), changes);

    let result = executor.complete();
    assert_eq!(result.operations.len(), 3);
    assert_eq!(result.events_to_archive.len(), 1);
    assert_eq!(result.events_to_delete.len(), 1);
    assert_eq!(result.events_to_modify.len(), 1);
    assert!(result.estimated_duration_ms > 0);
    Ok(())
}

#[sinex_test]
fn dry_run_executor_captures_dependencies() -> color_eyre::eyre::Result<()> {
    let mut executor = DryRunExecutor::new(ReplayConfig::default());
    let event_id = Id::<Event<JsonValue>>::new();
    let deps = vec![Id::<Event<JsonValue>>::new(), Id::<Event<JsonValue>>::new()];
    executor.check_integrity(event_id, deps);

    let result = executor.complete();
    assert_eq!(result.warnings.len(), 1);
    assert!(result.warnings[0].contains("2 dependent events"));
    Ok(())
}
