#![cfg(feature = "messaging")]

use sinex_node_sdk::automaton_base::{AutomatonFields, AutomatonStats};
use xtask::sandbox::prelude::*;

#[derive(Default)]
struct TestConfig;

#[sinex_test]
async fn automaton_stats_tracks_inputs_and_outputs() -> TestResult<()> {
    let mut stats = AutomatonStats::new();
    assert_eq!(stats.inputs_seen, 0);
    assert_eq!(stats.outputs_emitted, 0);
    assert!(stats.last_activity.is_none());

    stats.record_input(10);
    assert_eq!(stats.inputs_seen, 10);
    assert!(stats.last_activity.is_some());

    stats.record_output(5);
    assert_eq!(stats.outputs_emitted, 5);

    stats.record_input(0);
    stats.record_output(0);
    assert_eq!(stats.inputs_seen, 10);
    assert_eq!(stats.outputs_emitted, 5);
    Ok(())
}

#[sinex_test]
async fn automaton_fields_initializes_with_defaults() -> TestResult<()> {
    let fields: AutomatonFields<TestConfig> = AutomatonFields::new();
    assert!(fields.runtime.is_none());
    #[cfg(feature = "db")]
    assert!(fields.db_pool.is_none());
    assert!(fields.event_sender.is_none());
    assert!(fields.incoming_tx.is_none());
    assert!(fields.incoming_rx.is_none());
    assert!(fields.history.is_empty());
    Ok(())
}

#[sinex_test]
async fn ensure_event_channel_creates_channel() -> TestResult<()> {
    let mut fields: AutomatonFields<TestConfig> = AutomatonFields::new();
    assert!(fields.incoming_tx.is_none());
    assert!(fields.incoming_rx.is_none());

    fields.ensure_event_channel();
    assert!(fields.incoming_tx.is_some());
    assert!(fields.incoming_rx.is_some());
    Ok(())
}

#[sinex_test]
async fn runtime_returns_error_when_not_initialized() -> TestResult<()> {
    let fields: AutomatonFields<TestConfig> = AutomatonFields::new();
    assert!(fields.runtime().is_err());
    Ok(())
}

#[sinex_test]
async fn db_pool_returns_error_when_not_initialized() -> TestResult<()> {
    let fields: AutomatonFields<TestConfig> = AutomatonFields::new();
    assert!(fields.db_pool().is_err());
    Ok(())
}

#[sinex_test]
async fn event_sender_returns_error_when_not_initialized() -> TestResult<()> {
    let fields: AutomatonFields<TestConfig> = AutomatonFields::new();
    assert!(fields.event_sender().is_err());
    Ok(())
}
