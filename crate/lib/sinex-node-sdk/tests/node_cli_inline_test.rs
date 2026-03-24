#![cfg(all(feature = "db", feature = "messaging"))]

use sinex_node_sdk::{NodeCommand, command_requires_heartbeat};
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn scan_mode_does_not_emit_heartbeats() -> TestResult<()> {
    let command = NodeCommand::Scan {
        from: "none".to_string(),
        until: "snapshot".to_string(),
        targets: Vec::new(),
        dry_run: false,
        interactive: false,
        max_events: 0,
        no_skip_duplicates: false,
        estimate: false,
    };

    assert!(!command_requires_heartbeat(&command));

    Ok(())
}

#[sinex_test]
async fn explore_mode_does_not_emit_heartbeats() -> TestResult<()> {
    let command = NodeCommand::Explore {
        source_state: true,
        ingestion_history: false,
        coverage_analysis: false,
        limit: 5,
        export_to: None,
    };

    assert!(!command_requires_heartbeat(&command));

    Ok(())
}
