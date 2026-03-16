//! Regression test for Slice 4.3: Guards against re-introduction of replay configuration.
//!
//! This test file ensures that:
//! 1. `NodeConfig` does NOT have a `replay` field
//! 2. `NodeCommand` does NOT have a `Replay` variant

use clap::Parser;
use sinex_node_sdk::NodeConfig;
use sinex_node_sdk::node_cli::NodeCli;
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn test_no_replay_config_field() -> TestResult<()> {
    let config = NodeConfig::builder()
        .service_name("test-service")
        .log_level("info".to_string())
        .database_pool_size(10)
        .dry_run(false)
        .build();

    let json = serde_json::to_value(&config)?;
    let obj = json.as_object().unwrap_or_else(|| {
        panic!(
            "NodeConfig should serialize to a JSON object but got: {json:?}"
        )
    });

    assert!(
        !obj.contains_key("replay"),
        "NodeConfig should not have a 'replay' field, but found one in: {:?}",
        obj.keys().collect::<Vec<_>>()
    );

    Ok(())
}

#[sinex_test]
async fn test_node_cli_no_replay_subcommand() -> TestResult<()> {
    let result = NodeCli::try_parse_from(["sinex-node", "replay"]);

    assert!(
        result.is_err(),
        "NodeCli should reject 'replay' subcommand, but parsing succeeded: {result:?}"
    );

    Ok(())
}
