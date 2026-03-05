#![cfg(feature = "messaging")]

use sinex_node_sdk::SelfObserverConfig;
use std::time::Duration;
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn test_config_defaults() -> TestResult<()> {
    let config = SelfObserverConfig::default();
    assert!(config.enabled);
    assert_eq!(config.min_emission_interval, Duration::from_secs(1));
    Ok(())
}
