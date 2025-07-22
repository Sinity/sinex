// Unified Test Common Module - Single API Entry Point
//
// This provides a streamlined test infrastructure through the unified TestContext.
// All test operations flow through TestContext, eliminating fragmented APIs.

// Core infrastructure (public)
pub mod test_context;
pub mod test_macros;
pub mod error_testing;
pub mod property_testing;

// Data creation and management (public - used by TestContext)
pub mod builders;
pub mod fixtures;

// Internal infrastructure (private - accessed only through TestContext)
mod database_pool;
mod timing_utils;
mod coverage_assurance;

// Integration test utilities (public - used by TestContext)
pub mod channel_behavior_utils;
pub mod satellite_management_utils;
pub mod deployment_scenario_utils;

// Public API - Everything tests need
pub use test_context::{
    TestContext, TestResult, TestConfig,
    // Schema testing utilities
    SchemaTestUtils, ValidatedEventBuilder,
    // Contextual assertions
    ContextualAssert,
    // Fixture builders
    ScenarioFixtures, PerformanceFixtures, ErrorFixtures,
    // System testing utilities
    ChannelTestUtils, ProcessTestUtils, DeploymentTestUtils
};

// Essential macros
pub use test_macros::*;

// Core types from other crates
pub use sinex_core_types::RawEvent;
pub use sinex_ulid::Ulid;
pub use serde_json::{json, Value};
pub use chrono::{DateTime, Utc};
pub use anyhow::Result as AnyhowResult;

// Re-export constants for convenience
pub use sinex_events::{sources, event_types};

/// Prelude for convenient test imports
pub mod prelude {
    pub use super::{
        TestContext, TestResult, TestConfig,
        RawEvent, Ulid, json, Value,
        DateTime, Utc, AnyhowResult,
    };
    
    // Essential macros
    pub use super::sinex_test;
    pub use super::assert_event_eq;
    pub use super::assert_error_contains;
    pub use super::eventually;
}

/// Legacy compatibility - these functions are deprecated
/// Use TestContext unified API instead
#[deprecated(since = "1.0.0", note = "Use ctx.event().filesystem().path().created().insert() instead")]
pub fn filesystem_event(path: &str) -> RawEvent {
    use sinex_events::EventFactory;
    EventFactory::new(sources::FS).create_event(
        event_types::filesystem::FILE_CREATED, 
        json!({"path": path})
    )
}

#[deprecated(since = "1.0.0", note = "Use ctx.event().terminal().command().success().insert() instead")]  
pub fn kitty_event(command: &str) -> RawEvent {
    use sinex_events::EventFactory;
    EventFactory::new(sources::SHELL_KITTY).create_event(
        event_types::shell::COMMAND_EXECUTED,
        json!({"command": command, "exit_code": 0})
    )
}

#[deprecated(since = "1.0.0", note = "Use ctx.event().agent().name().heartbeat().insert() instead")]
pub fn agent_event(agent_name: &str) -> RawEvent {
    use sinex_events::EventFactory;
    EventFactory::new(sources::SINEX).create_event(
        event_types::sinex::AUTOMATON_HEARTBEAT,
        json!({"agent_name": agent_name, "status": "running"})
    )
}

/// Configuration for backward compatibility
pub fn test_database_url() -> String {
    std::env::var("TEST_DATABASE_URL")
        .unwrap_or_else(|_| "postgres://sinex_test:testpass@localhost:5433/sinex_test".to_string())
}