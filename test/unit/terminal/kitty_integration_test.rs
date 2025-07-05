use sinex_events_terminal::kitty::{KittyEventSource, KittyConfig, KittyCommandExecuted, KittyScrollbackCaptured};
use sinex_core::{EventSource, EventSourceContext};

#[tokio::test]
async fn test_kitty_event_source_creation() {
    // Test that we can create a KittyEventSource - this will likely fail to find socket but should not panic
    let ctx = EventSourceContext::for_test(); // Use test constructor
    
    // This should not panic and create the source (socket discovery may fail but that's expected)
    let result = KittyEventSource::initialize(ctx).await;
    assert!(result.is_ok(), "Should be able to create KittyEventSource even without socket");
}

#[test]
fn test_kitty_config_serialization() {
    let config = KittyConfig {
        poll_interval_seconds: 5,
        socket_path: Some("/tmp/kitty.sock".to_string()),
        enabled: true,
    };
    
    // Should serialize/deserialize properly
    let serialized = serde_json::to_string(&config).expect("Should serialize");
    let deserialized: KittyConfig = serde_json::from_str(&serialized).expect("Should deserialize");
    
    assert_eq!(config.poll_interval_seconds, deserialized.poll_interval_seconds);
    assert_eq!(config.socket_path, deserialized.socket_path);
    assert_eq!(config.enabled, deserialized.enabled);
}

#[test]
fn test_kitty_event_types() {
    // Verify event type constants
    assert_eq!(KittyCommandExecuted::EVENT_NAME, "command.executed");
    assert_eq!(KittyScrollbackCaptured::EVENT_NAME, "scrollback.captured");
    assert_eq!(KittyEventSource::SOURCE_NAME, "terminal.kitty");
}