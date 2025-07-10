use chrono::Utc;
use serde_json::json;
use sinex_db::{models::AgentManifest, JsonValue, RawEvent};
use sinex_automaton::{EventScanner, ScannerConfig, WorkRouter};
use sinex_ulid::Ulid;

/// Helper to create a test event
fn create_test_event(source: &str, event_type: &str) -> RawEvent {
    RawEvent {
        id: Ulid::new(),
        source: source.to_string(),
        event_type: event_type.to_string(),
        ts_ingest: Utc::now(),
        ts_orig: None,
        host: "test-host".to_string(),
        ingestor_version: Some("1.0.0".to_string()),
        payload_schema_id: None,
        payload: json!({
            "test": "data"
        }),
    }
}

/// Helper to create a test agent manifest
fn create_test_manifest(agent_name: &str, status: &str, subscriptions: JsonValue) -> AgentManifest {
    AgentManifest {
        agent_name: agent_name.to_string(),
        description: Some("Test agent".to_string()),
        version: "1.0.0".to_string(),
        status: status.to_string(),
        agent_type: "promoter".to_string(),
        config_template_json: None,
        produces_event_types: None,
        subscribes_to_event_types: Some(subscriptions),
        required_capabilities: None,
        llm_dependencies: None,
        repo_url: None,
        last_heartbeat_ts: None,
        last_error_ts: None,
        last_error_summary: None,
        registered_at: Utc::now(),
        updated_at: Utc::now(),
    }
}

#[tokio::test]
async fn test_work_router_integration() {
    // Create test manifests
    let manifests = vec![
        create_test_manifest(
            "agent1",
            "running",
            json!({
                "test.source": ["event1", "event2"],
                "other.source": ["event3"]
            }),
        ),
        create_test_manifest(
            "agent2",
            "running",
            json!({
                "test.source": ["event2"],
                "*": ["special_event"]
            }),
        ),
        create_test_manifest(
            "agent3",
            "stopped",
            json!({
                "test.source": ["event1"]
            }),
        ),
    ];

    let router = WorkRouter::from_manifests(manifests);

    // Test event routing
    let event1 = create_test_event("test.source", "event1");
    let agents = router.route_event(&event1);
    assert_eq!(agents, vec!["agent1"]);

    let event2 = create_test_event("test.source", "event2");
    let agents = router.route_event(&event2);
    assert_eq!(agents, vec!["agent1", "agent2"]);

    let event3 = create_test_event("other.source", "event3");
    let agents = router.route_event(&event3);
    assert_eq!(agents, vec!["agent1"]);

    let special_event = create_test_event("any.source", "special_event");
    let agents = router.route_event(&special_event);
    assert_eq!(agents, vec!["agent2"]);

    let unmatched_event = create_test_event("unknown.source", "unknown_event");
    let agents = router.route_event(&unmatched_event);
    assert!(agents.is_empty());
}

#[test]
fn test_scanner_state_management() {
    let config = ScannerConfig {
        batch_size: 100,
        initial_lookback: chrono::Duration::hours(12),
        process_historical: false,
    };

    let scanner = EventScanner::new(config);

    // Initial state should be empty
    assert!(scanner.state().last_event_ids.is_empty());
    assert!(scanner.state().last_scan_ts.is_none());

    // Test state restoration instead of direct mutation
    let mut test_state = sinex_automaton::scanner::ScannerState::default();
    let event_id1 = Ulid::new();
    let event_id2 = Ulid::new();
    test_state
        .last_event_ids
        .insert("source1".to_string(), event_id1);
    test_state
        .last_event_ids
        .insert("source2".to_string(), event_id2);

    let mut scanner2 = EventScanner::new(ScannerConfig {
        batch_size: 100,
        initial_lookback: chrono::Duration::hours(12),
        process_historical: false,
    });
    scanner2.restore_state(test_state);

    // Verify state tracking
    assert_eq!(scanner2.state().last_event_ids.len(), 2);
    assert_eq!(
        scanner2.state().last_event_ids.get("source1"),
        Some(&event_id1)
    );
    assert_eq!(
        scanner2.state().last_event_ids.get("source2"),
        Some(&event_id2)
    );
}

#[test]
fn test_router_with_complex_subscriptions() {
    let manifests = vec![
        create_test_manifest(
            "data-processor",
            "running",
            json!({
                "filesystem.watcher": ["file_created", "file_modified", "file_deleted"],
                "terminal.kitty": ["command_executed"],
                "hyprland": ["window_focused", "workspace_changed"]
            }),
        ),
        create_test_manifest(
            "metrics-collector",
            "running",
            json!({
                "*": ["heartbeat", "error", "metric"]
            }),
        ),
        create_test_manifest(
            "specific-handler",
            "running",
            json!({
                "app.chrome": ["tab_opened", "tab_closed"],
                "app.firefox": ["tab_opened", "tab_closed"]
            }),
        ),
    ];

    let router = WorkRouter::from_manifests(manifests);

    // Test filesystem events
    let file_event = create_test_event("filesystem.watcher", "file_created");
    assert_eq!(router.route_event(&file_event), vec!["data-processor"]);

    // Test terminal events
    let cmd_event = create_test_event("terminal.kitty", "command_executed");
    assert_eq!(router.route_event(&cmd_event), vec!["data-processor"]);

    // Test heartbeat events (wildcard match)
    let heartbeat = create_test_event("any.service", "heartbeat");
    assert_eq!(router.route_event(&heartbeat), vec!["metrics-collector"]);

    // Test browser events
    let chrome_event = create_test_event("app.chrome", "tab_opened");
    assert_eq!(router.route_event(&chrome_event), vec!["specific-handler"]);

    // Test unmatched event
    let unknown = create_test_event("unknown.source", "unknown_type");
    assert!(router.route_event(&unknown).is_empty());
}

// Note: Database integration tests would require a test database setup
// For now, we're focusing on unit tests for the core logic
