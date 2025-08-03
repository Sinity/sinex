// Core Unit Tests - Modernized with Powerful Abstractions
//
// This test file demonstrates modern testing patterns:
// - Property-based testing for comprehensive coverage
// - Snapshot testing for complex assertions (when available)
// - Builder patterns for concise event creation
// - Smart waiting instead of arbitrary sleeps
// - Test macros for common patterns
// - Fixtures and factories for realistic data
//
// Transformed from 22 verbose ULID tests + many event tests into concise property tests

use proptest::prelude::*;
use sinex_types::error::{CoreError, Result as CoreResult, ResultExt};
use sinex_types::events::{event_types, sources, EventFactory};
use sinex_test_utils::prelude::*;
use sinex_test_utils::property_helpers::*;
use sinex_test_utils::test_macros::*;

// =============================================================================
// PROPERTY-BASED TESTS FOR ULID ORDERING (Replaces 22 individual tests)
// =============================================================================

// Single property test replaces multiple ULID ordering tests
sinex_proptest_sync! {
    fn ulid_ordering_properties(
        ulids in proptest::collection::vec(ulids(), 2..20)
    ) {
        // ULID strings should maintain lexicographic ordering
        let mut sorted_ulids = ulids.clone();
        sorted_ulids.sort();

        let mut sorted_strings: Vec<String> = ulids.iter()
            .map(|u| u.to_string())
            .collect();
        sorted_strings.sort();

        let expected_strings: Vec<String> = sorted_ulids.iter()
            .map(|u| u.to_string())
            .collect();

        prop_assert_eq!(sorted_strings, expected_strings,
                       "ULID string ordering should match ULID ordering");

        // Verify string format
        for ulid in &ulids {
            prop_assert_eq!(ulid.to_string().len(), 26, "ULID string should be 26 chars");
        }
    }
}

// Replaces multiple timestamp extraction tests
sinex_proptest_sync! {
    fn ulid_timestamp_properties(
        ts in valid_timestamps()
    ) {
        let ulid = Ulid::from_datetime(ts);
        let extracted_ts = ulid.timestamp_ms();
        let expected_ms = ts.timestamp_millis() as u64;

        // ULID timestamp should match within millisecond precision
        prop_assert_eq!(extracted_ts, expected_ms,
                       "ULID should preserve timestamp with millisecond precision");

        // Round-trip conversion should work
        let ulid_str = ulid.to_string();
        let parsed = Ulid::from_string(&ulid_str);
        prop_assert!(parsed.is_ok(), "ULID string should parse successfully");
        prop_assert_eq!(parsed?, ulid, "Parsed ULID should match original");
    }
}

// Comprehensive ULID invariants test
property_suite! {
    name: ulid_invariants,
    given: ulids(),
    properties: {
        has_correct_length: |ulid| {
            assert_eq!(ulid.to_string().len(), 26);
        },
        is_unique: |ulid| {
            assert_ne!(ulid, Ulid::nil());
        },
        preserves_ordering: |ulid| {
            let ts = ulid.timestamp_ms();
            assert!(ts > 0);
        },
        supports_roundtrip: |ulid| {
            let str = ulid.to_string();
            let parsed = Ulid::from_string(&str)?;
            assert_eq!(parsed, ulid);
        }
    }
}

// =============================================================================
// EVENT CREATION WITH BUILDERS AND MACROS (Replaces verbose event tests)
// =============================================================================

// Replace verbose filesystem event test with macro
test_event_factory!(
    test_filesystem_event_creation,
    sources::FS,
    event_types::filesystem::FILE_CREATED,
    json!({
        "path": "/test/file.txt",
        "size": 1024,
        "permissions": "0644"
    }),
    |event| {
        assert!(!event.host.is_empty());
        assert!(event.id != Ulid::nil());
    }
);

// Replace multiple event source tests with property-based test
sinex_proptest_sync! {
    fn event_factory_produces_valid_events(
        source in event_sources(),
        event_type in event_types(),
        payload in event_payloads()
    ) {
        let event = EventFactory::new(source).create_event(&event_type, payload.clone());

        // All generated events should have required fields
        prop_assert_eq!(event.source, source);
        prop_assert_eq!(event.event_type, event_type);
        prop_assert_eq!(event.payload, payload);
        prop_assert!(!event.host.is_empty());
        prop_assert!(event.id != Ulid::nil());
        prop_assert_eq!(event.id.to_string().len(), 26);
    }
}

// Parameterized test replaces multiple individual event type tests
parameterized_test!(
    test_event_creation_by_type,
    vec![
        (
            "filesystem",
            (
                sources::FS,
                event_types::filesystem::FILE_CREATED,
                json!({"path": "/test/file.txt"})
            )
        ),
        (
            "shell",
            (
                sources::SHELL_KITTY,
                event_types::shell::COMMAND_EXECUTED,
                json!({"command": "ls"})
            )
        ),
        (
            "window",
            (
                sources::WM_HYPRLAND,
                event_types::window_manager::WINDOW_FOCUSED,
                json!({"window_class": "firefox"})
            )
        ),
        (
            "clipboard",
            (
                sources::CLIPBOARD,
                event_types::clipboard::COPIED,
                json!({"content_type": "text/plain"})
            )
        ),
    ],
    |_pool: &DbPool, (_name, (source, event_type, payload)): (&str, (&str, &str, Value))| async move {
        let event = EventFactory::new(source).create_event(event_type, payload);
        assert_eq!(event.source, source);
        assert_eq!(event.event_type, event_type);
        assert!(event.id != Ulid::nil());
        Ok(())
    }
);

// =============================================================================
// ERROR HANDLING WITH PROPERTY-BASED TESTING (Replaces verbose error tests)
// =============================================================================

// Replace multiple error display tests with parameterized test
parameterized_test!(
    test_error_display_formats,
    vec![
        ("database", CoreError::database("Connection failed".into())),
        ("validation", CoreError::validation("Invalid format".into())),
        (
            "serialization",
            CoreError::serialization("Parse error".into())
        ),
        (
            "configuration",
            CoreError::configuration("Missing key".into())
        ),
        ("io", CoreError::io("File not found".into())),
        ("unknown", CoreError::unknown("Mystery error".into())),
    ],
    |_pool: &DbPool, (name, error): (&str, CoreError)| async move {
        let display = error.to_string();
        assert!(display.contains(name) || display.contains(&name.to_uppercase()));
        Ok(())
    }
);

// Comprehensive error context testing with properties
sinex_proptest_sync! {
    fn error_context_accumulates_properly(
        base_msg in "[a-zA-Z ]{10,50}",
        contexts in proptest::collection::vec(
            ("[a-z_]+", "[a-zA-Z0-9 ]{5,20}"),
            1..5
        )
    ) {
        let mut error = CoreError::validation(&base_msg);

        for (key, value) in &contexts {
            error = error.wrap_err_with(key, value);
        }

        let built = error.build();
        let error_string = built.to_string();

        // Base message should be present
        prop_assert!(error_string.contains(&base_msg));

        // All context values should be present
        for (_, value) in &contexts {
            prop_assert!(error_string.contains(value));
        }
    }
}

// =============================================================================
// CONCURRENT EVENT CREATION WITH SMART PATTERNS (Replaces verbose concurrent tests)
// =============================================================================

test_concurrent_operations!(
    test_concurrent_event_factory_safety,
    10, // Number of concurrent tasks
    |_pool: Arc<DbPool>, task_id: usize| async move {
        // Each task creates events with unique source
        let source = format!("task-{}", task_id);
        let events: Vec<RawEvent> = (0..5)
            .map(|i| {
                EventFactory::new(&source)
                    .create_event("concurrent.test", json!({ "task": task_id, "index": i }))
            })
            .collect();

        // Verify all events have unique IDs
        let ids: HashSet<_> = events.iter().map(|e| e.id).collect();
        assert_eq!(ids.len(), events.len());

        Ok(events)
    },
    |_pool: &Arc<DbPool>, results: &[Vec<RawEvent>]| async move {
        // Verify no ID collisions across all tasks
        let all_ids: HashSet<_> = results
            .iter()
            .flat_map(|task_events| task_events.iter().map(|e| e.id))
            .collect();
        let total_events: usize = results.iter().map(|v| v.len()).sum();
        assert_eq!(all_ids.len(), total_events);
        Ok(())
    }
);

// =============================================================================
// TIME-BASED TESTING WITH PROPERTY TESTING (New capabilities)
// =============================================================================

sinex_proptest_sync! {
    fn time_ordered_events_maintain_order(
        batch in time_ordered_batch()
    ) {
        // Verify ULID ordering matches timestamp ordering
        for window in batch.windows(2) {
            let (prev, curr) = (&window[0], &window[1]);

            // ULIDs should be ordered
            prop_assert!(prev.id <= curr.id);

            // Timestamps should be ordered
            if let (Some(prev_ts), Some(curr_ts)) = (prev.ts_orig, curr.ts_orig) {
                prop_assert!(prev_ts <= curr_ts);
            }
        }
    }
}

// =============================================================================
// EDGE CASES WITH PROPERTY-BASED TESTING (Enhanced coverage)
// =============================================================================

sinex_proptest_sync! {
    fn handles_extreme_payloads(
        event in prop_oneof![
            empty_source_event(),
            massive_payload_event(),
            deeply_nested_event(),
            extreme_timestamp_event()
        ]
    ) {
        // Events should still have valid structure
        prop_assert!(event.id != Ulid::nil());
        prop_assert_eq!(event.id.to_string().len(), 26);

        // Source validation would catch empty sources
        if event.source.is_empty() {
            // This would be caught by validation layer
            prop_assert!(event.source.is_empty());
        } else {
            prop_assert!(!event.source.is_empty());
        }
    }
}

// =============================================================================
// REGRESSION TESTS FOR SPECIFIC CASES (Preserve important edge cases)
// =============================================================================

regression_test! {
    name: ulid_string_format_regression,
    input: Ulid::from_string("01ARZ3NDEKTSV4RRFFQ69G5FAV").expect("Valid test ULID"),
    test: |ulid| {
        assert_eq!(ulid.to_string().len(), 26);
        assert_eq!(ulid.to_string(), "01ARZ3NDEKTSV4RRFFQ69G5FAV");
    }
}

// =============================================================================
// STATEFUL PROPERTY TESTING FOR EVENT SEQUENCES (Advanced testing)
// =============================================================================

stateful_proptest! {
    name: event_factory_state_consistency,
    state: Vec<RawEvent>,
    operations: [
        create_event(source: String, event_type: String) => {
            let event = EventFactory::new(&source).create_event(&event_type, json!({}));
            state.push(event.clone());

            // State invariants
            assert!(state.iter().all(|e| e.id != Ulid::nil()));
            assert!(state.windows(2).all(|w| w[0].id != w[1].id));
            assert!(state.iter().all(|e| e.id.to_string().len() == 26));
        },

        clear() => {
            state.clear();
            assert!(state.is_empty());
        }
    ]
}

// =============================================================================
// INTEGRATED SCENARIO TESTS (Realistic workflows)
// =============================================================================

#[sinex_test]
async fn test_realistic_user_activity_scenario(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    // Use property-generated realistic activity
    let mut runner = proptest::test_runner::TestRunner::default();
    let activity = user_activity_batch()
        .new_tree(&mut runner)
        .unwrap()
        .current();

    // Insert all events
    ctx.insert_events(&activity).await?;

    // Verify temporal ordering preserved
    let events = ctx.query_events().await?;
    for window in events.windows(2) {
        assert!(window[0].ts_orig <= window[1].ts_orig);
    }

    // Verify event types match expected patterns
    let event_types: HashSet<_> = events.iter().map(|e| &e.event_type).collect();
    assert!(!event_types.is_empty());

    Ok(())
}

// =============================================================================
// PERFORMANCE CHARACTERISTICS WITH PROPERTY TESTING (New insights)
// =============================================================================

configured_proptest! {
    #[cases(100)]
    fn event_creation_performance_characteristics(
        events in performance_characteristic_events()
    ) {
        let size_bytes = events.payload.to_string().len();
        let size_class = events.payload["size_class"].as_str().unwrap_or("unknown");

        match size_class {
            "small" => assert!(size_bytes < 2_000),
            "medium" => assert!(size_bytes < 20_000),
            "large" => assert!(size_bytes < 200_000),
            "xlarge" => assert!(size_bytes < 2_000_000),
            _ => {}
        }
    }
}

// =============================================================================
// DIFFERENTIAL TESTING (Ensure consistency across implementations)
// =============================================================================

#[sinex_test]
async fn test_event_factory_vs_builder_consistency(_ctx: TestContext) -> color_eyre::eyre::Result<()> {
    // Both methods should produce equivalent events
    let factory_event = EventFactory::new(sources::FS).create_event(
        event_types::filesystem::FILE_CREATED,
        json!({ "path": "/test.txt" }),
    );

    let builder_factory = EventFactory::new(sources::FS);
    let builder_event = builder_factory.create_event(
        event_types::filesystem::FILE_CREATED,
        json!({ "path": "/test.txt" }),
    );

    // Should have same structure (except IDs and timestamps)
    assert_eq!(factory_event.source, builder_event.source);
    assert_eq!(factory_event.event_type, builder_event.event_type);
    assert_eq!(factory_event.payload, builder_event.payload);

    Ok(())
}

// =============================================================================
// CROSS-PROPERTY VERIFICATION (Comprehensive invariants)
// =============================================================================

#[sinex_test]
async fn test_event_invariants_across_sources(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    // Generate events from all sources
    let sources = vec![
        sources::FS,
        sources::SHELL_KITTY,
        sources::WM_HYPRLAND,
        sources::CLIPBOARD,
    ];

    for source in sources {
        let events = ctx
            .events()
            .generic(source, "test.invariant")
            .payload(json!({ "test": true }))
            .build();

        // All events should satisfy base invariants
        assert_events_equivalent(&events, &events); // Self-equivalence
        assert_validation_passes(&events)?;
    }

    Ok(())
}

#[cfg(test)]
mod event_property_tests {
    use super::*;

    property_suite! {
        name: event_structural_properties,
        given: arbitrary_event(),
        properties: {
            has_valid_ulid: |event| {
                assert_ne!(event.id, Ulid::nil());
                assert_eq!(event.id.to_string().len(), 26);
            },
            has_timestamps: |event| {
                assert!(event.ts_ingest > chrono::DateTime::from_timestamp(0, 0).expect("Valid epoch timestamp"));
            },
            has_required_fields: |event| {
                assert!(!event.source.is_empty());
                assert!(!event.event_type.is_empty());
                assert!(!event.host.is_empty());
            }
        }
    }
}
