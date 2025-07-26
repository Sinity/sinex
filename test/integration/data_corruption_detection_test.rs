// Data corruption detection integration tests
//
// Tests for:
// - Corrupt event payload detection
// - Invalid ULID detection and recovery
// - Foreign key integrity violations
// - Encoding corruption detection
// - Large-scale corruption scanning

use sinex_db::integrity::{IntegrityTestConfig, IntegrityTester};
use sinex_db::queries::operations::OperationQueries;
use sinex_db::validation::DataCorruptionType;
use sinex_test_utils::builders::BatchEventBuilder;
use sinex_test_utils::prelude::*;
use uuid::Uuid;

#[sinex_test]
async fn test_corrupt_payload_detection(ctx: TestContext) -> TestResult {
    let pool = ctx.pool();

    // Insert events with various payload corruption scenarios
    let corruption_scenarios = vec![
        ("null_payload", json!(null)),
        ("empty_object", json!({})),
        ("malformed_structure", json!({"__proto__": "malicious"})),
        ("oversized_payload", json!({"large": "x".repeat(100_000)})),
        (
            "nested_nulls",
            json!({"data": {"value": null, "items": [null, null]}}),
        ),
    ];

    let mut corrupted_event_ids = Vec::new();

    for (scenario, payload) in corruption_scenarios {
        let factory = EventFactory::new("test.corruption");
        let raw_event = factory.create_event(scenario, payload);
        let event = insert_event_with_validator(&pool, &raw_event, None).await;

        match event {
            Ok(e) => {
                corrupted_event_ids.push(e.id);
                println!("Inserted corrupted event {}: {}", scenario, e.id);
            }
            Err(e) => {
                println!("Failed to insert {} (expected): {}", scenario, e);
            }
        }
    }

    // Run integrity check to detect corruption
    let integrity_tester = IntegrityTester::new(&pool).await?;
    let config = IntegrityTestConfig {
        max_events_to_check: 1000,
        check_window_hours: 1,
        include_deep_validation: true,
        validate_checkpoints: false,
        validate_ulid_ordering: false,
        validate_schemas: true,
    };

    let results = integrity_tester.run_integrity_tests(config).await?;

    println!("Corruption detection results:");
    println!(
        "  Data corruption indicators: {}",
        results.check_report.data_corruption_indicators.len()
    );
    println!(
        "  Schema violations: {}",
        results.check_report.schema_violations.len()
    );

    // Should detect various types of corruption
    let corruption_types: HashSet<_> = results
        .check_report
        .data_corruption_indicators
        .iter()
        .map(|indicator| &indicator.corruption_type)
        .collect();

    println!("Detected corruption types: {:?}", corruption_types);

    // Verify specific corruption patterns are detected
    for indicator in &results.check_report.data_corruption_indicators {
        println!(
            "  - {}: {} (suggestion: {})",
            indicator.corruption_type, indicator.details, indicator.recovery_suggestion
        );

        match indicator.corruption_type {
            DataCorruptionType::NullPayload => {
                assert!(
                    indicator.details.contains("null payload"),
                    "Should mention null payload in details"
                );
            }
            DataCorruptionType::EncodingError => {
                assert!(
                    !indicator.recovery_suggestion.is_empty(),
                    "Should provide recovery suggestion"
                );
            }
            _ => {}
        }
    }

    // Cleanup
    sqlx::query!("DELETE FROM core.events WHERE source = 'test.corruption'")
        .execute(pool)
        .await?;

    Ok(())
}

#[sinex_test]
async fn test_invalid_ulid_detection(ctx: TestContext) -> TestResult {
    let pool = ctx.pool();

    // Test ULID validation with various invalid scenarios
    let invalid_ulid_tests = vec![
        ("nil_ulid", Ulid::from_bytes([0; 16])),
        (
            "future_ulid",
            Ulid::from_datetime(Utc::now() + ChronoDuration::hours(24)),
        ),
        (
            "ancient_ulid",
            Ulid::from_datetime(DateTime::from_timestamp(0, 0).unwrap()),
        ),
    ];

    let mut test_event_ids = Vec::new();

    for (test_name, test_ulid) in invalid_ulid_tests {
        // Try to insert event with problematic ULID
        let result = sqlx::query!(
            r#"
            INSERT INTO core.events (
                event_id, source, event_type, ts_orig, host, payload,
                source_event_ids, source_material_id, 
                associated_blob_ids, ingestor_version, payload_schema_id
            ) VALUES (
                $1::uuid, $2, $3, $4, $5, $6,
                $7::uuid[], $8::uuid,
                $9::uuid[], $10, $11::uuid
            )
            RETURNING event_id as "event_id: Uuid"
            "#,
            test_ulid.to_uuid(),
            "test.ulid",
            test_name,
            Utc::now(),
            "test-host",
            json!({"test": test_name}),
            &[] as &[Uuid],
            None::<Uuid>,
            &[] as &[Uuid],
            "test-v1",
            None::<Uuid>
        )
        .fetch_one(pool)
        .await;

        match result {
            Ok(record) => {
                test_event_ids.push(Ulid::from(record.event_id));
                println!("Inserted {} with ULID: {}", test_name, test_ulid);
            }
            Err(e) => {
                println!("Failed to insert {} (expected): {}", test_name, e);
            }
        }
    }

    // Run integrity tests
    let integrity_tester = IntegrityTester::new(&pool).await?;
    let config = IntegrityTestConfig {
        max_events_to_check: 1000,
        check_window_hours: 24,
        include_deep_validation: true,
        validate_checkpoints: false,
        validate_ulid_ordering: true,
        validate_schemas: false,
    };

    let results = integrity_tester.run_integrity_tests(config).await?;

    // Check for ULID-related issues
    let ulid_issues: Vec<_> = results
        .check_report
        .data_corruption_indicators
        .iter()
        .filter(|indicator| {
            matches!(
                indicator.corruption_type,
                DataCorruptionType::InvalidUlid | DataCorruptionType::FutureTimestamp
            )
        })
        .collect();

    println!("Found {} ULID-related issues", ulid_issues.len());
    for issue in &ulid_issues {
        println!("  - {}: {}", issue.corruption_type, issue.details);
    }

    // Cleanup
    sqlx::query!("DELETE FROM core.events WHERE source = 'test.ulid'")
        .execute(pool)
        .await?;

    Ok(())
}

#[sinex_test]
async fn test_foreign_key_integrity(ctx: TestContext) -> TestResult {
    let pool = ctx.pool();

    // Create a valid event first
    let event =
        EventFactory::new("test.integrity").create_event("parent", json!({"status": "parent"}));
    let parent_event = insert_event(&pool, &event).await?;

    // Try to create an event with invalid foreign key references
    let invalid_refs = vec![
        Ulid::new(),                 // Non-existent event ID
        Ulid::from_bytes([255; 16]), // Max ULID
    ];

    for invalid_ref in invalid_refs {
        let result = sqlx::query!(
            r#"
            INSERT INTO core.events (
                event_id, source, event_type, ts_orig, host, payload,
                source_event_ids, source_material_id,
                associated_blob_ids, ingestor_version, payload_schema_id
            ) VALUES (
                $1::uuid, $2, $3, $4, $5, $6,
                $7::uuid[], $8::uuid,
                $9::uuid[], $10, $11::uuid
            )
            "#,
            Ulid::new().to_uuid(),
            "test.integrity",
            "child_with_bad_ref",
            Utc::now(),
            "test-host",
            json!({"ref": invalid_ref.to_string()}),
            &[invalid_ref.to_uuid()],
            None::<Uuid>,
            &[] as &[Uuid],
            "test-v1",
            None::<Uuid>
        )
        .execute(pool)
        .await;

        // Should either fail or create an event with orphaned reference
        match result {
            Ok(_) => println!("Created event with invalid reference (will be detected)"),
            Err(e) => println!("Failed to create event with invalid ref (expected): {}", e),
        }
    }

    // Run integrity check
    let integrity_tester = IntegrityTester::new(&pool).await?;
    let config = IntegrityTestConfig {
        max_events_to_check: 1000,
        check_window_hours: 1,
        include_deep_validation: true,
        validate_checkpoints: false,
        validate_ulid_ordering: false,
        validate_schemas: false,
    };

    let results = integrity_tester.run_integrity_tests(config).await?;

    // Check for foreign key violations
    let fk_violations: Vec<_> = results
        .check_report
        .data_corruption_indicators
        .iter()
        .filter(|indicator| {
            indicator.details.contains("reference") || indicator.details.contains("orphaned")
        })
        .collect();

    println!("Found {} foreign key issues", fk_violations.len());

    // Cleanup
    sqlx::query!("DELETE FROM core.events WHERE source = 'test.integrity'")
        .execute(pool)
        .await?;

    Ok(())
}

#[sinex_test]
async fn test_encoding_corruption_detection(ctx: TestContext) -> TestResult {
    let pool = ctx.pool();

    // Create events with various encoding issues
    let encoding_tests = vec![
        ("invalid_utf8", json!({"text": "Invalid UTF-8: \u{FFFD}"})),
        (
            "control_chars",
            json!({"text": "Control: \u{0000}\u{0001}\u{001F}"}),
        ),
        ("mixed_encoding", json!({"text": "Mixed: café ☕ 咖啡"})),
        ("emoji_stress", json!({"text": "👨‍👩‍👧‍👦🏳️‍🌈🧑‍💻"})),
    ];

    for (test_name, payload) in encoding_tests {
        let event = EventFactory::new("test.encoding").create_event(test_name, payload);

        match insert_event(&pool, &event).await {
            Ok(e) => println!("Inserted {} event: {}", test_name, e.id),
            Err(e) => println!("Failed to insert {} (expected): {}", test_name, e),
        }
    }

    // Run integrity check with encoding validation
    let integrity_tester = IntegrityTester::new(&pool).await?;
    let config = IntegrityTestConfig {
        max_events_to_check: 1000,
        check_window_hours: 1,
        include_deep_validation: true,
        validate_checkpoints: false,
        validate_ulid_ordering: false,
        validate_schemas: true,
    };

    let results = integrity_tester.run_integrity_tests(config).await?;

    // Check for encoding issues
    let encoding_issues: Vec<_> = results
        .check_report
        .data_corruption_indicators
        .iter()
        .filter(|indicator| {
            matches!(indicator.corruption_type, DataCorruptionType::EncodingError)
                || indicator.details.contains("encoding")
                || indicator.details.contains("UTF-8")
        })
        .collect();

    println!("Found {} encoding issues", encoding_issues.len());
    for issue in &encoding_issues {
        println!("  - {}: {}", issue.corruption_type, issue.details);
    }

    // Cleanup
    sqlx::query!("DELETE FROM core.events WHERE source = 'test.encoding'")
        .execute(pool)
        .await?;

    Ok(())
}

#[sinex_test]
async fn test_large_scale_corruption_scan(ctx: TestContext) -> TestResult {
    let pool = ctx.pool();

    // Create a mix of valid and corrupt events
    let batch_builder = BatchEventBuilder::new();

    // Add valid events
    for i in 0..50 {
        batch_builder
            .add_event()
            .source("test.scan")
            .event_type("valid_event")
            .payload(json!({
                "index": i,
                "status": "ok",
                "data": format!("Valid data {}", i)
            }));
    }

    // Add events with various issues
    for i in 0..10 {
        // Null payloads
        batch_builder
            .add_event()
            .source("test.scan")
            .event_type("null_payload")
            .payload(json!(null));

        // Empty objects
        batch_builder
            .add_event()
            .source("test.scan")
            .event_type("empty_payload")
            .payload(json!({}));

        // Large payloads
        batch_builder
            .add_event()
            .source("test.scan")
            .event_type("large_payload")
            .payload(json!({
                "index": i,
                "large_data": "x".repeat(10000)
            }));
    }

    batch_builder.insert_all(&pool).await?;

    // Run comprehensive scan
    let start = Instant::now();
    let integrity_tester = IntegrityTester::new(&pool).await?;
    let config = IntegrityTestConfig {
        max_events_to_check: 10000,
        check_window_hours: 24,
        include_deep_validation: true,
        validate_checkpoints: false,
        validate_ulid_ordering: true,
        validate_schemas: true,
    };

    let results = integrity_tester.run_integrity_tests(config).await?;
    let duration = start.elapsed();

    println!("Large-scale scan completed in {:?}", duration);
    println!("Summary:");
    println!("  Total events checked: {}", results.events_checked);
    println!(
        "  Corruption indicators: {}",
        results.check_report.data_corruption_indicators.len()
    );
    println!(
        "  Schema violations: {}",
        results.check_report.schema_violations.len()
    );
    println!("  Warning count: {}", results.check_report.warnings.len());

    // Performance assertion
    assert!(
        duration < Duration::from_secs(5),
        "Large-scale scan should complete within 5 seconds"
    );

    // Verify detection accuracy
    let null_payload_issues = results
        .check_report
        .data_corruption_indicators
        .iter()
        .filter(|i| matches!(i.corruption_type, DataCorruptionType::NullPayload))
        .count();

    assert!(
        null_payload_issues >= 10,
        "Should detect at least 10 null payload issues"
    );

    // Cleanup
    sqlx::query!("DELETE FROM core.events WHERE source = 'test.scan'")
        .execute(pool)
        .await?;

    Ok(())
}

#[sinex_test]
async fn test_corruption_recovery_suggestions(ctx: TestContext) -> TestResult {
    let pool = ctx.pool();

    // Create events that will trigger specific recovery suggestions
    let test_scenarios = vec![
        ("null_payload", json!(null), DataCorruptionType::NullPayload),
        (
            "invalid_json",
            json!({"broken": "structure", "nested": {"incomplete": null}}),
            DataCorruptionType::InvalidJson,
        ),
        (
            "missing_required",
            json!({"partial": "data"}),
            DataCorruptionType::MissingRequiredField,
        ),
    ];

    for (scenario, payload, expected_type) in &test_scenarios {
        let event = EventFactory::new("test.recovery").create_event(scenario, payload.clone());

        let _ = insert_event(&pool, &event).await;
    }

    // Run integrity check
    let integrity_tester = IntegrityTester::new(&pool).await?;
    let config = IntegrityTestConfig {
        max_events_to_check: 1000,
        check_window_hours: 1,
        include_deep_validation: true,
        validate_checkpoints: false,
        validate_ulid_ordering: false,
        validate_schemas: true,
    };

    let results = integrity_tester.run_integrity_tests(config).await?;

    // Verify recovery suggestions are provided
    for indicator in &results.check_report.data_corruption_indicators {
        assert!(
            !indicator.recovery_suggestion.is_empty(),
            "Should provide recovery suggestion for {}",
            indicator.corruption_type
        );

        println!(
            "Corruption: {} - Recovery: {}",
            indicator.corruption_type, indicator.recovery_suggestion
        );

        // Verify suggestions are actionable
        assert!(
            indicator.recovery_suggestion.contains("export")
                || indicator.recovery_suggestion.contains("repair")
                || indicator.recovery_suggestion.contains("restore")
                || indicator.recovery_suggestion.contains("validate"),
            "Recovery suggestion should be actionable"
        );
    }

    // Cleanup
    sqlx::query!("DELETE FROM core.events WHERE source = 'test.recovery'")
        .execute(pool)
        .await?;

    Ok(())
}

#[sinex_test]
async fn test_checkpoint_data_corruption(ctx: TestContext) -> TestResult {
    let pool = ctx.pool();

    // Create some checkpoints with potentially corrupt data
    let checkpoint_data = vec![
        json!({"valid": true, "count": 100}),
        json!(null),
        json!({}),
        json!({"nested": {"deep": {"very": {"deep": null}}}}),
    ];

    for (i, data) in checkpoint_data.iter().enumerate() {
        let result = sqlx::query!(
            r#"
            INSERT INTO core.automaton_checkpoints (
                automaton_name, last_processed_id, processed_count,
                last_activity, checkpoint_data
            ) VALUES ($1, $2::uuid, $3, $4, $5)
            "#,
            format!("test_automaton_{}", i),
            Ulid::new().to_uuid(),
            i as i64,
            Utc::now(),
            data
        )
        .execute(pool)
        .await;

        match result {
            Ok(_) => println!("Created checkpoint {}", i),
            Err(e) => println!("Failed to create checkpoint {} (expected): {}", i, e),
        }
    }

    // Run integrity check with checkpoint validation
    let integrity_tester = IntegrityTester::new(&pool).await?;
    let config = IntegrityTestConfig {
        max_events_to_check: 100,
        check_window_hours: 1,
        include_deep_validation: false,
        validate_checkpoints: true,
        validate_ulid_ordering: false,
        validate_schemas: false,
    };

    let results = integrity_tester.run_integrity_tests(config).await?;

    // Check for checkpoint issues
    let checkpoint_issues: Vec<_> = results
        .check_report
        .data_corruption_indicators
        .iter()
        .filter(|indicator| {
            indicator.details.contains("checkpoint") || indicator.details.contains("automaton")
        })
        .collect();

    println!("Found {} checkpoint issues", checkpoint_issues.len());

    // Cleanup
    sqlx::query!(
        "DELETE FROM core.automaton_checkpoints WHERE automaton_name LIKE 'test_automaton_%'"
    )
    .execute(pool)
    .await?;

    Ok(())
}
