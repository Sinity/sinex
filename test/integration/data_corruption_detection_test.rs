// Data corruption detection integration tests
//
// Tests for:
// - Corrupt event payload detection
// - Invalid ULID detection and recovery
// - Foreign key integrity violations
// - Encoding corruption detection
// - Large-scale corruption scanning

use crate::common::prelude::*;
use serde_json::Value;
use sinex_db::integrity::{IntegrityTestConfig, IntegrityTester};
use sinex_db::validation::{DataCorruptionIndicator, DataCorruptionType};
use sinex_db::queries::{EventQueries};
use sinex_db::query_builder::{QueryBuilder, QueryParam};
use sinex_events::{EventFactory, services, event_types};
use uuid::Uuid;

#[sinex_test]
async fn test_corrupt_payload_detection(ctx: TestContext) -> TestResult {
    let pool = ctx.pool().clone();

    // Insert events with various payload corruption scenarios
    let corruption_scenarios = vec![
        ("null_payload", json!(null)),
        ("empty_object", json!({})),
        ("malformed_structure", json!({"__proto__": "malicious"})),
        ("oversized_payload", json!({"large": "x".repeat(100_000)})),
        ("circular_reference", json!({"self": "reference"})), // Simplified
    ];

    let mut corrupted_event_ids = Vec::new();

    for (scenario, payload) in corruption_scenarios {
        let factory = EventFactory::new("test.corruption");
        let raw_event = factory.create_event(scenario, payload);
        let event = sinex_db::insert_event_with_validator(
            pool,
            &raw_event,
            None,
        )
        .await;

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
    let integrity_tester = IntegrityTester::new(pool.clone()).await?;
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
    let corruption_types: std::collections::HashSet<_> = results
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
    EventQueries::delete_by_source("test.corruption".to_string())
        .execute(&pool)
        .await?;

    Ok(())
}

#[sinex_test]
async fn test_invalid_ulid_detection(ctx: TestContext) -> TestResult {
    let pool = ctx.pool().clone();

    // Test ULID validation with various invalid scenarios
    let invalid_ulid_tests = vec![
        ("nil_ulid", Ulid::from_bytes([0; 16]).unwrap()),
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
            "#,
            test_ulid.to_uuid(),
            "test.invalid_ulid",
            test_name,
            test_ulid.timestamp(),
            "localhost",
            json!({"test": test_name}),
            None::<Vec<Uuid>>,  // source_event_ids
            None::<Uuid>,      // source_material_id
            None::<Vec<Uuid>>, // associated_blob_ids
            Some("1.0.0"),     // ingestor_version
            None::<Uuid>,      // payload_schema_id
        )
        .execute(&pool)
        .await;

        match result {
            Ok(_) => {
                test_event_ids.push(test_ulid);
                println!("Inserted test event with {}: {}", test_name, test_ulid);
            }
            Err(e) => {
                println!("Failed to insert {} (may be expected): {}", test_name, e);
            }
        }
    }

    // Check for nil ULIDs specifically - keeping as raw SQL for UUID literal check
    let nil_ulid_count: i64 = sqlx::query_scalar!(
        "SELECT COUNT(*) FROM core.events WHERE event_id::uuid = '00000000-0000-0000-0000-000000000000'::uuid"
    )
    .fetch_one(&pool)
    .await?
    .unwrap_or(0);

    if nil_ulid_count > 0 {
        println!("Found {} nil ULIDs in database", nil_ulid_count);
    }

    // Run integrity check to detect invalid ULIDs
    let integrity_tester = IntegrityTester::new(pool.clone()).await?;
    let config = IntegrityTestConfig {
        max_events_to_check: 1000,
        check_window_hours: 24, // Look back far to catch ancient timestamps
        include_deep_validation: true,
        validate_checkpoints: false,
        validate_ulid_ordering: true,
        validate_schemas: false,
    };

    let results = integrity_tester.run_integrity_tests(config).await?;

    // Check for ULID-related issues
    let ulid_issues: Vec<_> = results
        .check_report
        .ulid_ordering_violations
        .iter()
        .filter(|violation| {
            violation.details.contains("invalid") || violation.details.contains("Invalid")
        })
        .collect();

    let corruption_issues: Vec<_> = results
        .check_report
        .data_corruption_indicators
        .iter()
        .filter(|indicator| matches!(indicator.corruption_type, DataCorruptionType::InvalidUlid))
        .collect();

    println!("ULID validation results:");
    println!("  ULID ordering violations: {}", ulid_issues.len());
    println!(
        "  Invalid ULID corruption indicators: {}",
        corruption_issues.len()
    );

    // Print details of detected issues
    for violation in &ulid_issues {
        println!(
            "  ULID violation: {} - {}",
            violation.violation_type, violation.details
        );
    }

    for indicator in &corruption_issues {
        println!("  ULID corruption: {}", indicator.details);
        assert!(
            !indicator.recovery_suggestion.is_empty(),
            "Should provide recovery guidance for ULID corruption"
        );
    }

    // Cleanup
    EventQueries::delete_by_source("test.invalid_ulid".to_string())
        .execute(&pool)
        .await?;

    Ok(())
}

#[sinex_test]
async fn test_foreign_key_integrity_violations(ctx: TestContext) -> TestResult {
    let pool = ctx.pool().clone();

    // Create test automaton for foreign key testing
    let automaton_name = format!("fk_test_automaton_{}", Ulid::new());

    sqlx::query!(
        "INSERT INTO sinex_schemas.processor_manifests (processor_name, processor_type, version, description)
         VALUES ($1, 'automaton', '1.0.0', 'Foreign key integrity test automaton')",
        automaton_name
    )
    .execute(&pool)
    .await?;

    // Insert valid event
    let valid_event = {
            let factory = EventFactory::new("test.fk_integrity");
            let event = factory.create_event("valid_event", json!({"data": "valid"}));
            insert_event_with_validator(
                pool,
                &event,
                None,
            )
        }
    .await?;

    // Note: Work queue integrity tests removed - work_queue table deprecated in satellite architecture

    // Run integrity check to detect foreign key issues
    let integrity_tester = IntegrityTester::new(pool.clone()).await?;
    let config = IntegrityTestConfig {
        max_events_to_check: 1000,
        check_window_hours: 1,
        include_deep_validation: true,
        validate_checkpoints: false,
        validate_ulid_ordering: false,
        validate_schemas: false,
    };

    let results = integrity_tester.run_integrity_tests(config).await?;

    // Check for foreign key violations in corruption indicators
    let fk_violations: Vec<_> = results
        .check_report
        .data_corruption_indicators
        .iter()
        .filter(|indicator| {
            matches!(
                indicator.corruption_type,
                DataCorruptionType::ForeignKeyViolation
            )
        })
        .collect();

    println!("Foreign key integrity results:");
    println!("  FK violations detected: {}", fk_violations.len());

    for violation in &fk_violations {
        println!("  - {}: {}", violation.corruption_type, violation.details);
        assert!(
            !violation.recovery_suggestion.is_empty(),
            "Should provide recovery guidance for FK violations"
        );
    }

    // Note: Manual work_queue FK checks removed - work_queue table deprecated in satellite architecture

    // Cleanup test events
    EventQueries::delete_by_source("test.fk_integrity".to_string())
        .execute(&pool)
        .await?;

    Ok(())
}

#[sinex_test]
async fn test_encoding_corruption_detection(ctx: TestContext) -> TestResult {
    let pool = ctx.pool().clone();

    // Test various encoding corruption scenarios
    let encoding_tests = vec![
        ("null_bytes", "test\0file.txt"),
        ("control_chars", "test\x01\x02\x03file.txt"),
        ("invalid_utf8", "test\u{00C0}\u{0080}file.txt"), // Overlong encoding as unicode
        ("high_unicode", "test\u{1F4A9}file.txt"),        // Emoji - should be valid
        ("mixed_encoding", "test\u{00FF}\u{00FE}file.txt"), // BOM-like bytes as unicode
    ];

    let mut encoding_event_ids = Vec::new();

    for (test_name, corrupt_string) in encoding_tests {
        // Try to insert events with potentially corrupted strings
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
            corrupt_string, // Use corrupt string as source
            "encoding_test",
            Utc::now(),
            "localhost",
            json!({"path": corrupt_string, "test": test_name}),
            None,
            None::<Uuid>,
            None,
            Some("1.0.0"),
            None::<Uuid>,
        )
        .execute(&pool)
        .await;

        match result {
            Ok(_) => {
                println!("Inserted encoding test event: {}", test_name);
            }
            Err(e) => {
                println!("Failed to insert {} (may be expected): {}", test_name, e);
            }
        }
    }

    // Also test with corrupt payload content
    let payload_corruption_tests = vec![
        json!({"path": "test\0file.txt"}),
        json!({"command": "echo 'test\x01\x02'"}),
        json!({"content": "\u{00FF}\u{00FE}\x00invalid"}),
    ];

    for (i, corrupt_payload) in payload_corruption_tests.iter().enumerate() {
        let factory = EventFactory::new("test.encoding_corruption");
        let raw_event = factory.create_event(&format!("payload_corruption_{}", i), corrupt_payload.clone());
        let result = sinex_db::insert_event_with_validator(
            pool,
            &raw_event,
            None,
        )
        .await;

        match result {
            Ok(event) => {
                encoding_event_ids.push(event.id);
                println!("Inserted payload corruption test {}: {}", i, event.id);
            }
            Err(e) => {
                println!(
                    "Payload corruption test {} failed (may be expected): {}",
                    i, e
                );
            }
        }
    }

    // Run integrity check to detect encoding issues
    let integrity_tester = IntegrityTester::new(pool.clone()).await?;
    let config = IntegrityTestConfig {
        max_events_to_check: 1000,
        check_window_hours: 1,
        include_deep_validation: true,
        validate_checkpoints: false,
        validate_ulid_ordering: false,
        validate_schemas: true,
    };

    let results = integrity_tester.run_integrity_tests(config).await?;

    // Check for encoding corruption detection
    let encoding_issues: Vec<_> = results
        .check_report
        .data_corruption_indicators
        .iter()
        .filter(|indicator| matches!(indicator.corruption_type, DataCorruptionType::EncodingError))
        .collect();

    let schema_violations: Vec<_> = results
        .check_report
        .schema_violations
        .iter()
        .filter(|violation| {
            violation.details.contains("character")
                || violation.details.contains("encoding")
                || violation.details.contains("byte")
        })
        .collect();

    println!("Encoding corruption detection results:");
    println!(
        "  Encoding corruption indicators: {}",
        encoding_issues.len()
    );
    println!("  Related schema violations: {}", schema_violations.len());

    for issue in &encoding_issues {
        println!("  - Encoding issue: {}", issue.details);
        assert!(
            issue.details.contains("Control characters")
                || issue.details.contains("encoding")
                || issue.details.contains("character"),
            "Should mention character/encoding issues"
        );
    }

    for violation in &schema_violations {
        println!("  - Schema violation: {}", violation.details);
    }

    // Manual encoding validation check - keeping complex regex SQL as raw
    let manual_encoding_check = sqlx::query!(
        r#"
        SELECT 
            event_id::text as event_id,
            source,
            event_type,
            CASE 
                WHEN source ~ '[[:cntrl:]]' THEN 'source_has_control_chars'
                WHEN event_type ~ '[[:cntrl:]]' THEN 'event_type_has_control_chars'
                WHEN host ~ '[[:cntrl:]]' THEN 'host_has_control_chars'
                ELSE 'no_obvious_issues'
            END as encoding_issue
        FROM core.events
        WHERE source LIKE 'test%' OR source = 'test.encoding_corruption'
        AND (
            source ~ '[[:cntrl:]]' OR
            event_type ~ '[[:cntrl:]]' OR
            host ~ '[[:cntrl:]]'
        )
        "#
    )
    .fetch_all(&pool)
    .await?;

    println!(
        "Manual encoding check found {} events with control characters",
        manual_encoding_check.len()
    );

    for issue in &manual_encoding_check {
        println!(
            "  Event {}: {} in {}/{}",
            issue.event_id.as_ref().map(|s| s.as_str()).unwrap_or("unknown"), 
            issue.encoding_issue.as_ref().map(|s| s.as_str()).unwrap_or("no_issue"), 
            issue.source.as_str(),
            issue.event_type.as_str()
        );
    }

    // Cleanup
    EventQueries::delete_by_source("test.encoding_corruption".to_string())
        .execute(&pool)
        .await?;
    // Also cleanup test% sources
    sqlx::query!("DELETE FROM core.events WHERE source LIKE 'test%'")
        .execute(&pool)
        .await?;

    Ok(())
}

#[sinex_test]
async fn test_large_scale_corruption_scanning(ctx: TestContext) -> TestResult {
    let pool = ctx.pool().clone();

    // Generate a large dataset with some corrupted entries
    let total_events = 500;
    let corruption_rate = 0.1; // 10% corruption
    let corrupt_count = (total_events as f64 * corruption_rate) as usize;

    println!(
        "Generating {} events with {} corrupted entries",
        total_events, corrupt_count
    );

    let mut all_event_ids = Vec::new();
    let mut corrupt_event_ids = Vec::new();

    // Generate mostly valid events
    for i in 0..(total_events - corrupt_count) {
        let event = {
            let factory = EventFactory::new("test.large_scale");
            let event = factory.create_event("valid_event", json!({"sequence": i}));
            let event_id = sinex_db::insert_event_with_validator(
                pool,
                &event,
                None,
            )
            .await?;
            event
        };
        all_event_ids.push(event.id);

        // Small delay to spread out timestamps
        if i % 50 == 0 {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    }

    // Generate corrupted events
    for i in 0..corrupt_count {
        let corruption_type = i % 4; // Rotate through corruption types

        let (event_type, payload) = match corruption_type {
            0 => ("null_payload", json!(null)),
            1 => ("oversized_payload", json!({"large": "x".repeat(50_000)})),
            2 => (
                "malformed_payload",
                json!({"__proto__": "evil", "constructor": "hack"}),
            ),
            _ => (
                "encoding_issue",
                json!({"path": format!("test\0corrupt_{}.txt", i)}),
            ),
        };

        let factory = EventFactory::new("test.large_scale");
        let raw_event = factory.create_event(event_type, payload);
        let result = sinex_db::insert_event_with_validator(
            pool,
            &raw_event,
            None,
        )
        .await;

        match result {
            Ok(event) => {
                all_event_ids.push(event.id);
                corrupt_event_ids.push(event.id);
            }
            Err(_) => {
                // Some corrupted events may fail to insert (which is good)
                println!("Corrupt event {} failed to insert (expected)", i);
            }
        }
    }

    println!(
        "Inserted {} total events, {} potentially corrupt",
        all_event_ids.len(),
        corrupt_event_ids.len()
    );

    // Run large-scale integrity scan
    let scan_start = std::time::Instant::now();

    let integrity_tester = IntegrityTester::new(pool.clone()).await?;
    let config = IntegrityTestConfig {
        max_events_to_check: total_events as u64,
        check_window_hours: 1,
        include_deep_validation: true,
        validate_checkpoints: false,
        validate_ulid_ordering: true,
        validate_schemas: true,
    };

    let results = integrity_tester.run_integrity_tests(config).await?;

    let scan_duration = scan_start.elapsed();
    let scan_rate = total_events as f64 / scan_duration.as_secs_f64();

    println!("Large-scale corruption scan results:");
    println!("  Scan duration: {:?}", scan_duration);
    println!("  Scan rate: {:.2} events/sec", scan_rate);
    println!(
        "  Events scanned: {}",
        results.check_report.total_events_checked
    );
    println!(
        "  Data corruption indicators: {}",
        results.check_report.data_corruption_indicators.len()
    );
    println!(
        "  Schema violations: {}",
        results.check_report.schema_violations.len()
    );
    println!("  Overall severity: {:?}", results.check_report.severity);

    // Analyze detection accuracy
    let detected_corruption_count = results.check_report.data_corruption_indicators.len()
        + results.check_report.schema_violations.len();

    let detection_rate = detected_corruption_count as f64 / corrupt_event_ids.len().max(1) as f64;

    println!(
        "  Detection rate: {:.2}% ({}/{})",
        detection_rate * 100.0,
        detected_corruption_count,
        corrupt_event_ids.len()
    );

    // Performance assertions
    assert!(
        scan_rate > 10.0,
        "Large-scale scan should process at least 10 events/sec"
    );
    assert!(
        results.check_report.total_events_checked >= total_events as u64 * 8 / 10,
        "Should scan at least 80% of expected events"
    );

    // Should detect significant corruption if it exists
    if corrupt_event_ids.len() > 10 {
        assert!(
            detected_corruption_count > 0,
            "Should detect some corruption in large dataset"
        );
    }

    // Categorize detected corruption types
    let mut corruption_type_counts = std::collections::HashMap::new();

    for indicator in &results.check_report.data_corruption_indicators {
        *corruption_type_counts
            .entry(&indicator.corruption_type)
            .or_insert(0) += 1;
    }

    println!("  Corruption types detected:");
    for (corruption_type, count) in &corruption_type_counts {
        println!("    {:?}: {}", corruption_type, count);
    }

    // Verify recommendations are generated for large-scale issues
    let performance_recommendations: Vec<_> = results
        .recommendations
        .iter()
        .filter(|rec| {
            rec.description.to_lowercase().contains("performance")
                || rec.description.to_lowercase().contains("large")
                || rec.description.to_lowercase().contains("optimization")
        })
        .collect();

    if results.check_report.total_events_checked > 100 {
        assert!(
            !performance_recommendations.is_empty(),
            "Should generate performance recommendations for large datasets"
        );
    }

    println!(
        "  Performance recommendations: {}",
        performance_recommendations.len()
    );
    for rec in &performance_recommendations {
        println!("    - {}", rec.description);
    }

    // Cleanup
    EventQueries::delete_by_source("test.large_scale".to_string())
        .execute(&pool)
        .await?;

    Ok(())
}
