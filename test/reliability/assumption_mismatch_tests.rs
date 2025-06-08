use chrono::Utc;
use serde_json::json;
use sinex_shared::{DatabaseService, RawEventBuilder, sources, event_type_constants, EventValidator};
use std::collections::HashMap;

/// Test that detects when event payloads don't match their declared source/type
#[sqlx::test]
async fn test_detect_semantic_mismatches(pool: sqlx::PgPool) -> Result<(), Box<dyn std::error::Error>> {
    let db_service = DatabaseService::from_pool_no_validation(pool.clone()); // Disable validation to insert bad data
    
    // Insert events with mismatched payloads
    let mismatches = vec![
        // Filesystem source but hyprland-style payload
        RawEventBuilder::new(
            sources::FILESYSTEM,
            event_type_constants::filesystem::FILE_CREATED,
            json!({
                "window": "firefox",
                "workspace": 2,
                "class": "Firefox"
            })
        ).build(),
        
        // Hyprland source but filesystem-style payload
        RawEventBuilder::new(
            sources::HYPRLAND,
            event_type_constants::hyprland::WINDOW_FOCUSED,
            json!({
                "path": "/home/user/file.txt",
                "size": 1024,
                "permissions": "644"
            })
        ).build(),
        
        // Terminal source but sinex-style payload
        RawEventBuilder::new(
            sources::TERMINAL_KITTY,
            event_type_constants::terminal::COMMAND_EXECUTED,
            json!({
                "agent_name": "fake-agent",
                "timestamp_iso": Utc::now().to_rfc3339(),
                "status_reported": "healthy"
            })
        ).build(),
    ];
    
    // Insert all mismatched events
    for event in &mismatches {
        db_service.insert_event(event).await?;
    }
    
    // Now detect the mismatches using field analysis
    let detected_mismatches = sqlx::query!(
        r#"
        WITH field_patterns AS (
            -- Define expected fields for each source/event_type
            SELECT 'filesystem' as source, 'file_created' as event_type, 
                   ARRAY['path', 'size'] as expected_fields
            UNION ALL
            SELECT 'filesystem', 'file_modified', ARRAY['path', 'old_size', 'new_size', 'modification_type']
            UNION ALL
            SELECT 'hyprland', 'window_focused', ARRAY['window', 'workspace']
            UNION ALL
            SELECT 'hyprland', 'workspace_changed', ARRAY['workspace']
            UNION ALL
            SELECT 'terminal.kitty', 'command_executed', ARRAY['command', 'exit_code']
            UNION ALL
            SELECT 'sinex', 'agent.heartbeat', ARRAY['agent_name', 'timestamp_iso']
        ),
        event_fields AS (
            -- Extract top-level fields from each event
            SELECT 
                id,
                source,
                event_type,
                payload,
                ARRAY(SELECT jsonb_object_keys(payload)) as actual_fields
            FROM raw.events
        ),
        mismatches AS (
            -- Find events where fields don't match expectations
            SELECT 
                e.id,
                e.source,
                e.event_type,
                e.actual_fields,
                fp.expected_fields,
                -- Calculate field similarity
                COALESCE(
                    ARRAY_LENGTH(
                        ARRAY(
                            SELECT unnest(e.actual_fields) 
                            INTERSECT 
                            SELECT unnest(fp.expected_fields)
                        ), 1
                    ), 0
                )::float / GREATEST(
                    ARRAY_LENGTH(fp.expected_fields, 1), 
                    ARRAY_LENGTH(e.actual_fields, 1)
                )::float as field_match_ratio
            FROM event_fields e
            LEFT JOIN field_patterns fp 
                ON e.source = fp.source AND e.event_type = fp.event_type
        )
        SELECT 
            id::text,
            source,
            event_type,
            actual_fields,
            expected_fields,
            field_match_ratio,
            CASE 
                WHEN field_match_ratio < 0.5 THEN 'LIKELY_MISMATCH'
                WHEN field_match_ratio < 0.8 THEN 'POSSIBLE_MISMATCH'
                ELSE 'OK'
            END as mismatch_status
        FROM mismatches
        WHERE field_match_ratio < 0.8 OR expected_fields IS NULL
        ORDER BY field_match_ratio ASC
        "#
    )
    .fetch_all(&pool)
    .await?;
    
    // We should detect at least 3 mismatches
    assert!(detected_mismatches.len() >= 3, "Should detect field mismatches");
    
    for mismatch in detected_mismatches {
        println!("Detected mismatch: {} {} - expected fields {:?}, got {:?} (match ratio: {:.2})",
            mismatch.source,
            mismatch.event_type,
            mismatch.expected_fields,
            mismatch.actual_fields,
            mismatch.field_match_ratio.unwrap_or(0.0)
        );
    }
    
    Ok(())
}

/// Test that compares actual payloads against what the validator expects
#[test]
fn test_payload_expectation_analysis() {
    let validator = EventValidator::new();
    
    // Define test scenarios with actual vs expected payloads
    let scenarios = vec![
        (
            "Hyprland ingestor sending filesystem-like data",
            sources::HYPRLAND,
            event_type_constants::hyprland::WINDOW_FOCUSED,
            json!({
                "path": "/usr/bin/firefox",  // Wrong: this looks like filesystem
                "pid": 12345,
                "size": 1024  // Wrong: window doesn't have file size
            }),
            vec!["path", "size"], // Unexpected fields
            vec!["window"], // Missing expected fields
        ),
        (
            "Filesystem ingestor sending window manager data",
            sources::FILESYSTEM,
            event_type_constants::filesystem::FILE_CREATED,
            json!({
                "window_id": "0x1234567",  // Wrong: files don't have window IDs
                "workspace": 2,
                "decorations": true
            }),
            vec!["window_id", "workspace", "decorations"],
            vec!["path", "size"],
        ),
        (
            "Terminal ingestor confused with system events",
            sources::TERMINAL_KITTY,
            event_type_constants::terminal::COMMAND_EXECUTED,
            json!({
                "cpu_usage": 45.2,  // Wrong: not terminal data
                "memory_mb": 1024,
                "process_count": 42
            }),
            vec!["cpu_usage", "memory_mb", "process_count"],
            vec!["command"],
        ),
    ];
    
    for (description, source, event_type, payload, unexpected, missing) in scenarios {
        println!("\nScenario: {}", description);
        
        // Try to validate - it should fail
        let result = validator.validate(source, event_type, &payload);
        assert!(result.is_err(), "Validation should fail for mismatched payload");
        
        println!("  Validation error: {:?}", result.unwrap_err());
        println!("  Unexpected fields: {:?}", unexpected);
        println!("  Missing fields: {:?}", missing);
    }
}

/// Test statistical analysis of field usage patterns
#[sqlx::test]
async fn test_field_usage_pattern_analysis(pool: sqlx::PgPool) -> Result<(), Box<dyn std::error::Error>> {
    let db_service = DatabaseService::from_pool_no_validation(pool.clone());
    
    // Insert a mix of correct and incorrect events
    let events = vec![
        // Correct filesystem events
        (sources::FILESYSTEM, event_type_constants::filesystem::FILE_CREATED, json!({"path": "/a.txt", "size": 100})),
        (sources::FILESYSTEM, event_type_constants::filesystem::FILE_CREATED, json!({"path": "/b.txt", "size": 200})),
        (sources::FILESYSTEM, event_type_constants::filesystem::FILE_CREATED, json!({"path": "/c.txt", "size": 300})),
        
        // Incorrect filesystem event (outlier)
        (sources::FILESYSTEM, event_type_constants::filesystem::FILE_CREATED, json!({"window": "terminal", "pid": 123})),
        
        // Correct hyprland events
        (sources::HYPRLAND, event_type_constants::hyprland::WINDOW_FOCUSED, json!({"window": "firefox", "workspace": 1})),
        (sources::HYPRLAND, event_type_constants::hyprland::WINDOW_FOCUSED, json!({"window": "terminal", "workspace": 2})),
        
        // Incorrect hyprland event (outlier)
        (sources::HYPRLAND, event_type_constants::hyprland::WINDOW_FOCUSED, json!({"path": "/fake", "size": 999})),
    ];
    
    for (source, event_type, payload) in events {
        let event = RawEventBuilder::new(source, event_type, payload).build();
        db_service.insert_event(&event).await?;
    }
    
    // Analyze field usage patterns to detect outliers
    let field_analysis = sqlx::query!(
        r#"
        WITH field_usage AS (
            -- Count how often each field appears for each source/event_type combo
            SELECT 
                source,
                event_type,
                jsonb_object_keys(payload) as field_name,
                COUNT(*) as usage_count
            FROM raw.events
            GROUP BY source, event_type, jsonb_object_keys(payload)
        ),
        total_events AS (
            -- Count total events per source/event_type
            SELECT 
                source,
                event_type,
                COUNT(*) as total_count
            FROM raw.events
            GROUP BY source, event_type
        ),
        field_percentages AS (
            -- Calculate what percentage of events have each field
            SELECT 
                fu.source,
                fu.event_type,
                fu.field_name,
                fu.usage_count,
                te.total_count,
                (fu.usage_count::float / te.total_count::float * 100) as usage_percentage
            FROM field_usage fu
            JOIN total_events te ON fu.source = te.source AND fu.event_type = te.event_type
        )
        SELECT 
            source,
            event_type,
            field_name,
            usage_count::int,
            total_count::int,
            usage_percentage,
            CASE 
                WHEN usage_percentage >= 80 THEN 'LIKELY_REQUIRED'
                WHEN usage_percentage >= 50 THEN 'COMMON'
                WHEN usage_percentage >= 20 THEN 'OCCASIONAL'
                ELSE 'RARE_OUTLIER'
            END as field_status
        FROM field_percentages
        ORDER BY source, event_type, usage_percentage DESC
        "#
    )
    .fetch_all(&pool)
    .await?;
    
    println!("\nField usage analysis:");
    for analysis in field_analysis {
        println!("{} {} - field '{}': {}/{} ({:.1}%) - {}",
            analysis.source,
            analysis.event_type,
            analysis.field_name,
            analysis.usage_count,
            analysis.total_count,
            analysis.usage_percentage.unwrap_or(0.0),
            analysis.field_status.unwrap_or_default()
        );
    }
    
    // Detect events that don't follow the common pattern
    let outliers = sqlx::query!(
        r#"
        WITH common_fields AS (
            -- Find fields that appear in >50% of events for each type
            SELECT 
                source,
                event_type,
                jsonb_object_keys(payload) as field_name,
                COUNT(*) as usage_count
            FROM raw.events
            GROUP BY source, event_type, jsonb_object_keys(payload)
            HAVING COUNT(*) >= (
                SELECT COUNT(*) * 0.5 
                FROM raw.events e2 
                WHERE e2.source = raw.events.source 
                AND e2.event_type = raw.events.event_type
            )
        ),
        expected_fields AS (
            -- Aggregate common fields per event type
            SELECT 
                source,
                event_type,
                ARRAY_AGG(field_name) as common_fields
            FROM common_fields
            GROUP BY source, event_type
        )
        SELECT 
            e.id::text,
            e.source,
            e.event_type,
            ARRAY(SELECT jsonb_object_keys(e.payload)) as actual_fields,
            ef.common_fields,
            -- Events missing common fields are outliers
            ARRAY_LENGTH(
                ARRAY(
                    SELECT unnest(ef.common_fields) 
                    EXCEPT 
                    SELECT jsonb_object_keys(e.payload)
                ), 1
            ) as missing_common_fields_count
        FROM raw.events e
        LEFT JOIN expected_fields ef ON e.source = ef.source AND e.event_type = ef.event_type
        WHERE ARRAY_LENGTH(
            ARRAY(
                SELECT unnest(ef.common_fields) 
                EXCEPT 
                SELECT jsonb_object_keys(e.payload)
            ), 1
        ) > 0
        "#
    )
    .fetch_all(&pool)
    .await?;
    
    // We should detect the outlier events
    assert!(!outliers.is_empty(), "Should detect outlier events");
    
    println!("\nDetected outliers:");
    for outlier in outliers {
        println!("Event {} ({} {}) is missing {} common fields",
            outlier.id,
            outlier.source,
            outlier.event_type,
            outlier.missing_common_fields_count.unwrap_or(0)
        );
    }
    
    Ok(())
}

/// Test cross-reference validation between different sources
#[sqlx::test]
async fn test_cross_source_field_confusion(pool: sqlx::PgPool) -> Result<(), Box<dyn std::error::Error>> {
    let db_service = DatabaseService::from_pool_no_validation(pool.clone());
    
    // Insert events to establish patterns
    let _ = db_service.insert_events_batch(&vec![
        // Establish filesystem pattern
        RawEventBuilder::new(sources::FILESYSTEM, "file_created", json!({"path": "/a.txt", "size": 100})).build(),
        RawEventBuilder::new(sources::FILESYSTEM, "file_created", json!({"path": "/b.txt", "size": 200})).build(),
        
        // Establish hyprland pattern
        RawEventBuilder::new(sources::HYPRLAND, "window_focused", json!({"window": "firefox", "workspace": 1})).build(),
        RawEventBuilder::new(sources::HYPRLAND, "window_focused", json!({"window": "terminal", "workspace": 2})).build(),
    ]).await?;
    
    // Query to find fields that appear in multiple sources (potential confusion)
    let cross_source_fields = sqlx::query!(
        r#"
        WITH field_sources AS (
            SELECT DISTINCT
                jsonb_object_keys(payload) as field_name,
                source
            FROM raw.events
        ),
        multi_source_fields AS (
            SELECT 
                field_name,
                ARRAY_AGG(DISTINCT source ORDER BY source) as sources,
                COUNT(DISTINCT source) as source_count
            FROM field_sources
            GROUP BY field_name
            HAVING COUNT(DISTINCT source) > 1
        )
        SELECT 
            field_name,
            sources,
            source_count::int
        FROM multi_source_fields
        ORDER BY source_count DESC, field_name
        "#
    )
    .fetch_all(&pool)
    .await?;
    
    // Check if any fields appear in multiple sources (indicating potential confusion)
    for field in cross_source_fields {
        println!("Field '{}' appears in {} sources: {:?}",
            field.field_name,
            field.source_count,
            field.sources
        );
        
        // This indicates potential confusion between sources
        if field.source_count > 1 {
            println!("  WARNING: This field appears in multiple sources - possible confusion!");
        }
    }
    
    Ok(())
}

/// Test to create a "field signature" for each event type
#[test]
fn test_event_type_signatures() {
    use std::collections::HashSet;
    
    // Define expected "signatures" for each event type
    fn get_expected_signature(source: &str, event_type: &str) -> HashSet<&'static str> {
        match (source, event_type) {
            ("filesystem", "file_created") => ["path", "size", "permissions"].iter().cloned().collect(),
            ("filesystem", "file_modified") => ["path", "old_size", "new_size", "modification_type"].iter().cloned().collect(),
            ("filesystem", "file_deleted") => ["path", "was_directory"].iter().cloned().collect(),
            ("hyprland", "window_focused") => ["window", "workspace"].iter().cloned().collect(),
            ("hyprland", "workspace_changed") => ["workspace"].iter().cloned().collect(),
            ("terminal.kitty", "command_executed") => ["command", "exit_code", "duration"].iter().cloned().collect(),
            ("sinex", "agent.heartbeat") => ["agent_name", "timestamp_iso", "status_reported", "metrics_snapshot"].iter().cloned().collect(),
            _ => HashSet::new(),
        }
    }
    
    // Test various payloads against their signatures
    let test_cases = vec![
        // Correct payload
        (
            sources::FILESYSTEM,
            event_type_constants::filesystem::FILE_CREATED,
            json!({"path": "/test.txt", "size": 1024}),
            true, // Should match
        ),
        // Wrong payload for event type
        (
            sources::FILESYSTEM,
            event_type_constants::filesystem::FILE_CREATED,
            json!({"window": "firefox", "workspace": 1}), // Hyprland fields!
            false, // Should not match
        ),
        // Mixed payload (some correct, some wrong fields)
        (
            sources::HYPRLAND,
            event_type_constants::hyprland::WINDOW_FOCUSED,
            json!({"window": "firefox", "path": "/usr/bin/firefox"}), // Mixed!
            false, // Suspicious mix
        ),
    ];
    
    for (source, event_type, payload, should_match) in test_cases {
        let expected_fields = get_expected_signature(source, event_type);
        let actual_fields: HashSet<&str> = payload.as_object()
            .unwrap()
            .keys()
            .map(|s| s.as_str())
            .collect();
        
        let intersection = expected_fields.intersection(&actual_fields).count();
        let union = expected_fields.union(&actual_fields).count();
        let similarity = if union > 0 { 
            intersection as f64 / union as f64 
        } else { 
            0.0 
        };
        
        println!("Testing {} {} with payload {:?}", source, event_type, payload);
        println!("  Expected fields: {:?}", expected_fields);
        println!("  Actual fields: {:?}", actual_fields);
        println!("  Similarity: {:.2}", similarity);
        println!("  Matches expectation: {}", similarity > 0.5);
        
        assert_eq!(similarity > 0.5, should_match, "Field signature match failed");
    }
}