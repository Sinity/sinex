# Test-Driven Specifications

This document defines system behavior through executable test examples. Each test serves as both specification and verification.

## Core Event Model Tests

```rust
#[cfg(test)]
mod event_model_specs {
    use super::*;
    use chrono::{DateTime, Utc};
    use serde_json::json;
    
    #[test]
    fn event_must_have_required_fields() {
        // This test documents the minimal valid event structure
        let event = Event {
            id: Ulid::new(),
            source: "test".to_string(),
            event_type: "test.example".to_string(),
            ts_ingest: Utc::now(),
            ts_orig: None, // Optional: when source provides timestamp
            host: hostname::get().unwrap().to_string_lossy().to_string(),
            ingestor_version: env!("CARGO_PKG_VERSION").to_string(),
            payload: json!({"data": "example"}),
        };
        
        // Events must be serializable to JSON for storage
        let serialized = serde_json::to_string(&event).unwrap();
        assert!(serialized.contains("test.example"));
    }
    
    #[test]
    fn ulid_provides_time_ordering() {
        // This test documents why we use ULID instead of UUID
        let earlier = Ulid::new();
        std::thread::sleep(std::time::Duration::from_millis(2));
        let later = Ulid::new();
        
        // ULIDs are lexicographically sortable by time
        assert!(earlier.to_string() < later.to_string());
        
        // This enables efficient time-range queries:
        // SELECT * FROM events WHERE id > $1 AND id < $2
    }
    
    #[test]
    fn event_source_follows_naming_convention() {
        // This test documents the source naming hierarchy
        let valid_sources = vec![
            "hyprland",                  // Top-level source
            "terminal.kitty",            // Sub-source with dot notation
            "filesystem.watcher",        // Another sub-source
            "sinex.agent.processor",     // System sources use 'sinex' prefix
        ];
        
        for source in valid_sources {
            assert!(source.chars().all(|c| c.is_ascii_lowercase() || c == '.' || c == '_'));
            assert!(!source.starts_with('.'));
            assert!(!source.ends_with('.'));
        }
    }
}
```

## Ingestor Behavior Tests

```rust
#[cfg(test)]
mod ingestor_specs {
    use super::*;
    
    #[tokio::test]
    async fn ingestor_must_handle_connection_failures() {
        // This test documents required resilience behavior
        let mut attempts = 0;
        let max_attempts = 5;
        let mut backoff = Duration::from_millis(100);
        
        while attempts < max_attempts {
            match connect_to_source().await {
                Ok(connection) => break,
                Err(e) => {
                    attempts += 1;
                    log::warn!("Connection attempt {} failed: {}", attempts, e);
                    
                    // Exponential backoff with jitter
                    tokio::time::sleep(backoff).await;
                    backoff = backoff.mul_f64(2.0 * (1.0 + rand::random::<f64>() * 0.1));
                    
                    // Cap maximum backoff
                    if backoff > Duration::from_secs(30) {
                        backoff = Duration::from_secs(30);
                    }
                }
            }
        }
        
        assert!(attempts < max_attempts, "Must reconnect within {} attempts", max_attempts);
    }
    
    #[tokio::test]
    async fn ingestor_must_batch_high_volume_events() {
        // This test documents batching requirements
        let db = setup_test_db().await;
        let mut ingestor = Ingestor::new(db.clone());
        
        // Generate high-volume events
        let events: Vec<Event> = (0..150)
            .map(|i| create_test_event(&format!("event_{}", i)))
            .collect();
        
        let start = Instant::now();
        for event in events {
            ingestor.process_event(event).await.unwrap();
        }
        
        // Force flush
        ingestor.flush().await.unwrap();
        let elapsed = start.elapsed();
        
        // Should batch, not insert individually
        let insert_count = count_db_operations(&db, "INSERT").await;
        assert!(insert_count < 10, "Should batch inserts, not do {} individual inserts", insert_count);
        
        // Should complete quickly
        assert!(elapsed < Duration::from_secs(1), "150 events should process in <1s, took {:?}", elapsed);
    }
    
    #[tokio::test] 
    async fn failed_events_go_to_dlq() {
        // This test documents DLQ behavior
        let dlq = FileDLQ::new("test_dlq");
        let event = create_test_event("will_fail");
        
        // Simulate processing failure
        let result = process_event_that_fails(event.clone()).await;
        assert!(result.is_err());
        
        // Failed event must be in DLQ
        dlq.enqueue(&event, &result.unwrap_err().to_string()).await.unwrap();
        
        let dlq_contents = dlq.list_entries().await.unwrap();
        assert_eq!(dlq_contents.len(), 1);
        assert_eq!(dlq_contents[0].event.id, event.id);
        assert!(dlq_contents[0].error.contains("fail"));
        
        // DLQ entries must be retryable
        let retry_entry = dlq.dequeue(1).await.unwrap().pop().unwrap();
        assert_eq!(retry_entry.event.id, event.id);
        assert_eq!(retry_entry.retry_count, 0);
    }
}
```

## Query Interface Tests

```rust
#[cfg(test)]
mod query_specs {
    use super::*;
    
    #[tokio::test]
    async fn time_range_queries_use_id_ordering() {
        // This test documents the efficient query pattern
        let db = setup_test_db().await;
        
        // Insert events with known timestamps
        let t1 = Utc::now() - Duration::hours(2);
        let t2 = Utc::now() - Duration::hours(1);
        let t3 = Utc::now();
        
        insert_event_at_time(&db, "event1", t1).await;
        insert_event_at_time(&db, "event2", t2).await;
        insert_event_at_time(&db, "event3", t3).await;
        
        // Query by time range using ULID ordering
        let query = "
            SELECT * FROM raw.events 
            WHERE id > ulid_from_timestamp($1)
            AND id < ulid_from_timestamp($2)
            ORDER BY id
        ";
        
        let results: Vec<Event> = sqlx::query_as(query)
            .bind(t1 + Duration::minutes(30))
            .bind(t3 - Duration::minutes(30))
            .fetch_all(&db)
            .await
            .unwrap();
        
        // Should only find middle event efficiently
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].payload["name"], "event2");
    }
    
    #[test]
    fn cli_supports_human_time_expressions() {
        // This test documents the CLI time parsing
        let test_cases = vec![
            ("--last 1h", Duration::hours(1)),
            ("--last 30m", Duration::minutes(30)),
            ("--last 2d", Duration::days(2)),
            ("--last 1w", Duration::weeks(1)),
        ];
        
        for (input, expected) in test_cases {
            let parsed = parse_time_expression(input).unwrap();
            assert_eq!(parsed, expected);
        }
    }
}
```

## Event Relations Tests

```rust
#[cfg(test)]
mod event_relations_specs {
    use super::*;
    
    #[tokio::test]
    async fn event_relations_enable_workflow_tracing() {
        // This test documents the current approach for linking related events
        let db = setup_test_db().await;
        
        // Create a multi-step workflow through event relations:
        // 1. User opens terminal
        let terminal_event = Event::builder()
            .source("terminal.kitty")
            .event_type("session_started")
            .build();
        let terminal_id = insert_event(&db, terminal_event).await;
        
        // 2. User runs git command  
        let git_event = Event::builder()
            .source("terminal.kitty")
            .event_type("command_executed")
            .payload(json!({"command": "git status"}))
            .build();
        let git_id = insert_event(&db, git_event).await;
        
        // 3. User opens editor
        let editor_event = Event::builder()
            .source("hyprland")
            .event_type("window_focused")
            .payload(json!({"app_class": "neovim"}))
            .build();
        let editor_id = insert_event(&db, editor_event).await;
        
        // Create explicit relations between events
        create_event_relation(&db, EventRelation {
            from_event_id: terminal_id,
            to_event_id: git_id,
            relation_type: "followed_by".to_string(),
            confidence: 0.95,
            detected_by: "temporal_analysis".to_string(),
            metadata: json!({"time_gap_ms": 500}),
        }).await;
        
        create_event_relation(&db, EventRelation {
            from_event_id: git_id,
            to_event_id: editor_id,
            relation_type: "caused_by".to_string(),
            confidence: 0.85,
            detected_by: "user_annotation".to_string(),
            metadata: json!({"context": "editing files from git status"}),
        }).await;
        
        // Query workflow through relations
        let workflow_events = query_related_events(&db, terminal_id, 2).await;
        
        assert_eq!(workflow_events.len(), 3);
        assert_eq!(workflow_events[0].id, terminal_id);
        assert_eq!(workflow_events[1].id, git_id);
        assert_eq!(workflow_events[2].id, editor_id);
        
        // Verify relation metadata
        let relations = get_event_relations(&db, terminal_id).await;
        assert!(relations.iter().any(|r| r.relation_type == "followed_by"));
        assert!(relations.iter().any(|r| r.confidence > 0.8));
    }
    
    #[tokio::test]
    async fn event_clusters_group_related_activities() {
        // Test event clustering for higher-level workflow organization
        let db = setup_test_db().await;
        
        // Create several related events
        let events = create_test_workflow_events(&db).await;
        
        // Create a cluster for this development session
        let cluster = EventCluster {
            name: "Debug session: fix authentication bug".to_string(),
            cluster_type: "development_session".to_string(),
            summary: Some("Identified and fixed JWT token validation issue".to_string()),
            time_start: events[0].ts_ingest,
            time_end: events.last().unwrap().ts_ingest,
            metadata: json!({"project": "auth-service", "bug_id": "AUTH-123"}),
        };
        
        let cluster_id = create_event_cluster(&db, cluster).await;
        
        // Add events to cluster with roles
        for (i, event) in events.iter().enumerate() {
            let role = match i {
                0 => "session_start",
                n if n == events.len() - 1 => "session_end", 
                _ => "activity",
            };
            
            add_event_to_cluster(&db, cluster_id, event.id, role).await;
        }
        
        // Query by cluster
        let cluster_events = get_cluster_events(&db, cluster_id).await;
        assert_eq!(cluster_events.len(), events.len());
        
        // Verify cluster metadata provides context
        let retrieved_cluster = get_event_cluster(&db, cluster_id).await;
        assert_eq!(retrieved_cluster.cluster_type, "development_session");
        assert!(retrieved_cluster.summary.is_some());
    }
}
```

## Health Check Tests

```rust
#[cfg(test)]
mod health_specs {
    use super::*;
    use actix_web::{test, App};
    
    #[actix_web::test]
    async fn health_endpoint_reports_component_status() {
        // This test documents the health check response format
        let app = test::init_service(
            App::new()
                .app_data(create_test_state())
                .route("/health", web::get().to(health_check))
        ).await;
        
        let req = test::TestRequest::get()
            .uri("/health")
            .to_request();
            
        let resp = test::call_service(&app, req).await;
        assert!(resp.status().is_success());
        
        let body: HealthStatus = test::read_body_json(resp).await;
        
        // Required fields in health response
        assert_eq!(body.status, "healthy");
        assert!(body.uptime_seconds > 0);
        assert!(body.version.len() > 0);
        
        // Component checks
        assert!(body.checks.contains_key("database"));
        assert_eq!(body.checks["database"].status, "healthy");
        
        assert!(body.checks.contains_key("source_connection"));
        assert_eq!(body.checks["source_connection"].status, "healthy");
    }
    
    #[actix_web::test]
    async fn unhealthy_database_degrades_health() {
        // This test documents degraded health behavior
        let app = test::init_service(
            App::new()
                .app_data(create_failing_db_state())
                .route("/health", web::get().to(health_check))
        ).await;
        
        let req = test::TestRequest::get().uri("/health").to_request();
        let resp = test::call_service(&app, req).await;
        
        // Should return 503 Service Unavailable
        assert_eq!(resp.status(), 503);
        
        let body: HealthStatus = test::read_body_json(resp).await;
        assert_eq!(body.status, "unhealthy");
        assert_eq!(body.checks["database"].status, "unhealthy");
        assert!(body.checks["database"].message.is_some());
    }
}
```

## Property-Based Tests

```rust
#[cfg(test)]
mod property_specs {
    use proptest::prelude::*;
    
    proptest! {
        #[test]
        fn events_always_serializable(
            source in "[a-z]+",
            event_type in "[a-z.]+",
            payload_size in 1..10000usize
        ) {
            // This test ensures all events can be stored
            let event = Event {
                id: Ulid::new(),
                source,
                event_type,
                ts_ingest: Utc::now(),
                ts_orig: None,
                host: "test".to_string(),
                ingestor_version: "test".to_string(),
                payload: generate_random_json(payload_size),
            };
            
            // Must serialize to JSON
            let serialized = serde_json::to_string(&event).unwrap();
            
            // Must round-trip
            let deserialized: Event = serde_json::from_str(&serialized).unwrap();
            assert_eq!(event.id, deserialized.id);
        }
        
        #[test]
        fn ulid_ordering_matches_time_ordering(
            delays in prop::collection::vec(0u64..1000, 1..100)
        ) {
            // This property ensures ULID ordering invariant
            let mut ids = Vec::new();
            let mut timestamps = Vec::new();
            
            for delay_ms in delays {
                std::thread::sleep(Duration::from_millis(delay_ms));
                timestamps.push(Utc::now());
                ids.push(Ulid::new());
            }
            
            // Sort both by ID and timestamp
            let mut id_sorted = ids.iter().zip(&timestamps).collect::<Vec<_>>();
            id_sorted.sort_by_key(|(id, _)| *id);
            
            let mut time_sorted = ids.iter().zip(&timestamps).collect::<Vec<_>>();
            time_sorted.sort_by_key(|(_, ts)| *ts);
            
            // Order should match
            assert_eq!(id_sorted, time_sorted);
        }
    }
}
```

## Test Organization

```
tests/
├── unit/
│   ├── event_model.rs      # Core data model tests
│   ├── ingestors.rs        # Ingestor behavior tests
│   └── query.rs            # Query interface tests
├── integration/
│   ├── end_to_end.rs       # Full pipeline tests
│   ├── correlation.rs      # Multi-process correlation tests
│   └── performance.rs      # Load and stress tests
└── property/
    ├── invariants.rs       # System-wide properties
    └── generators.rs       # Test data generators
```

## Running Tests as Documentation

```bash
# Generate documentation from tests
cargo test -- --nocapture | grep "test.*documents" > test_specs.txt

# Run specific documentation tests
cargo test event_model_specs -- --nocapture

# Generate coverage to ensure specs are complete
cargo tarpaulin --out Html --output-dir coverage/
```

## Test-Driven Development Workflow

1. Write test that documents expected behavior
2. Test fails (red)
3. Implement minimal code to pass
4. Test passes (green)
5. Refactor if needed
6. Test still passes and serves as living documentation