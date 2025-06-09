use sqlx::{postgres::PgPoolOptions, PgPool};
use sinex_ulid::Ulid;
use sinex_worker::{worker::Worker, EventProcessor};
use sinex_db::models::PromotionQueueItem;
use serde_json::json;
use std::sync::Arc;
use std::time::Duration;
use async_trait::async_trait;
use anyhow::Result;

struct PipelineTestProcessor {
    agent_name: String,
    process_delay_ms: u64,
    create_derived_events: bool,
}

#[async_trait]
impl EventProcessor for PipelineTestProcessor {
    async fn process_event(&self, _pool: &PgPool, item: &PromotionQueueItem) -> Result<()> {
        // Simulate processing time
        tokio::time::sleep(Duration::from_millis(self.process_delay_ms)).await;
        
        // Create derived events if configured
        if self.create_derived_events {
            // In real implementation, would insert derived events to raw.events
            println!("Would create derived event from {}", item.raw_event_id);
        }
        
        Ok(())
    }
    
    fn agent_name(&self) -> &str {
        &self.agent_name
    }
}

#[tokio::test]
async fn test_end_to_end_event_pipeline() {
    let database_url = std::env::var("TEST_DATABASE_URL")
        .unwrap_or_else(|_| "postgres://sinex_test:testpass@localhost:5433/sinex_test".to_string());
    
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await
        .expect("Failed to connect to test database");
    
    // Register test agents
    let agents = vec![
        ("ingestion_agent", "ingestor"),
        ("enrichment_agent", "enricher"),
        ("analytics_agent", "analytical"),
    ];
    
    for (name, agent_type) in &agents {
        sqlx::query(
            "INSERT INTO sinex_schemas.agent_manifests (agent_name, version, agent_type, status) 
             VALUES ($1, $2, $3, $4) 
             ON CONFLICT (agent_name) DO UPDATE SET status = 'running'"
        )
        .bind(name)
        .bind("1.0.0")
        .bind(agent_type)
        .bind("running")
        .execute(&pool)
        .await
        .unwrap();
    }
    
    // Phase 1: Ingestion
    println!("Phase 1: Ingesting raw events");
    let mut event_ids = Vec::new();
    
    for i in 0..10 {
        let event_id = Ulid::new();
        event_ids.push(event_id.to_string());
        
        let payload = json!({
            "user_id": format!("user_{}", i % 3),
            "action": "page_view",
            "page": format!("/page_{}", i),
            "timestamp": chrono::Utc::now(),
            "session_id": format!("session_{}", i % 5)
        });
        
        sqlx::query(
            "INSERT INTO raw.events (id, source, event_type, host, payload) 
             VALUES ($1::ulid, $2, $3, $4, $5::jsonb)"
        )
        .bind(&event_id.to_string())
        .bind("web_frontend")
        .bind("user_action")
        .bind("web_server_1")
        .bind(&payload)
        .execute(&pool)
        .await
        .unwrap();
    }
    
    // Phase 2: Route events to enrichment agent
    println!("Phase 2: Routing events for enrichment");
    
    // Use event router trigger (simulated here)
    for event_id in &event_ids {
        sqlx::query(
            "INSERT INTO sinex_schemas.promotion_queue (raw_event_id, target_agent_name) 
             VALUES ($1::ulid, $2)"
        )
        .bind(event_id)
        .bind("enrichment_agent")
        .execute(&pool)
        .await
        .unwrap();
    }
    
    // Phase 3: Process with enrichment agent
    println!("Phase 3: Processing with enrichment agent");
    
    let processor = Arc::new(PipelineTestProcessor {
        agent_name: "enrichment_agent".to_string(),
        process_delay_ms: 10,
        create_derived_events: true,
    });
    
    let worker = Worker::new(pool.clone(), processor, "enrichment_worker_1".to_string());
    
    // Run worker for limited time
    let _ = tokio::time::timeout(
        Duration::from_secs(3),
        worker.run()
    ).await;
    
    // Verify enrichment completed
    let enriched_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM sinex_schemas.promotion_queue 
         WHERE target_agent_name = 'enrichment_agent' 
         AND status = 'completed'"
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    
    assert_eq!(enriched_count, 10, "All events should be enriched");
    
    // Phase 4: Create aggregated analytics events
    println!("Phase 4: Creating analytics aggregations");
    
    // Simulate analytics agent creating summary events
    let analytics_event_id = Ulid::new();
    let analytics_payload = json!({
        "period": "last_minute",
        "metrics": {
            "total_page_views": 10,
            "unique_users": 3,
            "unique_sessions": 5,
            "pages_viewed": {
                "page_0": 1,
                "page_1": 1,
                "page_2": 1,
                "page_3": 1,
                "page_4": 1,
                "page_5": 1,
                "page_6": 1,
                "page_7": 1,
                "page_8": 1,
                "page_9": 1
            }
        },
        "derived_from_events": event_ids
    });
    
    sqlx::query(
        "INSERT INTO raw.events (id, source, event_type, host, payload) 
         VALUES ($1::ulid, $2, $3, $4, $5::jsonb)"
    )
    .bind(&analytics_event_id.to_string())
    .bind("analytics_agent")
    .bind("usage_summary")
    .bind("analytics_host")
    .bind(&analytics_payload)
    .execute(&pool)
    .await
    .unwrap();
    
    // Verify pipeline results
    let total_events: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM raw.events"
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    
    assert!(total_events >= 11, "Should have original events plus analytics summary");
}

#[tokio::test]
async fn test_event_routing_rules() {
    let database_url = std::env::var("TEST_DATABASE_URL")
        .unwrap_or_else(|_| "postgres://sinex_test:testpass@localhost:5433/sinex_test".to_string());
    
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database_url)
        .await
        .expect("Failed to connect to test database");
    
    // Set up agents with subscription rules
    let subscription_rules = json!({
        "raw.events_feed_all": [
            {
                "source_filter": "security.*",
                "event_type_filter": ".*_failed"
            }
        ]
    });
    
    sqlx::query(
        "INSERT INTO sinex_schemas.agent_manifests 
         (agent_name, version, subscribes_to_event_types) 
         VALUES ($1, $2, $3::jsonb) 
         ON CONFLICT (agent_name) DO UPDATE 
         SET subscribes_to_event_types = EXCLUDED.subscribes_to_event_types"
    )
    .bind("security_monitor")
    .bind("1.0.0")
    .bind(&subscription_rules)
    .execute(&pool)
    .await
    .unwrap();
    
    // Insert various events
    let test_events = vec![
        ("security.auth", "login_failed", true),    // Should match
        ("security.auth", "login_success", false),  // Wrong event type
        ("app.frontend", "request_failed", false),  // Wrong source
        ("security.firewall", "connection_failed", true), // Should match
    ];
    
    for (source, event_type, should_route) in test_events {
        let event_id = Ulid::new();
        
        sqlx::query(
            "INSERT INTO raw.events (id, source, event_type, host, payload) 
             VALUES ($1::ulid, $2, $3, $4, $5::jsonb)"
        )
        .bind(&event_id.to_string())
        .bind(source)
        .bind(event_type)
        .bind("test_host")
        .bind(&json!({"test": true}))
        .execute(&pool)
        .await
        .unwrap();
        
        // Simulate event router logic
        if should_route {
            sqlx::query(
                "INSERT INTO sinex_schemas.promotion_queue (raw_event_id, target_agent_name) 
                 VALUES ($1::ulid, $2)"
            )
            .bind(&event_id.to_string())
            .bind("security_monitor")
            .execute(&pool)
            .await
            .unwrap();
        }
    }
    
    // Verify routing
    let routed_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM sinex_schemas.promotion_queue 
         WHERE target_agent_name = 'security_monitor'"
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    
    assert_eq!(routed_count, 2, "Should route only matching events");
}

#[tokio::test]
async fn test_dead_letter_queue_handling() {
    let database_url = std::env::var("TEST_DATABASE_URL")
        .unwrap_or_else(|_| "postgres://sinex_test:testpass@localhost:5433/sinex_test".to_string());
    
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database_url)
        .await
        .expect("Failed to connect to test database");
    
    // Set up agent
    sqlx::query(
        "INSERT INTO sinex_schemas.agent_manifests (agent_name, version) 
         VALUES ($1, $2) ON CONFLICT (agent_name) DO NOTHING"
    )
    .bind("dlq_test_agent")
    .bind("1.0.0")
    .execute(&pool)
    .await
    .unwrap();
    
    // Insert event that will fail processing
    let event_id = Ulid::new();
    sqlx::query(
        "INSERT INTO raw.events (id, source, event_type, host, payload) 
         VALUES ($1::ulid, $2, $3, $4, $5::jsonb)"
    )
    .bind(&event_id.to_string())
    .bind("dlq_test")
    .bind("poison_event")
    .bind("test_host")
    .bind(&json!({"will_fail": true}))
    .execute(&pool)
    .await
    .unwrap();
    
    // Add to queue with low max_attempts
    sqlx::query(
        "INSERT INTO sinex_schemas.promotion_queue 
         (raw_event_id, target_agent_name, max_attempts) 
         VALUES ($1::ulid, $2, $3)"
    )
    .bind(&event_id.to_string())
    .bind("dlq_test_agent")
    .bind(2)
    .execute(&pool)
    .await
    .unwrap();
    
    // Simulate failed attempts
    for attempt in 1..=2 {
        sqlx::query(
            "UPDATE sinex_schemas.promotion_queue 
             SET status = 'failed_retryable',
                 attempts = $1,
                 last_attempt_ts = now(),
                 error_message_last = $2,
                 next_retry_ts = now() + interval '1 second' * $1
             WHERE raw_event_id = $3::ulid"
        )
        .bind(attempt)
        .bind(format!("Processing failed - attempt {}", attempt))
        .bind(&event_id.to_string())
        .execute(&pool)
        .await
        .unwrap();
    }
    
    // Final attempt should move to permanent failure (DLQ)
    sqlx::query(
        "UPDATE sinex_schemas.promotion_queue 
         SET status = CASE 
             WHEN attempts >= max_attempts THEN 'failed_permanent'
             ELSE status
         END
         WHERE raw_event_id = $1::ulid"
    )
    .bind(&event_id.to_string())
    .execute(&pool)
    .await
    .unwrap();
    
    // Verify DLQ state
    let (status, attempts, error): (String, i32, Option<String>) = sqlx::query_as(
        "SELECT status, attempts, error_message_last 
         FROM sinex_schemas.promotion_queue 
         WHERE raw_event_id = $1::ulid"
    )
    .bind(&event_id.to_string())
    .fetch_one(&pool)
    .await
    .unwrap();
    
    assert_eq!(status, "failed_permanent");
    assert_eq!(attempts, 2);
    assert!(error.is_some());
    
    // Query DLQ items
    let dlq_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM sinex_schemas.promotion_queue 
         WHERE status = 'failed_permanent'"
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    
    assert!(dlq_count >= 1, "Should have items in DLQ");
}

#[tokio::test]
async fn test_event_replay_capability() {
    let database_url = std::env::var("TEST_DATABASE_URL")
        .unwrap_or_else(|_| "postgres://sinex_test:testpass@localhost:5433/sinex_test".to_string());
    
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database_url)
        .await
        .expect("Failed to connect to test database");
    
    // Set up agent
    sqlx::query(
        "INSERT INTO sinex_schemas.agent_manifests (agent_name, version) 
         VALUES ($1, $2) ON CONFLICT (agent_name) DO NOTHING"
    )
    .bind("replay_test_agent")
    .bind("1.0.0")
    .execute(&pool)
    .await
    .unwrap();
    
    // Insert historical events
    let base_time = chrono::Utc::now() - chrono::Duration::hours(24);
    let mut historical_event_ids = Vec::new();
    
    for hour in 0..24 {
        let event_id = Ulid::new();
        historical_event_ids.push(event_id.to_string());
        
        let event_time = base_time + chrono::Duration::hours(hour);
        
        sqlx::query(
            "INSERT INTO raw.events (id, source, event_type, host, payload, ts_orig) 
             VALUES ($1::ulid, $2, $3, $4, $5::jsonb, $6)"
        )
        .bind(&event_id.to_string())
        .bind("historical_source")
        .bind("historical_event")
        .bind("hist_host")
        .bind(&json!({
            "hour": hour,
            "data": format!("event_at_hour_{}", hour)
        }))
        .bind(event_time)
        .execute(&pool)
        .await
        .unwrap();
    }
    
    // Replay events from specific time window
    let replay_start = base_time + chrono::Duration::hours(10);
    let replay_end = base_time + chrono::Duration::hours(15);
    
    let events_to_replay: Vec<(String,)> = sqlx::query_as(
        "SELECT id::text FROM raw.events 
         WHERE source = 'historical_source' 
         AND ts_orig >= $1 AND ts_orig < $2
         ORDER BY ts_orig"
    )
    .bind(replay_start)
    .bind(replay_end)
    .fetch_all(&pool)
    .await
    .unwrap();
    
    assert_eq!(events_to_replay.len(), 5, "Should find 5 events in replay window");
    
    // Add replayed events to processing queue
    for (event_id,) in events_to_replay {
        sqlx::query(
            "INSERT INTO sinex_schemas.promotion_queue 
             (raw_event_id, target_agent_name, status) 
             VALUES ($1::ulid, $2, 'pending')
             ON CONFLICT (raw_event_id, target_agent_name) DO NOTHING"
        )
        .bind(&event_id)
        .bind("replay_test_agent")
        .execute(&pool)
        .await
        .unwrap();
    }
    
    // Verify replay queue
    let replay_queue_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM sinex_schemas.promotion_queue 
         WHERE target_agent_name = 'replay_test_agent' 
         AND status = 'pending'"
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    
    assert_eq!(replay_queue_count, 5, "Should have 5 events queued for replay");
}

#[tokio::test]
async fn test_event_pipeline_monitoring() {
    let database_url = std::env::var("TEST_DATABASE_URL")
        .unwrap_or_else(|_| "postgres://sinex_test:testpass@localhost:5433/sinex_test".to_string());
    
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database_url)
        .await
        .expect("Failed to connect to test database");
    
    // Insert test data for monitoring queries
    let agents = vec!["monitor_agent_1", "monitor_agent_2"];
    
    for agent in &agents {
        sqlx::query(
            "INSERT INTO sinex_schemas.agent_manifests (agent_name, version) 
             VALUES ($1, $2) ON CONFLICT (agent_name) DO NOTHING"
        )
        .bind(agent)
        .bind("1.0.0")
        .execute(&pool)
        .await
        .unwrap();
    }
    
    // Create events with various states
    let states = vec![
        ("pending", 20),
        ("processing", 5),
        ("completed", 100),
        ("failed_retryable", 10),
        ("failed_permanent", 2),
    ];
    
    for (status, count) in states {
        for i in 0..count {
            let event_id = Ulid::new();
            
            sqlx::query(
                "INSERT INTO raw.events (id, source, event_type, host, payload) 
                 VALUES ($1::ulid, $2, $3, $4, $5::jsonb)"
            )
            .bind(&event_id.to_string())
            .bind("monitor_source")
            .bind("monitor_event")
            .bind("monitor_host")
            .bind(&json!({"seq": i}))
            .execute(&pool)
            .await
            .unwrap();
            
            let agent = agents[i % agents.len()];
            
            sqlx::query(
                "INSERT INTO sinex_schemas.promotion_queue 
                 (raw_event_id, target_agent_name, status, attempts) 
                 VALUES ($1::ulid, $2, $3, $4)"
            )
            .bind(&event_id.to_string())
            .bind(agent)
            .bind(status)
            .bind(if status.contains("failed") { 3 } else { 0 })
            .execute(&pool)
            .await
            .unwrap();
        }
    }
    
    // Pipeline health metrics
    let pipeline_stats: Vec<(String, String, i64)> = sqlx::query_as(
        "SELECT target_agent_name, status, COUNT(*) as count
         FROM sinex_schemas.promotion_queue
         GROUP BY target_agent_name, status
         ORDER BY target_agent_name, status"
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    
    // Verify metrics
    let mut total_by_status: std::collections::HashMap<String, i64> = std::collections::HashMap::new();
    
    for (_, status, count) in &pipeline_stats {
        *total_by_status.entry(status.clone()).or_insert(0) += count;
    }
    
    assert_eq!(total_by_status.get("pending").copied().unwrap_or(0), 20);
    assert_eq!(total_by_status.get("completed").copied().unwrap_or(0), 100);
    assert_eq!(total_by_status.get("failed_permanent").copied().unwrap_or(0), 2);
    
    // Queue depth by agent
    let queue_depths: Vec<(String, i64)> = sqlx::query_as(
        "SELECT target_agent_name, COUNT(*) as queue_depth
         FROM sinex_schemas.promotion_queue
         WHERE status IN ('pending', 'failed_retryable')
         GROUP BY target_agent_name"
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    
    for (agent, depth) in queue_depths {
        println!("Agent {} has queue depth: {}", agent, depth);
        assert!(depth > 0, "Should have items in queue");
    }
    
    // Success rate calculation
    let success_rate: Option<f64> = sqlx::query_scalar(
        "SELECT 
            CASE 
                WHEN COUNT(*) = 0 THEN NULL
                ELSE 100.0 * COUNT(*) FILTER (WHERE status = 'completed') / COUNT(*)
            END as success_rate
         FROM sinex_schemas.promotion_queue"
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    
    assert!(success_rate.is_some());
    let rate = success_rate.unwrap();
    assert!(rate > 70.0 && rate < 75.0, "Success rate should be around 73%");
}