//! Example demonstrating compile-time safe SQLX queries with ULID custom type
//! 
//! Run with: cargo run --example ulid_compile_time_safe

use anyhow::Result;
use sinex_db::{create_pool, queries_macro_safe::*};
use sinex_ulid::Ulid;
use std::env;

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    tracing_subscriber::fmt::init();
    
    // Get database URL from environment
    let database_url = env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgresql:///sinex_dev?host=/run/postgresql".to_string());
    
    println!("Connecting to database: {}", database_url);
    let pool = create_pool(&database_url).await?;
    
    // Example 1: Insert an event using compile-time safe query
    println!("\n=== Example 1: Inserting event with compile-time checking ===");
    let event = insert_raw_event_safe(
        &pool,
        "example.compile_time",
        "demo_event",
        "example_host",
        serde_json::json!({
            "message": "This event was inserted using compile-time safe SQLX macros",
            "timestamp": chrono::Utc::now(),
            "data": {
                "key": "value",
                "number": 42
            }
        }),
        Some(chrono::Utc::now()),
        Some("1.0.0"),
        None,
    )
    .await?;
    
    println!("Inserted event with ULID: {}", event.id);
    println!("Event source: {}", event.source);
    println!("Event type: {}", event.event_type);
    
    // Example 2: Fetch the event by ID
    println!("\n=== Example 2: Fetching event by ULID ===");
    if let Some(fetched) = get_event_by_id(&pool, event.id).await? {
        println!("Fetched event:");
        println!("  ID: {}", fetched.id);
        println!("  Ingestion time: {}", fetched.ts_ingest);
        println!("  Payload: {}", serde_json::to_string_pretty(&fetched.payload)?);
    }
    
    // Example 3: Insert multiple events and fetch them
    println!("\n=== Example 3: Batch operations ===");
    let mut event_ids = Vec::new();
    
    for i in 0..5 {
        let event = insert_raw_event_safe(
            &pool,
            "example.batch",
            "batch_event",
            "example_host",
            serde_json::json!({
                "sequence": i,
                "message": format!("Batch event {}", i)
            }),
            None,
            Some("1.0.0"),
            None,
        )
        .await?;
        
        event_ids.push(event.id);
        println!("Inserted batch event {}: {}", i, event.id);
    }
    
    // Fetch all batch events
    let batch_events = get_events_by_ids(&pool, &event_ids).await?;
    println!("\nFetched {} batch events", batch_events.len());
    
    // Example 4: Get recent events
    println!("\n=== Example 4: Getting recent events ===");
    let recent = get_recent_events(&pool, 10, Some("example.batch")).await?;
    println!("Found {} recent events from 'example.batch'", recent.len());
    
    for (i, event) in recent.iter().enumerate() {
        println!("  {}: {} - {}", i + 1, event.id, event.event_type);
    }
    
    // Example 5: Demonstrate ULID ordering
    println!("\n=== Example 5: ULID ordering demonstration ===");
    let ulid1 = Ulid::new();
    tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
    let ulid2 = Ulid::new();
    
    println!("ULID 1: {}", ulid1);
    println!("ULID 2: {}", ulid2);
    println!("ULID 2 > ULID 1: {}", ulid2 > ulid1);
    
    // Show the UUID representation
    println!("\nUUID representations:");
    println!("ULID 1 as UUID: {}", ulid1.as_uuid());
    println!("ULID 2 as UUID: {}", ulid2.as_uuid());
    
    println!("\n✅ All examples completed successfully!");
    
    Ok(())
}