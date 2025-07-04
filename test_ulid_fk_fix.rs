//! Minimal test to verify ULID FK fix
//! 
//! This test reproduces the FK constraint issue and verifies the fix.

use anyhow::Result;
use sinex_db::prelude::*;
use sinex_ulid::Ulid;

#[tokio::main]
async fn main() -> Result<()> {
    let database_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgresql:///sinex_dev?host=/run/postgresql".to_string());
    
    let pool = sinex_db::create_pool(&database_url).await?;
    
    println!("Testing ULID FK constraint issue...");
    
    // Test 1: Current problematic approach (should fail intermittently)
    test_problematic_approach(&pool).await?;
    
    // Test 2: Fixed approach (should always work)
    test_fixed_approach(&pool).await?;
    
    Ok(())
}

async fn test_problematic_approach(pool: &DbPool) -> Result<()> {
    println!("\n=== Test 1: Problematic approach with ::uuid::ulid cast ===");
    
    for i in 0..10 {
        let event_id = Ulid::new();
        let queue_id = Ulid::new();
        
        // Insert parent event using cast (this works)
        let result = sqlx::query!(
            "INSERT INTO raw.events (id, source, event_type, payload, ts_orig, host)
             VALUES ($1::uuid::ulid, $2, $3, $4, $5, $6)",
            sinex_db::ulid_to_uuid(event_id),
            "test_source",
            "test.event",
            serde_json::json!({"test": true}),
            chrono::Utc::now(),
            "test_host"
        ).execute(pool).await;
        
        if let Err(e) = result {
            println!("  Attempt {}: Parent insert failed: {}", i, e);
            continue;
        }
        
        // Insert child with FK using cast (this might fail)
        let result = sqlx::query!(
            "INSERT INTO sinex_schemas.work_queue (queue_id, raw_event_id, target_agent_name, status)
             VALUES ($1::uuid::ulid, $2::uuid::ulid, $3, $4)",
            sinex_db::ulid_to_uuid(queue_id),
            sinex_db::ulid_to_uuid(event_id),
            "test_agent",
            "pending"
        ).execute(pool).await;
        
        match result {
            Ok(_) => println!("  Attempt {}: SUCCESS", i),
            Err(e) => {
                if e.to_string().contains("work_queue_raw_event_id_fkey") {
                    println!("  Attempt {}: FK CONSTRAINT FAILURE (expected bug): {}", i, e);
                    return Ok(()); // We reproduced the bug!
                } else {
                    println!("  Attempt {}: OTHER ERROR: {}", i, e);
                }
            }
        }
        
        // Clean up for next iteration
        let _ = sqlx::query!("DELETE FROM sinex_schemas.work_queue WHERE queue_id = $1", sinex_db::ulid_to_uuid(queue_id)).execute(pool).await;
        let _ = sqlx::query!("DELETE FROM raw.events WHERE id = $1", sinex_db::ulid_to_uuid(event_id)).execute(pool).await;
    }
    
    println!("  No FK failures reproduced in 10 attempts (might need more tries)");
    Ok(())
}

async fn test_fixed_approach(pool: &DbPool) -> Result<()> {
    println!("\n=== Test 2: Fixed approach with direct ULID binding ===");
    
    for i in 0..10 {
        let event_id = Ulid::new();
        let queue_id = Ulid::new();
        
        // Insert parent event without cast
        let result = sqlx::query!(
            "INSERT INTO raw.events (id, source, event_type, payload, ts_orig, host)
             VALUES ($1, $2, $3, $4, $5, $6)",
            event_id, // Direct ULID binding
            "test_source",
            "test.event",
            serde_json::json!({"test": true}),
            chrono::Utc::now(),
            "test_host"
        ).execute(pool).await;
        
        if let Err(e) = result {
            println!("  Attempt {}: Parent insert failed: {}", i, e);
            continue;
        }
        
        // Insert child with FK without cast
        let result = sqlx::query!(
            "INSERT INTO sinex_schemas.work_queue (queue_id, raw_event_id, target_agent_name, status)
             VALUES ($1, $2, $3, $4)",
            queue_id,  // Direct ULID binding
            event_id,  // Direct ULID binding
            "test_agent",
            "pending"
        ).execute(pool).await;
        
        match result {
            Ok(_) => println!("  Attempt {}: SUCCESS", i),
            Err(e) => {
                println!("  Attempt {}: FAILED: {}", i, e);
                return Err(e.into());
            }
        }
        
        // Clean up for next iteration
        let _ = sqlx::query!("DELETE FROM sinex_schemas.work_queue WHERE queue_id = $1", queue_id).execute(pool).await;
        let _ = sqlx::query!("DELETE FROM raw.events WHERE id = $1", event_id).execute(pool).await;
    }
    
    println!("  All 10 attempts succeeded with direct ULID binding!");
    Ok(())
}