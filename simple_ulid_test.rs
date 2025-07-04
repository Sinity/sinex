//! Simple test to verify ULID can be bound directly to SQLx queries
use sinex_ulid::Ulid;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let database_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgresql:///sinex_dev?host=/run/postgresql".to_string());
    
    let pool = sqlx::PgPool::connect(&database_url).await?;
    
    let test_ulid = Ulid::new();
    println!("Testing ULID: {}", test_ulid);
    
    // Test 1: Can we insert a ULID directly without ::uuid::ulid cast?
    let result = sqlx::query!(
        "INSERT INTO raw.events (id, source, event_type, payload, ts_orig, host)
         VALUES ($1, 'test', 'direct.test', '{}', NOW(), 'test')",
        test_ulid  // Direct ULID binding - no cast!
    ).execute(&pool).await;
    
    match result {
        Ok(_) => {
            println!("✅ SUCCESS: Direct ULID binding works!");
            
            // Test 2: Can we query it back?
            let found = sqlx::query!(
                "SELECT id FROM raw.events WHERE id = $1",
                test_ulid
            ).fetch_optional(&pool).await?;
            
            if found.is_some() {
                println!("✅ SUCCESS: Direct ULID querying works!");
            } else {
                println!("❌ FAIL: Could not query back the ULID");
            }
            
            // Clean up
            let _ = sqlx::query!("DELETE FROM raw.events WHERE id = $1", test_ulid).execute(&pool).await;
        }
        Err(e) => {
            println!("❌ FAIL: Direct ULID binding failed: {}", e);
        }
    }
    
    Ok(())
}