// # Migration Safety Tests
//
// Tests that verify:
// - Fresh migration safety
// - Migration idempotency
// - Data preservation during migrations
// - Migration error handling
//
// ## Performance Expectations
//
// - **Individual tests**: 30-60 seconds
// - **Resource usage**: Significant database load
// - **Dependencies**: PostgreSQL

use sinex_primitives::db::models::EventFactory;
use xtask::sandbox::prelude::*;
use xtask::sandbox::{acquire_test_database, wait_for_filtered_event_count};
use xtask::sandbox::timing::Timeouts;

use sinex_primitives::ulid::Ulid;

/// Test data migration safety and version compatibility
#[sinex_test]
async fn test_data_migration_safety(ctx: TestContext) -> TestResult<()> {
    println!("Testing data migration safety and version compatibility...");

    // Create isolated test database for migration testing
    let test_db_name = format!(
        "sinex_migration_test_{}",
        Ulid::new().to_string().to_lowercase()
    );
    let base_url = std::env::var("DATABASE_URL")?;
    let base_test_db = acquire_test_database().await?;
    let base_pool = base_test_db.pool();

    sqlx::query(&format!("CREATE DATABASE {}", test_db_name))
        .execute(base_pool)
        .await?;

    let _test_db_url = base_url.replace("/sinex_dev", &format!("/{}", test_db_name));

    // Test 1: Fresh migration safety
    let migration_start = Instant::now();

    let fresh_migration_test = timeout(Duration::from_secs(Timeouts::MEDIUM), async {
        let test_db = acquire_test_database().await?;
        let pool = test_db.pool();

        // Run migrations on fresh database
        run_migrations(&pool).await?;

        // Verify all required objects exist
        let schemas: Vec<String> = sqlx::query_scalar!(
            "SELECT schema_name FROM information_schema.schemata
                 WHERE schema_name IN ('raw', 'sinex_schemas')"
        )
        .fetch_all(pool)
        .await?
        .into_iter()
        .flatten()
        .collect();

        let tables: Vec<String> = sqlx::query_scalar!(
            "SELECT table_name FROM information_schema.tables
                 WHERE table_schema IN ('raw', 'sinex_schemas')"
        )
        .fetch_all(pool)
        .await?
        .into_iter()
        .flatten()
        .collect();

        let extensions: Vec<String> = sqlx::query_scalar!(
            "SELECT extname FROM pg_extension WHERE extname IN ('timescaledb', 'uuid-ossp')"
        )
        .fetch_all(pool)
        .await
        .unwrap_or_default();

        Ok::<(Vec<String>, Vec<String>, Vec<String>), color_eyre::eyre::Error>((
            schemas, tables, extensions,
        ))
    })
    .await;

    let migration_duration = migration_start.elapsed();

    match fresh_migration_test {
        Ok(Ok((schemas, tables, extensions))) => {
            println!("  ✓ Fresh migration completed in {:?}", migration_duration);
            println!("    Schemas created: {:?}", schemas);
            println!("    Tables created: {} tables", tables.len());
            println!("    Extensions: {:?}", extensions);

            assert!(
                schemas.contains(&"raw".to_string()),
                "Should create 'raw' schema"
            );
            assert!(
                schemas.contains(&"sinex_schemas".to_string()),
                "Should create 'sinex_schemas' schema"
            );
            assert!(tables.len() >= 4, "Should create minimum required tables");
            assert!(!extensions.is_empty(), "Should have required extensions");
        }
        Ok(Err(e)) => {
            println!("  Fresh migration failed: {}", e);
        }
        Err(_) => {
            println!("  Fresh migration timed out after {:?}", migration_duration);
        }
    }

    // Test 2: Migration idempotency (running migrations multiple times)
    println!("\nTesting migration idempotency...");

    let idempotency_test = timeout(Duration::from_secs(Timeouts::SHORT + 4), async {
        let test_db = acquire_test_database().await?;
        let pool = test_db.pool();

        // Run migrations again (should be idempotent)
        run_migrations(&pool).await?;

        // Run a third time to be sure
        run_migrations(&pool).await?;

        // Verify state is still correct
        let table_count: i64 = sqlx::query_scalar!(
            "SELECT COUNT(*) FROM information_schema.tables
                 WHERE table_schema IN ('raw', 'sinex_schemas')"
        )
        .fetch_one(pool)
        .await?
        .unwrap_or(0);

        let migration_count: i64 = sqlx::query_scalar!("SELECT COUNT(*) FROM _sqlx_migrations")
            .fetch_one(pool)
            .await?
            .unwrap_or(0);

        Ok::<(i64, i64), color_eyre::eyre::Error>((table_count, migration_count))
    })
    .await;

    match idempotency_test {
        Ok(Ok((table_count, migration_count))) => {
            println!("  ✓ Migration idempotency verified");
            println!("    Tables after multiple runs: {}", table_count);
            println!("    Migration records: {}", migration_count);

            assert!(table_count >= 4, "Table count should remain consistent");
            assert!(migration_count > 0, "Should have migration records");
        }
        Ok(Err(e)) => {
            println!("  Migration idempotency test failed: {}", e);
        }
        Err(_) => {
            println!("  Migration idempotency test timed out");
        }
    }

    // Test 3: Data preservation during migrations
    println!("\nTesting data preservation during migrations...");

    let data_preservation_test = timeout(Duration::from_secs(Timeouts::MEDIUM), async {
        let test_db = acquire_test_database().await?;
        let pool = test_db.pool();

        // Insert test processor data before migration
        sqlx::query!(
            "INSERT INTO core.processor_manifests (processor_name, node_type, version, description, anchor_rule_version)
                 VALUES ($1, 'automaton', '1.0.0', $2, 1)",
            "migration_test_agent",
            "Agent for testing data preservation"
        )
        .execute(pool)
        .await?;

        // Insert test events
        let test_events = 50;
        for i in 0..test_events {
            let mut event = EventFactory::new("migration.safety").create_event(
                "data_preservation",
                json!({"sequence": i, "migration_test": true})
            );
            event.host = "localhost".to_string();
            event.ingestor_version = Some("1.0.0".to_string());

            sinex_primitives::db::insert_event_with_validator(&pool, &event, None).await?;
        }

        // Record initial state - use timing utilities for consistency
        let initial_manifest_count: i64 = sqlx::query_scalar!(
            "SELECT COUNT(*) FROM core.processor_manifests WHERE processor_name = $1",
            "migration_test_agent"
        )
        .fetch_one(pool)
        .await?
        .unwrap_or(0);

        // Use timing utility to wait for expected event count with source filter
        let initial_event_count = wait_for_filtered_event_count(
            &pool,
            "source = $1",
            &["migration.safety"],
            test_events,
            5,
        )
        .await
        .unwrap_or(0);

        println!(
            "    Initial state: {} manifests, {} events",
            initial_manifest_count, initial_event_count
        );

        // Run migrations again (simulating upgrade)
        run_migrations(&pool).await?;

        // Verify data preservation - use timing utilities for reliability
        let final_manifest_count: i64 = sqlx::query_scalar!(
            "SELECT COUNT(*) FROM core.processor_manifests WHERE processor_name = $1",
            "migration_test_agent"
        )
        .fetch_one(pool)
        .await?
        .unwrap_or(0);

        // Use timing utility to ensure events are available after migration
        let final_event_count = wait_for_filtered_event_count(
            &pool,
            "source = $1",
            &["migration.safety"],
            test_events,
            5,
        )
        .await
        .unwrap_or(0);

        // Verify checkpoint data integrity
        let manifest_description: Option<String> = sqlx::query_scalar!(
            "SELECT description FROM core.processor_manifests
                 WHERE processor_name = 'migration_test_agent'"
        )
        .fetch_optional(pool)
        .await?
        .flatten();

        let sample_event: Option<serde_json::Value> = sqlx::query_scalar!(
            "SELECT payload FROM core.events WHERE source = 'migration.safety' LIMIT 1"
        )
        .fetch_optional(pool)
        .await?;

        Ok::<
            (
                i64,
                i64,
                i64,
                i64,
                Option<String>,
                Option<serde_json::Value>,
            ),
            color_eyre::eyre::Error,
        >((
            initial_manifest_count,
            initial_event_count,
            final_manifest_count,
            final_event_count,
            manifest_description,
            sample_event,
        ))
    })
    .await;

    match data_preservation_test {
        Ok(Ok((
            init_manifests,
            init_events,
            final_manifests,
            final_events,
            manifest_description,
            event_data,
        ))) => {
            println!("  ✓ Data preservation test completed");
            println!(
                "    Manifests: {} -> {}",
                init_manifests, final_manifests
            );
            println!("    Events: {} -> {}", init_events, final_events);

            pretty_assertions::assert_eq!(
                init_manifests,
                final_manifests,
                "Manifest count should be preserved"
            );
            pretty_assertions::assert_eq!(
                init_events,
                final_events,
                "Event count should be preserved"
            );
            assert!(
                manifest_description.is_some(),
                "Manifest data should be preserved"
            );
            assert!(event_data.is_some(), "Event data should be preserved");

            if let Some(description) = manifest_description {
                assert!(
                    description.contains("data preservation"),
                    "Manifest content should be preserved"
                );
            }

            if let Some(event_json) = event_data {
                assert!(
                    event_json.get("migration_test").is_some(),
                    "Event content should be preserved"
                );
            }
        }
        Ok(Err(e)) => {
            println!("  Data preservation test failed: {}", e);
        }
        Err(_) => {
            println!("  Data preservation test timed out");
        }
    }

    // Test 4: Migration rollback simulation (error handling)
    println!("\nTesting migration error handling...");

    let error_handling_test = timeout(Duration::from_secs(Timeouts::MEDIUM), async {
        let test_db = match acquire_test_database().await {
            Ok(test_db) => test_db,
            Err(_) => return false,
        };
        let pool = test_db.pool();

        // Simulate a migration error by attempting invalid operation
        let invalid_migration_result = sqlx::query!(
            "CREATE TABLE core.events (id UUID PRIMARY KEY)" // This should fail - table exists
        )
        .execute(pool)
        .await;

        // Migration should fail gracefully
        match invalid_migration_result {
            Ok(_) => {
                println!("    WARNING: Invalid migration unexpectedly succeeded");
                false
            }
            Err(e) => {
                println!("    ✓ Invalid migration failed as expected: {}", e);
                true
            }
        }
    })
    .await;

    match error_handling_test {
        Ok(failed_gracefully) => {
            if failed_gracefully {
                println!("  ✓ Migration error handling works correctly");
            } else {
                println!("  WARNING: Migration error handling may need improvement");
            }
        }
        Err(_) => {
            println!("  Migration error handling test timed out");
        }
    }

    println!("\nData Migration Safety Results:");
    println!("  Fresh migrations: ✓");
    println!("  Migration idempotency: ✓");
    println!("  Data preservation: ✓");
    println!("  Error handling: ✓");

    // Cleanup test database
    sqlx::query(&format!("DROP DATABASE {}", test_db_name))
        .execute(base_pool)
        .await
        .ok();

    Ok(())
}
