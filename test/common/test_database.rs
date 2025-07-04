//! Test database isolation through separate databases
//!
//! Each test gets its own database for perfect isolation.
//! This approach ensures complete test independence at the cost
//! of increased setup time and resource usage.
//!
//! # Usage
//! ```rust
//! let test_db = TestDatabase::create("my_test").await?;
//! // Use test_db.pool for database operations
//! // Database is automatically cleaned up on drop
//! ```

use crate::common::prelude::*;
use sqlx::postgres::PgConnection;
use sqlx::Connection;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::OnceLock;
use std::time::Duration;
use tokio::sync::Mutex;

static DB_COUNTER: AtomicU32 = AtomicU32::new(0);

/// Template database name cached for the current test process  
static TEMPLATE_DB_NAME: OnceLock<String> = OnceLock::new();

/// Mutex to ensure only one thread creates the template database
static TEMPLATE_CREATION_LOCK: Mutex<()> = Mutex::const_new(());

/// A test database that provides complete isolation
pub struct TestDatabase {
    pub pool: DbPool,
    pub name: String,
    admin_url: String,
}

impl TestDatabase {
    /// Create a new test database using template database (much faster)
    pub async fn create(test_name: &str) -> Result<Self> {
        // Get admin connection URL (to main database)
        let base_url = std::env::var("DATABASE_URL")
            .unwrap_or_else(|_| "postgresql:///sinex_dev?host=/run/postgresql".to_string());

        // Parse and modify to connect to postgres database for admin operations
        let admin_url = base_url.replace("/sinex_dev", "/postgres");

        // Ensure we have a template database with all migrations applied
        let template_name = Self::ensure_template_database(&admin_url, &base_url).await?;

        // Generate unique database name with safety checks
        let counter = DB_COUNTER.fetch_add(1, Ordering::SeqCst);
        let sanitized_name = test_name
            .replace("::", "_")
            .replace(" ", "_")
            .replace("-", "_")
            .replace(".", "_")
            .to_lowercase();
        let name = format!(
            "test_{}_{}_{}",
            sanitized_name.chars().take(20).collect::<String>(), // Limit length
            std::process::id(),
            counter
        );

        // Create the database from template (much faster than running migrations)
        let mut admin_conn = PgConnection::connect(&admin_url).await?;

        // Drop if exists (in case of previous failed test)
        let drop_query = format!("DROP DATABASE IF EXISTS {}", name);
        sqlx::query(&drop_query).execute(&mut admin_conn).await?;

        // Create fresh database from template (includes all migrations!)
        let create_query = format!("CREATE DATABASE {} WITH TEMPLATE {}", name, template_name);
        sqlx::query(&create_query).execute(&mut admin_conn).await?;

        admin_conn.close().await?;

        // Connect to the new database
        let db_url = base_url.replace("/sinex_dev", &format!("/{}", name));
        let pool: DbPool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(5)
            .min_connections(1)
            .max_lifetime(Duration::from_secs(300))
            .idle_timeout(Duration::from_secs(10))
            .acquire_timeout(Duration::from_secs(5))
            .connect(&db_url)
            .await?;

        // Apply test optimizations to this database session
        Self::apply_test_session_optimizations(&pool).await?;

        // No migrations needed - template already has them!
        
        Ok(TestDatabase {
            pool,
            name,
            admin_url,
        })
    }

    /// Ensure we have a template database with all migrations applied
    /// This is created once per test process and reused for all test databases
    pub async fn ensure_template_database(admin_url: &str, base_url: &str) -> Result<String> {
        // Check if we already have a template database cached
        if let Some(template_name) = TEMPLATE_DB_NAME.get() {
            return Ok(template_name.clone());
        }

        // Acquire lock to prevent race condition between parallel tests
        let _lock = TEMPLATE_CREATION_LOCK.lock().await;
        
        // Check again after acquiring lock (another thread might have created it)
        if let Some(template_name) = TEMPLATE_DB_NAME.get() {
            return Ok(template_name.clone());
        }

        // Create the template database name - use a shared name based on migrations hash
        // This allows multiple test processes to share the same template
        let template_name = "sinex_test_template_shared";
        
        eprintln!("🔧 Checking template database {} ...", template_name);
        let template_start = std::time::Instant::now();

        // Create template database with aggressive connection handling
        let admin_conn_future = async {
            let mut admin_conn = tokio::time::timeout(
                Duration::from_secs(5),
                PgConnection::connect(admin_url)
            ).await
            .map_err(|_| CoreError::database("Admin connection timeout").build())??;
            
            // Check if template already exists
            let exists: bool = sqlx::query_scalar(&format!(
                "SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname = '{}')",
                template_name
            ))
            .fetch_one(&mut admin_conn)
            .await?;
            
            if exists {
                eprintln!("✅ Template database already exists, reusing it");
                admin_conn.close().await?;
                return Ok::<bool, anyhow::Error>(false);  // false = no migrations needed
            }
            
            eprintln!("🔧 Creating template database {} (one-time setup)...", template_name);

            // First, aggressively terminate any existing connections to the template database
            let terminate_query = format!(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity 
                 WHERE datname = '{}' AND pid <> pg_backend_pid()",
                template_name
            );
            let _ = sqlx::query(&terminate_query).execute(&mut admin_conn).await;
            
            // Wait a bit for connections to close
            tokio::time::sleep(Duration::from_millis(100)).await;

            // Drop if exists (cleanup from previous runs) with CASCADE to force
            let drop_query = format!("DROP DATABASE IF EXISTS {} WITH (FORCE)", template_name);
            match sqlx::query(&drop_query).execute(&mut admin_conn).await {
                Ok(_) => {},
                Err(_) => {
                    // Fallback to regular DROP if FORCE not supported
                    let drop_query = format!("DROP DATABASE IF EXISTS {}", template_name);
                    sqlx::query(&drop_query).execute(&mut admin_conn).await?;
                }
            }

            // Create fresh template database
            let create_query = format!("CREATE DATABASE {}", template_name);
            tokio::time::timeout(
                Duration::from_secs(10),
                sqlx::query(&create_query).execute(&mut admin_conn)
            ).await
            .map_err(|_| CoreError::database("Create database timeout").build())??;

            admin_conn.close().await?;
            Ok::<bool, anyhow::Error>(true)  // true = needs migrations
        };

        // Execute admin operations with timeout
        let needs_migrations = tokio::time::timeout(Duration::from_secs(20), admin_conn_future).await
            .map_err(|_| CoreError::database("Admin operations timeout").build())??;

        // If template already exists, we're done
        if !needs_migrations {
            // Cache the template name for future use
            TEMPLATE_DB_NAME.set(template_name.to_string())
                .map_err(|_| CoreError::Other("Failed to cache template database name".to_string()))?;
            return Ok(template_name.to_string());
        }

        // Connect to template database and run all migrations
        let template_url = base_url.replace("/sinex_dev", &format!("/{}", template_name));
        
        let template_pool_future = async {
            let template_pool: DbPool = sqlx::postgres::PgPoolOptions::new()
                .max_connections(5)
                .min_connections(1)
                .max_lifetime(Duration::from_secs(300))
                .idle_timeout(Duration::from_secs(10))
                .acquire_timeout(Duration::from_secs(5))
                .connect(&template_url)
                .await?;

            // Apply test-specific optimizations for this session only
            Self::apply_test_session_optimizations(&template_pool).await?;

            // Run all migrations on template (this is the expensive part, but only once!)
            eprintln!("  📋 Running migrations on template database...");
            
            // Check for required extensions first
            match Self::check_required_extensions(&template_pool).await {
                Ok(_) => {},
                Err(e) => {
                    eprintln!("❌ Missing required PostgreSQL extensions: {}", e);
                    eprintln!("   Run ./check_postgresql_setup.sh for installation instructions.");
                    return Err(e);
                }
            }
            
            tokio::time::timeout(
                Duration::from_secs(30),
                sinex_db::run_migrations(&template_pool)
            ).await
            .map_err(|_| CoreError::database("Migration timeout - check if all required extensions are installed").build())??;
            
            // Optimize template for faster copying
            Self::optimize_template_for_tests(&template_pool).await?;
            
            template_pool.close().await;
            Ok::<(), anyhow::Error>(())
        };

        // Execute template setup with timeout
        tokio::time::timeout(Duration::from_secs(45), template_pool_future).await
            .map_err(|_| CoreError::database("Template setup timeout").build())??;

        let template_elapsed = template_start.elapsed();
        eprintln!("✅ Template database created in {:?}", template_elapsed);

        // Cache the template name for future use
        TEMPLATE_DB_NAME.set(template_name.to_string())
            .map_err(|_| CoreError::Other("Failed to cache template database name".to_string()))?;

        Ok(template_name.to_string())
    }

    /// Check if required PostgreSQL extensions are available
    async fn check_required_extensions(pool: &DbPool) -> Result<()> {
        let required_extensions = vec![
            ("ulid", "pgx_ulid for ULID primary keys"),
            ("timescaledb", "TimescaleDB for hypertable partitioning"),
            ("pg_jsonschema", "pg_jsonschema for JSON validation"),
            ("vector", "pgvector for vector similarity search"),
        ];
        
        let mut missing = Vec::new();
        
        for (ext_name, description) in required_extensions {
            let available: Option<String> = sqlx::query_scalar(
                "SELECT name FROM pg_available_extensions WHERE name = $1"
            )
            .bind(ext_name)
            .fetch_optional(pool)
            .await?;
            
            if available.is_none() {
                missing.push(format!("{} ({})", ext_name, description));
            }
        }
        
        if !missing.is_empty() {
            return Err(CoreError::database(
                format!("Missing required PostgreSQL extensions: {}", missing.join(", "))
            ).build().into());
        }
        
        Ok(())
    }
    
    /// Apply test-specific PostgreSQL optimizations (session-level only)
    async fn apply_test_session_optimizations(pool: &DbPool) -> Result<()> {
        if std::env::var("SINEX_TEST_OPTIMIZATIONS").is_ok() {
            eprintln!("⚡ Applying test session optimizations...");
            
            // These settings only affect this session/connection, not the global server
            // NOTE: Only use session-level settings, not server-level ones
            let optimizations = vec![
                "SET work_mem = '64MB'",
                "SET maintenance_work_mem = '256MB'", 
                "SET synchronous_commit = off",
                "SET random_page_cost = 1.1",  // Assume SSD/fast storage for tests
                "SET effective_cache_size = '1GB'",
                "SET temp_buffers = '32MB'",
                "SET statement_timeout = '30s'",  // Prevent runaway queries
            ];
            
            for setting in optimizations {
                if let Err(e) = sqlx::query(setting).execute(pool).await {
                    eprintln!("⚠️  Could not apply setting '{}': {}", setting, e);
                }
            }
        }
        Ok(())
    }

    /// Optimize template database for faster test copying
    async fn optimize_template_for_tests(pool: &DbPool) -> Result<()> {
        eprintln!("🔧 Optimizing template database for test performance...");
        
        // Add a timeout to prevent hanging
        let optimization_future = async {
        
        // Drop unnecessary indexes that slow down copying
        let expensive_indexes = vec![
            // Vector indexes are expensive to copy
            "idx_artifact_embeddings_vector",
            "idx_event_embeddings_vector", 
            "idx_embedding_cache_vector",
            
            // Full-text search indexes
            "idx_artifacts_search",
            "idx_ai_content_search",
            
            // Complex multi-column indexes for test data
            "idx_event_annotations_complex",
            "idx_artifact_relations_complex",
        ];
        
        for index in expensive_indexes {
            let drop_sql = format!("DROP INDEX IF EXISTS {}", index);
            if let Err(e) = sqlx::query(&drop_sql).execute(pool).await {
                // Don't fail if index doesn't exist
                eprintln!("⚠️  Could not drop index {}: {}", index, e);
            }
        }
        
        // CRITICAL: Disable TimescaleDB continuous aggregate policies in tests
        // These consume all background workers and cause timeouts
        eprintln!("  🔧 Disabling TimescaleDB continuous aggregate policies...");
        let disable_policies_sql = r#"
            SELECT alter_job(job_id, scheduled => false) 
            FROM timescaledb_information.jobs 
            WHERE application_name LIKE '%Continuous Aggregate%'
               OR application_name LIKE '%Telemetry%'
        "#;
        
        if let Err(e) = sqlx::query(disable_policies_sql).execute(pool).await {
            eprintln!("  ⚠️  Could not disable TimescaleDB policies: {}", e);
        }
        
        // Disable autovacuum on template (tests don't need it)
        let disable_autovacuum_tables = vec![
            "raw.events",
            "core.artifacts", 
            "core.event_annotations",
            "sinex_schemas.work_queue",
        ];
        
        for table in disable_autovacuum_tables {
            let disable_sql = format!("ALTER TABLE {} SET (autovacuum_enabled = false)", table);
            if let Err(e) = sqlx::query(&disable_sql).execute(pool).await {
                eprintln!("⚠️  Could not disable autovacuum on {}: {}", table, e);
            }
        }
        
        // Set test-friendly table settings
        sqlx::query("ALTER TABLE raw.events SET (fillfactor = 100)")
            .execute(pool)
            .await
            .unwrap_or_else(|_| {
                eprintln!("⚠️  Could not set fillfactor on raw.events");
                Default::default()
            });
        
        // Clean up any test data that might have snuck in
        sqlx::query("DELETE FROM raw.events WHERE source LIKE 'test_%'")
            .execute(pool)
            .await
            .unwrap_or_else(|_| {
                eprintln!("⚠️  Could not clean test data");
                Default::default()
            });
        
        // Note: CHECKPOINT removed as it can hang or require special privileges
        // The template database will be in a clean state anyway since we just created it
        
            eprintln!("✅ Template database optimized for test performance");
            Ok::<(), anyhow::Error>(())
        };
        
        // Apply a reasonable timeout
        match tokio::time::timeout(Duration::from_secs(20), optimization_future).await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(e)) => Err(e.into()),
            Err(_) => {
                eprintln!("⚠️  Template optimization timed out after 20s, continuing anyway");
                Ok(()) // Don't fail, optimizations are optional
            }
        }
    }

}

impl Drop for TestDatabase {
    fn drop(&mut self) {
        let admin_url = self.admin_url.clone();
        let db_name = self.name.clone();
        
        // Try to clean up in existing runtime if available
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            // We have a runtime, use it
            let dummy_pool = DbPool::connect_lazy("postgresql://localhost/postgres").unwrap();
            let pool = std::mem::replace(&mut self.pool, dummy_pool);
            handle.spawn(async move {
                // Close pool first
                pool.close().await;
                
                // Then drop database
                if let Ok(mut conn) = PgConnection::connect(&admin_url).await {
                    let _ = sqlx::query(&format!(
                        "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '{}' AND pid <> pg_backend_pid()",
                        db_name
                    )).execute(&mut conn).await;
                    
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    
                    let _ = sqlx::query(&format!("DROP DATABASE IF EXISTS {}", db_name))
                        .execute(&mut conn)
                        .await;
                    
                    let _ = conn.close().await;
                }
            });
        } else {
            // No runtime, do basic cleanup
            let dummy_pool = DbPool::connect_lazy("postgresql://localhost/postgres").unwrap();
            let pool = std::mem::replace(&mut self.pool, dummy_pool);
            std::thread::spawn(move || {
                // Block on closing the pool
                let _ = futures::executor::block_on(pool.close());
                
                // Try to drop database using psql command
                let _ = std::process::Command::new("psql")
                    .arg(&admin_url)
                    .arg("-c")
                    .arg(&format!("DROP DATABASE IF EXISTS {}", db_name))
                    .output();
            });
        }
    }
}

/// Utility functions for test database management
impl TestDatabase {
    /// Check if the database is healthy
    pub async fn check_health(&self) -> Result<bool> {
        match sqlx::query!("SELECT 1 as health_check")
            .fetch_one(&self.pool)
            .await
        {
            Ok(_) => Ok(true),
            Err(_) => Ok(false),
        }
    }

    /// Get database statistics
    pub async fn get_stats(&self) -> Result<DatabaseStats> {
        let row = sqlx::query!(
            r#"
            SELECT
                (SELECT COUNT(*) FROM raw.events) as event_count,
                (SELECT COUNT(*) FROM sinex_schemas.agent_manifests) as agent_count,
                (SELECT COUNT(*) FROM sinex_schemas.work_queue) as work_queue_count
            "#
        )
        .fetch_one(&self.pool)
        .await?;

        Ok(DatabaseStats {
            event_count: row.event_count.unwrap_or(0),
            agent_count: row.agent_count.unwrap_or(0),
            work_queue_count: row.work_queue_count.unwrap_or(0),
        })
    }
}

/// Database statistics for debugging
#[derive(Debug, Clone)]
pub struct DatabaseStats {
    pub event_count: i64,
    pub agent_count: i64,
    pub work_queue_count: i64,
}
