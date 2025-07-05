//! Pre-initialized database pool for test isolation
//!
//! This creates a fixed pool of databases at startup and distributes them to tests.
//! Databases are cleaned BEFORE being given to a test, not after.

use crate::common::prelude::*;
use std::sync::atomic::{AtomicU32, AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};
use once_cell::sync::Lazy;
use std::time::Duration;
use sqlx::postgres::PgConnection;
use sqlx::Connection;

static DB_COUNTER: AtomicU32 = AtomicU32::new(0);

/// Template database name cached for the current test process  
static TEMPLATE_DB_NAME: OnceLock<String> = OnceLock::new();

/// Mutex to ensure only one thread creates the template database
use lazy_static::lazy_static;

lazy_static! {
    static ref TEMPLATE_CREATION_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::new(());
}

/// Database pool configuration
struct PoolConfig {
    size: usize,
    admin_url: String,
    base_url: String,
    template_name: String,
}

impl Default for PoolConfig {
    fn default() -> Self {
        let base_url = std::env::var("DATABASE_URL")
            .unwrap_or_else(|_| "postgresql:///sinex_dev?host=/run/postgresql".to_string());
        let admin_url = base_url.replace("/sinex_dev", "/postgres");
        
        Self {
            size: 64,  // Large pool to minimize contention on high-core systems
            admin_url,
            base_url,
            template_name: "sinex_test_template_shared".to_string(),
        }
    }
}

/// A test database handle
pub struct TestDatabase {
    name: String,
    pool: DbPool,
    slot: Arc<DatabaseSlot>,
}

impl TestDatabase {
    pub fn name(&self) -> &str {
        &self.name
    }
    
    pub fn pool(&self) -> &DbPool {
        &self.pool
    }
}

impl Drop for TestDatabase {
    fn drop(&mut self) {
        // Mark slot as available
        self.slot.in_use.store(false, Ordering::Release);
    }
}

/// A slot in the database pool
struct DatabaseSlot {
    name: String,
    pool: DbPool,
    in_use: AtomicBool,
}

/// The global database pool
struct DatabasePool {
    slots: Vec<Arc<DatabaseSlot>>,
}

impl DatabasePool {
    /// Initialize the pool
    async fn new(config: PoolConfig) -> Result<Self> {
        eprintln!("🚀 Initializing database pool with {} databases (reusing existing if available)...", config.size);
        
        // Ensure template exists
        ensure_template_database(
            &config.admin_url,
            &config.base_url,
        ).await?;
        
        // Create admin connection
        let admin_pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(10)  // Increased for parallel database creation
            .connect(&config.admin_url)
            .await?;
        
        // Clean up any non-pool test databases (from old test runs)
        let non_pool_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM pg_database WHERE datname LIKE 'sinex_test_%' 
             AND datname NOT LIKE 'sinex_test_pool_%' 
             AND datname NOT LIKE '%template%'"
        )
        .fetch_one(&admin_pool)
        .await?;
        
        if non_pool_count > 0 {
            eprintln!("🧹 Cleaning up {} non-pool test databases...", non_pool_count);
            
            // Get list of non-pool databases
            let dbs_to_drop: Vec<String> = sqlx::query_scalar(
                "SELECT datname FROM pg_database WHERE datname LIKE 'sinex_test_%' 
                 AND datname NOT LIKE 'sinex_test_pool_%' 
                 AND datname NOT LIKE '%template%'"
            )
            .fetch_all(&admin_pool)
            .await?;
            
            // Drop them
            for db in dbs_to_drop {
                let _ = sqlx::query(&format!("DROP DATABASE IF EXISTS {}", db))
                    .execute(&admin_pool)
                    .await;
            }
        }
        
        // Create all databases in parallel
        let mut slots = Vec::with_capacity(config.size);
        let mut tasks = Vec::new();
        
        for i in 0..config.size {
            let admin_pool = admin_pool.clone();
            let base_url = config.base_url.clone();
            let template_name = config.template_name.clone();
            
            let task = tokio::spawn(async move {
                let name = format!("sinex_test_pool_{}", i);
                
                let mut conn = admin_pool.acquire().await?;
                
                // Check if database already exists
                let exists: bool = sqlx::query_scalar(&format!(
                    "SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname = '{}')",
                    name
                ))
                .fetch_one(&mut *conn)
                .await?;
                
                if !exists {
                    sqlx::query(&format!("CREATE DATABASE {} WITH TEMPLATE {}", name, template_name))
                        .execute(&mut *conn).await?;
                    eprintln!("  Created new pool database: {}", name);
                } else {
                    eprintln!("  Reusing existing pool database: {}", name);
                }
                
                drop(conn);
                
                // Create connection pool
                let url = base_url.replace("/sinex_dev", &format!("/{}", name));
                let pool = sqlx::postgres::PgPoolOptions::new()
                    .max_connections(15)  // Increased from 5 for better test concurrency
                    .acquire_timeout(Duration::from_secs(10))  // Increased timeout for parallel tests
                    .connect(&url)
                    .await?;
                
                Result::<_, anyhow::Error>::Ok((name, pool))
            });
            
            tasks.push(task);
        }
        
        // Wait for all databases to be created
        for task in tasks {
            let (name, pool) = task.await??;
            slots.push(Arc::new(DatabaseSlot {
                name,
                pool,
                in_use: AtomicBool::new(false),
            }));
        }
        
        eprintln!("✅ Database pool initialized with {} databases", slots.len());
        
        Ok(Self {
            slots,
        })
    }
    
    /// Acquire a database from the pool
    async fn acquire(&self) -> Result<TestDatabase> {
        let start_time = std::time::Instant::now();
        let mut attempts = 0;
        loop {
            for slot in &self.slots {
                if !slot.in_use.swap(true, Ordering::AcqRel) {
                    // Got a slot! Clean it before use
                    let clean_start = std::time::Instant::now();
                    match clean_database(&slot.pool).await {
                        Ok(_) => {
                            let clean_time = clean_start.elapsed();
                            if clean_time.as_millis() > 100 {
                                eprintln!("🔧 Database {} cleaned in {:.1?}", slot.name, clean_time);
                            }
                            return Ok(TestDatabase {
                                name: slot.name.clone(),
                                pool: slot.pool.clone(),
                                slot: slot.clone(),
                            });
                        }
                        Err(e) => {
                            eprintln!("⚠️  Failed to clean database {}: {}", slot.name, e);
                            slot.in_use.store(false, Ordering::Release);
                        }
                    }
                }
            }
            
            attempts += 1;
            if attempts > 1000 {
                let total_time = start_time.elapsed();
                return Err(anyhow::anyhow!("Failed to acquire database after 1000 attempts ({:.1?}) - all {} slots in use", total_time, self.slots.len()));
            }
            
            // Log warning after many attempts
            if attempts % 50 == 0 {
                let elapsed = start_time.elapsed();
                eprintln!("⚠️  Waiting for database slot (attempt {}, {:.1?} elapsed). All {} slots in use.", attempts, elapsed, self.slots.len());
            }
            
            // All slots in use, wait a bit
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}

/// Clean a database for reuse
async fn clean_database(pool: &DbPool) -> Result<()> {
    // Clean in proper dependency order based on foreign key constraints
    // First clean tables that reference other tables
    let cleanup_queries = [
        "DELETE FROM sinex_schemas.work_queue",
        "DELETE FROM core.event_annotations", 
        "DELETE FROM core.event_artifact_refs",
        "DELETE FROM core.event_relations",
        "DELETE FROM core.event_cluster_members",
        "DELETE FROM core.artifact_event_sources",
        "DELETE FROM core.event_embeddings",
        "DELETE FROM core.artifact_embeddings",
        "DELETE FROM core.artifact_contents",
        "DELETE FROM core.artifact_tags",
        "DELETE FROM core.artifact_relations", 
        "DELETE FROM core.entity_relations",
        "DELETE FROM core.artifacts",
        "DELETE FROM raw.events",
        "DELETE FROM sinex_schemas.agent_manifests",
    ];
    
    for query in cleanup_queries {
        let _ = sqlx::query(query).execute(pool).await;
    }
    
    Ok(())
}

// Global pool instance - initialized on first use
static POOL: Lazy<tokio::sync::Mutex<Option<Arc<DatabasePool>>>> = Lazy::new(|| {
    tokio::sync::Mutex::new(None)
});

/// Acquire a test database
pub async fn acquire_test_database() -> Result<TestDatabase> {
    // Get or initialize the pool
    let mut pool_lock = POOL.lock().await;
    
    if pool_lock.is_none() {
        let config = PoolConfig::default();
        let pool = Arc::new(DatabasePool::new(config).await?);
        *pool_lock = Some(pool);
    }
    
    let pool = pool_lock.as_ref().unwrap().clone();
    drop(pool_lock);
    
    pool.acquire().await
}

/// Ensure we have a template database with all migrations applied
/// This is created once per test process and reused for all test databases
async fn ensure_template_database(admin_url: &str, base_url: &str) -> Result<String> {
    // Check if we already have a template database cached
    if let Some(template_name) = TEMPLATE_DB_NAME.get() {
        return Ok(template_name.clone());
    }

    // Acquire lock to prevent race condition between parallel tests
    let _lock = TEMPLATE_CREATION_LOCK.lock().await;
    
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
            .max_connections(15)  // Increased for template database setup
            .min_connections(1)
            .max_lifetime(Duration::from_secs(300))
            .idle_timeout(Duration::from_secs(10))
            .acquire_timeout(Duration::from_secs(15))  // Increased for parallel template operations
            .connect(&template_url)
            .await?;

        // Apply test-specific optimizations for this session only
        apply_test_session_optimizations(&template_pool).await?;

        // Run all migrations on template (this is the expensive part, but only once!)
        eprintln!("  📋 Running migrations on template database...");
        
        // Check for required extensions first
        match check_required_extensions(&template_pool).await {
            Ok(_) => {},
            Err(e) => {
                eprintln!("❌ Missing required PostgreSQL extensions: {}", e);
                eprintln!("   Check NixOS PostgreSQL configuration and required extensions.");
                return Err(e);
            }
        }
        
        tokio::time::timeout(
            Duration::from_secs(30),
            sinex_db::run_migrations(&template_pool)
        ).await
        .map_err(|_| CoreError::database("Migration timeout - check if all required extensions are installed").build())??;
        
        // Optimize template for faster copying
        optimize_template_for_tests(&template_pool).await?;
        
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

