//! Redis Pool for Test Isolation
//!
//! Similar to our database pool, this provides fast, isolated Redis instances for tests.
//! Uses Redis databases (0-15+) or key prefixing for complete test isolation.

use once_cell::sync::Lazy;
use redis::{Client, Connection, RedisResult, Value};
use sinex_error::{Result, SinexError};
use sinex_ulid::Ulid;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex as TokioMutex;

/// Pool configuration
#[derive(Clone)]
pub struct RedisPoolConfig {
    /// Redis connection URL
    pub url: String,
    /// Number of Redis databases to use (0-15 typically)
    pub max_databases: u16,
    /// Use key prefixing for additional isolation
    pub use_key_prefixes: bool,
    /// Connection timeout
    pub connection_timeout: Duration,
}

impl Default for RedisPoolConfig {
    fn default() -> Self {
        let url =
            std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string());

        Self {
            url,
            max_databases: 16, // Redis default
            use_key_prefixes: true,
            connection_timeout: Duration::from_secs(5),
        }
    }
}

/// A slot in the Redis pool
struct RedisSlot {
    /// Database index (0-15+)
    db_index: u16,
    /// Redis client
    client: Client,
    /// Unique key prefix for this slot
    key_prefix: String,
    /// Whether slot is currently in use
    in_use: AtomicBool,
    /// Number of times this slot has been used
    use_count: AtomicU64,
}

/// Redis pool for test isolation
pub struct RedisPool {
    config: RedisPoolConfig,
    slots: Vec<Arc<RedisSlot>>,
}

/// Test Redis handle - automatically cleans up on drop
pub struct TestRedis {
    slot: Arc<RedisSlot>,
    conn: Connection,
    config: RedisPoolConfig,
}

impl TestRedis {
    /// Get the Redis connection
    pub fn conn(&mut self) -> &mut Connection {
        &mut self.conn
    }

    /// Get the Redis client (for creating new connections)
    pub fn client(&self) -> &Client {
        &self.slot.client
    }

    /// Generate a namespaced key
    pub fn key(&self, key: &str) -> String {
        if self.config.use_key_prefixes {
            format!("{}:{}", self.slot.key_prefix, key)
        } else {
            key.to_string()
        }
    }

    /// Execute a Redis command with automatic key namespacing
    pub fn cmd(&self, cmd: &str) -> redis::Cmd {
        redis::cmd(cmd)
    }

    /// Set a value (with automatic namespacing)
    pub async fn set<V: redis::ToRedisArgs>(&mut self, key: &str, value: V) -> Result<()> {
        let namespaced_key = self.key(key);
        redis::cmd("SET")
            .arg(&namespaced_key)
            .arg(value)
            .query(&mut self.conn)
            .map_err(|e| SinexError::database(format!("Redis SET failed: {}", e)))
    }

    /// Get a value (with automatic namespacing)
    pub async fn get(&mut self, key: &str) -> Result<Value> {
        let namespaced_key = self.key(key);
        redis::cmd("GET")
            .arg(&namespaced_key)
            .query(&mut self.conn)
            .map_err(|e| SinexError::database(format!("Redis GET failed: {}", e)))
    }

    /// Add to a stream (with automatic namespacing)
    pub async fn xadd<F: redis::ToRedisArgs>(
        &mut self,
        stream: &str,
        id: &str,
        fields: &[(String, F)],
    ) -> Result<String> {
        let namespaced_stream = self.key(stream);
        let mut cmd = redis::cmd("XADD");
        cmd.arg(&namespaced_stream).arg(id);

        for (key, value) in fields {
            cmd.arg(key).arg(value);
        }

        cmd.query(&mut self.conn)
            .map_err(|e| SinexError::database(format!("Redis XADD failed: {}", e)))
    }

    /// Read from a stream (with automatic namespacing)
    pub async fn xread(
        &mut self,
        streams: &[&str],
        ids: &[&str],
        count: Option<usize>,
        block: Option<usize>,
    ) -> Result<Value> {
        let mut cmd = redis::cmd("XREAD");

        if let Some(c) = count {
            cmd.arg("COUNT").arg(c);
        }

        if let Some(b) = block {
            cmd.arg("BLOCK").arg(b);
        }

        cmd.arg("STREAMS");

        // Add namespaced streams
        for stream in streams {
            cmd.arg(self.key(stream));
        }

        // Add IDs
        for id in ids {
            cmd.arg(id);
        }

        cmd.query(&mut self.conn)
            .map_err(|e| SinexError::database(format!("Redis XREAD failed: {}", e)))
    }
}

impl Drop for TestRedis {
    fn drop(&mut self) {
        // Clean up namespace if using key prefixes
        if self.config.use_key_prefixes {
            // Use non-blocking cleanup
            let prefix = self.slot.key_prefix.clone();
            let mut conn_clone = self.client().get_connection().ok();

            if let Some(conn) = conn_clone.as_mut() {
                // Best effort cleanup - scan and delete keys with our prefix
                let _ = clean_redis_namespace(conn, &prefix);
            }
        } else {
            // If not using prefixes, flush the entire database
            let _ = redis::cmd("FLUSHDB")
                .arg("ASYNC")
                .query::<()>(&mut self.conn);
        }

        // Mark slot as available
        self.slot.in_use.store(false, Ordering::Release);
    }
}

/// Clean all keys with a given prefix
fn clean_redis_namespace(conn: &mut Connection, prefix: &str) -> RedisResult<()> {
    let pattern = format!("{}:*", prefix);
    let mut cursor = 0i64;

    loop {
        let (new_cursor, keys): (i64, Vec<String>) = redis::cmd("SCAN")
            .arg(cursor)
            .arg("MATCH")
            .arg(&pattern)
            .arg("COUNT")
            .arg(1000)
            .query(conn)?;

        if !keys.is_empty() {
            redis::cmd("UNLINK").arg(&keys).query::<()>(conn)?;
        }

        cursor = new_cursor;
        if cursor == 0 {
            break;
        }
    }

    Ok(())
}

impl RedisPool {
    /// Create a new Redis pool
    pub async fn new(config: RedisPoolConfig) -> Result<Self> {
        let client = Client::open(config.url.as_str())
            .map_err(|e| SinexError::database(format!("Failed to create Redis client: {}", e)))?;

        // Test connection
        let mut test_conn = client
            .get_connection()
            .map_err(|e| SinexError::database(format!("Failed to connect to Redis: {}", e)))?;

        // Check Redis version and available databases
        let _info: String = redis::cmd("INFO")
            .arg("server")
            .query(&mut test_conn)
            .map_err(|e| SinexError::database(format!("Failed to get Redis info: {}", e)))?;

        println!(
            "🔧 Initializing Redis test pool with {} databases",
            config.max_databases
        );

        let mut slots = Vec::new();

        for db_index in 0..config.max_databases {
            // Test if we can select this database
            let mut conn = client.get_connection().map_err(|e| {
                SinexError::database(format!(
                    "Failed to get connection for DB {}: {}",
                    db_index, e
                ))
            })?;

            redis::cmd("SELECT")
                .arg(db_index)
                .query::<()>(&mut conn)
                .map_err(|e| {
                    SinexError::database(format!("Failed to select DB {}: {}", db_index, e))
                })?;

            let slot = Arc::new(RedisSlot {
                db_index,
                client: client.clone(),
                key_prefix: format!("test_{}", Ulid::new()),
                in_use: AtomicBool::new(false),
                use_count: AtomicU64::new(0),
            });

            slots.push(slot);
        }

        println!("✅ Redis pool initialized with {} slots", slots.len());

        Ok(Self { config, slots })
    }

    /// Acquire a Redis instance from the pool
    pub async fn acquire(&self) -> Result<TestRedis> {
        let start_time = Instant::now();
        let mut attempts = 0;

        loop {
            attempts += 1;

            // Try to find a free slot
            for slot in &self.slots {
                if slot
                    .in_use
                    .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
                {
                    // Got a slot!
                    slot.use_count.fetch_add(1, Ordering::Relaxed);

                    // Get connection and select database
                    let mut conn = slot.client.get_connection().map_err(|e| {
                        SinexError::database(format!("Failed to get Redis connection: {}", e))
                    })?;

                    redis::cmd("SELECT")
                        .arg(slot.db_index)
                        .query::<()>(&mut conn)
                        .map_err(|e| {
                            SinexError::database(format!(
                                "Failed to select DB {}: {}",
                                slot.db_index, e
                            ))
                        })?;

                    // Clean the database
                    redis::cmd("FLUSHDB")
                        .arg("ASYNC")
                        .query::<()>(&mut conn)
                        .map_err(|e| SinexError::database(format!("Failed to flush DB: {}", e)))?;

                    let acquisition_time = start_time.elapsed();
                    if acquisition_time > Duration::from_millis(100) {
                        eprintln!(
                            "⚠️  Slow Redis acquisition: {:?} (attempt {})",
                            acquisition_time, attempts
                        );
                    }

                    return Ok(TestRedis {
                        slot: slot.clone(),
                        conn,
                        config: self.config.clone(),
                    });
                }
            }

            // No free slots, wait a bit
            if start_time.elapsed() > self.config.connection_timeout {
                return Err(SinexError::database(
                    "Timeout waiting for available Redis slot",
                ))
                .into();
            }

            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}

/// Global Redis pool instance
static REDIS_POOL: Lazy<TokioMutex<Option<Arc<RedisPool>>>> = Lazy::new(|| TokioMutex::new(None));

/// Acquire a test Redis instance
pub async fn acquire_test_redis() -> Result<TestRedis> {
    let mut pool_lock = REDIS_POOL.lock().await;

    if pool_lock.is_none() {
        let config = RedisPoolConfig::default();
        let pool = Arc::new(RedisPool::new(config).await?);
        *pool_lock = Some(pool);
    }

    let pool = pool_lock.as_ref().unwrap().clone();
    drop(pool_lock); // Release lock before acquiring

    pool.acquire().await
}

/// Get pool statistics
pub async fn get_redis_pool_stats() -> Option<RedisPoolStats> {
    let pool_lock = REDIS_POOL.lock().await;

    pool_lock.as_ref().map(|pool| {
        let total_slots = pool.slots.len();
        let in_use = pool
            .slots
            .iter()
            .filter(|s| s.in_use.load(Ordering::Acquire))
            .count();
        let total_uses: u64 = pool
            .slots
            .iter()
            .map(|s| s.use_count.load(Ordering::Relaxed))
            .sum();

        RedisPoolStats {
            total_slots,
            slots_in_use: in_use,
            slots_free: total_slots - in_use,
            total_acquisitions: total_uses,
        }
    })
}

/// Redis pool statistics
#[derive(Debug)]
pub struct RedisPoolStats {
    pub total_slots: usize,
    pub slots_in_use: usize,
    pub slots_free: usize,
    pub total_acquisitions: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sinex_test;
    use anyhow::Result;

    #[sinex_test]
    async fn test_redis_pool_basic() -> Result<()> {
        let mut redis = acquire_test_redis().await?;

        // Basic operations should work
        redis.set("test_key", "test_value").await?;
        let value: Value = redis.get("test_key").await?;

        match value {
            Value::Data(data) => {
                let string_value = String::from_utf8(data).unwrap();
                assert_eq!(string_value, "test_value");
            }
            _ => panic!("Expected string value"),
        }

        Ok(())
    }

    #[sinex_test]
    async fn test_redis_pool_isolation() -> Result<()> {
        let mut redis1 = acquire_test_redis().await?;
        let mut redis2 = acquire_test_redis().await?;

        // Set in first instance
        redis1.set("shared_key", "value1").await?;

        // Should not be visible in second instance
        let result: RedisResult<Option<String>> = redis::cmd("GET")
            .arg(redis2.key("shared_key"))
            .query(&mut redis2.conn);

        assert!(
            result.unwrap().is_none(),
            "Redis instances should be isolated"
        );

        Ok(())
    }

    #[sinex_test]
    async fn test_redis_streams() -> Result<()> {
        let mut redis = acquire_test_redis().await?;

        // Test stream operations
        let stream_id = redis
            .xadd(
                "test_stream",
                "*",
                &[("field1".to_string(), "value1".to_string())],
            )
            .await?;

        assert!(!stream_id.is_empty());

        let entries = redis
            .xread(&["test_stream"], &["0"], Some(10), None)
            .await?;

        // Check we got data back
        match entries {
            Value::Bulk(streams) => {
                assert!(!streams.is_empty(), "Should have stream data");
            }
            _ => panic!("Expected array of streams"),
        }

        Ok(())
    }

    #[sinex_test]
    async fn test_redis_pool_cleanup() -> Result<()> {
        let mut redis = acquire_test_redis().await?;
        let key = redis.key("cleanup_test");

        // Set a value
        redis::cmd("SET")
            .arg(&key)
            .arg("test_value")
            .query::<()>(&mut redis.conn)
            .map_err(|e| SinexError::service(format!("Redis error: {}", e)))?;

        // Verify it exists
        let exists: bool = redis::cmd("EXISTS")
            .arg(&key)
            .query(&mut redis.conn)
            .map_err(|e| SinexError::service(format!("Redis error: {}", e)))?;
        assert!(exists);

        // Drop the handle to trigger cleanup
        drop(redis);

        // Get a new instance - should be clean
        let mut redis2 = acquire_test_redis().await?;
        let key2 = redis2.key("cleanup_test");

        let exists2: bool = redis::cmd("EXISTS").arg(&key2).query(&mut redis2.conn)?;
        assert!(!exists2, "Key should be cleaned up");

        Ok(())
    }

    #[sinex_test]
    async fn test_redis_pool_concurrent_access() -> Result<()> {
        // Spawn multiple tasks that acquire Redis instances
        let handles: Vec<_> = (0..10)
            .map(|i| {
                tokio::spawn(async move {
                    let mut redis = acquire_test_redis().await?;

                    // Each task writes to its own key
                    let key = format!("task_{}", i);
                    redis.set(&key, &format!("value_{}", i)).await?;

                    // Verify write
                    let value: Value = redis.get(&key).await?;
                    match value {
                        Value::Data(data) => {
                            let string_value = String::from_utf8(data).unwrap();
                            assert_eq!(string_value, format!("value_{}", i));
                        }
                        _ => panic!("Expected string value"),
                    }

                    Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
                })
            })
            .collect();

        let results = futures::future::join_all(handles).await;

        // All tasks should succeed
        for (i, result) in results.iter().enumerate() {
            assert!(result.is_ok(), "Task {} failed: {:?}", i, result);
            assert!(result.as_ref().unwrap().is_ok());
        }

        Ok(())
    }

    #[sinex_test]
    async fn test_redis_pool_stats() -> Result<()> {
        // Get initial stats
        let initial_stats = get_redis_pool_stats().await;
        let initial_acquisitions = initial_stats
            .as_ref()
            .map(|s| s.total_acquisitions)
            .unwrap_or(0);

        // Acquire and release some instances
        for _ in 0..5 {
            let _redis = acquire_test_redis().await?;
            // Drops immediately
        }

        // Check stats updated
        let final_stats = get_redis_pool_stats().await.unwrap();
        assert_eq!(
            final_stats.total_acquisitions,
            initial_acquisitions + 5,
            "Acquisition count should increase"
        );

        Ok(())
    }

    #[sinex_test]
    async fn test_redis_key_namespacing() -> Result<()> {
        let mut redis = acquire_test_redis().await?;

        // Test that keys are properly namespaced
        let raw_key = "test_key";
        let namespaced_key = redis.key(raw_key);

        assert!(namespaced_key.starts_with("test_"));
        assert!(namespaced_key.contains(':'));
        assert!(namespaced_key.ends_with(raw_key));

        Ok(())
    }

    #[sinex_test]
    async fn test_redis_connection_reuse() -> Result<()> {
        let mut redis = acquire_test_redis().await?;

        // Multiple operations on same connection
        for i in 0..10 {
            redis
                .set(&format!("key_{}", i), &format!("value_{}", i))
                .await?;
        }

        // Verify all values
        for i in 0..10 {
            let value: Value = redis.get(&format!("key_{}", i)).await?;
            match value {
                Value::Data(data) => {
                    let string_value = String::from_utf8(data).unwrap();
                    assert_eq!(string_value, format!("value_{}", i));
                }
                _ => panic!("Expected string value"),
            }
        }

        Ok(())
    }
}
