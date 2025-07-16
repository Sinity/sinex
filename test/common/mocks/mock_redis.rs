//! Mock Redis implementation for testing
//!
//! Provides a controllable Redis substitute that can simulate:
//! - Connection failures
//! - Data loss scenarios
//! - Performance degradation
//! - Network partitions

use crate::common::prelude::*;
use redis::aio::MultiplexedConnection;
use redis::{AsyncCommands, RedisError, RedisResult};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};
use tokio::time::{Duration, Instant};

/// Configuration for MockRedis behavior
#[derive(Debug, Clone)]
pub struct MockRedisConfig {
    /// Probability of connection failure (0.0 to 1.0)
    pub connection_failure_rate: f64,
    /// Probability of command failure (0.0 to 1.0)
    pub command_failure_rate: f64,
    /// Simulated network latency
    pub network_latency: Duration,
    /// Maximum number of concurrent connections
    pub max_connections: usize,
    /// Memory limit in bytes
    pub memory_limit: usize,
    /// Whether to persist data across restarts
    pub persist_data: bool,
    /// Failure patterns to inject
    pub failure_patterns: Vec<super::FailurePattern>,
}

impl Default for MockRedisConfig {
    fn default() -> Self {
        Self {
            connection_failure_rate: 0.0,
            command_failure_rate: 0.0,
            network_latency: Duration::from_millis(1),
            max_connections: 100,
            memory_limit: 10 * 1024 * 1024, // 10MB
            persist_data: false,
            failure_patterns: Vec::new(),
        }
    }
}

/// Mock Redis implementation with failure simulation
pub struct MockRedis {
    config: MockRedisConfig,
    data: Arc<RwLock<HashMap<String, redis::Value>>>,
    streams: Arc<RwLock<HashMap<String, MockRedisStream>>>,
    connections: Arc<Mutex<usize>>,
    memory_usage: Arc<Mutex<usize>>,
    failure_injector: Arc<Mutex<super::failure_injector::FailureInjector>>,
    start_time: Instant,
}

#[derive(Debug, Clone)]
struct MockRedisStream {
    entries: Vec<MockStreamEntry>,
    consumer_groups: HashMap<String, MockConsumerGroup>,
    last_id: String,
}

#[derive(Debug, Clone)]
struct MockStreamEntry {
    id: String,
    fields: HashMap<String, String>,
    timestamp: Instant,
}

#[derive(Debug, Clone)]
struct MockConsumerGroup {
    name: String,
    consumers: HashMap<String, MockConsumer>,
    last_delivered_id: String,
    pending_entries: Vec<String>,
}

#[derive(Debug, Clone)]
struct MockConsumer {
    name: String,
    pending_count: usize,
    last_seen: Instant,
}

impl MockRedis {
    pub fn new(config: MockRedisConfig) -> Self {
        let failure_injector = super::failure_injector::FailureInjector::new(
            super::failure_injector::FailureConfig {
                patterns: config.failure_patterns.clone(),
                enabled: true,
            }
        );

        Self {
            config,
            data: Arc::new(RwLock::new(HashMap::new())),
            streams: Arc::new(RwLock::new(HashMap::new())),
            connections: Arc::new(Mutex::new(0)),
            memory_usage: Arc::new(Mutex::new(0)),
            failure_injector: Arc::new(Mutex::new(failure_injector)),
            start_time: Instant::now(),
        }
    }

    pub async fn connect(&self) -> RedisResult<MockRedisConnection> {
        // Simulate connection failure
        if self.should_fail_connection().await {
            return Err(RedisError::from((
                redis::ErrorKind::IoError,
                "Connection failed",
                "Mock connection failure".to_string(),
            )));
        }

        // Check connection limit
        let mut connections = self.connections.lock().await;
        if *connections >= self.config.max_connections {
            return Err(RedisError::from((
                redis::ErrorKind::IoError,
                "Too many connections",
                "Maximum connections exceeded".to_string(),
            )));
        }

        *connections += 1;
        drop(connections);

        // Simulate network latency
        tokio::time::sleep(self.config.network_latency).await;

        Ok(MockRedisConnection {
            redis: self.clone(),
            connected: true,
        })
    }

    async fn should_fail_connection(&self) -> bool {
        let mut injector = self.failure_injector.lock().await;
        injector.should_fail("connection").await
            || fastrand::f64() < self.config.connection_failure_rate
    }

    async fn should_fail_command(&self, command: &str) -> bool {
        let mut injector = self.failure_injector.lock().await;
        injector.should_fail(command).await
            || fastrand::f64() < self.config.command_failure_rate
    }

    pub async fn get_stats(&self) -> MockRedisStats {
        let data = self.data.read().await;
        let streams = self.streams.read().await;
        let connections = *self.connections.lock().await;
        let memory_usage = *self.memory_usage.lock().await;

        MockRedisStats {
            keys_count: data.len(),
            streams_count: streams.len(),
            connections_count: connections,
            memory_usage,
            uptime: self.start_time.elapsed(),
        }
    }

    pub async fn reset(&self) {
        let mut data = self.data.write().await;
        data.clear();
        let mut streams = self.streams.write().await;
        streams.clear();
        let mut connections = self.connections.lock().await;
        *connections = 0;
        let mut memory_usage = self.memory_usage.lock().await;
        *memory_usage = 0;
    }

    pub async fn inject_failure(&self, pattern: super::failure_injector::FailurePattern) {
        let mut injector = self.failure_injector.lock().await;
        injector.add_pattern(pattern).await;
    }

    /// Simulate a partition by blocking all commands
    pub async fn simulate_partition(&self, duration: Duration) {
        let partition_pattern = super::failure_injector::FailurePattern::Temporary {
            operation: "*".to_string(),
            failure_rate: 1.0,
            duration,
        };
        self.inject_failure(partition_pattern).await;
    }

    /// Simulate memory pressure by reducing available memory
    pub async fn simulate_memory_pressure(&self, percentage: f64) {
        let new_limit = (self.config.memory_limit as f64 * (1.0 - percentage)) as usize;
        // In a real implementation, this would update the memory limit
        // For now, we'll just record the pressure
        let mut memory_usage = self.memory_usage.lock().await;
        *memory_usage = new_limit;
    }

    /// Simulate slow network by increasing latency
    pub async fn simulate_slow_network(&self, multiplier: f64) {
        // In a real implementation, this would dynamically adjust latency
        // For now, this is a placeholder for the concept
    }
}

impl Clone for MockRedis {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            data: self.data.clone(),
            streams: self.streams.clone(),
            connections: self.connections.clone(),
            memory_usage: self.memory_usage.clone(),
            failure_injector: self.failure_injector.clone(),
            start_time: self.start_time,
        }
    }
}

/// Mock Redis connection
pub struct MockRedisConnection {
    redis: MockRedis,
    connected: bool,
}

impl MockRedisConnection {
    pub async fn xadd<K: AsRef<str>, ID: AsRef<str>, F: AsRef<str>, V: AsRef<str>>(
        &mut self,
        key: K,
        id: ID,
        fields: &[(F, V)],
    ) -> RedisResult<String> {
        if !self.connected {
            return Err(RedisError::from((
                redis::ErrorKind::IoError,
                "Not connected",
                "Connection closed".to_string(),
            )));
        }

        if self.redis.should_fail_command("xadd").await {
            return Err(RedisError::from((
                redis::ErrorKind::IoError,
                "Command failed",
                "Mock command failure".to_string(),
            )));
        }

        let key_str = key.as_ref();
        let id_str = id.as_ref();
        
        let mut streams = self.redis.streams.write().await;
        let stream = streams.entry(key_str.to_string()).or_insert_with(|| MockRedisStream {
            entries: Vec::new(),
            consumer_groups: HashMap::new(),
            last_id: "0-0".to_string(),
        });

        let actual_id = if id_str == "*" {
            // Generate time-based ID
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap();
            format!("{}-{}", now.as_millis(), stream.entries.len())
        } else {
            id_str.to_string()
        };

        let entry = MockStreamEntry {
            id: actual_id.clone(),
            fields: fields.iter().map(|(k, v)| (k.as_ref().to_string(), v.as_ref().to_string())).collect(),
            timestamp: Instant::now(),
        };

        stream.entries.push(entry);
        stream.last_id = actual_id.clone();

        // Update memory usage
        let mut memory_usage = self.redis.memory_usage.lock().await;
        *memory_usage += 100; // Rough estimate

        Ok(actual_id)
    }

    pub async fn xlen<K: AsRef<str>>(&mut self, key: K) -> RedisResult<usize> {
        if !self.connected {
            return Err(RedisError::from((
                redis::ErrorKind::IoError,
                "Not connected",
                "Connection closed".to_string(),
            )));
        }

        if self.redis.should_fail_command("xlen").await {
            return Err(RedisError::from((
                redis::ErrorKind::IoError,
                "Command failed",
                "Mock command failure".to_string(),
            )));
        }

        let streams = self.redis.streams.read().await;
        let len = streams.get(key.as_ref()).map(|s| s.entries.len()).unwrap_or(0);
        Ok(len)
    }

    pub async fn xgroup_create<K: AsRef<str>, G: AsRef<str>, ID: AsRef<str>>(
        &mut self,
        key: K,
        group: G,
        id: ID,
    ) -> RedisResult<String> {
        if !self.connected {
            return Err(RedisError::from((
                redis::ErrorKind::IoError,
                "Not connected",
                "Connection closed".to_string(),
            )));
        }

        if self.redis.should_fail_command("xgroup_create").await {
            return Err(RedisError::from((
                redis::ErrorKind::IoError,
                "Command failed",
                "Mock command failure".to_string(),
            )));
        }

        let key_str = key.as_ref();
        let group_str = group.as_ref();
        
        let mut streams = self.redis.streams.write().await;
        let stream = streams.entry(key_str.to_string()).or_insert_with(|| MockRedisStream {
            entries: Vec::new(),
            consumer_groups: HashMap::new(),
            last_id: "0-0".to_string(),
        });

        if stream.consumer_groups.contains_key(group_str) {
            return Err(RedisError::from((
                redis::ErrorKind::ResponseError,
                "Group already exists",
                "BUSYGROUP Consumer Group name already exists".to_string(),
            )));
        }

        let consumer_group = MockConsumerGroup {
            name: group_str.to_string(),
            consumers: HashMap::new(),
            last_delivered_id: id.as_ref().to_string(),
            pending_entries: Vec::new(),
        };

        stream.consumer_groups.insert(group_str.to_string(), consumer_group);

        Ok("OK".to_string())
    }

    pub async fn xreadgroup<K: AsRef<str>, G: AsRef<str>, C: AsRef<str>, ID: AsRef<str>>(
        &mut self,
        group: G,
        consumer: C,
        count: Option<usize>,
        block: bool,
        streams: &[(K, ID)],
    ) -> RedisResult<Vec<(String, Vec<(String, Vec<(String, String)>)>)>> {
        if !self.connected {
            return Err(RedisError::from((
                redis::ErrorKind::IoError,
                "Not connected",
                "Connection closed".to_string(),
            )));
        }

        if self.redis.should_fail_command("xreadgroup").await {
            return Err(RedisError::from((
                redis::ErrorKind::IoError,
                "Command failed",
                "Mock command failure".to_string(),
            )));
        }

        let group_str = group.as_ref();
        let consumer_str = consumer.as_ref();
        let max_count = count.unwrap_or(10);
        
        let mut redis_streams = self.redis.streams.write().await;
        let mut result = Vec::new();

        for (stream_key, start_id) in streams {
            let stream_key_str = stream_key.as_ref();
            let start_id_str = start_id.as_ref();
            
            if let Some(stream) = redis_streams.get_mut(stream_key_str) {
                if let Some(consumer_group) = stream.consumer_groups.get_mut(group_str) {
                    // Ensure consumer exists
                    consumer_group.consumers.entry(consumer_str.to_string()).or_insert_with(|| MockConsumer {
                        name: consumer_str.to_string(),
                        pending_count: 0,
                        last_seen: Instant::now(),
                    });

                    let mut stream_entries = Vec::new();
                    let mut delivered_count = 0;

                    for entry in &stream.entries {
                        if delivered_count >= max_count {
                            break;
                        }

                        // Simple ID comparison (in real Redis this would be more sophisticated)
                        if start_id_str == ">" || entry.id > consumer_group.last_delivered_id {
                            let fields: Vec<(String, String)> = entry.fields.iter()
                                .map(|(k, v)| (k.clone(), v.clone()))
                                .collect();
                            
                            stream_entries.push((entry.id.clone(), fields));
                            consumer_group.last_delivered_id = entry.id.clone();
                            delivered_count += 1;
                        }
                    }

                    if !stream_entries.is_empty() {
                        result.push((stream_key_str.to_string(), stream_entries));
                    }
                }
            }
        }

        Ok(result)
    }

    pub async fn xack<K: AsRef<str>, G: AsRef<str>, ID: AsRef<str>>(
        &mut self,
        key: K,
        group: G,
        ids: &[ID],
    ) -> RedisResult<usize> {
        if !self.connected {
            return Err(RedisError::from((
                redis::ErrorKind::IoError,
                "Not connected",
                "Connection closed".to_string(),
            )));
        }

        if self.redis.should_fail_command("xack").await {
            return Err(RedisError::from((
                redis::ErrorKind::IoError,
                "Command failed",
                "Mock command failure".to_string(),
            )));
        }

        // For simplicity, just return the count of IDs
        Ok(ids.len())
    }

    pub async fn del<K: AsRef<str>>(&mut self, keys: &[K]) -> RedisResult<usize> {
        if !self.connected {
            return Err(RedisError::from((
                redis::ErrorKind::IoError,
                "Not connected",
                "Connection closed".to_string(),
            )));
        }

        if self.redis.should_fail_command("del").await {
            return Err(RedisError::from((
                redis::ErrorKind::IoError,
                "Command failed",
                "Mock command failure".to_string(),
            )));
        }

        let mut deleted = 0;
        let mut data = self.redis.data.write().await;
        let mut streams = self.redis.streams.write().await;

        for key in keys {
            let key_str = key.as_ref();
            if data.remove(key_str).is_some() {
                deleted += 1;
            }
            if streams.remove(key_str).is_some() {
                deleted += 1;
            }
        }

        Ok(deleted)
    }

    pub async fn set<K: AsRef<str>, V: AsRef<str>>(&mut self, key: K, value: V) -> RedisResult<String> {
        if !self.connected {
            return Err(RedisError::from((
                redis::ErrorKind::IoError,
                "Not connected",
                "Connection closed".to_string(),
            )));
        }

        if self.redis.should_fail_command("set").await {
            return Err(RedisError::from((
                redis::ErrorKind::IoError,
                "Command failed",
                "Mock command failure".to_string(),
            )));
        }

        let mut data = self.redis.data.write().await;
        data.insert(key.as_ref().to_string(), redis::Value::Data(value.as_ref().as_bytes().to_vec()));

        Ok("OK".to_string())
    }

    pub async fn get<K: AsRef<str>>(&mut self, key: K) -> RedisResult<Option<String>> {
        if !self.connected {
            return Err(RedisError::from((
                redis::ErrorKind::IoError,
                "Not connected",
                "Connection closed".to_string(),
            )));
        }

        if self.redis.should_fail_command("get").await {
            return Err(RedisError::from((
                redis::ErrorKind::IoError,
                "Command failed",
                "Mock command failure".to_string(),
            )));
        }

        let data = self.redis.data.read().await;
        match data.get(key.as_ref()) {
            Some(redis::Value::Data(bytes)) => {
                Ok(Some(String::from_utf8_lossy(bytes).to_string()))
            }
            _ => Ok(None),
        }
    }

    pub async fn disconnect(&mut self) -> RedisResult<()> {
        if self.connected {
            self.connected = false;
            let mut connections = self.redis.connections.lock().await;
            *connections = connections.saturating_sub(1);
        }
        Ok(())
    }
}

impl Drop for MockRedisConnection {
    fn drop(&mut self) {
        if self.connected {
            // Note: This is a synchronous drop, in real code we'd need to handle this differently
            self.connected = false;
        }
    }
}

/// Statistics for MockRedis
#[derive(Debug, Clone)]
pub struct MockRedisStats {
    pub keys_count: usize,
    pub streams_count: usize,
    pub connections_count: usize,
    pub memory_usage: usize,
    pub uptime: Duration,
}

/// Test utilities for MockRedis
impl MockRedis {
    pub fn for_testing() -> Self {
        Self::new(MockRedisConfig::default())
    }

    pub fn with_failures(failure_rate: f64) -> Self {
        let config = MockRedisConfig {
            connection_failure_rate: failure_rate,
            command_failure_rate: failure_rate,
            ..Default::default()
        };
        Self::new(config)
    }

    pub fn with_latency(latency: Duration) -> Self {
        let config = MockRedisConfig {
            network_latency: latency,
            ..Default::default()
        };
        Self::new(config)
    }

    pub async fn verify_streams(&self, expected_streams: &[&str]) -> bool {
        let streams = self.streams.read().await;
        expected_streams.iter().all(|key| streams.contains_key(*key))
    }

    pub async fn get_stream_length(&self, key: &str) -> usize {
        let streams = self.streams.read().await;
        streams.get(key).map(|s| s.entries.len()).unwrap_or(0)
    }

    pub async fn get_consumer_groups(&self, stream_key: &str) -> Vec<String> {
        let streams = self.streams.read().await;
        streams.get(stream_key)
            .map(|s| s.consumer_groups.keys().cloned().collect())
            .unwrap_or_default()
    }
}