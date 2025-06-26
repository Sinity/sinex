use anyhow::Result;
use chrono::{DateTime, Utc};
use serde::Serialize;
use serde_json::{json, Value as JsonValue};
use sinex_core::EventSender;
use sinex_db::models::RawEvent;
use sinex_ulid::Ulid;
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use sysinfo::System;
use tokio::sync::RwLock;
use tracing::{info, warn};

/// Single second of metric data
#[derive(Debug, Clone, Serialize)]
pub struct SecondMetrics {
    pub timestamp: Timestamp,
    pub cpu_percent: f32,
    pub memory_mb: u64,
    pub events_count: u64,
    pub errors_count: u64,
    pub queue_depth: usize,
    pub active_sources: usize,
    pub db_pool_size: u32,
    pub db_pool_idle: u32,
}

/// Ring buffer storing high-resolution metrics
pub struct MetricsRingBuffer {
    /// Fixed-size buffer of per-second metrics
    buffer: VecDeque<SecondMetrics>,
    /// Maximum entries to keep (5 minutes = 300 seconds)
    max_size: usize,
    /// Last time we emitted metrics
    last_emit: Instant,
    /// Emit interval (normally 30 seconds)
    emit_interval: Duration,
}

impl MetricsRingBuffer {
    pub fn new() -> Self {
        Self {
            buffer: VecDeque::with_capacity(300),
            max_size: 300,
            last_emit: Instant::now(),
            emit_interval: Duration::from_secs(30),
        }
    }
    
    /// Add a new second of metrics
    pub fn push(&mut self, metrics: SecondMetrics) {
        // Remove oldest if at capacity
        if self.buffer.len() >= self.max_size {
            self.buffer.pop_front();
        }
        self.buffer.push_back(metrics);
    }
    
    /// Check if it's time to emit metrics
    pub fn should_emit(&self) -> bool {
        self.last_emit.elapsed() >= self.emit_interval
    }
    
    /// Get all metrics and reset emit timer
    pub fn take_metrics(&mut self) -> Vec<SecondMetrics> {
        self.last_emit = Instant::now();
        self.buffer.iter().cloned().collect()
    }
    
    /// Adjust emit interval for adaptive sampling
    pub fn set_emit_interval(&mut self, interval: Duration) {
        self.emit_interval = interval;
    }
}

/// Tracks metrics for a specific event source
#[derive(Debug, Clone, Serialize)]
pub struct SourceMetrics {
    pub events_total: u64,
    pub errors_total: u64,
    pub bytes_processed: u64,
    pub last_event_time: OptionalTimestamp,
    #[serde(flatten)]
    pub custom: HashMap<String, JsonValue>,
}

/// Main metrics collector for the unified collector
pub struct CollectorMetrics {
    /// System info provider
    system: Arc<RwLock<System>>,
    
    /// Process start time
    start_time: Instant,
    
    /// Ring buffer of high-res metrics
    ring_buffer: Arc<RwLock<MetricsRingBuffer>>,
    
    /// Atomic counters for lock-free updates
    events_processed: Arc<AtomicU64>,
    errors_total: Arc<AtomicU64>,
    
    /// Per-source metrics
    source_metrics: Arc<RwLock<HashMap<String, SourceMetrics>>>,
    
    /// Current state for adaptive sampling
    adaptive_state: Arc<RwLock<AdaptiveState>>,
}

/// State for adaptive sampling decisions
#[derive(Debug)]
struct AdaptiveState {
    high_load: bool,
    recent_errors: bool,
    cpu_spike: bool,
}

impl CollectorMetrics {
    pub fn new() -> Self {
        Self {
            system: Arc::new(RwLock::new(System::new_all())),
            start_time: Instant::now(),
            ring_buffer: Arc::new(RwLock::new(MetricsRingBuffer::new())),
            events_processed: Arc::new(AtomicU64::new(0)),
            errors_total: Arc::new(AtomicU64::new(0)),
            source_metrics: Arc::new(RwLock::new(HashMap::new())),
            adaptive_state: Arc::new(RwLock::new(AdaptiveState {
                high_load: false,
                recent_errors: false,
                cpu_spike: false,
            })),
        }
    }
    
    /// Start the metrics collection loop
    pub async fn start(
        self: Arc<Self>,
        event_tx: EventSender,
        db_pool: Option<sqlx::PgPool>,
    ) {
        info!("Starting high-resolution metrics collection");
        
        // Spawn 1-second sampling task
        let sampler = self.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(1));
            loop {
                interval.tick().await;
                if let Err(e) = sampler.sample_metrics(db_pool.as_ref()).await {
                    warn!("Failed to sample metrics: {}", e);
                }
            }
        });
        
        // Spawn emission task
        let emitter = self.clone();
        tokio::spawn(async move {
            let mut check_interval = tokio::time::interval(Duration::from_secs(5));
            loop {
                check_interval.tick().await;
                
                // Check if we should emit
                let should_emit = {
                    let buffer = emitter.ring_buffer.read().await;
                    buffer.should_emit()
                };
                
                if should_emit {
                    if let Ok(event) = emitter.create_metrics_event().await {
                        if let Err(e) = event_tx.send(event).await {
                            warn!("Failed to send metrics event: {}", e);
                        }
                    }
                    
                    // Check adaptive sampling
                    emitter.update_adaptive_sampling().await;
                }
            }
        });
    }
    
    /// Sample current metrics (called every second)
    async fn sample_metrics(&self, db_pool: Option<&sqlx::PgPool>) -> Result<()> {
        // Update system info
        {
            let mut system = self.system.write().await;
            system.refresh_processes();
            system.refresh_memory();
            system.refresh_cpu_usage();
        }
        
        // Get current process info
        let (cpu_percent, memory_mb) = {
            let system = self.system.read().await;
            let pid = std::process::id() as usize;
            
            let process = system.process(sysinfo::Pid::from(pid))
                .ok_or_else(|| anyhow::anyhow!("Process not found"))?;
            
            let cpu = process.cpu_usage();
            let memory = process.memory() / 1024; // KB to MB
            
            (cpu, memory)
        };
        
        // Get database pool stats
        let (db_pool_size, db_pool_idle) = if let Some(pool) = db_pool {
            (pool.size(), pool.num_idle() as u32)
        } else {
            (0, 0)
        };
        
        // Get source count
        let active_sources = {
            let sources = self.source_metrics.read().await;
            sources.len()
        };
        
        // Create metrics snapshot
        let metrics = SecondMetrics {
            timestamp: Utc::now(),
            cpu_percent,
            memory_mb,
            events_count: self.events_processed.load(Ordering::Relaxed),
            errors_count: self.errors_total.load(Ordering::Relaxed),
            queue_depth: 0, // TODO: Get from actual queue
            active_sources,
            db_pool_size,
            db_pool_idle,
        };
        
        // Add to ring buffer
        {
            let mut buffer = self.ring_buffer.write().await;
            buffer.push(metrics);
        }
        
        Ok(())
    }
    
    /// Create a metrics event from collected data
    async fn create_metrics_event(&self) -> Result<RawEvent> {
        let timeseries = {
            let mut buffer = self.ring_buffer.write().await;
            buffer.take_metrics()
        };
        
        let source_metrics = {
            let sources = self.source_metrics.read().await;
            sources.clone()
        };
        
        let uptime_seconds = self.start_time.elapsed().as_secs();
        
        // Calculate aggregates over the timeseries
        let (avg_cpu, max_memory, events_per_second) = if !timeseries.is_empty() {
            let sum_cpu: f32 = timeseries.iter().map(|m| m.cpu_percent).sum();
            let avg_cpu = sum_cpu / timeseries.len() as f32;
            
            let max_memory = timeseries.iter()
                .map(|m| m.memory_mb)
                .max()
                .unwrap_or(0);
            
            let events_start = timeseries.first().map(|m| m.events_count).unwrap_or(0);
            let events_end = timeseries.last().map(|m| m.events_count).unwrap_or(0);
            let duration = timeseries.len() as f64;
            let events_per_second = if duration > 0.0 {
                (events_end - events_start) as f64 / duration
            } else {
                0.0
            };
            
            (avg_cpu, max_memory, events_per_second)
        } else {
            (0.0, 0, 0.0)
        };
        
        // Get adaptive state for inclusion in metrics
        let adaptive_state = {
            let state = self.adaptive_state.read().await;
            json!({
                "high_load": state.high_load,
                "recent_errors": state.recent_errors,
                "cpu_spike": state.cpu_spike,
            })
        };
        
        let event = RawEvent {
            id: Ulid::new(),
            source: "sinex.metrics.collector".to_string(),
            event_type: "metrics_timeseries".to_string(),
            ts_ingest: Utc::now(),
            ts_orig: None,
            host: gethostname::gethostname().to_string_lossy().to_string(),
            ingestor_version: Some(env!("CARGO_PKG_VERSION").to_string()),
            payload_schema_id: None,
            payload: json!({
                "interval_seconds": 30,
                "uptime_seconds": uptime_seconds,
                
                // Aggregated metrics for quick queries
                "summary": {
                    "avg_cpu_percent": avg_cpu,
                    "max_memory_mb": max_memory,
                    "events_per_second": events_per_second,
                    "total_events": self.events_processed.load(Ordering::Relaxed),
                    "total_errors": self.errors_total.load(Ordering::Relaxed),
                },
                
                // Per-source breakdown
                "sources": source_metrics,
                
                // Full resolution timeseries (1-second granularity)
                "timeseries": {
                    "resolution_seconds": 1,
                    "datapoints": timeseries.iter().map(|m| {
                        json!({
                            "ts": m.timestamp.to_rfc3339(),
                            "cpu": m.cpu_percent,
                            "mem": m.memory_mb,
                            "events": m.events_count,
                            "errors": m.errors_count,
                            "queue": m.queue_depth,
                            "sources": m.active_sources,
                            "db_pool": m.db_pool_size,
                            "db_idle": m.db_pool_idle,
                        })
                    }).collect::<Vec<_>>(),
                },
                
                // Context for debugging
                "context": {
                    "version": env!("CARGO_PKG_VERSION"),
                    "rustc_version": option_env!("RUSTC_VERSION").unwrap_or("unknown"),
                    "adaptive_sampling": adaptive_state
                }
            }),
        };
        
        Ok(event)
    }
    
    /// Update adaptive sampling based on current conditions
    async fn update_adaptive_sampling(&self) {
        let recent_metrics = {
            let buffer = self.ring_buffer.read().await;
            buffer.buffer.iter().rev().take(10).cloned().collect::<Vec<_>>()
        };
        
        if recent_metrics.is_empty() {
            return;
        }
        
        // Check for high load (>1000 events/sec)
        let high_load = recent_metrics.iter().any(|m| {
            m.events_count > 1000
        });
        
        // Check for recent errors
        let recent_errors = recent_metrics.iter().any(|m| m.errors_count > 0);
        
        // Check for CPU spikes (>80%)
        let cpu_spike = recent_metrics.iter().any(|m| m.cpu_percent > 80.0);
        
        // Update state
        {
            let mut state = self.adaptive_state.write().await;
            state.high_load = high_load;
            state.recent_errors = recent_errors;
            state.cpu_spike = cpu_spike;
        }
        
        // Adjust emit interval based on conditions
        let new_interval = if recent_errors || cpu_spike {
            Duration::from_secs(5)  // 5 seconds during incidents
        } else if high_load {
            Duration::from_secs(10) // 10 seconds during high load
        } else {
            Duration::from_secs(30) // 30 seconds normal
        };
        
        {
            let mut buffer = self.ring_buffer.write().await;
            if buffer.emit_interval != new_interval {
                info!("Adjusting metrics interval to {:?}", new_interval);
                buffer.set_emit_interval(new_interval);
            }
        }
    }
    
    /// Increment event counter
    pub fn record_event(&self, _source: &str) {
        self.events_processed.fetch_add(1, Ordering::Relaxed);
    }
    
    /// Increment error counter
    pub fn record_error(&self, _source: &str) {
        self.errors_total.fetch_add(1, Ordering::Relaxed);
    }
    
    /// Update source-specific metrics
    pub async fn update_source_metrics(
        &self,
        source: &str,
        update: impl FnOnce(&mut SourceMetrics),
    ) {
        let mut sources = self.source_metrics.write().await;
        let metrics = sources.entry(source.to_string())
            .or_insert_with(|| SourceMetrics {
                events_total: 0,
                errors_total: 0,
                bytes_processed: 0,
                last_event_time: None,
                custom: HashMap::new(),
            });
        update(metrics);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_ring_buffer() {
        let mut buffer = MetricsRingBuffer::new();
        
        // Add some metrics
        for i in 0..5 {
            let metrics = SecondMetrics {
                timestamp: Utc::now(),
                cpu_percent: i as f32,
                memory_mb: 100 + i,
                events_count: i * 10,
                errors_count: 0,
                queue_depth: 0,
                active_sources: 1,
                db_pool_size: 10,
                db_pool_idle: 5,
            };
            buffer.push(metrics);
        }
        
        assert_eq!(buffer.buffer.len(), 5);
        
        // Check emit timing
        assert!(!buffer.should_emit()); // Too soon
        
        // Take metrics
        let metrics = buffer.take_metrics();
        assert_eq!(metrics.len(), 5);
        assert_eq!(metrics[0].cpu_percent, 0.0);
        assert_eq!(metrics[4].cpu_percent, 4.0);
    }
    
    #[tokio::test]
    async fn test_adaptive_sampling() {
        let metrics = Arc::new(CollectorMetrics::new());
        
        // Simulate high CPU
        {
            let mut buffer = metrics.ring_buffer.write().await;
            for _ in 0..5 {
                buffer.push(SecondMetrics {
                    timestamp: Utc::now(),
                    cpu_percent: 85.0, // High CPU
                    memory_mb: 512,
                    events_count: 100,
                    errors_count: 0,
                    queue_depth: 0,
                    active_sources: 1,
                    db_pool_size: 10,
                    db_pool_idle: 5,
                });
            }
        }
        
        // Update adaptive sampling
        metrics.update_adaptive_sampling().await;
        
        // Check that interval was reduced
        let interval = {
            let buffer = metrics.ring_buffer.read().await;
            buffer.emit_interval
        };
        
        assert_eq!(interval, Duration::from_secs(5)); // Should be in incident mode
    }
}