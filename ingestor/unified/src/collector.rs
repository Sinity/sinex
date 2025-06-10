use anyhow::Result;
use async_trait::async_trait;
use chrono::Utc;
use serde::Serialize;
use sinex_db::models::RawEvent;
use sinex_shared::{event_types::RawEventBuilder, EventSink, SimpleIngestor};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time;
use tracing::{debug, error, warn};

use crate::config::UnifiedConfig;

/// The unified collector implementation
pub struct UnifiedCollector {
    config: UnifiedConfig,
}

impl UnifiedCollector {
    pub fn new(config: UnifiedConfig) -> Self {
        Self { config }
    }
    
    /// Collect system events
    async fn collect_system_events(&self, tx: &mpsc::Sender<RawEvent>) -> Result<()> {
        // Example: collect CPU usage
        let cpu_usage = self.get_cpu_usage()?;
        let event = RawEventBuilder::new(
            "system",
            "cpu.usage",
            serde_json::json!({
                "usage_percent": cpu_usage,
                "timestamp": Utc::now(),
            }),
        )
        .build();
        
        tx.send(event).await?;
        
        // Example: collect memory usage
        let memory_info = self.get_memory_info()?;
        let event = RawEventBuilder::new(
            "system",
            "memory.usage",
            memory_info,
        )
        .build();
        
        tx.send(event).await?;
        
        Ok(())
    }
    
    /// Collect network events
    async fn collect_network_events(&self, tx: &mpsc::Sender<RawEvent>) -> Result<()> {
        // Example: collect network interface stats
        let interfaces = self.get_network_interfaces()?;
        
        for (name, stats) in interfaces {
            let event = RawEventBuilder::new(
                "network",
                "interface.stats",
                serde_json::json!({
                    "interface": name,
                    "rx_bytes": stats.rx_bytes,
                    "tx_bytes": stats.tx_bytes,
                    "rx_packets": stats.rx_packets,
                    "tx_packets": stats.tx_packets,
                }),
            )
            .build();
            
            tx.send(event).await?;
        }
        
        Ok(())
    }
    
    /// Collect process events
    async fn collect_process_events(&self, tx: &mpsc::Sender<RawEvent>) -> Result<()> {
        // Example: collect running processes
        let processes = self.get_running_processes()?;
        
        let event = RawEventBuilder::new(
            "process",
            "snapshot",
            serde_json::json!({
                "process_count": processes.len(),
                "processes": processes,
                "timestamp": Utc::now(),
            }),
        )
        .build();
        
        tx.send(event).await?;
        
        Ok(())
    }
    
    // Mock implementations for demonstration
    fn get_cpu_usage(&self) -> Result<f64> {
        // In a real implementation, this would read from /proc/stat or use a system library
        Ok(23.5)
    }
    
    fn get_memory_info(&self) -> Result<serde_json::Value> {
        // In a real implementation, this would read from /proc/meminfo
        Ok(serde_json::json!({
            "total": 16_000_000_000u64,
            "used": 8_000_000_000u64,
            "free": 8_000_000_000u64,
            "cached": 4_000_000_000u64,
        }))
    }
    
    fn get_network_interfaces(&self) -> Result<HashMap<String, NetworkStats>> {
        // In a real implementation, this would read from /proc/net/dev
        let mut interfaces = HashMap::new();
        interfaces.insert("eth0".to_string(), NetworkStats {
            rx_bytes: 1_000_000,
            tx_bytes: 500_000,
            rx_packets: 1000,
            tx_packets: 500,
        });
        Ok(interfaces)
    }
    
    fn get_running_processes(&self) -> Result<Vec<ProcessInfo>> {
        // In a real implementation, this would read from /proc/*/stat
        Ok(vec![
            ProcessInfo {
                pid: 1,
                name: "systemd".to_string(),
                cpu_percent: 0.1,
                memory_bytes: 50_000_000,
            },
            ProcessInfo {
                pid: 1234,
                name: "firefox".to_string(),
                cpu_percent: 5.2,
                memory_bytes: 2_000_000_000,
            },
        ])
    }
}

#[derive(Debug)]
struct NetworkStats {
    rx_bytes: u64,
    tx_bytes: u64,
    rx_packets: u64,
    tx_packets: u64,
}

#[derive(Debug, Serialize)]
struct ProcessInfo {
    pid: u32,
    name: String,
    cpu_percent: f64,
    memory_bytes: u64,
}

#[async_trait]
impl SimpleIngestor for UnifiedCollector {
    fn name() -> &'static str {
        "unified-collector"
    }
    
    fn version() -> &'static str {
        env!("CARGO_PKG_VERSION")
    }
    
    async fn capture_events(&mut self, event_tx: mpsc::Sender<RawEvent>) -> Result<()> {
        let mut interval = time::interval(Duration::from_secs(self.config.collection.poll_interval_secs));
        
        loop {
            interval.tick().await;
            
            // Collect from enabled sources
            for source in &self.config.collection.enabled_sources {
                match source.as_str() {
                    "system" => {
                        if let Err(e) = self.collect_system_events(&event_tx).await {
                            warn!("Failed to collect system events: {}", e);
                        }
                    }
                    "network" => {
                        if let Err(e) = self.collect_network_events(&event_tx).await {
                            warn!("Failed to collect network events: {}", e);
                        }
                    }
                    "process" => {
                        if let Err(e) = self.collect_process_events(&event_tx).await {
                            warn!("Failed to collect process events: {}", e);
                        }
                    }
                    _ => {
                        debug!("Unknown source: {}", source);
                    }
                }
            }
        }
    }
}

/// The unified collector as a full Ingestor (for backward compatibility)
pub struct UnifiedIngestor {}

#[async_trait]
impl sinex_shared::ingestor_framework::Ingestor for UnifiedIngestor {
    type Config = UnifiedConfig;
    type Commands = sinex_shared::ingestor_framework::CommonCommands;
    
    fn name() -> &'static str {
        "unified-collector"
    }
    
    fn description() -> &'static str {
        "Unified system event collector"
    }
    
    fn produces_events() -> HashMap<String, Vec<String>> {
        let mut produces = HashMap::new();
        
        produces.insert(
            "system".to_string(),
            vec!["cpu.usage", "memory.usage"]
                .into_iter()
                .map(String::from)
                .collect(),
        );
        
        produces.insert(
            "network".to_string(),
            vec!["interface.stats"]
                .into_iter()
                .map(String::from)
                .collect(),
        );
        
        produces.insert(
            "process".to_string(),
            vec!["snapshot"]
                .into_iter()
                .map(String::from)
                .collect(),
        );
        
        produces
    }
    
    async fn new(config: Self::Config, event_sink: Arc<dyn EventSink>) -> Result<Self> {
        // Create the simple ingestor
        let collector = UnifiedCollector::new(config.clone());
        
        // Create runtime config
        let runtime_config = sinex_shared::RuntimeConfig {
            heartbeat_interval_secs: config.collection.heartbeat_interval_secs,
            batch_size: Some(config.collection.batch_size),
            batch_timeout_ms: Some(config.collection.batch_timeout_ms),
            ..Default::default()
        };
        
        // Create and run the runtime in the background
        let runtime = sinex_shared::IngestorRuntime::new(collector, event_sink, runtime_config)?;
        
        // Spawn the runtime task
        tokio::spawn(async move {
            if let Err(e) = runtime.run().await {
                error!("Unified collector runtime failed: {}", e);
            }
        });
        
        Ok(Self {})
    }
    
    async fn run(&mut self) -> Result<()> {
        // The runtime is already running in the background
        // Just wait indefinitely
        loop {
            tokio::time::sleep(Duration::from_secs(60)).await;
        }
    }
}