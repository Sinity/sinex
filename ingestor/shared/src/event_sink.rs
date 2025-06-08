use anyhow::Result;
use async_trait::async_trait;
use chrono::Utc;
use sinex_db::models::RawEvent;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{debug, info, warn};

use crate::DatabaseService;

/// Trait for handling event output - allows different backends for testing and production
#[async_trait]
pub trait EventSink: Send + Sync {
    /// Send a single event
    async fn send_event(&self, event: &RawEvent) -> Result<()>;
    
    /// Send multiple events as a batch
    async fn send_batch(&self, events: &[RawEvent]) -> Result<()> {
        for event in events {
            self.send_event(event).await?;
        }
        Ok(())
    }
    
    /// Flush any buffered events
    async fn flush(&self) -> Result<()> {
        Ok(())
    }
}

/// Database sink - writes events to PostgreSQL (production use)
pub struct DatabaseSink {
    db: Arc<DatabaseService>,
}

impl DatabaseSink {
    pub fn new(db: Arc<DatabaseService>) -> Self {
        Self { db }
    }
    
    /// Get the underlying database service (temporary method for migration)
    pub fn db(&self) -> &Arc<DatabaseService> {
        &self.db
    }
}

#[async_trait]
impl EventSink for DatabaseSink {
    async fn send_event(&self, event: &RawEvent) -> Result<()> {
        debug!(
            event_type = %event.event_type,
            source = %event.source,
            "Inserting event into database"
        );
        self.db.insert_event(event).await?;
        Ok(())
    }
}

/// Log sink - logs events as JSON (for dry-run and debugging)
pub struct LogSink {
    prefix: String,
}

impl LogSink {
    pub fn new(prefix: impl Into<String>) -> Self {
        Self {
            prefix: prefix.into(),
        }
    }
}

#[async_trait]
impl EventSink for LogSink {
    async fn send_event(&self, event: &RawEvent) -> Result<()> {
        info!(
            target: "event_sink",
            prefix = %self.prefix,
            event_type = %event.event_type,
            source = %event.source,
            host = %event.host,
            payload = %serde_json::to_string(&event.payload)?,
            "[DRY-RUN] Event generated"
        );
        
        // Also print to stdout for easy piping
        println!(
            "{} | {} | {} | {}",
            Utc::now().format("%Y-%m-%d %H:%M:%S"),
            event.source,
            event.event_type,
            serde_json::to_string(&event.payload)?
        );
        
        Ok(())
    }
}

/// File sink - writes events to a file (for testing and debugging)
pub struct FileSink {
    file_path: PathBuf,
    writer: Arc<Mutex<tokio::fs::File>>,
}

impl FileSink {
    pub async fn new(file_path: PathBuf) -> Result<Self> {
        use tokio::fs::OpenOptions;
        
        let writer = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&file_path)
            .await?;
            
        Ok(Self {
            file_path,
            writer: Arc::new(Mutex::new(writer)),
        })
    }
}

#[async_trait]
impl EventSink for FileSink {
    async fn send_event(&self, event: &RawEvent) -> Result<()> {
        use tokio::io::AsyncWriteExt;
        
        let json = serde_json::to_string(event)?;
        let mut writer = self.writer.lock().await;
        writer.write_all(json.as_bytes()).await?;
        writer.write_all(b"\n").await?;
        writer.flush().await?;
        
        debug!(
            file = %self.file_path.display(),
            event_type = %event.event_type,
            "Wrote event to file"
        );
        
        Ok(())
    }
}

/// Memory sink - stores events in memory (for unit tests)
#[derive(Default)]
pub struct MemorySink {
    events: Arc<Mutex<Vec<RawEvent>>>,
}

impl MemorySink {
    pub fn new() -> Self {
        Self::default()
    }
    
    pub async fn get_events(&self) -> Vec<RawEvent> {
        self.events.lock().await.clone()
    }
    
    pub async fn clear(&self) {
        self.events.lock().await.clear();
    }
    
    pub async fn event_count(&self) -> usize {
        self.events.lock().await.len()
    }
}

#[async_trait]
impl EventSink for MemorySink {
    async fn send_event(&self, event: &RawEvent) -> Result<()> {
        self.events.lock().await.push(event.clone());
        let count = self.events.lock().await.len();
        debug!(
            event_type = %event.event_type,
            total_events = count,
            "Stored event in memory"
        );
        Ok(())
    }
}

/// Multi sink - sends events to multiple sinks (useful for logging + database)
pub struct MultiSink {
    sinks: Vec<Box<dyn EventSink>>,
}

impl MultiSink {
    pub fn new(sinks: Vec<Box<dyn EventSink>>) -> Self {
        Self { sinks }
    }
}

#[async_trait]
impl EventSink for MultiSink {
    async fn send_event(&self, event: &RawEvent) -> Result<()> {
        let mut errors = Vec::new();
        
        for sink in &self.sinks {
            if let Err(e) = sink.send_event(event).await {
                warn!(
                    error = %e,
                    "Failed to send event to one of the sinks"
                );
                errors.push(e);
            }
        }
        
        if !errors.is_empty() {
            return Err(anyhow::anyhow!(
                "Failed to send event to {} sink(s)", 
                errors.len()
            ));
        }
        
        Ok(())
    }
    
    async fn flush(&self) -> Result<()> {
        for sink in &self.sinks {
            sink.flush().await?;
        }
        Ok(())
    }
}

/// Blanket implementation for Arc<T> where T: EventSink
#[async_trait]
impl<T: EventSink + ?Sized> EventSink for Arc<T> {
    async fn send_event(&self, event: &RawEvent) -> Result<()> {
        (**self).send_event(event).await
    }
    
    async fn send_batch(&self, events: &[RawEvent]) -> Result<()> {
        (**self).send_batch(events).await
    }
    
    async fn flush(&self) -> Result<()> {
        (**self).flush().await
    }
}