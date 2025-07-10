/// Typed collector module - demonstrates migration from JsonValue to strongly typed events
/// 
/// This module shows how the collector can handle both old JsonValue-based events
/// and new strongly-typed events during the migration period.
use sinex_core::{
    EventSender, EventReceiver,
    RawEvent,
};
use sinex_events::{
    TypedEventSender, TypedEventReceiver, EventEnvelope, typed_event_channel,
};
use tokio::sync::mpsc;
use tracing::{info, error};

/// Collector that can handle both typed and untyped events
pub struct HybridCollector {
    /// Channel for legacy JsonValue-based events
    json_rx: EventReceiver,
    /// Channel for new strongly-typed events
    typed_rx: TypedEventReceiver,
    /// Output channel (always sends RawEvent to database)
    output_tx: EventSender,
}

impl HybridCollector {
    pub fn new(json_rx: EventReceiver, typed_rx: TypedEventReceiver, output_tx: EventSender) -> Self {
        Self {
            json_rx,
            typed_rx,
            output_tx,
        }
    }
    
    /// Run the hybrid collector, processing both event types
    pub async fn run(mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        info!("Starting hybrid collector with typed and untyped event support");
        
        loop {
            tokio::select! {
                // Handle legacy JsonValue events
                Some(json_event) = self.json_rx.recv() => {
                    if let Err(e) = self.output_tx.send(json_event).await {
                        error!("Failed to send JSON event: {}", e);
                        break;
                    }
                }
                
                // Handle new typed events
                Some(typed_event) = self.typed_rx.recv() => {
                    // Convert to RawEvent for database storage
                    let raw_event = typed_event.to_json_event();
                    
                    // Log event type for monitoring
                    info!("Processing typed event: {} from {}", 
                        raw_event.event_type, 
                        raw_event.source
                    );
                    
                    if let Err(e) = self.output_tx.send(raw_event).await {
                        error!("Failed to send typed event: {}", e);
                        break;
                    }
                }
                
                // Both channels closed
                else => {
                    info!("All event channels closed, shutting down collector");
                    break;
                }
            }
        }
        
        Ok(())
    }
}

/// Adapter that converts a JsonValue event channel to typed events
/// This allows gradual migration of event sources
pub struct JsonToTypedAdapter {
    json_rx: EventReceiver,
    typed_tx: TypedEventSender,
}

impl JsonToTypedAdapter {
    pub fn new(json_rx: EventReceiver, typed_tx: TypedEventSender) -> Self {
        Self { json_rx, typed_tx }
    }
    
    /// Run the adapter, converting JSON events to typed when possible
    pub async fn run(mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        while let Some(json_event) = self.json_rx.recv().await {
            // Try to convert known event types to typed versions
            match self.convert_to_typed(json_event.clone()) {
                Some(typed_event) => {
                    if let Err(e) = self.typed_tx.send(typed_event) {
                        error!("Failed to send typed event: {}", e);
                        break;
                    }
                }
                None => {
                    // Unknown event type, wrap in Unknown variant
                    let unknown = EventEnvelope::Unknown(json_event);
                    if let Err(e) = self.typed_tx.send(unknown) {
                        error!("Failed to send unknown event: {}", e);
                        break;
                    }
                }
            }
        }
        
        Ok(())
    }
    
    /// Attempt to convert a JSON event to a typed event
    fn convert_to_typed(&self, _event: RawEvent) -> Option<EventEnvelope> {
        // This would contain logic to parse known event types
        // For now, return None to demonstrate the Unknown path
        None
    }
}

/// Example of how to set up the migration
pub async fn setup_hybrid_collection() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Create channels
    let (_json_tx, json_rx) = mpsc::channel::<RawEvent>(1000);
    let (_typed_tx, typed_rx) = typed_event_channel();
    let (output_tx, mut output_rx) = mpsc::channel::<RawEvent>(2000);
    
    // Create hybrid collector
    let collector = HybridCollector::new(json_rx, typed_rx, output_tx);
    
    // Spawn collector task
    let collector_handle = tokio::spawn(collector.run());
    
    // Spawn output processor (would write to database)
    let output_handle = tokio::spawn(async move {
        while let Some(event) = output_rx.recv().await {
            // This is where events would be written to the database
            info!("Would write event to database: {} - {}", event.source, event.event_type);
        }
    });
    
    // Example: Legacy event sources send to json_tx
    // Example: New event sources send to typed_tx
    
    // Wait for tasks
    collector_handle.await??;
    output_handle.await?;
    
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinex_core::{
        RawEventBuilder,
    };
    use sinex_events::{
        TypedEventBuilder, FileCreatedPayload,
    };
    
    #[tokio::test]
    async fn test_hybrid_collector() {
        // Create channels
        let (json_tx, json_rx) = mpsc::channel(10);
        let (typed_tx, typed_rx) = typed_event_channel();
        let (output_tx, mut output_rx) = mpsc::channel(20);
        
        // Create and spawn collector
        let collector = HybridCollector::new(json_rx, typed_rx, output_tx);
        let handle = tokio::spawn(collector.run());
        
        // Send a JSON event
        let json_event = RawEventBuilder::new(
            "test",
            "test.event", 
            serde_json::json!({"data": "test"})
        ).build();
        json_tx.send(json_event.clone()).await.unwrap();
        
        // Send a typed event
        let typed_payload = FileCreatedPayload {
            path: "/test.txt".to_string(),
            size: 1024,
            created_at: chrono::Utc::now(),
            permissions: Some(0o644),
        };
        let typed_event = TypedEventBuilder::new("fs", "file.created", typed_payload).build();
        typed_tx.send(EventEnvelope::FileCreated(typed_event)).unwrap();
        
        // Close input channels
        drop(json_tx);
        drop(typed_tx);
        
        // Collect output events
        let mut events = Vec::new();
        while let Ok(Some(event)) = tokio::time::timeout(
            std::time::Duration::from_millis(100),
            output_rx.recv()
        ).await {
            events.push(event);
        }
        
        // Verify we received both events
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].event_type, "test.event");
        assert_eq!(events[1].event_type, "file.created");
        
        // Wait for collector to finish
        handle.await.unwrap().unwrap();
    }
}