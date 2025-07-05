use anyhow::Result;
use serde::{Deserialize, Serialize};
use sqlx::{postgres::PgListener, PgPool};
use std::collections::HashMap;
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

/// PostgreSQL notification channels used by Sinex
pub mod channels {
    pub const EVENT_INSERTED: &str = "event_inserted";
    pub const WORK_QUEUE_UPDATED: &str = "work_queue_updated";
    pub const SCHEMA_CHANGED: &str = "schema_changed";
    pub const AGENT_HEARTBEAT: &str = "agent_heartbeat";
    pub const DLQ_UPDATED: &str = "dlq_updated";
}

/// Notification payload for event insertion
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventInsertedNotification {
    pub event_id: String,
    pub source: String,
    pub event_type: String,
    pub host: String,
    pub chunked: bool,
    pub chunk_count: Option<u32>,
}

/// Notification payload for work queue updates
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkQueueNotification {
    pub queue_id: String,
    pub event_id: String,
    pub agent_name: String,
    pub action: WorkQueueAction,
    pub priority: Option<i32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum WorkQueueAction {
    Added,
    Claimed,
    Completed,
    Failed,
    Retried,
}

/// Notification payload for schema changes
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaChangedNotification {
    pub schema_id: String,
    pub source: String,
    pub event_type: String,
    pub version: String,
    pub action: SchemaAction,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SchemaAction {
    Created,
    Updated,
    Activated,
    Deactivated,
}

/// Generic notification message
#[derive(Debug, Clone)]
pub enum NotificationMessage {
    EventInserted(EventInsertedNotification),
    WorkQueueUpdated(WorkQueueNotification),
    SchemaChanged(SchemaChangedNotification),
    AgentHeartbeat(String), // Agent name
    DlqUpdated(String),     // DLQ entry ID
    Unknown(String),        // Raw payload for unknown notifications
}

/// PostgreSQL LISTEN/NOTIFY service for real-time event processing
pub struct NotificationService {
    pool: PgPool,
    listeners: HashMap<String, PgListener>,
    sender: mpsc::UnboundedSender<NotificationMessage>,
}

impl NotificationService {
    /// Create a new notification service
    pub async fn new(pool: PgPool) -> Result<(Self, mpsc::UnboundedReceiver<NotificationMessage>)> {
        let (sender, receiver) = mpsc::unbounded_channel();
        
        let service = Self {
            pool,
            listeners: HashMap::new(),
            sender,
        };
        
        Ok((service, receiver))
    }

    /// Start listening to all Sinex notification channels
    pub async fn start_listening(&mut self) -> Result<()> {
        let channels = [
            channels::EVENT_INSERTED,
            channels::WORK_QUEUE_UPDATED,
            channels::SCHEMA_CHANGED,
            channels::AGENT_HEARTBEAT,
            channels::DLQ_UPDATED,
        ];

        for channel in channels {
            self.listen_to_channel(channel).await?;
        }

        info!("Started listening to {} notification channels", channels.len());
        Ok(())
    }

    /// Listen to a specific channel
    pub async fn listen_to_channel(&mut self, channel: &str) -> Result<()> {
        let mut listener = PgListener::connect_with(&self.pool).await?;
        listener.listen(channel).await?;
        
        info!("Listening to PostgreSQL channel: {}", channel);
        
        let sender = self.sender.clone();
        let channel_name = channel.to_string();
        
        tokio::spawn(async move {
            loop {
                match listener.recv().await {
                    Ok(notification) => {
                        debug!(
                            "Received notification on channel '{}': {}",
                            notification.channel(),
                            notification.payload()
                        );

                        let message = Self::parse_notification(&channel_name, notification.payload());
                        
                        if let Err(e) = sender.send(message) {
                            error!("Failed to send notification message: {}", e);
                            break;
                        }
                    }
                    Err(e) => {
                        error!("Error receiving notification on channel '{}': {}", channel_name, e);
                        // Reconnection logic could be added here
                        tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                    }
                }
            }
        });

        Ok(())
    }

    /// Parse notification payload into structured message
    fn parse_notification(channel: &str, payload: &str) -> NotificationMessage {
        match channel {
            channels::EVENT_INSERTED => {
                match serde_json::from_str::<EventInsertedNotification>(payload) {
                    Ok(notification) => NotificationMessage::EventInserted(notification),
                    Err(e) => {
                        warn!("Failed to parse event_inserted notification: {}. Raw: {}", e, payload);
                        NotificationMessage::Unknown(payload.to_string())
                    }
                }
            }
            channels::WORK_QUEUE_UPDATED => {
                match serde_json::from_str::<WorkQueueNotification>(payload) {
                    Ok(notification) => NotificationMessage::WorkQueueUpdated(notification),
                    Err(e) => {
                        warn!("Failed to parse work_queue_updated notification: {}. Raw: {}", e, payload);
                        NotificationMessage::Unknown(payload.to_string())
                    }
                }
            }
            channels::SCHEMA_CHANGED => {
                match serde_json::from_str::<SchemaChangedNotification>(payload) {
                    Ok(notification) => NotificationMessage::SchemaChanged(notification),
                    Err(e) => {
                        warn!("Failed to parse schema_changed notification: {}. Raw: {}", e, payload);
                        NotificationMessage::Unknown(payload.to_string())
                    }
                }
            }
            channels::AGENT_HEARTBEAT => {
                NotificationMessage::AgentHeartbeat(payload.to_string())
            }
            channels::DLQ_UPDATED => {
                NotificationMessage::DlqUpdated(payload.to_string())
            }
            _ => {
                debug!("Unknown notification channel '{}': {}", channel, payload);
                NotificationMessage::Unknown(payload.to_string())
            }
        }
    }

    /// Trigger a notification (for sending from the application)
    pub async fn notify(&self, channel: &str, payload: &str) -> Result<()> {
        let query = format!("SELECT pg_notify('{}', $1)", channel);
        sqlx::query(&query)
            .bind(payload)
            .execute(&self.pool)
            .await?;
        
        debug!("Sent notification to channel '{}': {}", channel, payload);
        Ok(())
    }

    /// Send structured notification
    pub async fn notify_event_inserted(&self, notification: &EventInsertedNotification) -> Result<()> {
        let payload = serde_json::to_string(notification)?;
        self.notify(channels::EVENT_INSERTED, &payload).await
    }

    /// Send work queue notification
    pub async fn notify_work_queue_updated(&self, notification: &WorkQueueNotification) -> Result<()> {
        let payload = serde_json::to_string(notification)?;
        self.notify(channels::WORK_QUEUE_UPDATED, &payload).await
    }

    /// Send schema change notification
    pub async fn notify_schema_changed(&self, notification: &SchemaChangedNotification) -> Result<()> {
        let payload = serde_json::to_string(notification)?;
        self.notify(channels::SCHEMA_CHANGED, &payload).await
    }

    /// Send agent heartbeat notification
    pub async fn notify_agent_heartbeat(&self, agent_name: &str) -> Result<()> {
        self.notify(channels::AGENT_HEARTBEAT, agent_name).await
    }

    /// Send DLQ update notification
    pub async fn notify_dlq_updated(&self, dlq_id: &str) -> Result<()> {
        self.notify(channels::DLQ_UPDATED, dlq_id).await
    }
}

/// Helper functions for database triggers to send notifications
pub mod triggers {
    use super::*;

    /// SQL function for triggering event_inserted notifications
    pub const CREATE_EVENT_NOTIFICATION_FUNCTION: &str = r#"
CREATE OR REPLACE FUNCTION notify_event_inserted()
RETURNS TRIGGER AS $$
DECLARE
    notification_payload JSON;
    is_chunked BOOLEAN;
    chunk_count INTEGER;
BEGIN
    -- Check if this event is part of a chunked payload
    is_chunked := NEW.payload ? 'chunk_info';
    chunk_count := CASE 
        WHEN is_chunked THEN (NEW.payload->'chunk_info'->>'total_chunks')::INTEGER
        ELSE NULL 
    END;

    notification_payload := json_build_object(
        'event_id', NEW.id::TEXT,
        'source', NEW.source,
        'event_type', NEW.event_type,
        'host', NEW.host,
        'chunked', is_chunked,
        'chunk_count', chunk_count
    );
    
    PERFORM pg_notify('event_inserted', notification_payload::TEXT);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
    "#;

    /// SQL trigger for event insertions
    pub const CREATE_EVENT_TRIGGER: &str = r#"
DROP TRIGGER IF EXISTS trigger_notify_event_inserted ON raw.events;
CREATE TRIGGER trigger_notify_event_inserted
    AFTER INSERT ON raw.events
    FOR EACH ROW
    EXECUTE FUNCTION notify_event_inserted();
    "#;

    /// SQL function for work queue notifications
    pub const CREATE_WORK_QUEUE_NOTIFICATION_FUNCTION: &str = r#"
CREATE OR REPLACE FUNCTION notify_work_queue_updated()
RETURNS TRIGGER AS $$
DECLARE
    notification_payload JSON;
    queue_action TEXT;
BEGIN
    -- Determine the action based on the change
    IF TG_OP = 'INSERT' THEN
        queue_action := 'added';
    ELSIF TG_OP = 'UPDATE' THEN
        IF OLD.status != NEW.status THEN
            CASE NEW.status
                WHEN 'processing' THEN queue_action := 'claimed';
                WHEN 'succeeded' THEN queue_action := 'completed';
                WHEN 'failed' THEN queue_action := 'failed';
                WHEN 'failed_retryable' THEN queue_action := 'retried';
                ELSE queue_action := 'updated';
            END CASE;
        ELSE
            queue_action := 'updated';
        END IF;
    ELSE
        RETURN NULL;
    END IF;

    notification_payload := json_build_object(
        'queue_id', COALESCE(NEW.queue_id, OLD.queue_id)::TEXT,
        'event_id', COALESCE(NEW.raw_event_id, OLD.raw_event_id)::TEXT,
        'agent_name', COALESCE(NEW.target_agent_name, OLD.target_agent_name),
        'action', queue_action,
        'priority', COALESCE(NEW.priority, OLD.priority)
    );
    
    PERFORM pg_notify('work_queue_updated', notification_payload::TEXT);
    RETURN COALESCE(NEW, OLD);
END;
$$ LANGUAGE plpgsql;
    "#;

    /// SQL trigger for work queue updates
    pub const CREATE_WORK_QUEUE_TRIGGER: &str = r#"
DROP TRIGGER IF EXISTS trigger_notify_work_queue_updated ON sinex_schemas.work_queue;
CREATE TRIGGER trigger_notify_work_queue_updated
    AFTER INSERT OR UPDATE ON sinex_schemas.work_queue
    FOR EACH ROW
    EXECUTE FUNCTION notify_work_queue_updated();
    "#;
}

/// Real-time event processor using notifications
pub struct RealtimeEventProcessor {
    notification_service: NotificationService,
    receiver: mpsc::UnboundedReceiver<NotificationMessage>,
}

impl RealtimeEventProcessor {
    /// Create a new real-time event processor
    pub async fn new(pool: PgPool) -> Result<Self> {
        let (notification_service, receiver) = NotificationService::new(pool).await?;
        
        Ok(Self {
            notification_service,
            receiver,
        })
    }

    /// Start processing notifications
    pub async fn start(&mut self) -> Result<()> {
        // Start listening to all channels
        self.notification_service.start_listening().await?;

        info!("Real-time event processor started");

        // Process incoming notifications
        while let Some(message) = self.receiver.recv().await {
            self.process_notification(message).await;
        }

        Ok(())
    }

    /// Process a single notification message
    async fn process_notification(&self, message: NotificationMessage) {
        match message {
            NotificationMessage::EventInserted(notification) => {
                info!(
                    "New event inserted: {} from {} (chunked: {})",
                    notification.event_type,
                    notification.source,
                    notification.chunked
                );
                
                // Here you could trigger immediate processing for high-priority events
                if notification.source == "critical.system" {
                    // Fast-track critical events
                    debug!("Fast-tracking critical event: {}", notification.event_id);
                }
            }
            NotificationMessage::WorkQueueUpdated(notification) => {
                debug!(
                    "Work queue updated: {} -> {:?} for agent {}",
                    notification.queue_id,
                    notification.action,
                    notification.agent_name
                );
                
                // Could trigger worker scaling decisions here
                match notification.action {
                    WorkQueueAction::Failed => {
                        warn!("Work item failed: {} for agent {}", notification.queue_id, notification.agent_name);
                    }
                    WorkQueueAction::Completed => {
                        debug!("Work item completed: {}", notification.queue_id);
                    }
                    _ => {}
                }
            }
            NotificationMessage::SchemaChanged(notification) => {
                info!(
                    "Schema changed: {}.{} v{} -> {:?}",
                    notification.source,
                    notification.event_type,
                    notification.version,
                    notification.action
                );
                
                // Could trigger cache invalidation or restart for schema changes
            }
            NotificationMessage::AgentHeartbeat(agent_name) => {
                debug!("Agent heartbeat: {}", agent_name);
                // Update agent status tracking
            }
            NotificationMessage::DlqUpdated(dlq_id) => {
                debug!("DLQ updated: {}", dlq_id);
                // Could trigger alerts for DLQ items
            }
            NotificationMessage::Unknown(payload) => {
                debug!("Unknown notification: {}", payload);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_notification_parsing() {
        let event_payload = r#"{
            "event_id": "01HZBC123456789ABCDEFGHIJK",
            "source": "fs",
            "event_type": "file.created",
            "host": "localhost",
            "chunked": false,
            "chunk_count": null
        }"#;

        let message = NotificationService::parse_notification(
            channels::EVENT_INSERTED,
            event_payload
        );

        match message {
            NotificationMessage::EventInserted(notification) => {
                assert_eq!(notification.event_id, "01HZBC123456789ABCDEFGHIJK");
                assert_eq!(notification.source, "fs");
                assert_eq!(notification.event_type, "file.created");
                assert!(!notification.chunked);
            }
            _ => panic!("Expected EventInserted notification"),
        }
    }

    #[test]
    fn test_work_queue_notification_parsing() {
        let work_queue_payload = r#"{
            "queue_id": "01HZBC123456789ABCDEFGHIJK",
            "event_id": "01HZBC987654321ZYXWVUTSRQP",
            "agent_name": "promotion-worker",
            "action": "completed",
            "priority": 1
        }"#;

        let message = NotificationService::parse_notification(
            channels::WORK_QUEUE_UPDATED,
            work_queue_payload
        );

        match message {
            NotificationMessage::WorkQueueUpdated(notification) => {
                assert_eq!(notification.queue_id, "01HZBC123456789ABCDEFGHIJK");
                assert_eq!(notification.agent_name, "promotion-worker");
                assert!(matches!(notification.action, WorkQueueAction::Completed));
                assert_eq!(notification.priority, Some(1));
            }
            _ => panic!("Expected WorkQueueUpdated notification"),
        }
    }

    #[test]
    fn test_schema_notification_parsing() {
        let schema_payload = r#"{
            "schema_id": "01HZBC123456789ABCDEFGHIJK",
            "source": "fs",
            "event_type": "file.created",
            "version": "v1.0",
            "action": "activated"
        }"#;

        let message = NotificationService::parse_notification(
            channels::SCHEMA_CHANGED,
            schema_payload
        );

        match message {
            NotificationMessage::SchemaChanged(notification) => {
                assert_eq!(notification.schema_id, "01HZBC123456789ABCDEFGHIJK");
                assert_eq!(notification.source, "fs");
                assert_eq!(notification.version, "v1.0");
                assert!(matches!(notification.action, SchemaAction::Activated));
            }
            _ => panic!("Expected SchemaChanged notification"),
        }
    }

    #[test]
    fn test_unknown_notification() {
        let unknown_payload = "some unknown payload";
        let message = NotificationService::parse_notification("unknown_channel", unknown_payload);

        match message {
            NotificationMessage::Unknown(payload) => {
                assert_eq!(payload, "some unknown payload");
            }
            _ => panic!("Expected Unknown notification"),
        }
    }
}