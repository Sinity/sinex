//! EventPublisher builder for fluent event publishing in tests.
//!
//! This module provides a unified builder pattern for publishing test events,
//! replacing the previous proliferation of `publish_json_event*` methods.
//!
//! # Usage
//!
//! ```rust,ignore
//! // Convenience method (recommended for most cases)
//! ctx.publish_event(EventSource::new("fs-watcher"), EventType::new("file.created"), json!({...})).await?;
//!
//! // Builder pattern for advanced options
//! ctx.publish()
//!     .source(EventSource::new("fs-watcher"))
//!     .event_type(EventType::new("file.created"))
//!     .payload(json!({...}))
//!     .at(timestamp)        // optional timestamp override
//!     .send().await?;
//! ```

use crate::test_context::TestContext;
use crate::TestResult;
use color_eyre::eyre::eyre;
use serde_json::Value as JsonValue;
use sinex_core::db::models::event::Event;
use sinex_core::types::Timestamp;
use sinex_core::{EventSource, EventType};

/// Builder for publishing test events with fluent configuration.
///
/// Created via `TestContext::publish()`. Configure using method chaining,
/// then call `.send().await` to publish.
pub struct EventPublisher<'a> {
    ctx: &'a TestContext,
    source: Option<EventSource>,
    event_type: Option<EventType>,
    payload: JsonValue,
    timestamp: Option<Timestamp>,
}

impl<'a> EventPublisher<'a> {
    pub(crate) fn new(ctx: &'a TestContext) -> Self {
        Self {
            ctx,
            source: None,
            event_type: None,
            payload: JsonValue::Null,
            timestamp: None,
        }
    }

    /// Set the event source.
    ///
    /// Accepts anything that converts to `EventSource`, including:
    /// - `EventSource` directly
    /// - `&str` (via `Into<EventSource>`)
    /// - `String` (via `Into<EventSource>`)
    pub fn source(mut self, source: impl Into<EventSource>) -> Self {
        self.source = Some(source.into());
        self
    }

    /// Set the event type.
    ///
    /// Accepts anything that converts to `EventType`, including:
    /// - `EventType` directly
    /// - `&str` (via `Into<EventType>`)
    /// - `String` (via `Into<EventType>`)
    pub fn event_type(mut self, event_type: impl Into<EventType>) -> Self {
        self.event_type = Some(event_type.into());
        self
    }

    /// Set the event payload.
    pub fn payload(mut self, payload: JsonValue) -> Self {
        self.payload = payload;
        self
    }

    /// Set the timestamp (optional, defaults to now).
    pub fn at(mut self, timestamp: impl Into<Timestamp>) -> Self {
        self.timestamp = Some(timestamp.into());
        self
    }

    /// Publish the event and return the persisted event.
    pub async fn send(self) -> TestResult<Event<JsonValue>> {
        let source = self
            .source
            .ok_or_else(|| eyre!("EventPublisher: source is required"))?;
        let event_type = self
            .event_type
            .ok_or_else(|| eyre!("EventPublisher: event_type is required"))?;

        // Delegate to the internal publish method
        self.ctx
            .publish_event_internal(source, event_type, self.payload, self.timestamp)
            .await
    }
}

// Implement IntoFuture so `.await` works directly (sends with current config)
impl<'a> std::future::IntoFuture for EventPublisher<'a> {
    type Output = TestResult<Event<JsonValue>>;
    type IntoFuture =
        std::pin::Pin<Box<dyn std::future::Future<Output = Self::Output> + Send + 'a>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(self.send())
    }
}
