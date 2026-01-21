//! EventPayload trait for strongly-typed event payloads

use crate::db::models::event::{Event, SourceMaterial};
use crate::db::models::event_builder::{EventBuilder, EventId, HasProvenance, Provenance};
use crate::domain::{EventSource, EventType};
use crate::types::Id;
use crate::SinexError;
use schemars::JsonSchema;
use serde::Serialize;

/// Trait for strongly-typed event payloads
///
/// Each payload type serves as the single source of truth for:
/// - Which source generates this event type (SOURCE)
/// - What the event type is (EVENT_TYPE)
/// - The schema version (VERSION)
///
/// Schema name is derived as "{SOURCE}.{EVENT_TYPE}"
/// Schema versioning is handled by the VERSION constant
///
/// # Fluent Event Construction
///
/// Payloads implementing this trait can be converted to events fluently:
///
/// ```ignore
/// let event = FileCreatedPayload { path, size, ... }
///     .from_material(material_id)
///     .at_time(timestamp)
///     .build();
/// ```
pub trait EventPayload: Serialize + JsonSchema + Send + Sync + 'static {
    /// The source that generates this type of event
    const SOURCE: EventSource;

    /// The event type identifier
    const EVENT_TYPE: EventType;

    /// The schema version (semantic versioning)
    const VERSION: &'static str;

    /// Start building an event from this payload with material provenance.
    ///
    /// Use this for first-order events derived from captured source material
    /// (files, sockets, subprocess streams).
    ///
    /// # Example
    /// ```ignore
    /// let event = FileCreatedPayload { path, size, ... }
    ///     .from_material(material_id)
    ///     .build();
    /// ```
    fn from_material(
        self,
        material_id: impl Into<Id<SourceMaterial>>,
    ) -> EventBuilder<Self, HasProvenance>
    where
        Self: Sized,
    {
        Event::builder(self).from_material(material_id, 0)
    }

    /// Start building an event from this payload with synthesis provenance.
    ///
    /// Use this for higher-order events derived from other events
    /// (automata transformations, aggregations).
    ///
    /// # Example
    /// ```ignore
    /// let event = HealthSummaryPayload { ... }
    ///     .from_parents(source_event_ids)?
    ///     .build();
    /// ```
    fn from_parents<I>(self, parents: I) -> Result<EventBuilder<Self, HasProvenance>, SinexError>
    where
        Self: Sized,
        I: IntoIterator<Item = EventId>,
    {
        Event::builder(self).from_parents(parents)
    }

    /// Convert this payload directly into an event with explicit provenance.
    ///
    /// Use when you already have a constructed Provenance value.
    ///
    /// # Example
    /// ```ignore
    /// let event = payload.into_event(provenance);
    /// ```
    fn into_event(self, provenance: Provenance) -> Event<Self>
    where
        Self: Sized,
    {
        Event::new(self, provenance)
    }
}

// Re-export common types used with payloads
pub type Timestamp = chrono::DateTime<chrono::Utc>;
pub type OptionalTimestamp = Option<chrono::DateTime<chrono::Utc>>;
pub type JsonValue = serde_json::Value;

// Special implementation for JsonValue to support heterogeneous event processing
impl EventPayload for serde_json::Value {
    const SOURCE: EventSource = EventSource::from_static("system");
    const EVENT_TYPE: EventType = EventType::from_static("generic");
    const VERSION: &'static str = "1.0.0";

    // JsonValue payloads are version-neutral.
}
