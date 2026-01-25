//! EventPayload trait for strongly-typed event payloads
//!
//! This module provides two key abstractions:
//!
//! - [`EventPayload`]: Trait for strongly-typed payloads with compile-time source/type
//! - [`Publishable`]: Trait for anything that can become an event (typed or dynamic)
//!
//! # Design Philosophy
//!
//! `EventPayload` types are the primary, type-safe way to create events. Each payload
//! type encodes its source and event_type as associated constants, ensuring compile-time
//! correctness.
//!
//! `Publishable` is a more general trait that abstracts over "anything that can become
//! an event". This includes:
//! - All `EventPayload` implementors (via blanket impl)
//! - `DynamicPayload` for runtime-specified source/type (escape hatch)
//!
//! This separation keeps the type-safe path as the default while providing a clean
//! escape hatch for tests and dynamic scenarios.

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

// ============================================================================
// Publishable Trait - Unified abstraction for event creation
// ============================================================================

/// Trait for anything that can be published as an event.
///
/// This is a more general abstraction than `EventPayload`. While `EventPayload`
/// requires compile-time known source/event_type constants, `Publishable` allows
/// both compile-time (via blanket impl) and runtime specification.
///
/// # Implementors
///
/// - All `EventPayload` types (via blanket implementation)
/// - `DynamicPayload` for runtime-specified source/type
///
/// # Usage
///
/// ```ignore
/// // Typed payload (recommended)
/// ctx.publish(FileCreatedPayload { path, size, ... }).await?;
///
/// // Dynamic payload (escape hatch)
/// ctx.publish(DynamicPayload::new("source", "type", json!({...}))).await?;
/// ```
pub trait Publishable: Send + Sync {
    /// The event source identifier
    fn source(&self) -> EventSource;

    /// The event type identifier
    fn event_type(&self) -> EventType;

    /// Convert the payload to a JSON value for storage/transmission
    fn to_json_value(&self) -> serde_json::Value;

    /// Schema version (optional, defaults to "1.0.0")
    fn version(&self) -> &str {
        "1.0.0"
    }
}

/// Blanket implementation: all EventPayload types are Publishable
impl<P> Publishable for P
where
    P: EventPayload,
{
    fn source(&self) -> EventSource {
        P::SOURCE.clone()
    }

    fn event_type(&self) -> EventType {
        P::EVENT_TYPE.clone()
    }

    fn to_json_value(&self) -> serde_json::Value {
        // EventPayload requires Serialize, so this should always succeed
        serde_json::to_value(self).expect("EventPayload serialization should not fail")
    }

    fn version(&self) -> &str {
        P::VERSION
    }
}

// ============================================================================
// DynamicPayload - Runtime-specified source/type escape hatch
// ============================================================================

/// A dynamic payload with runtime-specified source and event type.
///
/// Use this as an escape hatch when you need to construct events with
/// source/event_type that aren't known at compile time. Prefer typed
/// `EventPayload` implementations when possible.
///
/// # Examples
///
/// ```ignore
/// use sinex_core::DynamicPayload;
/// use serde_json::json;
///
/// // Simple construction
/// let payload = DynamicPayload::new("my-source", "my.event", json!({"key": "value"}));
///
/// // Use with publish
/// ctx.publish(payload).await?;
/// ```
#[derive(Debug, Clone, Serialize)]
pub struct DynamicPayload {
    source: EventSource,
    event_type: EventType,
    #[serde(flatten)]
    payload: serde_json::Value,
}

use crate::db::models::event_builder::NoProvenance;

impl DynamicPayload {
    /// Create a new dynamic payload.
    ///
    /// # Arguments
    ///
    /// * `source` - The event source (e.g., "fs-watcher", "terminal")
    /// * `event_type` - The event type (e.g., "file.created", "command.executed")
    /// * `payload` - The JSON payload data
    ///
    /// # Example
    ///
    /// ```ignore
    /// // For test publishing (test infra handles provenance)
    /// ctx.publish(DynamicPayload::new("source", "type", json!({...}))).await?;
    ///
    /// // For production event creation (explicit provenance required)
    /// let event = DynamicPayload::new("source", "type", json!({...}))
    ///     .from_material(material_id)
    ///     .build()?;
    /// ```
    pub fn new(
        source: impl Into<EventSource>,
        event_type: impl Into<EventType>,
        payload: serde_json::Value,
    ) -> Self {
        Self {
            source: source.into(),
            event_type: event_type.into(),
            payload,
        }
    }

    /// Access the underlying JSON payload
    pub fn payload(&self) -> &serde_json::Value {
        &self.payload
    }

    /// Take ownership of the JSON payload
    pub fn into_payload(self) -> serde_json::Value {
        self.payload
    }

    // ========================================================================
    // Fluent Event Construction (mirrors EventPayload API)
    // ========================================================================

    /// Start building an event with material provenance.
    ///
    /// Use this for first-order events derived from captured source material.
    ///
    /// # Example
    /// ```ignore
    /// let event = DynamicPayload::new("fs-watcher", "file.created", json!({...}))
    ///     .from_material(material_id)
    ///     .build()?;
    /// ```
    pub fn from_material(
        self,
        material_id: impl Into<Id<SourceMaterial>>,
    ) -> EventBuilder<serde_json::Value, HasProvenance> {
        self.into_builder().from_material(material_id, 0)
    }

    /// Start building an event with material provenance and anchor byte.
    ///
    /// Use when you need to specify the byte offset in the source material.
    ///
    /// # Example
    /// ```ignore
    /// let event = DynamicPayload::new("parser", "line.parsed", json!({...}))
    ///     .from_material_at(material_id, byte_offset)
    ///     .build()?;
    /// ```
    pub fn from_material_at(
        self,
        material_id: impl Into<Id<SourceMaterial>>,
        anchor_byte: i64,
    ) -> EventBuilder<serde_json::Value, HasProvenance> {
        self.into_builder().from_material(material_id, anchor_byte)
    }

    /// Start building an event with synthesis provenance.
    ///
    /// Use for higher-order events derived from other events.
    ///
    /// # Example
    /// ```ignore
    /// let event = DynamicPayload::new("analytics", "summary", json!({...}))
    ///     .from_parents(source_event_ids)?
    ///     .build()?;
    /// ```
    pub fn from_parents<I>(
        self,
        parents: I,
    ) -> Result<EventBuilder<serde_json::Value, HasProvenance>, SinexError>
    where
        I: IntoIterator<Item = EventId>,
    {
        self.into_builder().from_parents(parents)
    }

    /// Build an event with explicit provenance.
    ///
    /// Use when you already have a constructed Provenance value.
    ///
    /// # Example
    /// ```ignore
    /// let event = DynamicPayload::new("source", "type", json!({...}))
    ///     .with_provenance(provenance)
    ///     .build()?;
    /// ```
    pub fn with_provenance(
        self,
        provenance: Provenance,
    ) -> EventBuilder<serde_json::Value, HasProvenance> {
        self.into_builder().with_provenance(provenance)
    }

    /// Convert directly to an event with explicit provenance.
    ///
    /// Shorthand for `.with_provenance(prov).build().unwrap()`.
    pub fn into_event(self, provenance: Provenance) -> Event<serde_json::Value> {
        Event::new_json(self.source, self.event_type, self.payload, provenance)
    }

    /// Convert to an EventBuilder for full control over event construction.
    ///
    /// Use this when you need access to additional builder methods like
    /// `hostname()`, `ingestor_version()`, etc.
    ///
    /// # Example
    /// ```ignore
    /// let event = DynamicPayload::new("source", "type", json!({...}))
    ///     .into_builder()
    ///     .hostname("custom-host")
    ///     .from_material(material_id)
    ///     .at_time(timestamp)
    ///     .build()?;
    /// ```
    pub fn into_builder(self) -> EventBuilder<serde_json::Value, NoProvenance> {
        EventBuilder::new_internal(self.source, self.event_type, self.payload)
    }

    // ========================================================================
    // Pre-provenance builder methods (convenience pass-throughs)
    // ========================================================================

    /// Set custom hostname before adding provenance.
    ///
    /// # Example
    /// ```ignore
    /// let event = DynamicPayload::new("source", "type", json!({...}))
    ///     .hostname("custom-host")
    ///     .from_material(material_id)
    ///     .build()?;
    /// ```
    pub fn hostname(
        self,
        hostname: impl Into<crate::types::domain::HostName>,
    ) -> EventBuilder<serde_json::Value, NoProvenance> {
        self.into_builder().hostname(hostname)
    }

    /// Set ingestor version before adding provenance.
    pub fn ingestor_version(
        self,
        version: impl Into<String>,
    ) -> EventBuilder<serde_json::Value, NoProvenance> {
        self.into_builder().ingestor_version(version)
    }

    /// Set schema ID before adding provenance.
    pub fn schema_id(
        self,
        schema_id: crate::types::Ulid,
    ) -> EventBuilder<serde_json::Value, NoProvenance> {
        self.into_builder().schema_id(schema_id)
    }
}

impl Publishable for DynamicPayload {
    fn source(&self) -> EventSource {
        self.source.clone()
    }

    fn event_type(&self) -> EventType {
        self.event_type.clone()
    }

    fn to_json_value(&self) -> serde_json::Value {
        self.payload.clone()
    }
}
