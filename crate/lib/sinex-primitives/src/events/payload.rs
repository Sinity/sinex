use super::builder::{EventBuilder, EventId, HasProvenance, NoProvenance};
use super::{Event, Provenance, SourceMaterial};
use crate::domain::{EventSource, EventType};
use crate::error::{Result, SinexError};
use crate::ids::Id;
use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value as JsonValue;
use sinex_schema::primitives::Ulid;

/// Trait for types that can be used as event payloads.
///
/// Implementing this trait allows for strongly-typed event processing.
/// Each payload type defines its constant Source and `EventType`.
#[diagnostic::on_unimplemented(
    message = "`{Self}` cannot be used as an event payload",
    label = "this type does not implement `EventPayload`",
    note = "derive it with `#[derive(EventPayload)]` from sinex-macros, or implement manually"
)]
pub trait EventPayload: Serialize + DeserializeOwned + Send + Sync + 'static {
    /// The event source for this payload type
    const SOURCE: EventSource;
    /// The event type identifier
    const EVENT_TYPE: EventType;
    /// The schema version
    const VERSION: &'static str;

    /// Get the event source (defaults to constant)
    fn event_source(&self) -> EventSource {
        Self::SOURCE
    }

    /// Get the event type (defaults to constant)
    fn event_type(&self) -> EventType {
        Self::EVENT_TYPE
    }

    /// Start building an event from this payload with material provenance.
    ///
    /// The `anchor_byte` defaults to `0` (beginning of the material). Use
    /// [`EventBuilder::from_material`] directly if you need a specific byte offset.
    #[allow(clippy::wrong_self_convention)] // Intentional: consumes self to build event
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
    #[allow(clippy::wrong_self_convention)] // Intentional: consumes self to build event
    fn from_parents<I>(self, parents: I) -> Result<EventBuilder<Self, HasProvenance>>
    where
        Self: Sized,
        I: IntoIterator<Item = EventId>,
    {
        Event::builder(self).from_parents(parents)
    }

    /// Convert this payload directly into an event with explicit provenance.
    fn into_event(self, provenance: Provenance) -> Event<Self>
    where
        Self: Sized,
    {
        Event::new(self, provenance)
    }
}

/// Trait for types that can be converted into a publishable event.
///
/// This provides a uniform interface for publishing both typed payloads
/// (via `EventPayload` trait) and dynamic JSON payloads (via `DynamicPayload`).
#[diagnostic::on_unimplemented(
    message = "`{Self}` cannot be published as an event",
    label = "this type does not implement `Publishable`",
    note = "implement `EventPayload` (blanket impl covers it) or implement `Publishable` directly for dynamic payloads"
)]
pub trait Publishable: Send + Sync {
    /// Get the event source
    fn source(&self) -> EventSource;
    /// Get the event type
    fn event_type(&self) -> EventType;
    /// Convert payload to JSON value
    fn to_json_value(&self) -> Result<JsonValue>;
}

// Blanket implementation for typed payloads
impl<T> Publishable for T
where
    T: EventPayload,
{
    fn source(&self) -> EventSource {
        self.event_source()
    }

    fn event_type(&self) -> EventType {
        self.event_type()
    }

    fn to_json_value(&self) -> Result<JsonValue> {
        serde_json::to_value(self).map_err(|e| {
            SinexError::serialization("failed to serialize event payload").with_std_error(&e)
        })
    }
}

/// Extension trait providing fluent builder API for all typed payloads.
///
/// This simplifies event construction by allowing `payload.into_builder()`
/// directly, rather than `Event::builder(payload)`. `into_event` is inherited
/// from `EventPayload`.
pub trait PayloadExt: EventPayload + Sized {
    /// Create an `EventBuilder` initialized with this payload.
    fn into_builder(self) -> EventBuilder<Self, NoProvenance> {
        Event::builder(self)
    }
}

// Blanket implementation for all EventPayload types
impl<T: EventPayload> PayloadExt for T {}

/// Wrapper for dynamic (runtime-defined) event payloads.
///
/// Use this when the event source/type are determined at runtime,
/// or when working with untyped JSON data as a "bag of bytes".
#[derive(Debug, Clone, Serialize)]
pub struct DynamicPayload {
    source: EventSource,
    event_type: EventType,
    #[serde(flatten)]
    payload: JsonValue,
}

impl DynamicPayload {
    /// Create a new dynamic payload
    pub fn new(
        source: impl Into<EventSource>,
        event_type: impl Into<EventType>,
        payload: JsonValue,
    ) -> Self {
        Self {
            source: source.into(),
            event_type: event_type.into(),
            payload,
        }
    }

    /// Access the underlying JSON payload
    #[must_use]
    pub fn payload(&self) -> &JsonValue {
        &self.payload
    }

    /// Take ownership of the JSON payload
    #[must_use]
    pub fn into_payload(self) -> JsonValue {
        self.payload
    }

    /// Start building an event with material provenance.
    pub fn from_material(
        self,
        material_id: impl Into<Id<SourceMaterial>>,
    ) -> EventBuilder<JsonValue, HasProvenance> {
        self.into_builder().from_material(material_id, 0)
    }

    /// Start building an event with material provenance and anchor byte.
    pub fn from_material_at(
        self,
        material_id: impl Into<Id<SourceMaterial>>,
        anchor_byte: i64,
    ) -> EventBuilder<JsonValue, HasProvenance> {
        self.into_builder().from_material(material_id, anchor_byte)
    }

    /// Start building an event with synthesis provenance.
    pub fn from_parents<I>(self, parents: I) -> Result<EventBuilder<JsonValue, HasProvenance>>
    where
        I: IntoIterator<Item = EventId>,
    {
        self.into_builder().from_parents(parents)
    }

    /// Build an event with explicit provenance.
    #[must_use]
    pub fn with_provenance(self, provenance: Provenance) -> EventBuilder<JsonValue, HasProvenance> {
        self.into_builder().with_provenance(provenance)
    }

    /// Convert directly to an event with explicit provenance.
    #[must_use]
    pub fn into_event(self, provenance: Provenance) -> Event<JsonValue> {
        Event::new_json(self.source, self.event_type, self.payload, provenance)
    }

    /// Convert into an `EventBuilder`
    #[must_use]
    pub fn into_builder(self) -> EventBuilder<JsonValue, NoProvenance> {
        EventBuilder::new_internal(self.source, self.event_type, self.payload)
    }

    /// Set hostname before adding provenance.
    pub fn hostname(
        self,
        hostname: impl Into<crate::domain::HostName>,
    ) -> EventBuilder<JsonValue, NoProvenance> {
        self.into_builder().hostname(hostname)
    }

    /// Set node version before adding provenance.
    pub fn node_version(self, version: impl Into<String>) -> EventBuilder<JsonValue, NoProvenance> {
        self.into_builder().node_version(version)
    }

    /// Set schema ID before adding provenance.
    #[must_use]
    pub fn schema_id(self, schema_id: Ulid) -> EventBuilder<JsonValue, NoProvenance> {
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

    fn to_json_value(&self) -> Result<JsonValue> {
        Ok(self.payload.clone())
    }
}

// Helper macro for creating wrapper payloads with custom source/event_type
#[macro_export]
macro_rules! wrapped_payload {
    ($name:ident, $inner:ty, $source:expr, $event_type:expr) => {
        #[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
        pub struct $name(pub $inner);

        impl EventPayload for $name {
            const SOURCE: EventSource = EventSource::from_static($source);
            const EVENT_TYPE: EventType = EventType::from_static($event_type);
            const VERSION: &'static str = <$inner as EventPayload>::VERSION;
        }

        impl From<$inner> for $name {
            fn from(inner: $inner) -> Self {
                Self(inner)
            }
        }

        impl AsRef<$inner> for $name {
            fn as_ref(&self) -> &$inner {
                &self.0
            }
        }
    };
}
