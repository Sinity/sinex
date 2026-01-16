#![doc = include_str!("../../../docs/events_blanket_impls.md")]

use super::EventPayload;
use crate::domain::{EventSource, EventType};
use schemars::JsonSchema;
use serde::Serialize;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

// Blanket implementation for Option<T> where T is EventPayload
impl<T> EventPayload for Option<T>
where
    T: EventPayload,
    Option<T>: Serialize + JsonSchema + Send + Sync + 'static,
{
    const SOURCE: EventSource = T::SOURCE;
    const EVENT_TYPE: EventType = T::EVENT_TYPE;
    const VERSION: &'static str = T::VERSION;
}

// Blanket implementation for Vec<T> where T is EventPayload
impl<T> EventPayload for Vec<T>
where
    T: EventPayload,
    Vec<T>: Serialize + JsonSchema + Send + Sync + 'static,
{
    const SOURCE: EventSource = T::SOURCE;
    const EVENT_TYPE: EventType = T::EVENT_TYPE;
    const VERSION: &'static str = T::VERSION;
}

// Blanket implementation for Box<T> where T is EventPayload
impl<T> EventPayload for Box<T>
where
    T: EventPayload,
    Box<T>: Serialize + JsonSchema + Send + Sync + 'static,
{
    const SOURCE: EventSource = T::SOURCE;
    const EVENT_TYPE: EventType = T::EVENT_TYPE;
    const VERSION: &'static str = T::VERSION;
}

// Blanket implementation for Arc<T> where T is EventPayload
impl<T> EventPayload for Arc<T>
where
    T: EventPayload,
    Arc<T>: Serialize + JsonSchema + Send + Sync + 'static,
{
    const SOURCE: EventSource = T::SOURCE;
    const EVENT_TYPE: EventType = T::EVENT_TYPE;
    const VERSION: &'static str = T::VERSION;
}

// Blanket implementation for HashMap<String, T> where T is EventPayload
impl<T> EventPayload for HashMap<String, T>
where
    T: EventPayload,
    HashMap<String, T>: Serialize + JsonSchema + Send + Sync + 'static,
{
    const SOURCE: EventSource = T::SOURCE;
    const EVENT_TYPE: EventType = T::EVENT_TYPE;
    const VERSION: &'static str = T::VERSION;
}

// Blanket implementation for BTreeMap<String, T> where T is EventPayload
impl<T> EventPayload for BTreeMap<String, T>
where
    T: EventPayload,
    BTreeMap<String, T>: Serialize + JsonSchema + Send + Sync + 'static,
{
    const SOURCE: EventSource = T::SOURCE;
    const EVENT_TYPE: EventType = T::EVENT_TYPE;
    const VERSION: &'static str = T::VERSION;
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
