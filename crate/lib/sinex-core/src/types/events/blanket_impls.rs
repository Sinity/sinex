#![doc = include_str!("../../../docs/events_blanket_impls.md")]

use super::EventPayload;
use crate::domain::{EventSource, EventType};
use crate::error::SinexError;
use schemars::JsonSchema;
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::Value;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

// Blanket implementation for Option<T> where T is EventPayload
impl<T> EventPayload for Option<T>
where
    T: EventPayload + DeserializeOwned,
    Option<T>: Serialize + JsonSchema + Send + Sync + 'static,
{
    const SOURCE: EventSource = T::SOURCE;
    const EVENT_TYPE: EventType = T::EVENT_TYPE;
    const VERSION: &'static str = T::VERSION;

    fn try_from_legacy(value: Value, version: &str) -> Result<Self, SinexError>
    where
        Self: Sized + serde::de::DeserializeOwned,
    {
        if value.is_null() {
            Ok(None)
        } else {
            T::try_from_legacy(value, version).map(Some)
        }
    }
}

// Blanket implementation for Vec<T> where T is EventPayload
impl<T> EventPayload for Vec<T>
where
    T: EventPayload + DeserializeOwned,
    Vec<T>: Serialize + JsonSchema + Send + Sync + 'static,
{
    const SOURCE: EventSource = T::SOURCE;
    const EVENT_TYPE: EventType = T::EVENT_TYPE;
    const VERSION: &'static str = T::VERSION;

    fn try_from_legacy(value: Value, version: &str) -> Result<Self, SinexError>
    where
        Self: Sized + serde::de::DeserializeOwned,
    {
        match value {
            Value::Array(arr) => arr
                .into_iter()
                .map(|v| T::try_from_legacy(v, version))
                .collect(),
            _ => Err(SinexError::serialization("Expected array")),
        }
    }
}

// Blanket implementation for Box<T> where T is EventPayload
impl<T> EventPayload for Box<T>
where
    T: EventPayload + DeserializeOwned,
    Box<T>: Serialize + JsonSchema + Send + Sync + 'static,
{
    const SOURCE: EventSource = T::SOURCE;
    const EVENT_TYPE: EventType = T::EVENT_TYPE;
    const VERSION: &'static str = T::VERSION;

    fn try_from_legacy(value: Value, version: &str) -> Result<Self, SinexError>
    where
        Self: Sized + serde::de::DeserializeOwned,
    {
        T::try_from_legacy(value, version).map(Box::new)
    }
}

// Blanket implementation for Arc<T> where T is EventPayload
impl<T> EventPayload for Arc<T>
where
    T: EventPayload + DeserializeOwned,
    Arc<T>: Serialize + JsonSchema + Send + Sync + 'static,
{
    const SOURCE: EventSource = T::SOURCE;
    const EVENT_TYPE: EventType = T::EVENT_TYPE;
    const VERSION: &'static str = T::VERSION;

    fn try_from_legacy(value: Value, version: &str) -> Result<Self, SinexError>
    where
        Self: Sized + serde::de::DeserializeOwned,
    {
        T::try_from_legacy(value, version).map(Arc::new)
    }
}

// Blanket implementation for HashMap<String, T> where T is EventPayload
impl<T> EventPayload for HashMap<String, T>
where
    T: EventPayload + DeserializeOwned,
    HashMap<String, T>: Serialize + JsonSchema + Send + Sync + 'static,
{
    const SOURCE: EventSource = T::SOURCE;
    const EVENT_TYPE: EventType = T::EVENT_TYPE;
    const VERSION: &'static str = T::VERSION;

    fn try_from_legacy(value: Value, version: &str) -> Result<Self, SinexError>
    where
        Self: Sized + serde::de::DeserializeOwned,
    {
        match value {
            Value::Object(map) => map
                .into_iter()
                .map(|(k, v)| T::try_from_legacy(v, version).map(|t| (k, t)))
                .collect(),
            _ => Err(SinexError::serialization("Expected object")),
        }
    }
}

// Blanket implementation for BTreeMap<String, T> where T is EventPayload
impl<T> EventPayload for BTreeMap<String, T>
where
    T: EventPayload + DeserializeOwned,
    BTreeMap<String, T>: Serialize + JsonSchema + Send + Sync + 'static,
{
    const SOURCE: EventSource = T::SOURCE;
    const EVENT_TYPE: EventType = T::EVENT_TYPE;
    const VERSION: &'static str = T::VERSION;

    fn try_from_legacy(value: Value, version: &str) -> Result<Self, SinexError>
    where
        Self: Sized + serde::de::DeserializeOwned,
    {
        match value {
            Value::Object(map) => map
                .into_iter()
                .map(|(k, v)| T::try_from_legacy(v, version).map(|t| (k, t)))
                .collect(),
            _ => Err(SinexError::serialization("Expected object")),
        }
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

            fn try_from_legacy(value: Value, version: &str) -> Result<Self, SinexError>
            where
                Self: Sized + serde::de::DeserializeOwned,
            {
                <$inner as EventPayload>::try_from_legacy(value, version).map(Self)
            }
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
