//! Payloads declared by the Sinnix lake producers. The lane selects a source
//! type before decoding; the lake's metadata fields and wire values are kept as
//! written.
use crate::Timestamp;
use schemars::JsonSchema;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::{Map, Value};
pub mod accessibility;
pub mod audio;
pub mod desktop;
pub mod idle;
pub mod network;
pub mod peripheral;

fn supplied<'de, D: Deserializer<'de>, T: Deserialize<'de>>(
    de: D,
) -> Result<Option<Option<T>>, D::Error> {
    Option::<T>::deserialize(de).map(Some)
}
fn decode<T: serde::de::DeserializeOwned>(value: Value) -> Result<T, serde_json::Error> {
    serde_json::from_value(value)
}
fn invalid(message: &str) -> serde_json::Error {
    <serde_json::Error as serde::de::Error>::custom(message)
}
/// Known fields have concrete types. Unknown lanes or representation kinds
/// remain explicit; a malformed known field never selects the fallback.
#[derive(Clone, Debug, PartialEq, Serialize, JsonSchema)]
#[serde(untagged)]
pub enum CapturePayload {
    Selection(Box<desktop::Selection>),
    Notification(Box<desktop::Notification>),
    Media(Box<desktop::Media>),
    Frame(Box<desktop::Frame>),
    Navigation(Box<desktop::Navigation>),
    Reading(Box<desktop::Reading>),
    Monitor(Box<desktop::Monitor>),
    Environment(Box<desktop::Environment>),
    Flow(Box<network::Flow>),
    Focus(Box<accessibility::Focus>),
    TextChanged(Box<accessibility::TextChanged>),
    Subtree(Box<accessibility::Subtree>),
    Idle(Box<idle::Idle>),
    Devices(Box<audio::Devices>),
    Node(Box<audio::Node>),
    Port(Box<audio::Port>),
    Link(Box<audio::Link>),
    SpeechIndex(Box<audio::SpeechIndex>),
    Logitech(Box<peripheral::Logitech>),
    Battery(Vec<peripheral::Battery>),
    Transport(Vec<peripheral::Transport>),
    Unknown(Value),
}
impl CapturePayload {
    fn decode(lane: &str, value: Value) -> Result<Self, serde_json::Error> {
        match lane {
            "clipboard" | "primary" => decode(value).map(Box::new).map(Self::Selection),
            "notifications" => decode(value).map(Box::new).map(Self::Notification),
            "mpris" => decode(value).map(Box::new).map(Self::Media),
            "screen-frames" => decode(value).map(Box::new).map(Self::Frame),
            "browser-nav-edges" => decode(value).map(Box::new).map(Self::Navigation),
            "reading-stack" => decode(value).map(Box::new).map(Self::Reading),
            "monitor" => decode(value).map(Box::new).map(Self::Monitor),
            "environment" | "awair" => decode(value).map(Box::new).map(Self::Environment),
            "netflow" => decode(value).map(Box::new).map(Self::Flow),
            "idle-inhibitors" => decode(value).map(Box::new).map(Self::Idle),
            "logitech" => decode(value).map(Box::new).map(Self::Logitech),
            "bt-battery" => decode(value).map(Self::Battery),
            "bt-audio" => decode(value).map(Self::Transport),
            "a11y" => match representation(&value)? {
                "focus" => decode(value).map(Box::new).map(Self::Focus),
                "text-changed" => decode(value).map(Box::new).map(Self::TextChanged),
                "subtree" => decode(value).map(Box::new).map(Self::Subtree),
                _ => Ok(Self::Unknown(value)),
            },
            "audio-topology" => match representation(&value)? {
                "node" => decode(value).map(Box::new).map(Self::Node),
                "port" => decode(value).map(Box::new).map(Self::Port),
                "link" => decode(value).map(Box::new).map(Self::Link),
                _ => Ok(Self::Unknown(value)),
            },
            "audio-devices" => match representation(&value)? {
                "devices-state" => decode(value).map(Box::new).map(Self::Devices),
                _ => Ok(Self::Unknown(value)),
            },
            "audio-index" => match representation(&value)? {
                "speech-index" => decode(value).map(Box::new).map(Self::SpeechIndex),
                _ => Ok(Self::Unknown(value)),
            },
            _ => Ok(Self::Unknown(value)),
        }
    }
    fn unknown_fields(&self) -> Vec<String> {
        let mut fields = Vec::new();
        match self {
            Self::Selection(value) => value.collect_unknown("/payload", &mut fields),
            Self::Notification(value) => value.collect_unknown("/payload", &mut fields),
            Self::Media(value) => value.collect_unknown("/payload", &mut fields),
            Self::Frame(value) => value.collect_unknown("/payload", &mut fields),
            Self::Navigation(value) => value.collect_unknown("/payload", &mut fields),
            Self::Reading(value) => value.collect_unknown("/payload", &mut fields),
            Self::Monitor(value) => value.collect_unknown("/payload", &mut fields),
            Self::Environment(value) => value.collect_unknown("/payload", &mut fields),
            Self::Flow(value) => value.collect_unknown("/payload", &mut fields),
            Self::Focus(value) => value.collect_unknown("/payload", &mut fields),
            Self::TextChanged(value) => value.collect_unknown("/payload", &mut fields),
            Self::Subtree(value) => value.collect_unknown("/payload", &mut fields),
            Self::Idle(value) => value.collect_unknown("/payload", &mut fields),
            Self::Devices(value) => value.collect_unknown("/payload", &mut fields),
            Self::Node(value) => value.collect_unknown("/payload", &mut fields),
            Self::Port(value) => value.collect_unknown("/payload", &mut fields),
            Self::Link(value) => value.collect_unknown("/payload", &mut fields),
            Self::SpeechIndex(value) => value.collect_unknown("/payload", &mut fields),
            Self::Logitech(value) => value.collect_unknown("/payload", &mut fields),
            Self::Battery(values) => {
                for (i, value) in values.iter().enumerate() {
                    value.collect_unknown(&format!("/payload/{i}"), &mut fields);
                }
            }
            Self::Transport(values) => {
                for (i, value) in values.iter().enumerate() {
                    value.collect_unknown(&format!("/payload/{i}"), &mut fields);
                }
            }
            Self::Unknown(_) => {}
        }
        fields.sort();
        fields
    }
}
fn representation(value: &Value) -> Result<&str, serde_json::Error> {
    value
        .get("kind")
        .and_then(Value::as_str)
        .ok_or_else(|| invalid("capture payload has no string kind"))
}
fn pointer(key: &str) -> String {
    key.replace('~', "~0").replace('/', "~1")
}
fn extend_fields(extra: &Map<String, Value>, path: &str, fields: &mut Vec<String>) {
    fields.extend(extra.keys().map(|key| format!("{path}/{}", pointer(key))));
}

/// An observation keeps material-location identity. Host, lane and sequence
/// describe the producer envelope and never become a replacement key.
#[derive(Clone, Debug, PartialEq, crate::EventPayload)]
#[event(version = 2, type = "lake.observed", key = location, replace = repeat, time = observed_at, quality = captured)]
pub struct LakeObservation {
    pub observed_at: Timestamp,
    lane: String,
    pub host: String,
    pub seq: i64,
    payload: CapturePayload,
    pub raw_ref: Option<String>,
}
impl LakeObservation {
    pub fn lane(&self) -> &str {
        &self.lane
    }
    pub fn payload(&self) -> &CapturePayload {
        &self.payload
    }
    pub fn new(
        observed_at: Timestamp,
        lane: String,
        host: String,
        seq: i64,
        payload: Value,
        raw_ref: Option<String>,
    ) -> Result<Self, serde_json::Error> {
        let payload = CapturePayload::decode(&lane, payload)?;
        Ok(Self {
            observed_at,
            lane,
            host,
            seq,
            payload,
            raw_ref,
        })
    }
}
#[derive(Deserialize)]
struct WireRecord {
    observed_at: Timestamp,
    lane: String,
    host: String,
    seq: i64,
    payload: Value,
    raw_ref: Option<String>,
}
impl<'de> Deserialize<'de> for LakeObservation {
    fn deserialize<D: Deserializer<'de>>(de: D) -> Result<Self, D::Error> {
        let row = WireRecord::deserialize(de)?;
        Self::new(
            row.observed_at,
            row.lane,
            row.host,
            row.seq,
            row.payload,
            row.raw_ref,
        )
        .map_err(serde::de::Error::custom)
    }
}
impl Serialize for LakeObservation {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        #[derive(Serialize)]
        struct Row<'a> {
            observed_at: Timestamp,
            lane: &'a str,
            host: &'a str,
            seq: i64,
            payload: &'a CapturePayload,
            raw_ref: &'a Option<String>,
            #[serde(skip_serializing_if = "std::ops::Not::not")]
            untyped: bool,
            #[serde(skip_serializing_if = "Vec::is_empty")]
            unknown_fields: Vec<String>,
        }
        Row {
            observed_at: self.observed_at,
            lane: &self.lane,
            host: &self.host,
            seq: self.seq,
            payload: &self.payload,
            raw_ref: &self.raw_ref,
            untyped: matches!(self.payload, CapturePayload::Unknown(_)),
            unknown_fields: self.payload.unknown_fields(),
        }
        .serialize(serializer)
    }
}
impl JsonSchema for LakeObservation {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "LakeObservation".into()
    }
    fn json_schema(generator: &mut schemars::SchemaGenerator) -> schemars::Schema {
        #[derive(JsonSchema)]
        #[allow(dead_code)]
        struct CaptureObservedSchema {
            observed_at: Timestamp,
            lane: String,
            host: String,
            seq: i64,
            payload: CapturePayload,
            raw_ref: Option<String>,
            #[serde(default)]
            untyped: bool,
            #[serde(default)]
            unknown_fields: Vec<String>,
        }
        CaptureObservedSchema::json_schema(generator)
    }
}
#[cfg(test)]
mod tests;
