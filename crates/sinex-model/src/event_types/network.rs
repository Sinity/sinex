//! Network connections as the kernel's connection tracking reports them.
use crate::Timestamp;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

/// A tracked connection ended, with what it carried.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(type = "connection.ended", key = location, replace = repeat, time = ended_at, quality = captured)]
pub struct ConnectionEnded {
    pub ended_at: Timestamp,
    /// `tcp`, `udp`, `icmp`…
    pub protocol: Option<String>,
    pub source_address: Option<String>,
    pub source_port: Option<u16>,
    pub destination_address: Option<String>,
    pub destination_port: Option<u16>,
    pub bytes: Option<u64>,
    pub packets: Option<u64>,
    #[serde(default, skip_serializing_if = "Map::is_empty")]
    pub extra: Map<String, Value>,
}
