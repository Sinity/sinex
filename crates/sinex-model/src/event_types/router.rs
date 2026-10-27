//! What the home router reports, sampled over SSH: its log lines, its DHCP
//! leases, its per-device traffic totals and the Wi-Fi clients of each access
//! point. Each is one event per line, lease, row or client, with the time of
//! the sample that saw it (`sampled_at`), the host that took the sample and
//! its sequence number in the capture lake.
use crate::Timestamp;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

/// One line of the router's system log. Its time is the router's own clock;
/// a line without a readable time keeps it unknown.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(type = "router_log.line", key = location, replace = repeat, time = logged_at, quality = intrinsic)]
pub struct RouterLogLine {
    pub logged_at: Option<Timestamp>,
    /// `daemon`, `kern`, `authpriv`, ...
    pub facility: Option<String>,
    /// `err`, `notice`, `info`, ...
    pub severity: Option<String>,
    pub program: Option<String>,
    pub pid: Option<u32>,
    /// The text after the program; the whole line when it has no header.
    pub message: String,
    pub sampled_at: Timestamp,
    pub host: String,
    pub seq: i64,
}

/// One DHCP lease the router held when sampled.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(type = "dhcp_lease.sampled", key = location, replace = repeat, time = sampled_at, quality = captured)]
pub struct DhcpLeaseSampled {
    pub mac: String,
    pub ip: String,
    /// The name the device gave; unknown when it gave none.
    pub hostname: Option<String>,
    pub client_id: Option<String>,
    /// Unknown for a lease that does not expire.
    pub expires_at: Option<Timestamp>,
    pub sampled_at: Timestamp,
    pub host: String,
    pub seq: i64,
    #[serde(default, skip_serializing_if = "Map::is_empty")]
    pub extra: Map<String, Value>,
}

/// One row of the router's traffic accounting (nlbwmon): a device's
/// connections and bytes for one protocol over a period. Rows of the current
/// period are running totals, each its own sample; a closed period's row
/// is final and identified by the period and the row's fields, so a closed
/// month read again is a repeat.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct DeviceTrafficSampled {
    /// `current`, or the closed period's start as nlbwmon names it.
    pub period: String,
    pub mac: String,
    pub ip: String,
    /// 4 or 6.
    pub family: Option<u8>,
    pub proto: Option<String>,
    pub port: Option<u16>,
    /// The application protocol nlbwmon recognized, when it did.
    pub layer7: Option<String>,
    pub conns: Option<u64>,
    pub rx_bytes: Option<u64>,
    pub rx_pkts: Option<u64>,
    pub tx_bytes: Option<u64>,
    pub tx_pkts: Option<u64>,
    pub sampled_at: Timestamp,
    pub host: String,
    pub seq: i64,
    #[serde(default, skip_serializing_if = "Map::is_empty")]
    pub extra: Map<String, Value>,
}

impl DeviceTrafficSampled {
    pub const CURRENT_PERIOD: &'static str = "current";
}

impl crate::EventPayload for DeviceTrafficSampled {
    const EVENT_TYPE: &'static str = "device_traffic.sampled";
    const KEY: crate::KeyKind =
        crate::KeyKind::Fields(&["period", "mac", "ip", "family", "proto", "port", "layer7"]);
    const REPLACE: crate::ReplaceRule = crate::ReplaceRule::SuppressRepeat;

    fn key_kind(&self) -> crate::KeyKind {
        if self.period == Self::CURRENT_PERIOD {
            crate::KeyKind::Location
        } else {
            Self::KEY
        }
    }

    fn ts_orig(&self) -> crate::TsOrig {
        crate::TsOrig::known(self.sampled_at, crate::TimeQuality::Captured)
    }

    fn payload_schema() -> Value {
        serde_json::to_value(schemars::schema_for!(Self)).expect("schemas serialize")
    }
}

/// Received and sent counts of one kind.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct Directions {
    pub rx: Option<u64>,
    pub tx: Option<u64>,
}

/// One Wi-Fi client associated with an access point when sampled.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(type = "wifi_client.sampled", key = location, replace = repeat, time = sampled_at, quality = captured)]
pub struct WifiClientSampled {
    /// The access point's interface (`phy1-ap0`).
    pub ap: String,
    /// Its frequency in MHz.
    pub freq: Option<u32>,
    pub mac: String,
    /// Signal strength in dBm.
    pub signal: Option<i64>,
    pub aid: Option<u64>,
    pub assoc: Option<bool>,
    pub auth: Option<bool>,
    pub authorized: Option<bool>,
    pub bytes: Option<Directions>,
    pub packets: Option<Directions>,
    /// Bit rates in bits per second.
    pub rate: Option<Directions>,
    pub airtime: Option<Directions>,
    pub sampled_at: Timestamp,
    pub host: String,
    pub seq: i64,
    /// The rest of what hostapd reported (capabilities, standards).
    #[serde(default, skip_serializing_if = "Map::is_empty")]
    pub extra: Map<String, Value>,
}
