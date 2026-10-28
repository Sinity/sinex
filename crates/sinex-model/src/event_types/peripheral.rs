//! Devices attached to the machine, sampled periodically: the monitor's
//! settings, battery levels, Bluetooth audio links. Each sample is one event;
//! a sample listing several devices is one event per device.
use crate::Timestamp;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

/// A monitor's settings, read over DDC/CI.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(version = 2, type = "display.sampled", key = location, replace = repeat, time = sampled_at, quality = captured)]
pub struct DisplaySampled {
    pub sampled_at: Timestamp,
    pub brightness: Option<i64>,
    pub contrast: Option<i64>,
    /// The power mode code (VCP 0xD6).
    pub power_mode: Option<i64>,
    /// The input source code (VCP 0x60).
    pub input_source: Option<i64>,
    #[serde(default, skip_serializing_if = "Map::is_empty")]
    pub extra: Map<String, Value>,
}

/// One device's battery level.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(version = 2, type = "battery.sampled", key = location, replace = repeat, time = sampled_at, quality = captured)]
pub struct BatterySampled {
    pub sampled_at: Timestamp,
    /// The device's name.
    pub device: Option<String>,
    /// Its Bluetooth address or its receiver unit ID.
    pub device_id: Option<String>,
    /// What reported it (`bluez`, `solaar`).
    pub reported_by: String,
    pub percent: Option<f64>,
    pub millivolts: Option<i64>,
    /// The device's own charge status (`FULL`, `DISCHARGING`…).
    pub status: Option<String>,
    #[serde(default, skip_serializing_if = "Map::is_empty")]
    pub extra: Map<String, Value>,
}

/// One Bluetooth audio link's state.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(version = 2, type = "bluetooth_audio.sampled", key = location, replace = repeat, time = sampled_at, quality = captured)]
pub struct BluetoothAudioSampled {
    pub sampled_at: Timestamp,
    pub device: Option<String>,
    pub device_address: Option<String>,
    /// The audio profile's service UUID.
    pub profile: Option<String>,
    /// `idle`, `pending` or `active`.
    pub state: Option<String>,
    pub codec: Option<i64>,
    pub volume: Option<i64>,
    #[serde(default, skip_serializing_if = "Map::is_empty")]
    pub extra: Map<String, Value>,
}
