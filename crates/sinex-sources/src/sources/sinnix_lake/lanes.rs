//! Historical lake observations and domain records for lanes without a stream design.
use super::Envelope;
use serde::Deserialize;
use serde::de::DeserializeOwned;
use serde_json::{Map, Value};
use sinex_model::Timestamp;
use sinex_model::event_types::browser::{LinkFollowed, ReadingStackChanged};
use sinex_model::event_types::environment::AirSampled;
use sinex_model::event_types::machine::LakeObservation;
use sinex_model::event_types::peripheral::{BatterySampled, BluetoothAudioSampled, DisplaySampled};
use sinex_source_core::RecordError;

sinex_model::payload_enum! {
    #[derive(Clone, Debug, PartialEq)]
    pub enum LakeEvent {
        Observation(LakeObservation),
        Link(LinkFollowed),
        Reading(ReadingStackChanged),
        Display(DisplaySampled),
        Battery(BatterySampled),
        BluetoothAudio(BluetoothAudioSampled),
        Air(AirSampled),
    }
}

/// A time in seconds since the epoch, as the lake writes it. A float of
/// seconds holds about microseconds at today's epoch; whole seconds plus
/// rounded microseconds avoid inventing nanoseconds.
pub(super) fn seconds(value: f64) -> Result<Timestamp, RecordError> {
    if !value.is_finite() {
        return Err(RecordError::new(format!("time {value} is not a number")));
    }
    let whole = value.floor();
    let micros = ((value - whole) * 1e6).round() as i128;
    let nanos = (whole as i128)
        .checked_mul(1_000_000_000)
        .and_then(|whole| whole.checked_add(micros * 1_000))
        .ok_or_else(|| RecordError::new(format!("time {value} out of range")))?;
    Timestamp::from_unix_ns(nanos)
        .map_err(|e| RecordError::new(format!("time {value} out of range: {e}")))
}

fn decode<T: DeserializeOwned>(lane: &str, payload: &Value) -> Result<T, RecordError> {
    T::deserialize(payload).map_err(|e| RecordError::new(format!("{lane} payload: {e}")))
}

/// The events of one envelope record of `lane`.
pub(super) fn events(envelope: &Envelope) -> Result<Vec<LakeEvent>, RecordError> {
    let lane = envelope.lane.as_str();
    let payload = &envelope.payload;
    let at = envelope.observed_at;
    Ok(match lane {
        "clipboard" | "primary" | "notifications" | "mpris" | "a11y" | "idle-inhibitors"
        | "screen-frames" | "audio-devices" | "audio-topology" | "audio-index" | "netflow" => {
            vec![LakeEvent::Observation(LakeObservation {
                observed_at: at,
                lane: envelope.lane.clone(),
                host: envelope.host.clone(),
                seq: envelope.seq,
                payload: payload.clone(),
                raw_ref: envelope.raw_ref.clone(),
            })]
        }
        "browser-nav-edges" => vec![LakeEvent::Link(link(at, decode(lane, payload)?)?)],
        "reading-stack" => vec![LakeEvent::Reading(reading(at, decode(lane, payload)?)?)],
        "monitor" => vec![LakeEvent::Display(display(at, decode(lane, payload)?))],
        "bt-battery" => decode::<Vec<BluezBattery>>(lane, payload)?
            .into_iter()
            .map(|b| LakeEvent::Battery(bluez_battery(at, b)))
            .collect(),
        "logitech" => decode::<Logitech>(lane, payload)?
            .devices
            .into_iter()
            .flatten()
            .map(|d| LakeEvent::Battery(logitech_battery(at, d)))
            .collect(),
        "bt-audio" => decode::<Vec<BluetoothTransport>>(lane, payload)?
            .into_iter()
            .map(|t| LakeEvent::BluetoothAudio(bluetooth_audio(at, t)))
            .collect(),
        "awair" | "environment" => vec![LakeEvent::Air(air(at, decode(lane, payload)?))],
        other => return Err(RecordError::new(format!("lane {other:?} has no mapping"))),
    })
}

// Browsing.

#[derive(Deserialize)]
struct NavigationEdge {
    source_url: Option<String>,
    source_title: Option<String>,
    target_url: Option<String>,
    target_title: Option<String>,
    anchor_text: Option<String>,
    trigger: Option<String>,
    note: Option<String>,
    /// The extension's time for the click.
    ts: Option<f64>,
    #[serde(flatten)]
    extra: Map<String, Value>,
}

fn link(at: Timestamp, n: NavigationEdge) -> Result<LinkFollowed, RecordError> {
    Ok(LinkFollowed {
        followed_at: n.ts.map(seconds).transpose()?.unwrap_or(at),
        from_url: n.source_url,
        from_title: n.source_title,
        to_url: n.target_url,
        to_title: n.target_title,
        link_text: n.anchor_text,
        trigger: n.trigger,
        note: n.note,
        extra: n.extra,
    })
}

#[derive(Deserialize)]
struct ReadingItem {
    event: String,
    url: Option<String>,
    title: Option<String>,
    source_url: Option<String>,
    source_title: Option<String>,
    anchor_text: Option<String>,
    note: Option<String>,
    pushed_at: Option<f64>,
    #[serde(flatten)]
    extra: Map<String, Value>,
}

fn reading(at: Timestamp, r: ReadingItem) -> Result<ReadingStackChanged, RecordError> {
    Ok(ReadingStackChanged {
        changed_at: r.pushed_at.map(seconds).transpose()?.unwrap_or(at),
        action: r.event,
        url: r.url,
        title: r.title,
        from_url: r.source_url,
        from_title: r.source_title,
        link_text: r.anchor_text,
        note: r.note,
        extra: r.extra,
    })
}

// Devices.

#[derive(Deserialize)]
struct Monitor {
    brightness: Option<i64>,
    contrast: Option<i64>,
    power_sl: Option<i64>,
    input_sl: Option<i64>,
    #[serde(flatten)]
    extra: Map<String, Value>,
}

fn display(at: Timestamp, m: Monitor) -> DisplaySampled {
    DisplaySampled {
        sampled_at: at,
        brightness: m.brightness,
        contrast: m.contrast,
        power_mode: m.power_sl,
        input_source: m.input_sl,
        extra: m.extra,
    }
}

#[derive(Deserialize)]
struct BluezBattery {
    device_address: Option<String>,
    device_alias: Option<String>,
    source: Option<String>,
    percentage: Option<f64>,
    #[serde(flatten)]
    extra: Map<String, Value>,
}

fn bluez_battery(at: Timestamp, b: BluezBattery) -> BatterySampled {
    BatterySampled {
        sampled_at: at,
        device: b.device_alias,
        device_id: b.device_address,
        reported_by: b.source.unwrap_or_else(|| "bluez".into()),
        percent: b.percentage,
        millivolts: None,
        status: None,
        extra: b.extra,
    }
}

#[derive(Deserialize)]
struct Logitech {
    devices: Option<Vec<LogitechDevice>>,
}

#[derive(Deserialize)]
struct LogitechDevice {
    name: Option<String>,
    unit_id: Option<String>,
    battery_status: Option<String>,
    battery_percent: Option<f64>,
    battery_mv: Option<i64>,
    #[serde(flatten)]
    extra: Map<String, Value>,
}

fn logitech_battery(at: Timestamp, d: LogitechDevice) -> BatterySampled {
    BatterySampled {
        sampled_at: at,
        device: d.name,
        device_id: d.unit_id,
        reported_by: "solaar".into(),
        percent: d.battery_percent,
        millivolts: d.battery_mv,
        status: d.battery_status,
        extra: d.extra,
    }
}

#[derive(Deserialize)]
struct BluetoothTransport {
    device_address: Option<String>,
    device_alias: Option<String>,
    uuid: Option<String>,
    state: Option<String>,
    codec: Option<i64>,
    volume: Option<i64>,
    #[serde(flatten)]
    extra: Map<String, Value>,
}

fn bluetooth_audio(at: Timestamp, t: BluetoothTransport) -> BluetoothAudioSampled {
    BluetoothAudioSampled {
        sampled_at: at,
        device: t.device_alias,
        device_address: t.device_address,
        profile: t.uuid,
        state: t.state,
        codec: t.codec,
        volume: t.volume,
        extra: t.extra,
    }
}

// Room air.

#[derive(Deserialize)]
struct Air {
    /// The sensor's clock, RFC 3339.
    timestamp: Option<Timestamp>,
    score: Option<f64>,
    temp: Option<f64>,
    humid: Option<f64>,
    co2: Option<f64>,
    voc: Option<f64>,
    pm25: Option<f64>,
    #[serde(flatten)]
    extra: Map<String, Value>,
}

fn air(at: Timestamp, a: Air) -> AirSampled {
    AirSampled {
        sampled_at: a.timestamp.unwrap_or(at),
        score: a.score,
        temperature_c: a.temp,
        humidity: a.humid,
        co2_ppm: a.co2,
        voc_ppb: a.voc,
        pm25: a.pm25,
        extra: a.extra,
    }
}
