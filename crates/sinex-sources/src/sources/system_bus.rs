//! Machine state on the system bus: logind (sessions, sleep, shutdown),
//! NetworkManager, BlueZ and UPower, on `LiveStream`. Capture first writes one
//! snapshot line per service present (its objects and their properties, from
//! `GetManagedObjects`, or UPower's devices), then spools the services'
//! signals through `busctl --system monitor`, which needs root.
//!
//! Every captured signal is one event. Network, Bluetooth and battery changes
//! are read as those; every other signal is a `bus.signal` with its decoded
//! arguments. NetworkManager names a device only by its object path in a
//! state change, so the parser carries each device's kernel interface name
//! forward from the snapshot and from devices added later.
use super::dbus::{self, Bus, Message};
use crate::drivers::live_stream::{self, LiveStream};
use crate::registry::{DeclaredSource, Driver, SourceSpec};
use crate::{Parser, RecordError};
use serde_json::{Map, Value, json};
use sinex_model::event_types::system::{
    BatteryChanged, BluetoothDeviceChanged, BusSignal, NetworkChanged, NetworkDeviceChanged,
};
use sinex_model::{Class, Timestamp};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

pub const SPEC: SourceSpec = SourceSpec {
    name: "machine.system-bus",
    driver: Driver::LiveStream,
    excisable: false,
    purpose: "machine timeline: network, Bluetooth devices, batteries, sessions",
    // Device names and addresses, network names, login sessions.
    class: Class::Guarded,
    parser: "dbus-system",
    parser_version: 1,
    event_types: &[
        "bus.signal",
        "network.changed",
        "network.device_changed",
        "bluetooth.device_changed",
        "battery.changed",
    ],
    framer: crate::Framer::incremental(crate::drivers::append_file::lines),
};

/// The match rules capture monitors: each service's objects, and the object
/// manager signals that add and remove them.
pub const MATCHES: &[&str] = &[
    "type=signal,path_namespace=/org/freedesktop/login1",
    "type=signal,path_namespace=/org/freedesktop/NetworkManager",
    "type=signal,interface=org.freedesktop.DBus.ObjectManager,arg0path=/org/freedesktop/NetworkManager/",
    "type=signal,path_namespace=/org/bluez",
    "type=signal,interface=org.freedesktop.DBus.ObjectManager,arg0path=/org/bluez/",
    "type=signal,path_namespace=/org/freedesktop/UPower",
];

const NETWORK_MANAGER: &str = "org.freedesktop.NetworkManager";
const NETWORK_DEVICE: &str = "org.freedesktop.NetworkManager.Device";
const BLUETOOTH_DEVICE: &str = "org.bluez.Device1";
const BLUETOOTH_BATTERY: &str = "org.bluez.Battery1";
const POWER_DEVICE: &str = "org.freedesktop.UPower.Device";

sinex_model::payload_enum! {
    #[derive(Clone, Debug, PartialEq)]
    pub enum SystemBusReading {
        Signal(BusSignal),
        Network(NetworkChanged),
        NetworkDevice(NetworkDeviceChanged),
        Bluetooth(BluetoothDeviceChanged),
        Battery(BatteryChanged),
    }
}

pub struct SystemBusParser;

/// NetworkManager's overall state names.
fn network_state(code: u32) -> &'static str {
    match code {
        10 => "asleep",
        20 => "disconnected",
        30 => "disconnecting",
        40 => "connecting",
        50 => "connected_local",
        60 => "connected_site",
        70 => "connected_global",
        _ => "unknown",
    }
}

/// NetworkManager's device state names.
fn device_state(code: u32) -> &'static str {
    match code {
        10 => "unmanaged",
        20 => "unavailable",
        30 => "disconnected",
        40 => "prepare",
        50 => "config",
        60 => "need_auth",
        70 => "ip_config",
        80 => "ip_check",
        90 => "secondaries",
        100 => "activated",
        110 => "deactivating",
        120 => "failed",
        _ => "unknown",
    }
}

/// UPower's battery state names.
fn power_state(code: u64) -> &'static str {
    match code {
        1 => "charging",
        2 => "discharging",
        3 => "empty",
        4 => "fully_charged",
        5 => "pending_charge",
        6 => "pending_discharge",
        _ => "unknown",
    }
}

fn code(value: Option<&Value>) -> Option<u32> {
    value
        .and_then(Value::as_u64)
        .and_then(|n| u32::try_from(n).ok())
}

/// A Bluetooth device's address from its object path
/// (`/org/bluez/hci0/dev_AA_BB_CC_DD_EE_FF`).
fn address_from_path(path: &str) -> Option<String> {
    let last = path.rsplit('/').next()?;
    last.strip_prefix("dev_").map(|a| a.replace('_', ":"))
}

/// A Bluetooth device event from its properties by interface.
fn bluetooth(
    received_at: Timestamp,
    path: &str,
    change: &str,
    properties: Map<String, Value>,
) -> BluetoothDeviceChanged {
    let device = properties.get(BLUETOOTH_DEVICE).and_then(Value::as_object);
    let field = |name: &str| device.and_then(|d| d.get(name));
    BluetoothDeviceChanged {
        received_at,
        device: path.to_owned(),
        address: field("Address")
            .and_then(Value::as_str)
            .map(str::to_owned)
            .or_else(|| address_from_path(path)),
        change: change.to_owned(),
        name: field("Alias")
            .or_else(|| field("Name"))
            .and_then(Value::as_str)
            .map(str::to_owned),
        connected: field("Connected").and_then(Value::as_bool),
        paired: field("Paired").and_then(Value::as_bool),
        battery_percent: properties
            .get(BLUETOOTH_BATTERY)
            .and_then(|b| b.get("Percentage"))
            .and_then(Value::as_u64)
            .and_then(|p| u8::try_from(p).ok()),
        properties,
    }
}

fn battery(received_at: Timestamp, path: &str, properties: &Map<String, Value>) -> BatteryChanged {
    let seconds = |name: &str| properties.get(name).and_then(Value::as_i64);
    BatteryChanged {
        received_at,
        device: path.to_owned(),
        percentage: properties.get("Percentage").and_then(Value::as_f64),
        state: properties
            .get("State")
            .and_then(Value::as_u64)
            .map(|s| power_state(s).to_owned()),
        time_to_empty_s: seconds("TimeToEmpty"),
        time_to_full_s: seconds("TimeToFull"),
        properties: properties.clone(),
    }
}

fn network_device(
    received_at: Timestamp,
    path: &str,
    interfaces: &BTreeMap<String, String>,
    state: u32,
    previous: Option<u32>,
    reason: Option<u32>,
    snapshot: bool,
) -> NetworkDeviceChanged {
    NetworkDeviceChanged {
        received_at,
        device: path.to_owned(),
        interface: interfaces.get(path).cloned(),
        state: device_state(state).to_owned(),
        state_code: state,
        previous_state: previous.map(|p| device_state(p).to_owned()),
        reason_code: reason,
        snapshot,
    }
}

/// The events of one snapshot line: every device and the network state.
fn snapshot(
    received_at: Timestamp,
    objects: &Map<String, Value>,
    interfaces: &mut BTreeMap<String, String>,
) -> Vec<SystemBusReading> {
    let mut out = Vec::new();
    for (path, by_interface) in objects {
        let Some(by_interface) = by_interface.as_object() else {
            continue;
        };
        if let Some(manager) = by_interface.get(NETWORK_MANAGER)
            && let Some(state) = code(manager.get("State"))
        {
            out.push(SystemBusReading::Network(NetworkChanged {
                received_at,
                state: network_state(state).to_owned(),
                state_code: state,
                snapshot: true,
            }));
        }
        if let Some(device) = by_interface.get(NETWORK_DEVICE) {
            if let Some(name) = device.get("Interface").and_then(Value::as_str) {
                interfaces.insert(path.clone(), name.to_owned());
            }
            if let Some(state) = code(device.get("State")) {
                // `StateReason` is (state, reason).
                let reason = code(device.get("StateReason").and_then(|r| r.get(1)));
                out.push(SystemBusReading::NetworkDevice(network_device(
                    received_at,
                    path,
                    interfaces,
                    state,
                    None,
                    reason,
                    true,
                )));
            }
        }
        if by_interface.contains_key(BLUETOOTH_DEVICE) {
            out.push(SystemBusReading::Bluetooth(bluetooth(
                received_at,
                path,
                "present",
                by_interface.clone(),
            )));
        }
        if let Some(Value::Object(properties)) = by_interface.get(POWER_DEVICE) {
            out.push(SystemBusReading::Battery(battery(
                received_at,
                path,
                properties,
            )));
        }
    }
    out
}

/// One monitored signal's event.
fn signal(
    received_at: Timestamp,
    m: Message,
    interfaces: &mut BTreeMap<String, String>,
) -> Option<SystemBusReading> {
    if m.kind != "signal" {
        return None;
    }
    let arg = |i: usize| m.args.get(i);
    let read = match (m.interface.as_str(), m.member.as_str()) {
        (NETWORK_MANAGER, "StateChanged") => code(arg(0)).map(|state| {
            SystemBusReading::Network(NetworkChanged {
                received_at,
                state: network_state(state).to_owned(),
                state_code: state,
                snapshot: false,
            })
        }),
        (NETWORK_DEVICE, "StateChanged") => code(arg(0)).map(|state| {
            SystemBusReading::NetworkDevice(network_device(
                received_at,
                &m.path,
                interfaces,
                state,
                code(arg(1)),
                code(arg(2)),
                false,
            ))
        }),
        ("org.freedesktop.DBus.Properties", "PropertiesChanged") => {
            let interface = arg(0).and_then(Value::as_str).unwrap_or_default();
            let changed = arg(1)
                .and_then(Value::as_object)
                .cloned()
                .unwrap_or_default();
            match interface {
                BLUETOOTH_DEVICE | BLUETOOTH_BATTERY => {
                    Some(SystemBusReading::Bluetooth(bluetooth(
                        received_at,
                        &m.path,
                        "changed",
                        Map::from_iter([(interface.to_owned(), Value::Object(changed))]),
                    )))
                }
                POWER_DEVICE => Some(SystemBusReading::Battery(battery(
                    received_at,
                    &m.path,
                    &changed,
                ))),
                NETWORK_DEVICE => {
                    if let Some(name) = changed.get("Interface").and_then(Value::as_str) {
                        interfaces.insert(m.path.clone(), name.to_owned());
                    }
                    None
                }
                _ => None,
            }
        }
        ("org.freedesktop.DBus.ObjectManager", "InterfacesAdded") => {
            let path = arg(0).and_then(Value::as_str).unwrap_or_default();
            let added = arg(1)
                .and_then(Value::as_object)
                .cloned()
                .unwrap_or_default();
            if let Some(name) = added
                .get(NETWORK_DEVICE)
                .and_then(|d| d.get("Interface"))
                .and_then(Value::as_str)
            {
                interfaces.insert(path.to_owned(), name.to_owned());
            }
            added
                .contains_key(BLUETOOTH_DEVICE)
                .then(|| SystemBusReading::Bluetooth(bluetooth(received_at, path, "added", added)))
        }
        ("org.freedesktop.DBus.ObjectManager", "InterfacesRemoved") => {
            let path = arg(0).and_then(Value::as_str).unwrap_or_default();
            let removed: Vec<&str> = arg(1)
                .and_then(Value::as_array)
                .map(|names| names.iter().filter_map(Value::as_str).collect())
                .unwrap_or_default();
            if removed.contains(&NETWORK_DEVICE) {
                interfaces.remove(path);
            }
            removed.contains(&BLUETOOTH_DEVICE).then(|| {
                let names = removed.iter().map(|n| json!(n)).collect();
                SystemBusReading::Bluetooth(bluetooth(
                    received_at,
                    path,
                    "removed",
                    Map::from_iter([("removed".to_owned(), Value::Array(names))]),
                ))
            })
        }
        _ => None,
    };
    Some(read.unwrap_or(SystemBusReading::Signal(BusSignal {
        received_at,
        sender: m.sender,
        path: m.path,
        interface: m.interface,
        member: m.member,
        args: m.args,
    })))
}

impl Parser for SystemBusParser {
    type Record<'a> = Map<String, Value>;
    /// NetworkManager device object path to kernel interface name.
    type Carry = BTreeMap<String, String>;
    type Payload = SystemBusReading;

    fn parse(
        &self,
        record: &Map<String, Value>,
        interfaces: &BTreeMap<String, String>,
        _context: &crate::RecordContext<'_>,
    ) -> Result<(Vec<SystemBusReading>, BTreeMap<String, String>), RecordError> {
        let (received_at, bytes) = live_stream::received(record).map_err(RecordError::new)?;
        let mut interfaces = interfaces.clone();
        let value: Value = serde_json::from_slice(&bytes)
            .map_err(|e| RecordError::new(format!("not JSON: {e}")))?;
        if let Some(snap) = value.get("snapshot") {
            let objects = snap
                .get("objects")
                .and_then(Value::as_object)
                .ok_or_else(|| RecordError::new("snapshot without objects"))?;
            return Ok((snapshot(received_at, objects, &mut interfaces), interfaces));
        }
        let m = Message::parse(&bytes).map_err(RecordError::new)?;
        let out = signal(received_at, m, &mut interfaces)
            .into_iter()
            .collect();
        Ok((out, interfaces))
    }
}

pub fn source() -> DeclaredSource<SystemBusParser> {
    DeclaredSource {
        spec: SPEC,
        parser: SystemBusParser,
    }
}

pub fn driver(spool: PathBuf, host: String) -> LiveStream {
    dbus::driver(spool, host, "dbus:system")
}

/// One snapshot line per service present: its objects and their
/// properties, by object path and interface.
fn snapshots(stop: &crate::capture::CaptureStop) -> Vec<Vec<u8>> {
    let managed = |service: &str, path: &str| {
        dbus::call_on(
            Bus::System,
            stop,
            service,
            path,
            "org.freedesktop.DBus.ObjectManager",
            "GetManagedObjects",
            &[],
        )
        .and_then(|reply| reply.get(0).cloned())
    };
    let mut out: Vec<(&str, Value)> = Vec::new();
    if let Some(objects) = managed(NETWORK_MANAGER, "/org/freedesktop") {
        out.push((NETWORK_MANAGER, objects));
    }
    if let Some(objects) = managed("org.bluez", "/") {
        out.push(("org.bluez", objects));
    }
    let upower = "org.freedesktop.UPower";
    if let Some(Value::Array(devices)) = dbus::call_on(
        Bus::System,
        stop,
        upower,
        "/org/freedesktop/UPower",
        upower,
        "EnumerateDevices",
        &[],
    )
    .and_then(|reply| reply.get(0).cloned())
    {
        let objects: Map<String, Value> = devices
            .iter()
            .filter_map(Value::as_str)
            .filter_map(|path| {
                let properties = dbus::call_on(
                    Bus::System,
                    stop,
                    upower,
                    path,
                    "org.freedesktop.DBus.Properties",
                    "GetAll",
                    &["s", POWER_DEVICE],
                )?
                .get(0)
                .cloned()?;
                Some((path.to_owned(), json!({ POWER_DEVICE: properties })))
            })
            .collect();
        out.push((upower, Value::Object(objects)));
    }
    out.into_iter()
        .filter_map(|(service, objects)| {
            serde_json::to_vec(&json!({"snapshot": {"service": service, "objects": objects}})).ok()
        })
        .collect()
}

pub fn capture(spool: &Path, stop: &crate::capture::CaptureStop) -> std::io::Result<u64> {
    dbus::capture_on(Bus::System, MATCHES, &snapshots(stop), spool, stop)
}

/// `--read system-bus=SPOOL`: machine state on the system bus, captured into
/// SPOOL.
pub const FORMAT: crate::binding::InputFormat = crate::binding::InputFormat {
    name: "system-bus",
    specs: || vec![&SPEC],
    plan: Some(|spool, settings| Ok(crate::binding::single(&SPEC, spool, settings))),
    source: |_| Ok(Box::new(source())),
    reader: Some(|b| {
        Ok(crate::binding::stateless(driver(
            b.input.clone(),
            b.settings.host.clone(),
        )))
    }),
    capture: Some(|b, stop| capture(&b.input, stop).map_err(|e| e.to_string())),
};

#[cfg(test)]
mod tests;
