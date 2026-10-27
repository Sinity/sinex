use super::*;

fn rec(line: &Value) -> Map<String, Value> {
    serde_json::from_slice(&live_stream::frame(
        Timestamp::from_unix_secs(1_700_000_000).unwrap(),
        line.to_string().as_bytes(),
    ))
    .unwrap()
}

/// A signal as `busctl monitor --json=short` prints it.
fn signal_line(path: &str, interface: &str, member: &str, signature: &str, args: Value) -> Value {
    json!({"type": "signal", "endian": "l", "flags": 1, "version": 1, "cookie": 7,
           "timestamp-realtime": 1_700_000_000_000_000u64, "sender": ":1.9", "path": path,
           "interface": interface, "member": member,
           "payload": {"type": signature, "data": args}})
}

fn read(
    line: Value,
    interfaces: &BTreeMap<String, String>,
) -> (Vec<SystemBusReading>, BTreeMap<String, String>) {
    SystemBusParser
        .parse(&rec(&line), interfaces, &crate::RecordContext::none())
        .unwrap()
}

fn variant(signature: &str, data: Value) -> Value {
    json!({"type": signature, "data": data})
}

#[devkit::test]
fn a_snapshot_reads_the_network_and_its_devices() {
    let line = json!({"snapshot": {"service": "org.freedesktop.NetworkManager", "objects": {
        "/org/freedesktop/NetworkManager": {
            "org.freedesktop.NetworkManager": {"State": 70}
        },
        "/org/freedesktop/NetworkManager/Devices/2": {
            "org.freedesktop.NetworkManager.Device": {"Interface": "eth0", "State": 100, "StateReason": [100, 0]}
        },
        "/org/freedesktop/NetworkManager/AccessPoint/1": {
            "org.freedesktop.NetworkManager.AccessPoint": {"Strength": 40}
        }
    }}});
    let (out, interfaces) = read(line, &BTreeMap::new());
    assert_eq!(out.len(), 2, "{out:?}");
    assert!(out.iter().any(
        |r| matches!(r, SystemBusReading::Network(n) if n.state == "connected_global" && n.snapshot)
    ));
    let device = out
        .iter()
        .find_map(|r| match r {
            SystemBusReading::NetworkDevice(d) => Some(d),
            _ => None,
        })
        .unwrap();
    assert_eq!(device.interface.as_deref(), Some("eth0"));
    assert_eq!(
        (device.state.as_str(), device.reason_code),
        ("activated", Some(0))
    );
    assert_eq!(
        interfaces.get("/org/freedesktop/NetworkManager/Devices/2"),
        Some(&"eth0".to_owned())
    );
}

#[devkit::test]
fn a_device_state_change_names_the_interface_the_parser_carried() {
    let interfaces = BTreeMap::from([(
        "/org/freedesktop/NetworkManager/Devices/2".to_owned(),
        "eth0".to_owned(),
    )]);
    let (out, _) = read(
        signal_line(
            "/org/freedesktop/NetworkManager/Devices/2",
            "org.freedesktop.NetworkManager.Device",
            "StateChanged",
            "uuu",
            json!([30, 100, 39]),
        ),
        &interfaces,
    );
    let SystemBusReading::NetworkDevice(d) = &out[0] else {
        panic!("{out:?}");
    };
    assert_eq!(d.interface.as_deref(), Some("eth0"));
    assert_eq!(d.state, "disconnected");
    assert_eq!(d.previous_state.as_deref(), Some("activated"));
    assert_eq!(d.reason_code, Some(39));
    assert!(!d.snapshot);
    assert_eq!(d.received_at.unix_ns(), 1_700_000_000_000_000_000);
}

#[devkit::test]
fn the_network_state_changes() {
    let (out, _) = read(
        signal_line(
            "/org/freedesktop/NetworkManager",
            "org.freedesktop.NetworkManager",
            "StateChanged",
            "u",
            json!([20]),
        ),
        &BTreeMap::new(),
    );
    assert!(
        matches!(&out[0], SystemBusReading::Network(n) if n.state == "disconnected" && n.state_code == 20)
    );
}

#[devkit::test]
fn a_bluetooth_device_connects_and_reports_its_battery() {
    let path = "/org/bluez/hci0/dev_AA_BB_CC_DD_EE_01";
    let (out, _) = read(
        signal_line(
            path,
            "org.freedesktop.DBus.Properties",
            "PropertiesChanged",
            "sa{sv}as",
            json!(["org.bluez.Device1", {"Connected": variant("b", json!(true))}, []]),
        ),
        &BTreeMap::new(),
    );
    let SystemBusReading::Bluetooth(b) = &out[0] else {
        panic!("{out:?}");
    };
    assert_eq!(b.address.as_deref(), Some("AA:BB:CC:DD:EE:01"));
    assert_eq!((b.change.as_str(), b.connected), ("changed", Some(true)));
    assert_eq!(b.paired, None, "not sent, so unknown");
    let (out, _) = read(
        signal_line(
            path,
            "org.freedesktop.DBus.Properties",
            "PropertiesChanged",
            "sa{sv}as",
            json!(["org.bluez.Battery1", {"Percentage": variant("y", json!(80))}, []]),
        ),
        &BTreeMap::new(),
    );
    assert!(matches!(&out[0], SystemBusReading::Bluetooth(b) if b.battery_percent == Some(80)));
}

#[devkit::test]
fn bluetooth_devices_appear_and_go_away() {
    let path = "/org/bluez/hci0/dev_AA_BB_CC_DD_EE_02";
    let (out, _) = read(
        signal_line(
            "/",
            "org.freedesktop.DBus.ObjectManager",
            "InterfacesAdded",
            "oa{sa{sv}}",
            json!([path, {"org.bluez.Device1": {"Address": variant("s", json!("AA:BB:CC:DD:EE:02")),
                                                 "Name": variant("s", json!("Speaker")),
                                                 "Paired": variant("b", json!(false))}}]),
        ),
        &BTreeMap::new(),
    );
    assert!(
        matches!(&out[0], SystemBusReading::Bluetooth(b) if b.change == "added" && b.name.as_deref() == Some("Speaker") && b.paired == Some(false))
    );
    let (out, _) = read(
        signal_line(
            "/",
            "org.freedesktop.DBus.ObjectManager",
            "InterfacesRemoved",
            "oas",
            json!([
                path,
                ["org.bluez.Device1", "org.freedesktop.DBus.Properties"]
            ]),
        ),
        &BTreeMap::new(),
    );
    assert!(
        matches!(&out[0], SystemBusReading::Bluetooth(b) if b.change == "removed" && b.address.as_deref() == Some("AA:BB:CC:DD:EE:02"))
    );
}

#[devkit::test]
fn a_battery_change_names_its_state() {
    let (out, _) = read(
        signal_line(
            "/org/freedesktop/UPower/devices/battery_BAT0",
            "org.freedesktop.DBus.Properties",
            "PropertiesChanged",
            "sa{sv}as",
            json!(["org.freedesktop.UPower.Device",
                   {"Percentage": variant("d", json!(41.5)), "State": variant("u", json!(2)),
                    "TimeToEmpty": variant("x", json!(5400))}, []]),
        ),
        &BTreeMap::new(),
    );
    let SystemBusReading::Battery(b) = &out[0] else {
        panic!("{out:?}");
    };
    assert_eq!(b.percentage, Some(41.5));
    assert_eq!(b.state.as_deref(), Some("discharging"));
    assert_eq!((b.time_to_empty_s, b.time_to_full_s), (Some(5400), None));
}

#[devkit::test]
fn other_signals_are_kept_as_bus_signals() {
    let (out, _) = read(
        signal_line(
            "/org/freedesktop/login1",
            "org.freedesktop.login1.Manager",
            "PrepareForSleep",
            "b",
            json!([true]),
        ),
        &BTreeMap::new(),
    );
    let SystemBusReading::Signal(s) = &out[0] else {
        panic!("{out:?}");
    };
    assert_eq!(s.member, "PrepareForSleep");
    assert_eq!(s.args, vec![json!(true)]);
    // A device renamed: the carry follows, and the signal is kept.
    let (out, interfaces) = read(
        signal_line(
            "/org/freedesktop/NetworkManager/Devices/7",
            "org.freedesktop.DBus.Properties",
            "PropertiesChanged",
            "sa{sv}as",
            json!(["org.freedesktop.NetworkManager.Device", {"Interface": variant("s", json!("wg0"))}, []]),
        ),
        &BTreeMap::new(),
    );
    assert!(matches!(&out[0], SystemBusReading::Signal(_)));
    assert_eq!(
        interfaces.get("/org/freedesktop/NetworkManager/Devices/7"),
        Some(&"wg0".to_owned())
    );
}

#[devkit::test]
fn a_line_that_is_not_json_is_a_failure() {
    let record: Map<String, Value> = serde_json::from_slice(&live_stream::frame(
        Timestamp::from_unix_secs(1_700_000_000).unwrap(),
        b"not json",
    ))
    .unwrap();
    assert!(
        SystemBusParser
            .parse(&record, &BTreeMap::new(), &crate::RecordContext::none())
            .is_err()
    );
}
