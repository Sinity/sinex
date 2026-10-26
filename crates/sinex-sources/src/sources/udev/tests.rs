use super::*;

fn rec(block: &str) -> Map<String, Value> {
    serde_json::from_slice(&live_stream::frame(
        Timestamp::from_unix_secs(1_700_000_000).unwrap(),
        block.as_bytes(),
    ))
    .unwrap()
}

fn read(block: &str) -> Result<Vec<DeviceChanged>, RecordError> {
    UdevParser
        .parse(&rec(block), &(), &crate::RecordContext::none())
        .map(|(out, ())| out)
}

const MONITOR_ADD: &str =
    "UDEV  [5024.112233] add      /devices/pci0000:00/0000:00:14.0/usb1/1-4 (usb)
ACTION=add
DEVPATH=/devices/pci0000:00/0000:00:14.0/usb1/1-4
SUBSYSTEM=usb
DEVNAME=/dev/bus/usb/001/007
DEVTYPE=usb_device
DRIVER=usb
SEQNUM=4410
ID_VENDOR=Example
ID_VENDOR_FROM_DATABASE=Example Devices Ltd.
ID_MODEL=Keyboard
ID_SERIAL_SHORT=0001";

#[devkit::test]
fn a_monitored_event_is_a_device_change() {
    let out = read(MONITOR_ADD).unwrap();
    assert_eq!(out.len(), 1);
    let d = &out[0];
    assert_eq!(d.action, "add");
    assert_eq!(d.device, "/devices/pci0000:00/0000:00:14.0/usb1/1-4");
    assert_eq!(d.subsystem.as_deref(), Some("usb"));
    assert_eq!(d.device_type.as_deref(), Some("usb_device"));
    assert_eq!(d.node.as_deref(), Some("/dev/bus/usb/001/007"));
    assert_eq!(d.driver.as_deref(), Some("usb"));
    assert_eq!(d.sequence, Some(4410));
    assert_eq!(d.vendor.as_deref(), Some("Example Devices Ltd."));
    assert_eq!(d.model.as_deref(), Some("Keyboard"));
    assert_eq!(d.serial.as_deref(), Some("0001"));
    assert_eq!(d.properties["ID_VENDOR"], "Example");
    assert_eq!(d.received_at.unix_ns(), 1_700_000_000_000_000_000);
}

#[devkit::test]
fn an_exported_device_is_present_with_its_node() {
    let out = read(
        "P: /devices/virtual/block/loop0
M: loop0
N: loop0
U: block
E: DEVPATH=/devices/virtual/block/loop0
E: SUBSYSTEM=block
E: DEVTYPE=disk",
    )
    .unwrap();
    let d = &out[0];
    assert_eq!(d.action, "present");
    assert_eq!(d.device, "/devices/virtual/block/loop0");
    assert_eq!(d.node.as_deref(), Some("/dev/loop0"));
    assert_eq!(d.subsystem.as_deref(), Some("block"));
    assert_eq!(d.sequence, None, "a read, not an event");
    assert_eq!(d.vendor, None, "missing stays unknown");
}

#[devkit::test]
fn the_monitors_opening_lines_are_no_device() {
    let out = read(
        "monitor will print the received events for:
UDEV - the event which udev sends out after rule processing",
    )
    .unwrap();
    assert!(out.is_empty());
}

#[devkit::test]
fn an_event_without_an_action_is_a_failure() {
    assert!(read("UDEV  [1.0] add /devices/x (usb)\nDEVPATH=/devices/x").is_err());
}

#[devkit::test]
fn capture_spools_one_record_per_block() {
    let spool = std::env::temp_dir().join(format!("sinex-udev-{}", uuid::Uuid::now_v7()));
    let mut writer = SpoolWriter::open(&spool, Rotation::default()).unwrap();
    let stream = format!("header line\n\n{MONITOR_ADD}\n\n\nP: /devices/x\nE: SUBSYSTEM=y");
    let count = spool_blocks(stream.as_bytes(), &mut writer, Duration::from_secs(60)).unwrap();
    assert_eq!(count, 3, "the last block ends with the stream");
    let segment = live_stream::segments(&spool).unwrap()[0];
    let text = std::fs::read_to_string(spool.join(format!("{segment:012}.ndjson"))).unwrap();
    let blocks: Vec<String> = text
        .lines()
        .map(|line| {
            let record: Map<String, Value> = serde_json::from_str(line).unwrap();
            String::from_utf8(live_stream::received(&record).unwrap().1).unwrap()
        })
        .collect();
    assert_eq!(blocks[1], MONITOR_ADD);
    assert_eq!(blocks[2], "P: /devices/x\nE: SUBSYSTEM=y");
    std::fs::remove_dir_all(&spool).unwrap();
}
