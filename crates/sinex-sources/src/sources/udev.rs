//! Devices, from udev on `LiveStream`. Capture starts `udevadm monitor --udev
//! --property`, then writes every device udev knows (`udevadm info
//! --export-db`) to the spool before the monitor's events, so the spool holds
//! what was present when capture started and every change after it. udev
//! keeps no history, so the spool and then the content store are what keep a
//! device event.
//!
//! Both commands print one block of lines per device, ended by an empty line;
//! capture spools each block as one record, exactly as printed. A monitor
//! block starts with a `UDEV  [<time>] <action> <path> (<subsystem>)` line
//! followed by `KEY=value` properties; an exported block has `P: <path>`,
//! `N: <node>` and `E: KEY=value` lines.
use serde_json::{Map, Value};
use sinex_model::event_types::system::DeviceChanged;
use sinex_model::json::JsonExt;
use sinex_model::{InputRef, Timestamp};
use sinex_source_core::RecordError;
use sinex_source_core::RecordResult;
use sinex_source_core::discovery::{CaptureStream, CapturedBy, DiscoverySpec, Location};
use sinex_source_core::drivers::live_stream::{self, LiveStream, Rotation, SpoolWriter};
use sinex_source_core::registry::{Expectation, Origin, Shape, SourceSpec};
use sinex_source_core::{Stateless, StatelessParser};
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

pub const SPEC: SourceSpec = SourceSpec {
    name: "machine.udev",
    origin: Origin::Input(Shape::LiveStream),
    expect: Expectation::continuous_days(30),
    purpose: "which devices were attached when, peripheral and storage history",
    parser: "udev-blocks",
    event_types: &["device.changed"],
    framer: sinex_source_core::Framer::LINES,
    ..SourceSpec::DEFAULT
};

pub struct UdevParser;

/// The first value of the first property present among `names`.
fn first(properties: &Map<String, Value>, names: &[&str]) -> Option<String> {
    names
        .iter()
        .find_map(|name| properties.str(*name))
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
}

/// One block as a device event, or `None` for a block that describes no
/// device (the monitor's opening lines).
fn device(received_at: Timestamp, block: &str) -> Result<Option<DeviceChanged>, RecordError> {
    let mut lines = block.lines().filter(|line| !line.is_empty());
    let Some(head) = lines.next() else {
        return Ok(None);
    };
    let mut properties = Map::new();
    let (action, path, node) = if head.starts_with("UDEV") {
        for line in lines {
            if let Some((name, value)) = line.split_once('=') {
                properties.insert(name.to_owned(), Value::String(value.to_owned()));
            }
        }
        let action = first(&properties, &["ACTION"]).record("udev event without ACTION")?;
        (action, first(&properties, &["DEVPATH"]), None)
    } else if let Some(path) = head.strip_prefix("P: ") {
        let mut node = None;
        for line in lines {
            if let Some(property) = line.strip_prefix("E: ") {
                if let Some((name, value)) = property.split_once('=') {
                    properties.insert(name.to_owned(), Value::String(value.to_owned()));
                }
            } else if let Some(name) = line.strip_prefix("N: ") {
                node = Some(format!("/dev/{name}"));
            }
        }
        ("present".to_owned(), Some(path.to_owned()), node)
    } else {
        return Ok(None);
    };
    let device = path.record("udev block without a device path")?;
    Ok(Some(DeviceChanged {
        received_at,
        action,
        device,
        subsystem: first(&properties, &["SUBSYSTEM"]),
        device_type: first(&properties, &["DEVTYPE"]),
        node: first(&properties, &["DEVNAME"]).or(node),
        driver: first(&properties, &["DRIVER", "ID_USB_DRIVER"]),
        sequence: first(&properties, &["SEQNUM"]).and_then(|n| n.parse().ok()),
        vendor: first(
            &properties,
            &["ID_VENDOR_FROM_DATABASE", "ID_VENDOR", "ID_VENDOR_ID"],
        ),
        model: first(
            &properties,
            &["ID_MODEL_FROM_DATABASE", "ID_MODEL", "ID_MODEL_ID"],
        ),
        serial: first(&properties, &["ID_SERIAL_SHORT", "ID_SERIAL"]),
        properties,
    }))
}

impl StatelessParser for UdevParser {
    type Record<'a> = Map<String, Value>;
    type Payload = DeviceChanged;

    fn parse(
        &self,
        record: &Map<String, Value>,

        _context: &sinex_source_core::RecordContext<'_>,
    ) -> Result<Vec<DeviceChanged>, RecordError> {
        let (received_at, bytes) = live_stream::received(record).map_err(RecordError::new)?;
        let block = std::str::from_utf8(&bytes).record("udev block is not UTF-8")?;
        Ok(device(received_at, block)?.into_iter().collect())
    }
}

pub fn driver(spool: PathBuf, host: String) -> LiveStream {
    LiveStream::new(
        spool,
        InputRef {
            kind: "netlink".into(),
            locator: "udev".into(),
            host,
        },
    )
}

/// Copy `reader`'s blocks into the spool, one record each, syncing at least
/// every `sync_every`. A last block without its empty line is complete when
/// the stream ends. Returns the number of blocks.
fn spool_blocks(
    mut reader: impl BufRead,
    writer: &mut SpoolWriter,
    sync_every: Duration,
) -> std::io::Result<u64> {
    let mut block = Vec::new();
    let mut line = Vec::new();
    let mut count = 0;
    let mut last_sync = Instant::now();
    loop {
        line.clear();
        let ended = reader.read_until(b'\n', &mut line)? == 0;
        let blank = line.iter().all(u8::is_ascii_whitespace);
        if !blank {
            block.extend_from_slice(&line);
        }
        if (blank || ended) && !block.is_empty() {
            let body = block.strip_suffix(b"\n").unwrap_or(&block);
            writer.append(Timestamp::now(), body)?;
            count += 1;
            block.clear();
        }
        if ended {
            break;
        }
        if last_sync.elapsed() >= sync_every {
            writer.sync()?;
            last_sync = Instant::now();
        }
    }
    writer.sync()?;
    Ok(count)
}

/// Capture into the spool until the monitor ends or capture stops. Returns
/// the number of blocks captured.
pub fn capture(
    spool: &Path,
    stop: &sinex_source_core::capture::CaptureStop,
) -> std::io::Result<u64> {
    let mut writer = SpoolWriter::open(spool, Rotation::default())?;
    // The monitor starts first, so a change while the database is exported
    // is spooled after it rather than lost.
    let mut monitor = Command::new("udevadm");
    monitor
        .args(["monitor", "--udev", "--property"])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null());
    let Some(mut child) = stop.spawn(&mut monitor)? else {
        return Ok(0);
    };
    let mut count = 0;
    if let Some(present) = stop.output(Command::new("udevadm").args(["info", "--export-db"]))? {
        count += spool_blocks(&present[..], &mut writer, Duration::from_secs(1))?;
    }
    let stdout = child.stdout().expect("piped stdout");
    count += spool_blocks(BufReader::new(stdout), &mut writer, Duration::from_secs(1))?;
    let status = child.wait()?;
    if !status.success() && !stop.stopped() {
        return Err(std::io::Error::other(format!(
            "udevadm monitor exited with {status}"
        )));
    }
    Ok(count)
}

super::input_format! {
    /// `stage --parser udev SPOOL`: device events from udev, captured into SPOOL.
    pub FORMAT = "udev" {
    specs: [SPEC],
    parser: |_b| Stateless(UdevParser),
    discovery: DiscoverySpec {
        captured_by: CapturedBy::System,
        locations: &[Location::new("spool", "udev")],
        capabilities: &["udevadm"],
        streams: &[CaptureStream { spool: "udev", options: &[], capabilities: &[] }],
        ..DiscoverySpec::DEFAULT
    },
    plan: single,
    reader: polled(driver),
    capture: |b, _context, stop| capture(&b.input, stop).map_err(|e| e.to_string()),
} }

#[cfg(test)]
mod tests;
