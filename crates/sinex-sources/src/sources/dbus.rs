//! Capture from D-Bus, shared by the D-Bus sources: notifications and media
//! players on the session bus, machine state on the system bus. `busctl
//! monitor --json=short` prints one JSON object per message that matches;
//! capture spools those lines on `LiveStream` exactly as they arrive. Parsers
//! decode the message and its payload, whose values `busctl` writes as
//! `{"type": <signature>, "data": <value>}`.
use crate::drivers::live_stream::{self, LiveStream, Rotation, SpoolWriter};
use serde_json::{Map, Value};
use sinex_model::InputRef;
use std::io::BufReader;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::Duration;

/// Which bus a capture watches.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Bus {
    /// The operator's session bus.
    Session,
    /// The system bus. Monitoring it needs root.
    System,
}

impl Bus {
    fn flag(self) -> &'static str {
        match self {
            Bus::Session => "--user",
            Bus::System => "--system",
        }
    }
}

/// The spool driver of a bus source.
pub fn driver(spool: PathBuf, host: String, locator: &str) -> LiveStream {
    LiveStream {
        spool,
        input: InputRef {
            kind: "dbus".into(),
            locator: locator.into(),
            host,
        },
    }
}

/// Spool `busctl monitor` output for `matches` (D-Bus match rules), after the
/// `initial` lines (a source's reading of current state, written first so the
/// spool holds what was observed at capture start). Runs until the monitor
/// ends or capture stops. Returns the number of lines captured.
pub fn capture(
    matches: &[&str],
    initial: &[Vec<u8>],
    spool: &Path,
    stop: &crate::capture::CaptureStop,
) -> std::io::Result<u64> {
    capture_on(Bus::Session, matches, initial, spool, stop)
}

/// [`capture`] on `bus`.
pub fn capture_on(
    bus: Bus,
    matches: &[&str],
    initial: &[Vec<u8>],
    spool: &Path,
    stop: &crate::capture::CaptureStop,
) -> std::io::Result<u64> {
    let mut writer = SpoolWriter::open(spool, Rotation::default())?;
    for line in initial {
        writer.append(sinex_model::Timestamp::now(), line)?;
    }
    writer.sync()?;
    let mut command = Command::new("busctl");
    command.args([bus.flag(), "monitor", "--json=short"]);
    for m in matches {
        command.arg("--match").arg(m);
    }
    command
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null());
    let Some(mut child) = stop.spawn(&mut command)? else {
        return Ok(initial.len() as u64);
    };
    let stdout = child.stdout().expect("piped stdout");
    let n =
        live_stream::capture_lines(BufReader::new(stdout), &mut writer, Duration::from_secs(1))?;
    let _ = child.wait();
    Ok(n + initial.len() as u64)
}

/// Call a method with `busctl --user call … --json=short` and return its
/// decoded reply, or `None` when the call fails (the name went away).
pub fn call(
    destination: &str,
    path: &str,
    interface: &str,
    method: &str,
    args: &[&str],
) -> Option<Value> {
    call_until(
        &crate::capture::CaptureStop::new(None),
        destination,
        path,
        interface,
        method,
        args,
    )
}

pub(crate) fn call_until(
    stop: &crate::capture::CaptureStop,
    destination: &str,
    path: &str,
    interface: &str,
    method: &str,
    args: &[&str],
) -> Option<Value> {
    call_on(
        Bus::Session,
        stop,
        destination,
        path,
        interface,
        method,
        args,
    )
}

/// [`call_until`] on `bus`.
pub(crate) fn call_on(
    bus: Bus,
    stop: &crate::capture::CaptureStop,
    destination: &str,
    path: &str,
    interface: &str,
    method: &str,
    args: &[&str],
) -> Option<Value> {
    let out = stop
        .output(
            Command::new("busctl")
                .args([
                    bus.flag(),
                    "--json=short",
                    "call",
                    destination,
                    path,
                    interface,
                    method,
                ])
                .args(args),
        )
        .ok()??;
    let reply: Value = serde_json::from_slice(&out).ok()?;
    Some(decode(reply.get("data")?))
}

/// Names on the session bus.
pub fn names() -> Vec<String> {
    names_until(&crate::capture::CaptureStop::new(None))
}

pub(crate) fn names_until(stop: &crate::capture::CaptureStop) -> Vec<String> {
    let Ok(Some(out)) =
        stop.output(Command::new("busctl").args(["--user", "--json=short", "list"]))
    else {
        return Vec::new();
    };
    serde_json::from_slice::<Vec<Map<String, Value>>>(&out)
        .unwrap_or_default()
        .into_iter()
        .filter_map(|n| n.get("name").and_then(Value::as_str).map(str::to_owned))
        .collect()
}

/// A D-Bus value as `busctl` writes it, with every variant wrapper
/// (`{"type": …, "data": …}`) replaced by its data.
pub fn decode(value: &Value) -> Value {
    match value {
        Value::Object(map)
            if map.len() == 2 && map.contains_key("type") && map.contains_key("data") =>
        {
            decode(&map["data"])
        }
        Value::Object(map) => {
            Value::Object(map.iter().map(|(k, v)| (k.clone(), decode(v))).collect())
        }
        Value::Array(items) => Value::Array(items.iter().map(decode).collect()),
        other => other.clone(),
    }
}

/// One monitored message: its kind, member, interface, path, sender,
/// destination, serial numbers and decoded payload arguments.
pub struct Message {
    pub kind: String,
    pub member: String,
    pub interface: String,
    pub path: String,
    pub sender: String,
    pub destination: String,
    /// The sender's serial number for this message (`cookie`).
    pub serial: Option<u64>,
    /// For a reply or error, the serial of the call it answers.
    pub reply_serial: Option<u64>,
    pub args: Vec<Value>,
}

impl Message {
    pub fn parse(line: &[u8]) -> Result<Self, String> {
        let m: Map<String, Value> =
            serde_json::from_slice(line).map_err(|e| format!("not a busctl JSON message: {e}"))?;
        let text = |k: &str| {
            m.get(k)
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned()
        };
        let args = match m.get("payload").and_then(|p| p.get("data")) {
            Some(Value::Array(items)) => items.iter().map(decode).collect(),
            Some(other) => vec![decode(other)],
            None => Vec::new(),
        };
        Ok(Self {
            kind: text("type"),
            member: text("member"),
            interface: text("interface"),
            path: text("path"),
            sender: text("sender"),
            destination: text("destination"),
            serial: m.get("cookie").and_then(Value::as_u64),
            reply_serial: m.get("reply_cookie").and_then(Value::as_u64),
            args,
        })
    }
}

#[cfg(test)]
mod tests;
