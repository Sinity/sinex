//! Capture from the session bus, shared by the D-Bus sources (notifications,
//! media players). `busctl --user monitor --json=short` prints one JSON object
//! per message that matches; capture spools those lines on `LiveStream`
//! exactly as they arrive. Parsers decode the message and its payload, whose
//! values `busctl` writes as `{"type": <signature>, "data": <value>}`.
use serde_json::{Map, Value};
use sinex_model::InputRef;
use sinex_source_core::drivers::live_stream::{self, LiveStream, Rotation, SpoolWriter};
use std::io::BufReader;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::Duration;

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
    mut initial: impl FnMut(&sinex_source_core::capture::CaptureStop) -> Vec<Vec<u8>> + Send,
    spool: &Path,
    stop: &sinex_source_core::capture::CaptureStop,
) -> std::io::Result<u64> {
    let mut writer = SpoolWriter::open(spool, Rotation::default())?;
    writer.capture(stop, |writer, stop| {
        let permit = writer.read_permit()?;
        let initial = writer
            .record(stop, |recording| Ok(initial(recording)))?
            .unwrap_or_default();
        let mut initial_count = 0;
        for line in &initial {
            let dropped = writer.dropped;
            writer.append_read(permit, sinex_model::Timestamp::now(), line)?;
            initial_count += u64::from(writer.dropped == dropped);
        }
        writer.sync()?;
        let mut command = Command::new("busctl");
        command.args(["--user", "monitor", "--json=short"]);
        for m in matches {
            command.arg("--match").arg(m);
        }
        command
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::null());
        let Some(mut child) = stop.spawn(&mut command)? else {
            return Ok(initial_count);
        };
        let stdout = child.stdout().expect("piped stdout");
        let n = live_stream::capture_lines(BufReader::new(stdout), writer, Duration::from_secs(1))?;
        let _ = child.wait();
        Ok(n + initial_count)
    })
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
        &sinex_source_core::capture::CaptureStop::new(None),
        destination,
        path,
        interface,
        method,
        args,
    )
}

pub(crate) fn call_until(
    stop: &sinex_source_core::capture::CaptureStop,
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
                    "--user",
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
    names_until(&sinex_source_core::capture::CaptureStop::new(None))
}

pub(crate) fn names_until(stop: &sinex_source_core::capture::CaptureStop) -> Vec<String> {
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

/// One monitored message: its kind, member, interface, path, sender and
/// decoded payload arguments.
pub struct Message {
    pub kind: String,
    pub member: String,
    pub interface: String,
    pub path: String,
    pub sender: String,
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
            args,
        })
    }
}

#[cfg(test)]
mod tests;
