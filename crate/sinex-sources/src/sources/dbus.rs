//! Capture from the session bus, shared by the D-Bus sources (notifications,
//! media players). `busctl --user monitor --json=short` prints one JSON object
//! per message that matches; capture spools those lines on `LiveStream`
//! exactly as they arrive. Parsers decode the message and its payload, whose
//! values `busctl` writes as `{"type": <signature>, "data": <value>}`.
use crate::drivers::live_stream::{self, LiveStream, Rotation, SpoolWriter};
use serde_json::{Map, Value};
use sinex_model::InputRef;
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
/// ends or `seconds` pass. Returns the number of lines captured.
pub fn capture(
    matches: &[&str],
    initial: &[Vec<u8>],
    spool: &Path,
    seconds: Option<Duration>,
) -> std::io::Result<u64> {
    let mut writer = SpoolWriter::open(spool, Rotation::default())?;
    for line in initial {
        writer.append(sinex_model::Timestamp::now(), line)?;
    }
    writer.sync()?;
    let mut command = Command::new("busctl");
    command.args(["--user", "monitor", "--json=short"]);
    for m in matches {
        command.arg("--match").arg(m);
    }
    let mut child = command
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()?;
    let stdout = child.stdout.take().expect("piped stdout");
    let pid = child.id();
    if let Some(seconds) = seconds {
        std::thread::spawn(move || {
            std::thread::sleep(seconds);
            // Ending the monitor ends its output; everything it printed is
            // already in the spool.
            let _ = Command::new("kill").arg(pid.to_string()).status();
        });
    }
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
    let out = Command::new("busctl")
        .args([
            "--user",
            "--json=short",
            "call",
            destination,
            path,
            interface,
            method,
        ])
        .args(args)
        .stderr(Stdio::null())
        .output()
        .ok()?;
    if !out.status.success() {
        return None;
    }
    let reply: Value = serde_json::from_slice(&out.stdout).ok()?;
    Some(decode(reply.get("data")?))
}

/// Names on the session bus.
pub fn names() -> Vec<String> {
    let Ok(out) = Command::new("busctl")
        .args(["--user", "--json=short", "list"])
        .stderr(Stdio::null())
        .output()
    else {
        return Vec::new();
    };
    serde_json::from_slice::<Vec<Map<String, Value>>>(&out.stdout)
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
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn variants_decode_to_their_data() {
        let v = json!({"urgency": {"type": "y", "data": 1}, "list": {"type": "as", "data": ["a"]},
                       "nested": {"type": "a{sv}", "data": {"k": {"type": "s", "data": "v"}}}});
        assert_eq!(
            decode(&v),
            json!({"urgency": 1, "list": ["a"], "nested": {"k": "v"}})
        );
    }

    #[test]
    fn a_monitored_call_parses() {
        let line = br#"{"type":"method_call","sender":":1.2","path":"/org/freedesktop/Notifications","interface":"org.freedesktop.Notifications","member":"Notify","payload":{"type":"susssasa{sv}i","data":["app",0,"","s","b",[],{"urgency":{"type":"y","data":1}},1500]}}"#;
        let m = Message::parse(line).unwrap();
        assert_eq!(m.member, "Notify");
        assert_eq!(m.args.len(), 8);
        assert_eq!(m.args[6], json!({"urgency": 1}));
    }
}
