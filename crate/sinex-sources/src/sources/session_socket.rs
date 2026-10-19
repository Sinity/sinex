//! Producers inside the session that cannot speak NATS themselves (a Neovim
//! plugin, a kitty watcher) write one JSON object per line to a Unix socket
//! `sinex-session` listens on. Capture spools each line as received, on
//! `LiveStream`, and `sinex-session run` produces the spool over the NATS
//! ingress like every other session source. The integrations are in
//! `integrations/neovim` and `integrations/kitty`.
//!
//! A line names its producer in `source` (`neovim`, `kitty`) and its
//! happening in `event`; the rest are fields of that producer.
use crate::drivers::live_stream::{self, LiveStream, Rotation, SpoolWriter};
use crate::registry::{Declared, Driver, SourceSpec};
use crate::{Parser, RecordError};
use serde_json::{Map, Value};
use sinex_model::kinds::session::{EditorEvent, TerminalEvent};
use sinex_model::{Class, InputRef, Timestamp};
use std::io::{BufRead, BufReader};
use std::os::unix::net::UnixListener;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

pub const SPEC: SourceSpec = SourceSpec {
    name: "session.producers",
    driver: Driver::LiveStream,
    consumer: "recall: files edited, commands run in the terminal",
    // Paths and command lines.
    class: Class::Guarded,
    parser: "session-socket",
    parser_version: 1,
    event_types: &["editor.event", "terminal.event"],
    framer: crate::drivers::append_file::lines,
};

sinex_model::payload_enum! {
    #[derive(Clone, Debug, PartialEq)]
    pub enum Produced {
        Editor(EditorEvent),
        Terminal(TerminalEvent),
    }
}

/// `$SINEX_SESSION_SOCKET`, or `$XDG_RUNTIME_DIR/sinex/session.sock`.
pub fn default_path() -> Option<PathBuf> {
    if let Some(p) = std::env::var_os("SINEX_SESSION_SOCKET") {
        return Some(p.into());
    }
    Some(
        Path::new(&std::env::var_os("XDG_RUNTIME_DIR")?)
            .join("sinex")
            .join("session.sock"),
    )
}

/// Listen on `socket` and spool every line any connection writes, until
/// `seconds` pass (forever otherwise). Returns the lines captured.
pub fn capture(socket: &Path, spool: &Path, seconds: Option<Duration>) -> std::io::Result<u64> {
    if let Some(dir) = socket.parent() {
        std::fs::create_dir_all(dir)?;
    }
    // A socket left by a previous run is stale: nothing listens on it.
    let _ = std::fs::remove_file(socket);
    let listener = UnixListener::bind(socket)?;
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(socket, std::fs::Permissions::from_mode(0o600))?;
    listener.set_nonblocking(true)?;
    let writer = Arc::new(Mutex::new(SpoolWriter::open(spool, Rotation::default())?));
    let count = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let deadline = seconds.map(|s| Instant::now() + s);
    while deadline.is_none_or(|d| Instant::now() < d) {
        match listener.accept() {
            Ok((stream, _)) => {
                let (writer, count) = (writer.clone(), count.clone());
                std::thread::spawn(move || {
                    let _ = stream.set_nonblocking(false);
                    let mut reader = BufReader::new(stream);
                    let mut line = Vec::new();
                    while matches!(reader.read_until(b'\n', &mut line), Ok(n) if n > 0) {
                        let body = line.strip_suffix(b"\n").unwrap_or(&line);
                        if !body.is_empty() {
                            let mut w = writer.lock().expect("spool lock");
                            if w.append(Timestamp::now(), body)
                                .and_then(|()| w.sync())
                                .is_ok()
                            {
                                count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            }
                        }
                        line.clear();
                    }
                });
            }
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                std::thread::sleep(Duration::from_millis(50))
            }
            Err(e) => return Err(e),
        }
    }
    let _ = std::fs::remove_file(socket);
    // Let connections still open flush what they already sent.
    std::thread::sleep(Duration::from_millis(200));
    writer.lock().expect("spool lock").sync()?;
    Ok(count.load(std::sync::atomic::Ordering::Relaxed))
}

pub fn driver(spool: PathBuf, host: String) -> LiveStream {
    LiveStream {
        spool,
        input: InputRef {
            kind: "socket".into(),
            locator: "sinex-session:producers".into(),
            host,
        },
    }
}

pub struct SessionSocketParser;

fn take_str(m: &mut Map<String, Value>, k: &str) -> Option<String> {
    match m.remove(k) {
        Some(Value::String(s)) if !s.is_empty() => Some(s),
        Some(Value::Number(n)) => Some(n.to_string()),
        _ => None,
    }
}

fn take_i64(m: &mut Map<String, Value>, k: &str) -> Option<i64> {
    match m.remove(k) {
        Some(Value::Number(n)) => n.as_i64(),
        Some(Value::String(s)) => s.parse().ok(),
        _ => None,
    }
}

impl Parser for SessionSocketParser {
    type Record<'a> = Map<String, Value>;
    type Carry = ();
    type Payload = Produced;

    fn parse(
        &self,
        record: &Map<String, Value>,
        _: &(),
    ) -> Result<(Vec<Produced>, ()), RecordError> {
        let (received_at, bytes) = live_stream::received(record).map_err(RecordError::new)?;
        let mut m: Map<String, Value> = serde_json::from_slice(&bytes)
            .map_err(|e| RecordError::new(format!("not a JSON object: {e}")))?;
        let source = take_str(&mut m, "source").ok_or_else(|| RecordError::new("no `source`"))?;
        let event = take_str(&mut m, "event").ok_or_else(|| RecordError::new("no `event`"))?;
        let out = match source.as_str() {
            "neovim" | "vim" => Produced::Editor(EditorEvent {
                received_at,
                editor: source,
                event,
                path: take_str(&mut m, "path"),
                filetype: take_str(&mut m, "filetype"),
                cwd: take_str(&mut m, "cwd"),
                pid: take_i64(&mut m, "pid").and_then(|p| u32::try_from(p).ok()),
                extra: m,
            }),
            "kitty" => Produced::Terminal(TerminalEvent {
                received_at,
                terminal: source,
                event,
                window_id: take_i64(&mut m, "window_id"),
                cwd: take_str(&mut m, "cwd"),
                cmdline: take_str(&mut m, "cmdline"),
                exit_status: take_i64(&mut m, "exit_status"),
                title: take_str(&mut m, "title"),
                extra: m,
            }),
            other => return Err(RecordError::new(format!("unknown producer `{other}`"))),
        };
        Ok((vec![out], ()))
    }
}

pub fn source() -> Declared<SessionSocketParser> {
    Declared {
        spec: SPEC,
        parser: SessionSocketParser,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn parse(line: &str) -> Result<Produced, RecordError> {
        let rec: Map<String, Value> = serde_json::from_slice(&live_stream::frame(
            Timestamp::from_unix_ns(1).unwrap(),
            line.as_bytes(),
        ))
        .unwrap();
        SessionSocketParser
            .parse(&rec, &())
            .map(|(mut v, ())| v.remove(0))
    }

    #[test]
    fn neovim_and_kitty_lines_become_typed_events() {
        let Produced::Editor(e) = parse(r#"{"source":"neovim","event":"BufWritePost","path":"/a.rs","filetype":"rust","pid":42,"buf":3}"#).unwrap() else { panic!() };
        assert_eq!(
            (e.event.as_str(), e.path.as_deref(), e.pid),
            ("BufWritePost", Some("/a.rs"), Some(42))
        );
        assert_eq!(e.extra["buf"], 3);
        let Produced::Terminal(t) = parse(
            r#"{"source":"kitty","event":"cmd_end","window_id":7,"cmdline":"ls","exit_status":0}"#,
        )
        .unwrap() else {
            panic!()
        };
        assert_eq!(
            (t.window_id, t.exit_status, t.cmdline.as_deref()),
            (Some(7), Some(0), Some("ls"))
        );
    }

    #[test]
    fn unknown_producers_fail_by_name() {
        assert!(
            parse(r#"{"source":"emacs","event":"x"}"#)
                .unwrap_err()
                .to_string()
                .contains("emacs")
        );
        assert!(parse(r#"{"event":"x"}"#).is_err());
    }

    #[test]
    fn the_socket_spools_what_connections_write() {
        let dir = std::env::temp_dir().join(format!("sinex-sock-{}", uuid::Uuid::now_v7()));
        let socket = dir.join("s.sock");
        let spool = dir.join("spool");
        let (s2, sp2) = (socket.clone(), spool.clone());
        let h = std::thread::spawn(move || capture(&s2, &sp2, Some(Duration::from_millis(800))));
        let start = Instant::now();
        while !socket.exists() && start.elapsed() < Duration::from_secs(2) {
            std::thread::sleep(Duration::from_millis(10));
        }
        let mut c = std::os::unix::net::UnixStream::connect(&socket).unwrap();
        c.write_all(b"{\"source\":\"kitty\",\"event\":\"focus\"}\n{\"source\":\"neovim\",\"event\":\"BufEnter\"}\n").unwrap();
        drop(c);
        assert_eq!(h.join().unwrap().unwrap(), 2);
        let m = driver(spool, "h".into())
            .poll(&Value::Null)
            .unwrap()
            .material
            .unwrap();
        assert_eq!(m.records.len(), 2);
        let _ = std::fs::remove_dir_all(dir);
    }
}
