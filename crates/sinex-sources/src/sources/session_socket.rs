//! Producers inside the session that cannot speak NATS themselves (a Neovim
//! plugin, a kitty watcher) write one JSON object per line to a Unix socket
//! `sinex-session` listens on. Capture spools each line as received, on
//! `LiveStream`, and `sinex-session run` produces the spool over the NATS
//! ingress like every other session source. The integrations are in
//! `integrations/neovim` and `integrations/kitty`.
//!
//! A line names its producer in `source` (`neovim`, `kitty`) and its
//! happening in `event`; the rest are fields of that producer. kitty's
//! `scrollback` lines carry a window's new scrollback lines and become
//! `scrollback.captured`, the type the lake's snapshots have; its other
//! lines are `terminal.reported`, a `cmd_end` with the command's output.
use crate::sources::terminal_text::strip_escapes;
use serde_json::{Map, Value};
use sinex_model::event_types::machine::ScrollbackCaptured;
use sinex_model::event_types::session::{EditorReport, TerminalReport};
use sinex_model::{Class, InputRef, Timestamp};
use sinex_source_core::drivers::live_stream::{self, LiveStream, Rotation, SpoolWriter};
use sinex_source_core::registry::{DeclaredSource, Driver, SourceSpec};
use sinex_source_core::{Parser, RecordError};
use std::io::{BufRead, BufReader};
use std::net::Shutdown;
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

pub const SPEC: SourceSpec = SourceSpec {
    name: "session.integrations",
    driver: Driver::LiveStream,
    excisable: true,
    purpose: "recall: files edited, commands run in the terminal and what they printed",
    // Paths, command lines and terminal output.
    class: Class::Guarded,
    parser: "session-socket",
    parser_version: 2,
    event_types: &[
        "editor.reported",
        "terminal.reported",
        "scrollback.captured",
    ],
    framer: sinex_source_core::Framer::incremental(sinex_source_core::drivers::append_file::lines),
};

sinex_model::payload_enum! {
    #[derive(Clone, Debug, PartialEq)]
    pub enum Produced {
        Editor(EditorReport),
        Terminal(TerminalReport),
        Scrollback(ScrollbackCaptured),
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
/// `seconds` pass (forever otherwise). Stopping closes reception on accepted
/// connections and drains their queued bytes before returning the lines captured.
pub fn capture(socket: &Path, spool: &Path, seconds: Option<Duration>) -> std::io::Result<u64> {
    capture_until(
        socket,
        spool,
        &sinex_source_core::capture::CaptureStop::new(seconds),
    )
}

fn capture_until(
    socket: &Path,
    spool: &Path,
    stop: &sinex_source_core::capture::CaptureStop,
) -> std::io::Result<u64> {
    if let Some(dir) = socket.parent() {
        std::fs::create_dir_all(dir)?;
    }
    // A socket left by a previous run is stale: nothing listens on it.
    let _ = std::fs::remove_file(socket);
    let listener = UnixListener::bind(socket)?;
    let result = (|| {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(socket, std::fs::Permissions::from_mode(0o600))?;
        let writer = Arc::new(Mutex::new(SpoolWriter::open(spool, Rotation::default())?));
        capture_connections(listener, writer, || stop.stopped())
    })();
    let cleanup = std::fs::remove_file(socket);
    match result {
        Ok(count) => cleanup.map(|()| count),
        Err(error) => Err(error),
    }
}

const ACCEPT_BATCH: usize = 32;

fn capture_connections(
    listener: UnixListener,
    writer: Arc<Mutex<SpoolWriter>>,
    mut stop: impl FnMut() -> bool,
) -> std::io::Result<u64> {
    listener.set_nonblocking(true)?;
    let mut handlers = Vec::new();
    let mut count = 0;
    let mut failure = None;
    loop {
        // Check stop between bounded batches even when new clients keep
        // the accept queue populated. Only accepted readers are drained.
        for _ in 0..ACCEPT_BATCH {
            match listener.accept() {
                Ok((stream, _)) => {
                    let stop = match stream.try_clone() {
                        Ok(stop) => stop,
                        Err(error) => {
                            failure = Some(error);
                            break;
                        }
                    };
                    let writer = writer.clone();
                    match std::thread::Builder::new()
                        .spawn(move || capture_connection(stream, &writer))
                    {
                        Ok(handler) => handlers.push((stop, handler)),
                        Err(error) => {
                            failure = Some(error);
                            break;
                        }
                    }
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => break,
                Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(error) => {
                    failure = Some(error);
                    break;
                }
            }
        }
        let mut i = 0;
        while i < handlers.len() {
            if handlers[i].1.is_finished() {
                let (_, handler) = handlers.swap_remove(i);
                match join_connection(handler) {
                    Ok(lines) => count += lines,
                    Err(error) => {
                        failure.get_or_insert(error);
                    }
                }
            } else {
                i += 1;
            }
        }
        if failure.is_some() || stop() {
            break;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    drop(listener);
    // Shut down reception before joining: queued bytes remain readable up
    // to EOF, while idle peers wake and future writes cannot extend the drain.
    for (stop, _) in &handlers {
        if let Err(error) = stop.shutdown(Shutdown::Read)
            && error.kind() != std::io::ErrorKind::NotConnected
        {
            failure.get_or_insert(error);
        }
    }
    for (_, handler) in handlers {
        match join_connection(handler) {
            Ok(lines) => count += lines,
            Err(error) => {
                failure.get_or_insert(error);
            }
        }
    }
    if let Some(error) = failure {
        return Err(error);
    }
    writer
        .lock()
        .map_err(|_| std::io::Error::other("spool lock poisoned"))?
        .sync()?;
    Ok(count)
}

fn join_connection(handler: std::thread::JoinHandle<std::io::Result<u64>>) -> std::io::Result<u64> {
    handler
        .join()
        .map_err(|_| std::io::Error::other("integration socket reader panicked"))?
}

fn capture_connection(stream: UnixStream, writer: &Mutex<SpoolWriter>) -> std::io::Result<u64> {
    stream.set_nonblocking(false)?;
    let mut reader = BufReader::new(stream);
    let mut line = Vec::new();
    let mut count = 0;
    loop {
        let ended = match reader.read_until(b'\n', &mut line) {
            Ok(0) => true,
            Ok(_) => false,
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(error),
        };
        let body = line.strip_suffix(b"\n").unwrap_or(&line);
        if !body.is_empty() {
            let mut writer = writer
                .lock()
                .map_err(|_| std::io::Error::other("spool lock poisoned"))?;
            let dropped = writer.dropped;
            writer.append(Timestamp::now(), body)?;
            writer.sync()?;
            count += u64::from(writer.dropped == dropped);
        }
        line.clear();
        if ended {
            return Ok(count);
        }
    }
}

pub fn driver(spool: PathBuf, host: String) -> LiveStream {
    LiveStream {
        spool,
        input: InputRef {
            kind: "socket".into(),
            locator: "sinex-session:integrations".into(),
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
        _context: &sinex_source_core::RecordContext<'_>,
    ) -> Result<(Vec<Produced>, ()), RecordError> {
        let (received_at, bytes) = live_stream::received(record).map_err(RecordError::new)?;
        let mut m: Map<String, Value> = serde_json::from_slice(&bytes)
            .map_err(|e| RecordError::new(format!("not a JSON object: {e}")))?;
        let source = take_str(&mut m, "source").ok_or_else(|| RecordError::new("no `source`"))?;
        let event = take_str(&mut m, "event").ok_or_else(|| RecordError::new("no `event`"))?;
        let out = match source.as_str() {
            "neovim" | "vim" => Produced::Editor(EditorReport {
                received_at,
                editor: source,
                event,
                path: take_str(&mut m, "path"),
                filetype: take_str(&mut m, "filetype"),
                cwd: take_str(&mut m, "cwd"),
                pid: take_i64(&mut m, "pid").and_then(|p| u32::try_from(p).ok()),
                extra: m,
            }),
            "kitty" if event == "scrollback" => Produced::Scrollback(scrollback(received_at, m)?),
            "kitty" => Produced::Terminal(TerminalReport {
                received_at,
                terminal: source,
                event,
                window_id: take_i64(&mut m, "window_id"),
                cwd: take_str(&mut m, "cwd"),
                cmdline: take_str(&mut m, "cmdline"),
                exit_status: take_i64(&mut m, "exit_status"),
                title: take_str(&mut m, "title"),
                output: take_str(&mut m, "output").map(|ansi| plain(&ansi)),
                extra: m,
            }),
            other => return Err(RecordError::new(format!("unknown producer `{other}`"))),
        };
        Ok((vec![out], ()))
    }
}

fn plain(ansi: &str) -> String {
    let mut text = String::with_capacity(ansi.len());
    strip_escapes(ansi, &mut text);
    text
}

/// A kitty window's lines from `from_line` on, of the `lines` it held: the
/// integration sends only lines it has not sent before. The text is read
/// like a lake snapshot's.
fn scrollback(
    received_at: Timestamp,
    mut m: Map<String, Value>,
) -> Result<ScrollbackCaptured, RecordError> {
    let window_id =
        take_i64(&mut m, "window_id").ok_or_else(|| RecordError::new("no `window_id`"))?;
    let count = |m: &mut Map<String, Value>, key: &str| {
        take_i64(m, key)
            .and_then(|n| u64::try_from(n).ok())
            .ok_or_else(|| RecordError::new(format!("no `{key}`")))
    };
    let lines = count(&mut m, "lines")?;
    let from_line = count(&mut m, "from_line")?;
    let text = crate::sources::kitty_scrollback::plain_lines(
        &take_str(&mut m, "text").unwrap_or_default(),
    );
    Ok(ScrollbackCaptured {
        captured_at: received_at,
        host: take_str(&mut m, "host"),
        kitty_pid: take_i64(&mut m, "kitty_pid").and_then(|p| u32::try_from(p).ok()),
        window_id,
        title: take_str(&mut m, "title"),
        cwd: take_str(&mut m, "cwd"),
        lines,
        continues: from_line > 0,
        new_lines: text.len() as u64,
        text: text.join("\n"),
    })
}

pub fn source() -> DeclaredSource<SessionSocketParser> {
    DeclaredSource {
        spec: SPEC,
        parser: SessionSocketParser,
    }
}

/// `--read integration-socket=SPOOL`: lines the editor and terminal
/// integrations write to the integration socket (the `socket` option, or the default path),
/// captured into SPOOL.
pub const FORMAT: sinex_source_core::binding::InputFormat =
    sinex_source_core::binding::InputFormat {
        name: "integration-socket",
        specs: || vec![&SPEC],
        plan: Some(|spool, settings| {
            Ok(sinex_source_core::binding::single(&SPEC, spool, settings))
        }),
        source: |_| Ok(Box::new(source())),
        reader: Some(|b| {
            Ok(sinex_source_core::binding::polled(driver(
                b.input.clone(),
                b.settings.host.clone(),
            )))
        }),
        capture: Some(|b, stop| {
            let socket = b
                .settings
                .option("socket")
                .map(PathBuf::from)
                .or_else(default_path)
                .ok_or("no integration socket path (XDG_RUNTIME_DIR unset)")?;
            capture_until(&socket, &b.input, stop).map_err(|e| e.to_string())
        }),
    };

#[cfg(test)]
mod tests;
