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
//! The shell integration (`integrations/shell`, sent by `sinex-session
//! report-command`) writes one `shell` line per finished command execution,
//! which becomes `command.reported`.
use crate::sources::terminal_text::strip_escapes;
use serde_json::{Map, Value};
use sinex_model::event_types::CommandReported;
use sinex_model::event_types::machine::ScrollbackCaptured;
use sinex_model::event_types::session::{EditorReport, TerminalReport};
use sinex_model::event_types::work::GitIdentity;
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
    purpose: "recall: files edited, commands run in the terminal and what they printed",
    // Paths, command lines and terminal output.
    class: Class::Guarded,
    parser: "session-socket",
    parser_version: 4,
    event_types: &[
        "editor.reported",
        "terminal.reported",
        "scrollback.captured",
        "command.reported",
    ],
    framer: sinex_source_core::Framer::incremental(sinex_source_core::drivers::append_file::lines),
};

sinex_model::payload_enum! {
    #[derive(Clone, Debug, PartialEq)]
    pub enum Produced {
        Editor(EditorReport),
        Terminal(TerminalReport),
        Scrollback(ScrollbackCaptured),
        Command(CommandReported),
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
            writer.append(Timestamp::now(), body)?;
            writer.sync()?;
            count += 1;
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
                execution: take_str(&mut m, "execution"),
                cwd: take_str(&mut m, "cwd"),
                cmdline: take_str(&mut m, "cmdline"),
                exit_status: take_i64(&mut m, "exit_status"),
                title: take_str(&mut m, "title"),
                output: take_str(&mut m, "output").map(|ansi| plain(&ansi)),
                extra: m,
            }),
            "shell" if event == "command" => Produced::Command(command(m)?),
            other => return Err(RecordError::new(format!("unknown producer `{other}`"))),
        };
        Ok((vec![out], ()))
    }
}

/// One finished execution the shell integration reported. The execution
/// token, shell, host, command and start are required; the rest may be
/// unknown.
fn command(mut m: Map<String, Value>) -> Result<CommandReported, RecordError> {
    let required = |m: &mut Map<String, Value>, key: &str| {
        take_str(m, key).ok_or_else(|| RecordError::new(format!("no `{key}`")))
    };
    let execution = required(&mut m, "execution")?;
    let shell = required(&mut m, "shell")?;
    let host = required(&mut m, "host")?;
    // The command line is kept exactly as the shell received it.
    let command = match m.remove("command") {
        Some(Value::String(command)) => command,
        _ => return Err(RecordError::new("no `command`")),
    };
    let started_at: Timestamp = m
        .remove("started_at")
        .and_then(|v| serde_json::from_value(v).ok())
        .ok_or_else(|| RecordError::new("no readable `started_at`"))?;
    let git_identity = match m.remove("git_identity") {
        None | Some(Value::Null) => None,
        Some(value) => Some(
            serde_json::from_value::<GitIdentity>(value)
                .map_err(|e| RecordError::new(format!("unreadable `git_identity`: {e}")))?,
        ),
    };
    Ok(CommandReported {
        execution,
        shell,
        host,
        command,
        cwd: take_str(&mut m, "cwd"),
        started_at,
        duration_ns: take_i64(&mut m, "duration_ns"),
        exit_code: take_i64(&mut m, "exit_code"),
        session: take_str(&mut m, "session"),
        atuin_id: take_str(&mut m, "atuin_id"),
        git_identity,
        extra: m,
    })
}

/// One finished execution as the shell integration reports it, before it
/// is written to the socket.
#[derive(Clone, Debug, PartialEq)]
pub struct ShellReport {
    pub execution: String,
    pub shell: String,
    pub host: String,
    pub command: String,
    pub cwd: Option<String>,
    pub started_at: Timestamp,
    pub duration_ns: Option<i64>,
    pub exit_code: Option<i64>,
    pub session: Option<String>,
    pub atuin_id: Option<String>,
    pub git_identity: Option<GitIdentity>,
}

/// The socket line for a report: what [`SessionSocketParser`] reads as
/// `command.reported`.
pub fn shell_line(report: &ShellReport) -> Value {
    let mut line = Map::new();
    line.insert("source".into(), Value::from("shell"));
    line.insert("event".into(), Value::from("command"));
    line.insert("execution".into(), Value::from(report.execution.clone()));
    line.insert("shell".into(), Value::from(report.shell.clone()));
    line.insert("host".into(), Value::from(report.host.clone()));
    line.insert("command".into(), Value::from(report.command.clone()));
    line.insert(
        "started_at".into(),
        serde_json::to_value(report.started_at).expect("times serialize"),
    );
    let mut optional = |key: &str, value: Option<Value>| {
        if let Some(value) = value {
            line.insert(key.into(), value);
        }
    };
    optional("cwd", report.cwd.clone().map(Value::from));
    optional("duration_ns", report.duration_ns.map(Value::from));
    optional("exit_code", report.exit_code.map(Value::from));
    optional("session", report.session.clone().map(Value::from));
    optional("atuin_id", report.atuin_id.clone().map(Value::from));
    optional(
        "git_identity",
        report
            .git_identity
            .as_ref()
            .map(|identity| serde_json::to_value(identity).expect("identities serialize")),
    );
    Value::Object(line)
}

/// Repository facts beside `cwd`, observed now: none when it is not in a
/// repository or a probe fails or takes longer than `timeout`.
pub fn git_identity(cwd: &Path, timeout: Duration) -> Option<GitIdentity> {
    let git = |args: &[&str]| -> Option<String> {
        let mut child = std::process::Command::new("git")
            .arg("--no-optional-locks")
            .arg("-C")
            .arg(cwd)
            .args(args)
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::null())
            .spawn()
            .ok()?;
        let deadline = std::time::Instant::now() + timeout;
        loop {
            match child.try_wait().ok()? {
                Some(status) if status.success() => break,
                Some(_) => return None,
                None if std::time::Instant::now() >= deadline => {
                    let _ = child.kill();
                    let _ = child.wait();
                    return None;
                }
                None => std::thread::sleep(Duration::from_millis(5)),
            }
        }
        let mut out = String::new();
        std::io::Read::read_to_string(&mut child.stdout.take()?, &mut out).ok()?;
        Some(out.trim().to_owned())
    };
    let common_dir = git(&["rev-parse", "--path-format=absolute", "--git-common-dir"])?;
    let mut root_commits: Vec<String> = git(&["rev-list", "--max-parents=0", "HEAD"])?
        .lines()
        .map(str::to_owned)
        .collect();
    root_commits.sort();
    Some(GitIdentity {
        common_dir,
        root_commits,
        head: git(&["rev-parse", "HEAD"]),
        branch: git(&["symbolic-ref", "--quiet", "--short", "HEAD"]),
    })
}

/// Write one line to the integration socket. When nothing listens, or the
/// write takes longer than `timeout`, the line is dropped: a shell never
/// waits on Sinex.
pub fn send(socket: &Path, line: &Value, timeout: Duration) -> std::io::Result<()> {
    use std::io::Write;
    let mut stream = UnixStream::connect(socket)?;
    stream.set_write_timeout(Some(timeout))?;
    let mut bytes = serde_json::to_vec(line)?;
    bytes.push(b'\n');
    stream.write_all(&bytes)
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
        extra: m,
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
        discovery: sinex_source_core::discovery::DiscoverySpec {
            owner: sinex_source_core::discovery::Owner::Session,
            locations: &[],
            capabilities: &["runtime"],
            credentials: &[],
            streams: &[sinex_source_core::discovery::CaptureStream {
                spool: "integrations",
                options: &[],
                capabilities: &[],
            }],
            requests: &[],
            request_only: false,
        },
        discover: crate::discovery::discover_format,
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
