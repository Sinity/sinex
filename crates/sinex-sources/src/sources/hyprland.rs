//! Hyprland's event socket (`$XDG_RUNTIME_DIR/hypr/<instance>/.socket2.sock`),
//! on `LiveStream`. Each line is `NAME>>DATA`. Hyprland keeps no history of
//! these, so capture spools every line as it arrives and the stored line is the
//! only record.
//!
//! Hyprland sends many events twice, an old form and a `v2` form, for the
//! same change. Exactly one form of each is read, the one with more
//! information; the other is accounted as read with no event, so nothing is
//! stored twice. Focus is the exception: `activewindow` carries the class and
//! title and `activewindowv2` the address, so the first is carried until the
//! second arrives and they become one `window.focused`.
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use sinex_model::event_types::desktop::{
    DesktopOther, MonitorFocused, WindowClosed, WindowFocused, WindowMoved, WindowOpened,
    WindowTitleChanged, WorkspaceSwitched,
};
use sinex_model::{Class, InputRef, Timestamp};
use sinex_source_core::drivers::live_stream::{self, LiveStream, Rotation, SpoolWriter};
use sinex_source_core::registry::{DeclaredSource, Driver, SourceSpec};
use sinex_source_core::{Parser, RecordError};
use std::io::BufReader;
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::time::Duration;

pub const SPEC: SourceSpec = SourceSpec {
    name: "desktop.hyprland",
    driver: Driver::LiveStream,
    excisable: true,
    purpose: "focus intervals, attention and recall",
    // Window titles are free text that can contain anything.
    class: Class::Guarded,
    parser: "hyprland-socket2",
    parser_version: 1,
    event_types: &[
        "window.focused",
        "window.opened",
        "window.closed",
        "window.moved",
        "window.title_changed",
        "workspace.switched",
        "monitor.focused",
        "desktop.other",
    ],
    framer: sinex_source_core::Framer::incremental(sinex_source_core::drivers::append_file::lines),
};

sinex_model::payload_enum! {
    #[derive(Clone, Debug, PartialEq)]
    pub enum Desktop {
        Focused(WindowFocused),
        Opened(WindowOpened),
        Closed(WindowClosed),
        Moved(WindowMoved),
        TitleChanged(WindowTitleChanged),
        Workspace(WorkspaceSwitched),
        Monitor(MonitorFocused),
        Other(DesktopOther),
    }
}

/// Forms that duplicate another form of the same change, which is read instead.
const DUPLICATE_FORMS: &[&str] = &[
    "windowtitle",      // windowtitlev2 has the title
    "workspace",        // workspacev2 has the id too
    "movewindow",       // movewindowv2 has the id too
    "focusedmonv2",     // focusedmon has the workspace name
    "createworkspace",  // createworkspacev2
    "destroyworkspace", // destroyworkspacev2
    "moveworkspace",    // moveworkspacev2
    "activespecial",    // activespecialv2
    "screencast",       // screencastv2
];

/// `activewindow`'s class and title, waiting for `activewindowv2`.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Pending {
    pub class: String,
    pub title: String,
    pub received_ns: String,
}

pub struct HyprlandParser;

fn nonempty(s: &str) -> Option<String> {
    (!s.is_empty()).then(|| s.to_owned())
}

impl Parser for HyprlandParser {
    type Record<'a> = Map<String, Value>;
    type Carry = Option<Pending>;
    type Payload = Desktop;

    fn parse(
        &self,
        record: &Map<String, Value>,
        pending: &Option<Pending>,
        _context: &sinex_source_core::RecordContext<'_>,
    ) -> Result<(Vec<Desktop>, Option<Pending>), RecordError> {
        let (received_at, bytes) = live_stream::received(record).map_err(RecordError::new)?;
        let line = String::from_utf8(bytes)
            .map_err(|_| RecordError::new("Hyprland event is not UTF-8"))?;
        let mut out = Vec::new();
        // Anything but the second half of a focus change ends a waiting first
        // half: keep what it said rather than drop it.
        let flush = |out: &mut Vec<Desktop>, pending: &Option<Pending>| {
            if let Some(p) = pending {
                out.push(Desktop::Focused(WindowFocused {
                    received_at: pending_time(p).unwrap_or(received_at),
                    window: None,
                    class: nonempty(&p.class),
                    title: nonempty(&p.title),
                }));
            }
        };
        let Some((name, data)) = line.split_once(">>") else {
            flush(&mut out, pending);
            if !line.is_empty() {
                out.push(Desktop::Other(DesktopOther {
                    received_at,
                    name: String::new(),
                    data: line,
                }));
            }
            return Ok((out, None));
        };
        match name {
            "activewindow" => {
                flush(&mut out, pending);
                let (class, title) = data.split_once(',').unwrap_or((data, ""));
                let next = Pending {
                    class: class.to_owned(),
                    title: title.to_owned(),
                    received_ns: received_at.unix_ns().to_string(),
                };
                return Ok((out, Some(next)));
            }
            "activewindowv2" => {
                let (class, title, at) = match pending {
                    Some(p) => (
                        nonempty(&p.class),
                        nonempty(&p.title),
                        pending_time(p).unwrap_or(received_at),
                    ),
                    None => (None, None, received_at),
                };
                out.push(Desktop::Focused(WindowFocused {
                    received_at: at,
                    window: nonempty(data.trim()),
                    class,
                    title,
                }));
                return Ok((out, None));
            }
            _ => flush(&mut out, pending),
        }
        if DUPLICATE_FORMS.contains(&name) {
            return Ok((out, None));
        }
        let event = match name {
            // openwindow>>ADDRESS,WORKSPACENAME,CLASS,TITLE
            "openwindow" => {
                let mut parts = data.splitn(4, ',');
                Desktop::Opened(WindowOpened {
                    received_at,
                    window: parts.next().unwrap_or_default().to_owned(),
                    workspace_id: None,
                    workspace_name: parts.next().unwrap_or_default().to_owned(),
                    class: parts.next().unwrap_or_default().to_owned(),
                    title: parts.next().unwrap_or_default().to_owned(),
                })
            }
            "closewindow" => Desktop::Closed(WindowClosed {
                received_at,
                window: data.trim().to_owned(),
            }),
            // movewindowv2>>ADDRESS,WORKSPACEID,WORKSPACENAME
            "movewindowv2" => {
                let mut parts = data.splitn(3, ',');
                Desktop::Moved(WindowMoved {
                    received_at,
                    window: parts.next().unwrap_or_default().to_owned(),
                    workspace_id: parts.next().and_then(|s| s.trim().parse().ok()),
                    workspace_name: parts.next().unwrap_or_default().to_owned(),
                })
            }
            // windowtitlev2>>ADDRESS,TITLE
            "windowtitlev2" => {
                let (window, title) = data.split_once(',').unwrap_or((data, ""));
                Desktop::TitleChanged(WindowTitleChanged {
                    received_at,
                    window: window.to_owned(),
                    title: title.to_owned(),
                })
            }
            // workspacev2>>WORKSPACEID,WORKSPACENAME
            "workspacev2" => {
                let (id, name) = data.split_once(',').unwrap_or((data, ""));
                Desktop::Workspace(WorkspaceSwitched {
                    received_at,
                    workspace_id: id.trim().parse().ok(),
                    workspace_name: nonempty(name),
                })
            }
            // focusedmon>>MONITORNAME,WORKSPACENAME
            "focusedmon" => {
                let (monitor, workspace) = data.split_once(',').unwrap_or((data, ""));
                Desktop::Monitor(MonitorFocused {
                    received_at,
                    monitor: monitor.trim().to_owned(),
                    workspace_name: workspace.to_owned(),
                })
            }
            _ => Desktop::Other(DesktopOther {
                received_at,
                name: name.to_owned(),
                data: data.to_owned(),
            }),
        };
        out.push(event);
        Ok((out, None))
    }
}

fn pending_time(p: &Pending) -> Option<Timestamp> {
    p.received_ns
        .parse()
        .ok()
        .and_then(|ns| Timestamp::from_unix_ns(ns).ok())
}

pub fn source() -> DeclaredSource<HyprlandParser> {
    DeclaredSource {
        spec: SPEC,
        parser: HyprlandParser,
    }
}

/// The event socket of the Hyprland instance in this session's environment.
fn socket_path() -> Option<PathBuf> {
    let runtime = std::env::var_os("XDG_RUNTIME_DIR")?;
    let instance = std::env::var_os("HYPRLAND_INSTANCE_SIGNATURE")?;
    Some(
        Path::new(&runtime)
            .join("hypr")
            .join(instance)
            .join(".socket2.sock"),
    )
}

pub fn driver(spool: PathBuf, host: String) -> LiveStream {
    LiveStream {
        spool,
        input: InputRef {
            kind: "socket".into(),
            locator: "hyprland:socket2".into(),
            host,
        },
    }
}

/// Capture the socket into `spool` until the socket closes (Hyprland exited).
/// Returns the number of lines captured.
pub fn capture(socket: &Path, spool: &Path) -> std::io::Result<u64> {
    capture_until(
        socket,
        spool,
        &sinex_source_core::capture::CaptureStop::new(None),
    )
}

fn capture_until(
    socket: &Path,
    spool: &Path,
    stop: &sinex_source_core::capture::CaptureStop,
) -> std::io::Result<u64> {
    let mut writer = SpoolWriter::open(spool, Rotation::default())?;
    writer.capture(stop, |writer, active| {
        let stream = UnixStream::connect(socket)?;
        active.shutdown_on_stop(&stream)?;
        live_stream::capture_lines(BufReader::new(stream), writer, Duration::from_secs(1))
    })
}

/// `--read hyprland=SPOOL`: Hyprland's event socket, captured into SPOOL.
pub const FORMAT: sinex_source_core::binding::InputFormat =
    sinex_source_core::binding::InputFormat {
        name: "hyprland",
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
            let socket = socket_path().ok_or(
            "no Hyprland instance in this environment (HYPRLAND_INSTANCE_SIGNATURE or XDG_RUNTIME_DIR unset)",
        )?;
            capture_until(&socket, &b.input, stop).map_err(|e| e.to_string())
        }),
    };

#[cfg(test)]
mod tests;
