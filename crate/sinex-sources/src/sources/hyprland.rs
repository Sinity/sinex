//! Hyprland's event socket (`$XDG_RUNTIME_DIR/hypr/<instance>/.socket2.sock`),
//! on `LiveStream`. Each line is `NAME>>DATA`. Hyprland keeps no history of
//! these, so capture spools every line as it arrives and the stored line is
//! the only record (`sn6s`).
//!
//! Hyprland sends many events twice, an old form and a `v2` form, for the
//! same change. Exactly one form of each is read, the one with more
//! information; the other is accounted as read with no event, so nothing is
//! stored twice. Focus is the exception: `activewindow` carries the class and
//! title and `activewindowv2` the address, so the first is carried until the
//! second arrives and they become one `window.focused`.
use crate::drivers::live_stream::{self, LiveStream, Rotation, SpoolWriter};
use crate::registry::{Declared, Driver, SourceSpec};
use crate::{Parser, RecordError};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use sinex_model::kinds::desktop::{
    DesktopOther, MonitorFocused, WindowClosed, WindowFocused, WindowMoved, WindowOpened,
    WindowTitleChanged, WorkspaceSwitched,
};
use sinex_model::{Class, InputRef, Timestamp};
use std::io::BufReader;
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::time::Duration;

pub const SPEC: SourceSpec = SourceSpec {
    name: "desktop.hyprland",
    driver: Driver::LiveStream,
    consumer: "focus intervals, attention and recall",
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
    framer: crate::drivers::append_file::lines,
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

pub fn source() -> Declared<HyprlandParser> {
    Declared {
        spec: SPEC,
        parser: HyprlandParser,
    }
}

/// The event socket of the Hyprland instance in this session's environment.
pub fn socket_path() -> Option<PathBuf> {
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
    let stream = UnixStream::connect(socket)?;
    let mut writer = SpoolWriter::open(spool, Rotation::default())?;
    live_stream::capture_lines(BufReader::new(stream), &mut writer, Duration::from_secs(1))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{DrainInput, Limits, Source};
    use sinex_model::{MaterialId, ReadingOrder, RecordOutcome};

    fn rec(line: &str, ns: i128) -> Map<String, Value> {
        serde_json::from_slice(&live_stream::frame(
            Timestamp::from_unix_ns(ns).unwrap(),
            line.as_bytes(),
        ))
        .unwrap()
    }

    fn parse_all(lines: &[&str]) -> Vec<Desktop> {
        let mut carry = None;
        let mut out = Vec::new();
        for (i, line) in lines.iter().enumerate() {
            let (events, next) = HyprlandParser
                .parse(&rec(line, 1_000_000_000 + i as i128), &carry)
                .unwrap();
            out.extend(events);
            carry = next;
        }
        out
    }

    fn at(ns: i128) -> Timestamp {
        Timestamp::from_unix_ns(ns).unwrap()
    }

    #[test]
    fn focus_halves_merge_and_keep_the_first_halfs_time() {
        let out = parse_all(&["activewindow>>kitty,~/src", "activewindowv2>>55aa"]);
        assert_eq!(
            out,
            vec![Desktop::Focused(WindowFocused {
                received_at: at(1_000_000_000),
                window: Some("55aa".into()),
                class: Some("kitty".into()),
                title: Some("~/src".into()),
            })]
        );
    }

    #[test]
    fn a_lone_first_half_is_kept_when_something_else_arrives() {
        let out = parse_all(&["activewindow>>kitty,t", "closewindow>>55aa"]);
        assert_eq!(out.len(), 2);
        assert!(
            matches!(&out[0], Desktop::Focused(f) if f.window.is_none() && f.class.as_deref() == Some("kitty"))
        );
        assert!(matches!(&out[1], Desktop::Closed(c) if c.window == "55aa"));
    }

    #[test]
    fn duplicate_forms_are_read_once() {
        let out = parse_all(&[
            "windowtitle>>55aa",
            "windowtitlev2>>55aa,a, b",
            "workspace>>2",
            "workspacev2>>2,2",
            "focusedmon>>DP-3,2",
            "focusedmonv2>>DP-3,2",
            "movewindow>>55aa,3",
            "movewindowv2>>55aa,3,3",
        ]);
        assert_eq!(out.len(), 4);
        assert!(matches!(&out[0], Desktop::TitleChanged(t) if t.title == "a, b"));
        assert!(
            matches!(&out[1], Desktop::Workspace(w) if w.workspace_id == Some(2) && w.workspace_name.as_deref() == Some("2"))
        );
        assert!(
            matches!(&out[2], Desktop::Monitor(m) if m.monitor == "DP-3" && m.workspace_name == "2")
        );
        assert!(matches!(&out[3], Desktop::Moved(m) if m.workspace_id == Some(3)));
    }

    #[test]
    fn openwindow_title_may_contain_commas() {
        let out = parse_all(&["openwindow>>55aa,1,firefox,a, b, c"]);
        assert!(matches!(&out[0], Desktop::Opened(o)
            if o.window == "55aa" && o.workspace_name == "1" && o.class == "firefox" && o.title == "a, b, c"));
    }

    #[test]
    fn unknown_and_malformed_lines_are_kept_whole() {
        let out = parse_all(&["screencastv2>>1,monitor,DP-3", "garbage"]);
        assert!(
            matches!(&out[0], Desktop::Other(o) if o.name == "screencastv2" && o.data == "1,monitor,DP-3")
        );
        assert!(matches!(&out[1], Desktop::Other(o) if o.name.is_empty() && o.data == "garbage"));
    }

    #[test]
    fn named_workspaces_do_not_fail() {
        // The old reader required workspace ids to be integers and refused
        // named ones; workspacev2 carries both, so a name never fails.
        let out = parse_all(&["workspacev2>>-98,special:scratch"]);
        assert!(matches!(&out[0], Desktop::Workspace(w) if w.workspace_id == Some(-98)));
    }

    #[test]
    fn spooled_capture_drains_to_typed_events() {
        let spool = std::env::temp_dir().join(format!("sinex-hypr-{}", uuid::Uuid::now_v7()));
        let mut w = SpoolWriter::open(&spool, Rotation::default()).unwrap();
        live_stream::capture_lines(
            &b"activewindow>>kitty,t\nactivewindowv2>>55aa\nwindowtitle>>55aa\nwindowtitlev2>>55aa,x\n"[..],
            &mut w,
            Duration::ZERO,
        )
        .unwrap();
        let acquired = driver(spool, "h".into()).poll(&Value::Null).unwrap();
        let m = acquired.material.unwrap();
        let chunks = source()
            .drain(DrainInput {
                source: SPEC.name,
                class: SPEC.class,
                material: MaterialId::new(),
                bytes: &m.bytes,
                records: &m.records,
                carry: Value::Null,
                reading_order: &|a| ReadingOrder(a as i64),
                limits: Limits::default(),
            })
            .unwrap();
        let mut types: Vec<&str> = Vec::new();
        for outcome in chunks.iter().flat_map(|c| &c.outcomes) {
            match outcome {
                RecordOutcome::Events { events, .. } => {
                    types.extend(events.iter().map(|e| e.event_type))
                }
                RecordOutcome::Failure { reason, .. } => panic!("{reason}"),
            }
        }
        assert_eq!(types, vec!["window.focused", "window.title_changed"]);
        // Four records read, two events: every record accounted for.
        assert_eq!(chunks.iter().map(|c| c.outcomes.len()).sum::<usize>(), 4);
    }
}
