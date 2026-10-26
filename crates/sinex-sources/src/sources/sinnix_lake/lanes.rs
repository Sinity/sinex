//! Each lake lane's payload, as its Sinnix producer writes it, and the events
//! it becomes. A lane maps to the event types Sinex's own capture of the same
//! thing emits, so lake history and live capture form one series. Fields a
//! producer adds later are kept by name in the event's `extra`; a known field
//! of the wrong type fails the record.
use super::Envelope;
use crate::RecordError;
use serde::Deserialize;
use serde::de::DeserializeOwned;
use serde_json::{Map, Value};
use sinex_model::Timestamp;
use sinex_model::event_types::accessibility::{
    AccessibilityFocused, AccessibilityTextChanged, AccessibilityTree,
};
use sinex_model::event_types::audio::{
    AudioGraphChanged, AudioRecordingState, ExcludedDevice, FailedDevice, RecordedDevice,
    SpeechIndexed, SpeechSpan,
};
use sinex_model::event_types::browser::{LinkFollowed, ReadingStackChanged};
use sinex_model::event_types::desktop::{
    IdleInhibitorsReported, InhibitingWindow, SystemInhibitor,
};
use sinex_model::event_types::environment::AirSampled;
use sinex_model::event_types::network::ConnectionEnded;
use sinex_model::event_types::peripheral::{BatterySampled, BluetoothAudioSampled, DisplaySampled};
use sinex_model::event_types::session::{
    ClipboardCopied, FocusedWindow, FrameCaptured, NotificationShown, PlayerChanged,
};

sinex_model::payload_enum! {
    #[derive(Clone, Debug, PartialEq)]
    pub enum LakeEvent {
        Copied(ClipboardCopied),
        Notification(NotificationShown),
        Player(PlayerChanged),
        Focused(AccessibilityFocused),
        TextChanged(AccessibilityTextChanged),
        Tree(AccessibilityTree),
        IdleInhibitors(IdleInhibitorsReported),
        Frame(FrameCaptured),
        Recording(AudioRecordingState),
        Graph(AudioGraphChanged),
        Speech(SpeechIndexed),
        Link(LinkFollowed),
        Reading(ReadingStackChanged),
        Display(DisplaySampled),
        Battery(BatterySampled),
        BluetoothAudio(BluetoothAudioSampled),
        Connection(ConnectionEnded),
        Air(AirSampled),
    }
}

/// A time in seconds since the epoch, as the lake writes it. A float of
/// seconds holds about microseconds at today's epoch; whole seconds plus
/// rounded microseconds avoid inventing nanoseconds.
pub(super) fn seconds(value: f64) -> Result<Timestamp, RecordError> {
    if !value.is_finite() {
        return Err(RecordError::new(format!("time {value} is not a number")));
    }
    let whole = value.floor();
    let micros = ((value - whole) * 1e6).round() as i128;
    Timestamp::from_unix_ns(whole as i128 * 1_000_000_000 + micros * 1_000)
        .map_err(|e| RecordError::new(format!("time {value} out of range: {e}")))
}

fn decode<T: DeserializeOwned>(lane: &str, payload: &Value) -> Result<T, RecordError> {
    T::deserialize(payload).map_err(|e| RecordError::new(format!("{lane} payload: {e}")))
}

fn kind_of<'a>(lane: &str, payload: &'a Value) -> Result<&'a str, RecordError> {
    payload
        .get("kind")
        .and_then(Value::as_str)
        .ok_or_else(|| RecordError::new(format!("{lane} payload has no kind")))
}

fn non_empty(text: Option<String>) -> Option<String> {
    text.filter(|t| !t.is_empty())
}

/// The events of one envelope record of `lane`.
pub(super) fn events(envelope: &Envelope) -> Result<Vec<LakeEvent>, RecordError> {
    let lane = envelope.lane.as_str();
    let payload = &envelope.payload;
    let at = envelope.observed_at;
    Ok(match lane {
        "clipboard" | "primary" => vec![LakeEvent::Copied(selection(
            lane,
            at,
            decode(lane, payload)?,
        ))],
        "notifications" => vec![LakeEvent::Notification(notification(
            at,
            decode(lane, payload)?,
        )?)],
        "mpris" => vec![LakeEvent::Player(player(at, decode(lane, payload)?))],
        "a11y" => vec![match kind_of(lane, payload)? {
            "focus" => LakeEvent::Focused(focused(at, decode(lane, payload)?)),
            "text-changed" => LakeEvent::TextChanged(text_changed(at, decode(lane, payload)?)),
            "subtree" => LakeEvent::Tree(tree(at, decode(lane, payload)?)),
            other => {
                return Err(RecordError::new(format!(
                    "a11y record of unknown kind {other:?}"
                )));
            }
        }],
        "idle-inhibitors" => vec![LakeEvent::IdleInhibitors(idle_inhibitors(
            at,
            decode(lane, payload)?,
        ))],
        "screen-frames" => vec![LakeEvent::Frame(frame(
            at,
            envelope.raw_ref.clone(),
            decode(lane, payload)?,
        )?)],
        "audio-devices" => match kind_of(lane, payload)? {
            "devices-state" => vec![LakeEvent::Recording(recording(at, decode(lane, payload)?)?)],
            other => {
                return Err(RecordError::new(format!(
                    "audio-devices record of unknown kind {other:?}"
                )));
            }
        },
        "audio-topology" => match kind_of(lane, payload)? {
            "node" | "port" | "link" => vec![LakeEvent::Graph(graph(at, decode(lane, payload)?))],
            other => {
                return Err(RecordError::new(format!(
                    "audio-topology record of unknown kind {other:?}"
                )));
            }
        },
        "audio-index" => match kind_of(lane, payload)? {
            "speech-index" => vec![LakeEvent::Speech(speech(
                envelope.raw_ref.clone(),
                decode(lane, payload)?,
            )?)],
            other => {
                return Err(RecordError::new(format!(
                    "audio-index record of unknown kind {other:?}"
                )));
            }
        },
        "browser-nav-edges" => vec![LakeEvent::Link(link(at, decode(lane, payload)?)?)],
        "reading-stack" => vec![LakeEvent::Reading(reading(at, decode(lane, payload)?)?)],
        "monitor" => vec![LakeEvent::Display(display(at, decode(lane, payload)?))],
        "bt-battery" => decode::<Vec<BluezBattery>>(lane, payload)?
            .into_iter()
            .map(|b| LakeEvent::Battery(bluez_battery(at, b)))
            .collect(),
        "logitech" => decode::<Logitech>(lane, payload)?
            .devices
            .into_iter()
            .flatten()
            .map(|d| LakeEvent::Battery(logitech_battery(at, d)))
            .collect(),
        "bt-audio" => decode::<Vec<BluetoothTransport>>(lane, payload)?
            .into_iter()
            .map(|t| LakeEvent::BluetoothAudio(bluetooth_audio(at, t)))
            .collect(),
        "netflow" => vec![LakeEvent::Connection(connection(
            at,
            decode(lane, payload)?,
        ))],
        "awair" | "environment" => vec![LakeEvent::Air(air(at, decode(lane, payload)?))],
        other => return Err(RecordError::new(format!("lane {other:?} has no mapping"))),
    })
}

// Clipboard and primary selection.

#[derive(Deserialize)]
struct Selection {
    /// `text` or `binary`, which the MIME type already says.
    #[serde(default, rename = "category")]
    _category: serde::de::IgnoredAny,
    mime: Option<String>,
    text: Option<String>,
    sha256: Option<String>,
    size: Option<u64>,
    source_window: Option<Window>,
    /// The clipboard manager an imported item came from.
    source: Option<String>,
    #[serde(flatten)]
    extra: Map<String, Value>,
}

#[derive(Deserialize)]
struct Window {
    class: Option<String>,
    title: Option<String>,
}

fn selection(lane: &str, at: Timestamp, s: Selection) -> ClipboardCopied {
    ClipboardCopied {
        received_at: at,
        selection: lane.to_owned(),
        mime_types: None,
        mime_type: s.mime,
        text: s.text,
        bytes: s.size,
        content_hash: None,
        sha256: s.sha256,
        window: s.source_window.map(|w| FocusedWindow {
            class: w.class,
            title: w.title,
        }),
        imported_from: s.source,
        secret: false,
        extra: s.extra,
    }
}

// Notifications.

#[derive(Deserialize)]
struct Notification {
    app_name: Option<String>,
    app_icon: Option<String>,
    summary: Option<String>,
    body: Option<String>,
    sender: Option<String>,
    replaces_id: Option<u64>,
    urgency: Option<u8>,
    expire_timeout: Option<i32>,
    /// The bus monitor's time for the call.
    timestamp: Option<f64>,
    #[serde(default)]
    actions: Vec<Action>,
    #[serde(default)]
    hints: Map<String, Value>,
    #[serde(flatten)]
    extra: Map<String, Value>,
}

#[derive(Deserialize)]
struct Action {
    id: String,
    label: String,
}

fn notification(at: Timestamp, n: Notification) -> Result<NotificationShown, RecordError> {
    let mut hints = n.hints;
    if let Some(urgency) = n.urgency {
        hints.entry("urgency").or_insert(urgency.into());
    }
    Ok(NotificationShown {
        received_at: n.timestamp.map(seconds).transpose()?.unwrap_or(at),
        app: n.app_name.unwrap_or_default(),
        summary: n.summary.unwrap_or_default(),
        body: n.body.unwrap_or_default(),
        icon: non_empty(n.app_icon),
        replaces_id: n
            .replaces_id
            .filter(|id| *id != 0)
            .map(|id| u32::try_from(id).map_err(|_| RecordError::new("replaces_id beyond u32")))
            .transpose()?,
        actions: n
            .actions
            .into_iter()
            .flat_map(|a| [a.id, a.label])
            .collect(),
        hints,
        expire_timeout_ms: n
            .expire_timeout
            .ok_or_else(|| RecordError::new("notification has no expire_timeout"))?,
        sender: n
            .sender
            .ok_or_else(|| RecordError::new("notification has no sender"))?,
        extra: n.extra,
    })
}

// Media players.

#[derive(Deserialize)]
struct Player {
    /// `initial`, `status`, `track` or `heartbeat`.
    event: Option<String>,
    player: String,
    status: Option<String>,
    title: Option<String>,
    artist: Option<String>,
    album: Option<String>,
    duration_seconds: Option<f64>,
    position_seconds: Option<f64>,
    #[serde(flatten)]
    extra: Map<String, Value>,
}

fn micros(seconds: f64) -> Option<i64> {
    let us = (seconds * 1e6).round();
    (us.is_finite() && us.abs() < i64::MAX as f64).then_some(us as i64)
}

fn player(at: Timestamp, p: Player) -> PlayerChanged {
    PlayerChanged {
        received_at: at,
        player: p.player,
        status: p.status,
        title: non_empty(p.title),
        artists: non_empty(p.artist).into_iter().collect(),
        album: non_empty(p.album),
        url: None,
        length_us: p.duration_seconds.and_then(micros),
        position_us: p.position_seconds.and_then(micros),
        snapshot: p.event.as_deref() == Some("initial"),
        heartbeat: p.event.as_deref() == Some("heartbeat"),
        properties: p.extra,
    }
}

// Accessibility.

#[derive(Deserialize)]
struct Focus {
    app: Option<String>,
    role: Option<String>,
    name: Option<String>,
    window: Option<String>,
    #[serde(flatten)]
    extra: Map<String, Value>,
}

fn focused(at: Timestamp, f: Focus) -> AccessibilityFocused {
    let mut extra = f.extra;
    extra.remove("kind");
    AccessibilityFocused {
        received_at: at,
        app: f.app,
        role: f.role,
        name: f.name,
        window: f.window,
        extra,
    }
}

#[derive(Deserialize)]
struct TextChange {
    app: Option<String>,
    role: Option<String>,
    name: Option<String>,
    change_type: Option<String>,
    detail: Option<String>,
    #[serde(flatten)]
    extra: Map<String, Value>,
}

fn text_changed(at: Timestamp, t: TextChange) -> AccessibilityTextChanged {
    let mut extra = t.extra;
    extra.remove("kind");
    AccessibilityTextChanged {
        received_at: at,
        app: t.app,
        role: t.role,
        name: t.name,
        change_type: t.change_type,
        detail: t.detail,
        extra,
    }
}

#[derive(Deserialize)]
struct Subtree {
    app: Option<String>,
    window: Option<String>,
    tree: Control,
    #[serde(flatten)]
    extra: Map<String, Value>,
}

#[derive(Deserialize)]
struct Control {
    name: Option<String>,
    text: Option<String>,
    /// Present when the capture cut the control's text short.
    truncated: Option<Value>,
    #[serde(default)]
    children: Vec<Control>,
}

fn tree(at: Timestamp, s: Subtree) -> AccessibilityTree {
    let mut controls = 0;
    let mut truncated = 0;
    let mut seen = std::collections::HashSet::new();
    let mut lines: Vec<String> = Vec::new();
    let mut pending = vec![&s.tree];
    while let Some(control) = pending.pop() {
        controls += 1;
        if control
            .truncated
            .as_ref()
            .is_some_and(|t| t != &Value::Bool(false))
        {
            truncated += 1;
        }
        for line in [&control.name, &control.text].into_iter().flatten() {
            let line = line.trim();
            if !line.is_empty() && seen.insert(line.to_owned()) {
                lines.push(line.to_owned());
            }
        }
        pending.extend(control.children.iter().rev());
    }
    let mut extra = s.extra;
    extra.remove("kind");
    AccessibilityTree {
        received_at: at,
        app: s.app,
        window: s.window,
        controls,
        truncated,
        text: lines.join("\n"),
        extra,
    }
}

// Idle inhibitors.

#[derive(Deserialize)]
struct IdleReport {
    /// `change` or `heartbeat`.
    event: Option<String>,
    provenance: Option<Provenance>,
    #[serde(flatten)]
    extra: Map<String, Value>,
}

#[derive(Deserialize)]
struct Provenance {
    hyprland_clients: Option<Probe<Vec<HyprlandWindow>>>,
    systemd_logind: Option<Probe<Vec<LogindInhibitor>>>,
    active_window: Option<WindowProbe>,
    #[serde(flatten)]
    other: Map<String, Value>,
}

/// One probe's answer: its status and, when it answered, what it listed.
#[derive(Deserialize)]
struct Probe<T> {
    status: Option<String>,
    reason: Option<String>,
    inhibitors: Option<T>,
}

#[derive(Deserialize)]
struct WindowProbe {
    status: Option<String>,
    reason: Option<String>,
    window: Option<HyprlandWindow>,
}

#[derive(Deserialize)]
struct HyprlandWindow {
    address: Option<String>,
    class: Option<String>,
    title: Option<String>,
    pid: Option<i64>,
    workspace: Option<Workspace>,
}

#[derive(Deserialize)]
struct Workspace {
    id: Option<i64>,
    name: Option<String>,
}

#[derive(Deserialize)]
struct LogindInhibitor {
    who: Option<String>,
    what: Option<String>,
    why: Option<String>,
    mode: Option<String>,
    pid: Option<i64>,
    uid: Option<i64>,
}

fn inhibiting(w: HyprlandWindow) -> InhibitingWindow {
    let (workspace_id, workspace_name) = w.workspace.map(|s| (s.id, s.name)).unwrap_or_default();
    InhibitingWindow {
        address: w.address,
        class: w.class,
        title: w.title,
        pid: w.pid,
        workspace_id,
        workspace_name,
    }
}

fn answered(status: &Option<String>) -> bool {
    status.as_deref() == Some("available")
}

fn idle_inhibitors(at: Timestamp, r: IdleReport) -> IdleInhibitorsReported {
    let mut unavailable = Map::new();
    let mut note = |probe: &str, status: &Option<String>, reason: &Option<String>| {
        if !answered(status) {
            unavailable.insert(
                probe.to_owned(),
                Value::String(
                    reason
                        .clone()
                        .or_else(|| status.clone())
                        .unwrap_or_else(|| "no answer".into()),
                ),
            );
        }
    };
    let (mut windows, mut system, mut focused_window) = (None, None, None);
    let mut extra = r.extra;
    if let Some(p) = r.provenance {
        if let Some(probe) = p.hyprland_clients {
            note("hyprland_clients", &probe.status, &probe.reason);
            if answered(&probe.status) {
                windows = Some(
                    probe
                        .inhibitors
                        .unwrap_or_default()
                        .into_iter()
                        .map(inhibiting)
                        .collect(),
                );
            }
        }
        if let Some(probe) = p.systemd_logind {
            note("systemd_logind", &probe.status, &probe.reason);
            if answered(&probe.status) {
                system = Some(
                    probe
                        .inhibitors
                        .unwrap_or_default()
                        .into_iter()
                        .map(|i| SystemInhibitor {
                            who: i.who,
                            what: i.what,
                            why: i.why,
                            mode: i.mode,
                            pid: i.pid,
                            uid: i.uid,
                        })
                        .collect(),
                );
            }
        }
        if let Some(probe) = p.active_window {
            note("active_window", &probe.status, &probe.reason);
            focused_window = probe.window.map(inhibiting);
        }
        for (name, value) in p.other {
            match value.get("status").and_then(Value::as_str) {
                Some("available") => {
                    extra.insert(name, value);
                }
                status => {
                    let reason = value.get("reason").and_then(Value::as_str).or(status);
                    unavailable.insert(name, reason.unwrap_or("no answer").into());
                }
            }
        }
    }
    IdleInhibitorsReported {
        received_at: at,
        heartbeat: r.event.as_deref() == Some("heartbeat"),
        windows,
        system,
        focused_window,
        unavailable,
        extra,
    }
}

// Screen frames.

#[derive(Deserialize)]
struct Frame {
    window_class: Option<String>,
    window_title: Option<String>,
    workspace: Option<String>,
    monitor: Option<String>,
    sha256: Option<String>,
    trigger: Option<String>,
    /// The capture instant.
    ts: Option<f64>,
    geometry: Option<Geometry>,
    #[serde(flatten)]
    extra: Map<String, Value>,
}

#[derive(Deserialize)]
struct Geometry {
    x: Option<i64>,
    y: Option<i64>,
    width: Option<i64>,
    height: Option<i64>,
}

fn frame(at: Timestamp, image: Option<String>, f: Frame) -> Result<FrameCaptured, RecordError> {
    let g = f.geometry;
    Ok(FrameCaptured {
        captured_at: f.ts.map(seconds).transpose()?.unwrap_or(at),
        window_class: f.window_class,
        window_title: f.window_title,
        workspace: f.workspace,
        monitor: f.monitor,
        x: g.as_ref().and_then(|g| g.x),
        y: g.as_ref().and_then(|g| g.y),
        width: g.as_ref().and_then(|g| g.width),
        height: g.as_ref().and_then(|g| g.height),
        sha256: f.sha256,
        trigger: f.trigger,
        image,
        extra: f.extra,
    })
}

// Audio.

#[derive(Deserialize)]
struct DevicesState {
    reason: Option<String>,
    asr_source: Option<String>,
    #[serde(default)]
    recording: Vec<Recording>,
    #[serde(default)]
    excluded: Vec<Excluded>,
    #[serde(default)]
    draining: Vec<String>,
    #[serde(default)]
    failed: Vec<Failed>,
    #[serde(flatten)]
    extra: Map<String, Value>,
}

#[derive(Deserialize)]
struct Recording {
    node_name: Option<String>,
    device_kind: Option<String>,
    channel: Option<String>,
    description: Option<String>,
    since: Option<f64>,
}

#[derive(Deserialize)]
struct Excluded {
    key: Option<String>,
    pattern: Option<String>,
}

#[derive(Deserialize)]
struct Failed {
    key: Option<String>,
    error: Option<String>,
    at: Option<f64>,
}

fn recording(at: Timestamp, d: DevicesState) -> Result<AudioRecordingState, RecordError> {
    let mut extra = d.extra;
    extra.remove("kind");
    Ok(AudioRecordingState {
        received_at: at,
        reason: d.reason,
        speech_input: d.asr_source,
        recording: d
            .recording
            .into_iter()
            .map(|r| {
                Ok(RecordedDevice {
                    node_name: r.node_name,
                    device_kind: r.device_kind,
                    channel: r.channel,
                    description: r.description,
                    since: r.since.map(seconds).transpose()?,
                })
            })
            .collect::<Result<_, RecordError>>()?,
        excluded: d
            .excluded
            .into_iter()
            .map(|e| ExcludedDevice {
                key: e.key,
                pattern: e.pattern,
            })
            .collect(),
        draining: d.draining,
        failed: d
            .failed
            .into_iter()
            .map(|f| {
                Ok(FailedDevice {
                    key: f.key,
                    error: f.error,
                    at: f.at.map(seconds).transpose()?,
                })
            })
            .collect::<Result<_, RecordError>>()?,
        extra,
    })
}

#[derive(Deserialize)]
struct GraphChange {
    kind: String,
    action: String,
    id: i64,
    #[serde(rename = "node.name")]
    node_name: Option<String>,
    #[serde(rename = "node.description")]
    node_description: Option<String>,
    #[serde(rename = "media.class")]
    media_class: Option<String>,
    #[serde(rename = "port.name")]
    port_name: Option<String>,
    #[serde(rename = "port.direction")]
    port_direction: Option<String>,
    #[serde(rename = "node.id")]
    node_id: Option<String>,
    #[serde(rename = "output-node-id")]
    output_node_id: Option<String>,
    #[serde(rename = "output-port-id")]
    output_port_id: Option<String>,
    #[serde(rename = "input-node-id")]
    input_node_id: Option<String>,
    #[serde(rename = "input-port-id")]
    input_port_id: Option<String>,
    #[serde(flatten)]
    extra: Map<String, Value>,
}

fn graph(at: Timestamp, g: GraphChange) -> AudioGraphChanged {
    AudioGraphChanged {
        received_at: at,
        object: g.kind,
        action: g.action,
        object_id: g.id,
        node_name: g.node_name,
        node_description: g.node_description,
        media_class: g.media_class,
        port_name: g.port_name,
        port_direction: g.port_direction,
        node_id: g.node_id,
        output_node_id: g.output_node_id,
        output_port_id: g.output_port_id,
        input_node_id: g.input_node_id,
        input_port_id: g.input_port_id,
        extra: g.extra,
    }
}

#[derive(Deserialize)]
struct SpeechIndex {
    channel: Option<String>,
    segment: Option<String>,
    segment_start: f64,
    decoded_seconds: Option<f64>,
    span_seconds: Option<f64>,
    coverage: Option<f64>,
    #[serde(default)]
    speech_spans: Vec<Span>,
    decode_error: Option<String>,
    #[serde(flatten)]
    extra: Map<String, Value>,
}

#[derive(Deserialize)]
struct Span {
    start: f64,
    end: f64,
}

fn speech(path: Option<String>, s: SpeechIndex) -> Result<SpeechIndexed, RecordError> {
    let mut extra = s.extra;
    extra.remove("kind");
    Ok(SpeechIndexed {
        segment_start: seconds(s.segment_start)?,
        channel: s.channel,
        segment: s.segment,
        path,
        decoded_seconds: s.decoded_seconds,
        span_seconds: s.span_seconds,
        coverage: s.coverage,
        speech: s
            .speech_spans
            .into_iter()
            .map(|span| {
                Ok(SpeechSpan {
                    start: seconds(span.start)?,
                    end: seconds(span.end)?,
                })
            })
            .collect::<Result<_, RecordError>>()?,
        decode_error: s.decode_error,
        extra,
    })
}

// Browsing.

#[derive(Deserialize)]
struct NavigationEdge {
    source_url: Option<String>,
    source_title: Option<String>,
    target_url: Option<String>,
    target_title: Option<String>,
    anchor_text: Option<String>,
    trigger: Option<String>,
    note: Option<String>,
    /// The extension's time for the click.
    ts: Option<f64>,
    #[serde(flatten)]
    extra: Map<String, Value>,
}

fn link(at: Timestamp, n: NavigationEdge) -> Result<LinkFollowed, RecordError> {
    Ok(LinkFollowed {
        followed_at: n.ts.map(seconds).transpose()?.unwrap_or(at),
        from_url: n.source_url,
        from_title: n.source_title,
        to_url: n.target_url,
        to_title: n.target_title,
        link_text: n.anchor_text,
        trigger: n.trigger,
        note: n.note,
        extra: n.extra,
    })
}

#[derive(Deserialize)]
struct ReadingItem {
    event: String,
    url: Option<String>,
    title: Option<String>,
    source_url: Option<String>,
    source_title: Option<String>,
    anchor_text: Option<String>,
    note: Option<String>,
    pushed_at: Option<f64>,
    #[serde(flatten)]
    extra: Map<String, Value>,
}

fn reading(at: Timestamp, r: ReadingItem) -> Result<ReadingStackChanged, RecordError> {
    Ok(ReadingStackChanged {
        changed_at: r.pushed_at.map(seconds).transpose()?.unwrap_or(at),
        action: r.event,
        url: r.url,
        title: r.title,
        from_url: r.source_url,
        from_title: r.source_title,
        link_text: r.anchor_text,
        note: r.note,
        extra: r.extra,
    })
}

// Devices.

#[derive(Deserialize)]
struct Monitor {
    brightness: Option<i64>,
    contrast: Option<i64>,
    power_sl: Option<i64>,
    input_sl: Option<i64>,
    #[serde(flatten)]
    extra: Map<String, Value>,
}

fn display(at: Timestamp, m: Monitor) -> DisplaySampled {
    DisplaySampled {
        sampled_at: at,
        brightness: m.brightness,
        contrast: m.contrast,
        power_mode: m.power_sl,
        input_source: m.input_sl,
        extra: m.extra,
    }
}

#[derive(Deserialize)]
struct BluezBattery {
    device_address: Option<String>,
    device_alias: Option<String>,
    source: Option<String>,
    percentage: Option<f64>,
    #[serde(flatten)]
    extra: Map<String, Value>,
}

fn bluez_battery(at: Timestamp, b: BluezBattery) -> BatterySampled {
    BatterySampled {
        sampled_at: at,
        device: b.device_alias,
        device_id: b.device_address,
        reported_by: b.source.unwrap_or_else(|| "bluez".into()),
        percent: b.percentage,
        millivolts: None,
        status: None,
        extra: b.extra,
    }
}

#[derive(Deserialize)]
struct Logitech {
    devices: Option<Vec<LogitechDevice>>,
}

#[derive(Deserialize)]
struct LogitechDevice {
    name: Option<String>,
    unit_id: Option<String>,
    battery_status: Option<String>,
    battery_percent: Option<f64>,
    battery_mv: Option<i64>,
    #[serde(flatten)]
    extra: Map<String, Value>,
}

fn logitech_battery(at: Timestamp, d: LogitechDevice) -> BatterySampled {
    BatterySampled {
        sampled_at: at,
        device: d.name,
        device_id: d.unit_id,
        reported_by: "solaar".into(),
        percent: d.battery_percent,
        millivolts: d.battery_mv,
        status: d.battery_status,
        extra: d.extra,
    }
}

#[derive(Deserialize)]
struct BluetoothTransport {
    device_address: Option<String>,
    device_alias: Option<String>,
    uuid: Option<String>,
    state: Option<String>,
    codec: Option<i64>,
    volume: Option<i64>,
    #[serde(flatten)]
    extra: Map<String, Value>,
}

fn bluetooth_audio(at: Timestamp, t: BluetoothTransport) -> BluetoothAudioSampled {
    BluetoothAudioSampled {
        sampled_at: at,
        device: t.device_alias,
        device_address: t.device_address,
        profile: t.uuid,
        state: t.state,
        codec: t.codec,
        volume: t.volume,
        extra: t.extra,
    }
}

// Network.

#[derive(Deserialize)]
struct Flow {
    proto: Option<String>,
    src: Option<String>,
    sport: Option<u16>,
    dst: Option<String>,
    dport: Option<u16>,
    bytes: Option<u64>,
    packets: Option<u64>,
    #[serde(flatten)]
    extra: Map<String, Value>,
}

fn connection(at: Timestamp, f: Flow) -> ConnectionEnded {
    ConnectionEnded {
        ended_at: at,
        protocol: f.proto,
        source_address: f.src,
        source_port: f.sport,
        destination_address: f.dst,
        destination_port: f.dport,
        bytes: f.bytes,
        packets: f.packets,
        extra: f.extra,
    }
}

// Room air.

#[derive(Deserialize)]
struct Air {
    /// The sensor's clock, RFC 3339.
    timestamp: Option<Timestamp>,
    score: Option<f64>,
    temp: Option<f64>,
    humid: Option<f64>,
    co2: Option<f64>,
    voc: Option<f64>,
    pm25: Option<f64>,
    #[serde(flatten)]
    extra: Map<String, Value>,
}

fn air(at: Timestamp, a: Air) -> AirSampled {
    AirSampled {
        sampled_at: a.timestamp.unwrap_or(at),
        score: a.score,
        temperature_c: a.temp,
        humidity: a.humid,
        co2_ppm: a.co2,
        voc_ppb: a.voc,
        pm25: a.pm25,
        extra: a.extra,
    }
}
