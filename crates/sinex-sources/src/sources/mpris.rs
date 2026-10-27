//! Media players (MPRIS) on the session bus: each player's `PropertiesChanged`
//! on `org.mpris.MediaPlayer2.Player`, and the bus's `NameOwnerChanged` for
//! player names so a change can be attributed to the player that sent it.
//!
//! A signal names its sender only by unique bus name. Capture therefore first
//! writes one snapshot line per player present (its well-known name, owner and
//! current properties), then spools signals; the parser carries the map from
//! unique to well-known names forward.
use super::dbus::{self, Message};
use serde_json::{Map, Value, json};
use sinex_model::Class;
use sinex_model::event_types::session::PlayerChanged;
use sinex_source_core::drivers::live_stream::{self, LiveStream};
use sinex_source_core::registry::{DeclaredSource, Driver, SourceSpec};
use sinex_source_core::{Parser, RecordError};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

pub const SPEC: SourceSpec = SourceSpec {
    name: "media.mpris",
    driver: Driver::LiveStream,
    excisable: true,
    purpose: "listening and watching history, attention",
    // Titles and URLs of what is playing.
    class: Class::Guarded,
    parser: "dbus-mpris",
    parser_version: 1,
    event_types: &["player.changed"],
    framer: sinex_source_core::Framer::incremental(sinex_source_core::drivers::append_file::lines),
};

pub const MATCHES: &[&str] = &[
    "type=signal,interface=org.freedesktop.DBus.Properties,member=PropertiesChanged,path=/org/mpris/MediaPlayer2",
    "type=signal,sender=org.freedesktop.DBus,member=NameOwnerChanged,arg0namespace=org.mpris.MediaPlayer2",
];

const PLAYER: &str = "org.mpris.MediaPlayer2.Player";
const PREFIX: &str = "org.mpris.MediaPlayer2.";

pub struct MprisParser;

fn changed(
    received_at: sinex_model::Timestamp,
    player: String,
    props: &Map<String, Value>,
    snapshot: bool,
) -> PlayerChanged {
    let meta = props.get("Metadata").and_then(Value::as_object);
    let s = |k: &str| {
        meta.and_then(|m| m.get(k))
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .map(str::to_owned)
    };
    PlayerChanged {
        received_at,
        player,
        status: props
            .get("PlaybackStatus")
            .and_then(Value::as_str)
            .map(str::to_owned),
        title: s("xesam:title"),
        artists: meta
            .and_then(|m| m.get("xesam:artist"))
            .and_then(Value::as_array)
            .map(|xs| {
                xs.iter()
                    .filter_map(Value::as_str)
                    .filter(|a| !a.is_empty())
                    .map(str::to_owned)
                    .collect()
            })
            .unwrap_or_default(),
        album: s("xesam:album"),
        url: s("xesam:url"),
        length_us: meta
            .and_then(|m| m.get("mpris:length"))
            .and_then(Value::as_i64),
        snapshot,
        properties: props.clone(),
    }
}

impl Parser for MprisParser {
    type Record<'a> = Map<String, Value>;
    /// Unique bus name to the player's well-known name.
    type Carry = BTreeMap<String, String>;
    type Payload = PlayerChanged;

    fn parse(
        &self,
        record: &Map<String, Value>,
        owners: &BTreeMap<String, String>,
        _context: &sinex_source_core::RecordContext<'_>,
    ) -> Result<(Vec<PlayerChanged>, BTreeMap<String, String>), RecordError> {
        let (received_at, bytes) = live_stream::received(record).map_err(RecordError::new)?;
        let mut owners = owners.clone();
        let value: Value = serde_json::from_slice(&bytes)
            .map_err(|e| RecordError::new(format!("not JSON: {e}")))?;
        if let Some(snap) = value.get("snapshot") {
            let name = snap["name"].as_str().unwrap_or_default().to_owned();
            if let Some(owner) = snap["owner"].as_str() {
                owners.insert(owner.to_owned(), name.clone());
            }
            let props = snap["properties"].as_object().cloned().unwrap_or_default();
            return Ok((vec![changed(received_at, name, &props, true)], owners));
        }
        let m = Message::parse(&bytes).map_err(RecordError::new)?;
        let out = match m.member.as_str() {
            "NameOwnerChanged" => {
                let s = |i: usize| {
                    m.args
                        .get(i)
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_owned()
                };
                let (name, old, new) = (s(0), s(1), s(2));
                if name.starts_with(PREFIX) {
                    if !old.is_empty() {
                        owners.remove(&old);
                    }
                    if !new.is_empty() {
                        owners.insert(new, name);
                    }
                }
                Vec::new()
            }
            "PropertiesChanged" if m.args.first().and_then(Value::as_str) == Some(PLAYER) => {
                let props = m
                    .args
                    .get(1)
                    .and_then(Value::as_object)
                    .cloned()
                    .unwrap_or_default();
                let player = owners.get(&m.sender).cloned().unwrap_or(m.sender);
                vec![changed(received_at, player, &props, false)]
            }
            // The root interface's properties (identity, fullscreen…).
            _ => Vec::new(),
        };
        Ok((out, owners))
    }
}

pub fn source() -> DeclaredSource<MprisParser> {
    DeclaredSource {
        spec: SPEC,
        parser: MprisParser,
    }
}

pub fn driver(spool: PathBuf, host: String) -> LiveStream {
    dbus::driver(spool, host, "dbus:session:org.mpris.MediaPlayer2")
}

/// One snapshot line per player on the bus now.
pub fn snapshot() -> Vec<Vec<u8>> {
    snapshot_until(&sinex_source_core::capture::CaptureStop::new(None))
}

fn snapshot_until(stop: &sinex_source_core::capture::CaptureStop) -> Vec<Vec<u8>> {
    dbus::names_until(stop)
        .into_iter()
        .filter(|n| n.starts_with(PREFIX))
        .filter_map(|name| {
            let owner = dbus::call_until(
                stop,
                "org.freedesktop.DBus",
                "/org/freedesktop/DBus",
                "org.freedesktop.DBus",
                "GetNameOwner",
                &["s", &name],
            )
            .and_then(|v| v.get(0).and_then(Value::as_str).map(str::to_owned))?;
            let properties = dbus::call_until(
                stop,
                &name,
                "/org/mpris/MediaPlayer2",
                "org.freedesktop.DBus.Properties",
                "GetAll",
                &["s", PLAYER],
            )
            .and_then(|v| v.get(0).cloned())
            .unwrap_or(Value::Object(Map::new()));
            serde_json::to_vec(
                &json!({"snapshot": {"name": name, "owner": owner, "properties": properties}}),
            )
            .ok()
        })
        .collect()
}

pub fn capture(
    spool: &Path,
    stop: &sinex_source_core::capture::CaptureStop,
) -> std::io::Result<u64> {
    dbus::capture(MATCHES, &snapshot_until(stop), spool, stop)
}

/// `--read players=SPOOL`: media players (MPRIS), captured into SPOOL.
pub const FORMAT: sinex_source_core::binding::InputFormat =
    sinex_source_core::binding::InputFormat {
        name: "players",
        specs: || vec![&SPEC],
        plan: Some(|spool, settings| {
            Ok(sinex_source_core::binding::single(&SPEC, spool, settings))
        }),
        source: |_| Ok(Box::new(source())),
        reader: Some(|b| {
            Ok(sinex_source_core::binding::stateless(driver(
                b.input.clone(),
                b.settings.host.clone(),
            )))
        }),
        capture: Some(|b, stop| capture(&b.input, stop).map_err(|e| e.to_string())),
    };

#[cfg(test)]
mod tests;
