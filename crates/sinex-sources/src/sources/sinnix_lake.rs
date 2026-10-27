//! Sinnix capture-lake JSONL day files become stored material and events
//! under each lane's catalog source. Configuration names the directories
//! relative to the lake root and the accepted envelope lanes. An unbound lane
//! is a record failure. Each lane emits the event types Sinex's own capture of
//! the same thing emits (`lanes`), one event per record, network flows
//! included; a record listing several devices is one event per device.
use crate::drivers::append_file::{AppendFile, lines};
use crate::registry::{DeclaredSource, Driver, SourceSpec};
use crate::{Parser, RecordError};
use serde_json::{Map, Value};
use sinex_model::{Class, Timestamp};
use std::path::{Path, PathBuf};

/// One catalog source of the lake: the directories it is written to and the
/// `lane` values found in them.
#[derive(Clone, Debug)]
pub struct LakeSource {
    pub spec: SourceSpec,
    /// Directories relative to the lake root (`/realm`).
    pub dirs: &'static [&'static str],
    /// Accepted `lane` values; renamed lanes list both names.
    pub lane_values: &'static [&'static str],
}

const fn spec(
    name: &'static str,
    parser: &'static str,
    purpose: &'static str,
    event_types: &'static [&'static str],
) -> SourceSpec {
    SourceSpec {
        name,
        driver: Driver::AppendFile,
        excisable: false,
        purpose,
        // The class of each lane is the operator's to choose; floors raise it
        // without a code change.
        class: Class::Open,
        parser,
        parser_version: 3,
        event_types,
        framer: crate::Framer::incremental(lines),
    }
}

/// Every envelope lane the reader knows. The non-envelope lanes (keyboard,
/// terminal sessions, scrollback, IRC, media files, transcripts, telemetry,
/// audit, phone) have their own formats and readers.
const LAKE_SOURCES: &[LakeSource] = &[
    LakeSource {
        spec: SourceSpec {
            class: Class::Guarded,
            ..spec(
                "desktop.lake-clipboard",
                "sinnix-lake:desktop.lake-clipboard",
                "recall, prompt provenance",
                &["clipboard.copied"],
            )
        },
        dirs: &["activity/desktop/clipboard", "activity/desktop/selection"],
        lane_values: &["clipboard", "primary"],
    },
    LakeSource {
        spec: SourceSpec {
            class: Class::Guarded,
            ..spec(
                "desktop.lake-notifications",
                "sinnix-lake:desktop.lake-notifications",
                "recall, interruption analysis",
                &["notification.shown"],
            )
        },
        dirs: &["activity/desktop/notification"],
        lane_values: &["notifications"],
    },
    LakeSource {
        spec: spec(
            "desktop.media",
            "sinnix-lake:desktop.media",
            "listening and attention timeline",
            &["player.changed"],
        ),
        dirs: &["activity/desktop/media"],
        lane_values: &["mpris"],
    },
    LakeSource {
        spec: spec(
            "desktop.a11y",
            "sinnix-lake:desktop.a11y",
            "what was on screen, recall",
            &[
                "accessibility.focused",
                "accessibility.text_changed",
                "accessibility.tree",
            ],
        ),
        dirs: &["activity/desktop/accessibility"],
        lane_values: &["a11y"],
    },
    LakeSource {
        spec: spec(
            "desktop.idle-inhibitor",
            "sinnix-lake:desktop.idle-inhibitor",
            "presence and AFK evidence",
            &["idle_inhibitors.reported"],
        ),
        dirs: &["activity/desktop/idle-inhibitor"],
        lane_values: &["idle-inhibitors"],
    },
    LakeSource {
        spec: spec(
            "media.screen",
            "sinnix-lake:media.screen",
            "screen recall and OCR",
            &["frame.captured"],
        ),
        dirs: &["activity/desktop/frame"],
        lane_values: &["screen-frames"],
    },
    LakeSource {
        spec: spec(
            "media.audio",
            "sinnix-lake:media.audio",
            "audio recall, speech index, device state",
            &[
                "audio.recording_state",
                "audio.graph_changed",
                "speech.indexed",
            ],
        ),
        dirs: &[
            "activity/audio/device",
            "activity/audio/topology",
            "activity/audio/index",
        ],
        lane_values: &["audio-devices", "audio-topology", "audio-index"],
    },
    LakeSource {
        spec: spec(
            "browser.navigation",
            "sinnix-lake:browser.navigation",
            "browsing trails, recall",
            &["link.followed"],
        ),
        dirs: &["activity/browser/navigation"],
        lane_values: &["browser-nav-edges"],
    },
    LakeSource {
        spec: spec(
            "reading.stack",
            "sinnix-lake:reading.stack",
            "reading queue",
            &["reading_stack.changed"],
        ),
        dirs: &["activity/browser/reading"],
        lane_values: &["reading-stack"],
    },
    LakeSource {
        spec: spec(
            "desktop.display",
            "sinnix-lake:desktop.display",
            "machine state timeline",
            &["display.sampled"],
        ),
        dirs: &["device/sinnix-prime/monitor"],
        lane_values: &["monitor"],
    },
    LakeSource {
        spec: SourceSpec {
            parser_version: 4,
            ..spec(
                "network.netflow",
                "sinnix-lake:network.netflow",
                "network flows by connection",
                &["connection.ended"],
            )
        },
        dirs: &["device/sinnix-prime/netflow"],
        lane_values: &["netflow"],
    },
    LakeSource {
        spec: spec(
            "machine.peripheral",
            "sinnix-lake:machine.peripheral",
            "battery and audio device state",
            &["battery.sampled", "bluetooth_audio.sampled"],
        ),
        dirs: &[
            "device/sinnix-prime/peripheral/bt-audio",
            "device/sinnix-prime/peripheral/bt-battery",
            "device/sinnix-prime/peripheral/logitech",
        ],
        lane_values: &["bt-audio", "bt-battery", "logitech"],
    },
    LakeSource {
        spec: spec(
            "environment.awair",
            "sinnix-lake:environment.awair",
            "room air, sleep and focus analysis",
            &["air.sampled"],
        ),
        dirs: &["health/environment"],
        lane_values: &["awair", "environment"],
    },
];

pub struct EnvelopeParser {
    lake_source: &'static LakeSource,
}

/// The `sinnix-capture-v1` envelope around every record.
struct Envelope {
    lane: String,
    observed_at: Timestamp,
    payload: Value,
    /// The path of a file the record describes (a frame, an audio segment).
    raw_ref: Option<String>,
}

fn envelope(record: &Map<String, Value>, source: &LakeSource) -> Result<Envelope, RecordError> {
    match record.get("schema").and_then(Value::as_str) {
        Some("sinnix-capture-v1") => {}
        other => {
            return Err(RecordError::new(format!(
                "not a sinnix-capture-v1 envelope (schema {other:?})"
            )));
        }
    }
    if record.get("schema_version").and_then(Value::as_i64) != Some(1) {
        return Err(RecordError::new("envelope schema_version is not 1"));
    }
    let lane = record
        .get("lane")
        .and_then(Value::as_str)
        .ok_or_else(|| RecordError::new("envelope has no lane"))?;
    if !source.lane_values.contains(&lane) {
        return Err(RecordError::new(format!(
            "lane {lane:?} is not bound to {} (accepted: {})",
            source.spec.name,
            source.lane_values.join(", ")
        )));
    }
    if record
        .get("host")
        .and_then(Value::as_str)
        .is_none_or(str::is_empty)
    {
        return Err(RecordError::new("envelope host is not a nonempty string"));
    }
    if record.get("seq").and_then(Value::as_u64).is_none() {
        return Err(RecordError::new("envelope seq is not an unsigned integer"));
    }
    if record
        .get("raw_ref")
        .is_some_and(|value| !value.is_null() && !value.is_string())
    {
        return Err(RecordError::new("envelope raw_ref is not a string or null"));
    }
    let ts = record
        .get("ts")
        .and_then(Value::as_f64)
        .ok_or_else(|| RecordError::new("envelope has no ts"))?;
    Ok(Envelope {
        lane: lane.to_owned(),
        observed_at: lanes::seconds(ts)?,
        payload: record
            .get("payload")
            .cloned()
            .ok_or_else(|| RecordError::new("envelope has no payload"))?,
        raw_ref: record
            .get("raw_ref")
            .and_then(Value::as_str)
            .map(str::to_owned),
    })
}

impl Parser for EnvelopeParser {
    type Record<'a> = Map<String, Value>;
    type Carry = ();
    type Payload = lanes::LakeEvent;

    fn parse(
        &self,
        record: &Map<String, Value>,
        _: &(),
        _context: &crate::RecordContext<'_>,
    ) -> Result<(Vec<lanes::LakeEvent>, ()), RecordError> {
        Ok((lanes::events(&envelope(record, self.lake_source)?)?, ()))
    }
}

pub fn lake_source(name: &str) -> Option<&'static LakeSource> {
    LAKE_SOURCES.iter().find(|l| l.spec.name == name)
}

pub fn source(lake_source: &'static LakeSource) -> DeclaredSource<EnvelopeParser> {
    DeclaredSource {
        spec: lake_source.spec.clone(),
        parser: EnvelopeParser { lake_source },
    }
}

/// The source's day files (`L-YYYYMMDD.jsonl`) under `root`, in name order. The
/// index, sequence, lock and high-water files are not material.
fn day_files(root: &Path, source: &LakeSource) -> Result<Vec<PathBuf>, String> {
    let mut out = Vec::new();
    for dir in source.dirs {
        let path = root.join(dir);
        let entries = match std::fs::read_dir(&path) {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => return Err(format!("{}: {error}", path.display())),
        };
        for entry in entries {
            let entry = entry.map_err(|error| format!("{}: {error}", path.display()))?;
            let p = entry.path();
            let Some(name) = p.file_name().and_then(|n| n.to_str()) else {
                continue;
            };
            let Some(stem) = name.strip_suffix(".jsonl") else {
                continue;
            };
            let day = stem.rsplit('-').next().unwrap_or("");
            if day.len() == 8 && day.bytes().all(|b| b.is_ascii_digit()) && p.is_file() {
                out.push(p);
            }
        }
    }
    out.sort();
    Ok(out)
}

pub fn driver(path: PathBuf, host: String) -> AppendFile {
    AppendFile {
        path,
        host,
        framer: crate::Framer::incremental(lines),
    }
}

/// The lake source a binding reads, from its spec name.
fn lake_source_of(b: &crate::binding::Binding) -> Result<&'static LakeSource, String> {
    LAKE_SOURCES
        .iter()
        .find(|l| l.spec.parser == b.parser)
        .ok_or_else(|| format!("{}: not a lake source", b.parser))
}

/// `--read sinnix-lake=ROOT`: every lane's day files under the lake root
/// (`/realm`); each lane's directories are its configuration.
pub const FORMAT: crate::binding::InputFormat =
    crate::binding::InputFormat {
        name: "sinnix-lake",
        specs: || LAKE_SOURCES.iter().map(|l| &l.spec).collect(),
        plan: Some(|input, settings| {
            std::fs::read_dir(input).map_err(|error| format!("{}: {error}", input.display()))?;
            let mut bindings = Vec::new();
            for source in LAKE_SOURCES {
                bindings.extend(day_files(input, source)?.into_iter().map(|file| {
                    crate::binding::Binding::of(&source.spec, &file, settings.clone())
                }));
            }
            Ok(bindings)
        }),
        source: |b| Ok(Box::new(source(lake_source_of(b)?))),
        reader: Some(|b| {
            Ok(crate::binding::appended(driver(
                b.input.clone(),
                b.settings.host.clone(),
            )))
        }),
        capture: None,
    };

mod lanes;

#[cfg(test)]
mod tests;
