//! Sinnix capture-lake JSONL day files become stored material and typed
//! observations under each lane's catalog source. Configuration names the
//! directories relative to the lake root and the accepted envelope lanes. An
//! unbound lane is a record failure. Every record is one event, network flows
//! included.
use serde_json::{Map, Value};
use sinex_model::event_types::machine::LakeObservation;
use sinex_model::{Class, Timestamp};
use sinex_source_core::drivers::append_file::{AppendFile, lines};
use sinex_source_core::registry::{DeclaredSource, Driver, SourceSpec};
use sinex_source_core::{Parser, RecordError};
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

const fn spec(name: &'static str, parser: &'static str, purpose: &'static str) -> SourceSpec {
    SourceSpec {
        name,
        driver: Driver::AppendFile,
        excisable: false,
        purpose,
        // The class of each lane is the operator's to choose; floors raise it
        // without a code change.
        class: Class::Open,
        parser,
        parser_version: 2,
        event_types: &["lake.observed"],
        framer: sinex_source_core::Framer::incremental(lines),
    }
}

/// Every envelope lane the reader knows. The non-envelope lanes (keyboard,
/// terminal sessions, scrollback, IRC, media files, transcripts, telemetry,
/// audit, phone) have their own formats and readers.
const LAKE_SOURCES: &[LakeSource] = &[
    LakeSource {
        spec: spec(
            "desktop.clipboard",
            "sinnix-lake:desktop.clipboard",
            "recall, prompt provenance",
        ),
        dirs: &["activity/desktop/clipboard", "activity/desktop/selection"],
        lane_values: &["clipboard", "primary"],
    },
    LakeSource {
        spec: spec(
            "desktop.notifications",
            "sinnix-lake:desktop.notifications",
            "recall, interruption analysis",
        ),
        dirs: &["activity/desktop/notification"],
        lane_values: &["notifications"],
    },
    LakeSource {
        spec: spec(
            "desktop.media",
            "sinnix-lake:desktop.media",
            "listening and attention timeline",
        ),
        dirs: &["activity/desktop/media"],
        lane_values: &["mpris"],
    },
    LakeSource {
        spec: spec(
            "desktop.a11y",
            "sinnix-lake:desktop.a11y",
            "what was on screen, recall",
        ),
        dirs: &["activity/desktop/accessibility"],
        lane_values: &["a11y"],
    },
    LakeSource {
        spec: spec(
            "desktop.idle-inhibitor",
            "sinnix-lake:desktop.idle-inhibitor",
            "presence and AFK evidence",
        ),
        dirs: &["activity/desktop/idle-inhibitor"],
        lane_values: &["idle-inhibitors"],
    },
    LakeSource {
        spec: spec(
            "media.screen",
            "sinnix-lake:media.screen",
            "screen recall and OCR",
        ),
        dirs: &["activity/desktop/frame"],
        lane_values: &["screen-frames"],
    },
    LakeSource {
        spec: spec(
            "media.audio",
            "sinnix-lake:media.audio",
            "audio recall, speech index, device state",
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
        ),
        dirs: &["activity/browser/navigation"],
        lane_values: &["browser-nav-edges"],
    },
    LakeSource {
        spec: spec(
            "reading.stack",
            "sinnix-lake:reading.stack",
            "reading queue",
        ),
        dirs: &["activity/browser/reading"],
        lane_values: &["reading-stack"],
    },
    LakeSource {
        spec: spec(
            "desktop.display",
            "sinnix-lake:desktop.display",
            "machine state timeline",
        ),
        dirs: &["device/sinnix-prime/monitor"],
        lane_values: &["monitor"],
    },
    LakeSource {
        spec: SourceSpec {
            parser_version: 3,
            ..spec(
                "network.netflow",
                "sinnix-lake:network.netflow",
                "network flows by connection",
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
        ),
        dirs: &["health/environment"],
        lane_values: &["awair", "environment"],
    },
];

pub struct EnvelopeParser {
    lake_source: &'static LakeSource,
}

fn envelope(
    record: &Map<String, Value>,
    source: &LakeSource,
) -> Result<LakeObservation, RecordError> {
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
    let name = record
        .get("lane")
        .and_then(Value::as_str)
        .ok_or_else(|| RecordError::new("envelope has no lane"))?;
    if !source.lane_values.contains(&name) {
        return Err(RecordError::new(format!(
            "lane {name:?} is not bound to {} (accepted: {})",
            source.spec.name,
            source.lane_values.join(", ")
        )));
    }
    let ts = record
        .get("ts")
        .and_then(Value::as_f64)
        .ok_or_else(|| RecordError::new("envelope has no ts"))?;
    // A float of seconds holds about microseconds at today's epoch; whole
    // seconds plus rounded microseconds avoid inventing nanoseconds.
    let seconds = ts.floor();
    let micros = ((ts - seconds) * 1e6).round() as i128;
    let observed_at = Timestamp::from_unix_ns(seconds as i128 * 1_000_000_000 + micros * 1_000)
        .map_err(|e| RecordError::new(format!("ts {ts} out of range: {e}")))?;
    LakeObservation::new(
        observed_at,
        name.to_string(),
        record
            .get("host")
            .and_then(Value::as_str)
            .ok_or_else(|| RecordError::new("envelope has no host"))?
            .to_string(),
        record
            .get("seq")
            .and_then(Value::as_i64)
            .ok_or_else(|| RecordError::new("envelope has no seq"))?,
        record
            .get("payload")
            .cloned()
            .ok_or_else(|| RecordError::new("envelope has no payload"))?,
        record
            .get("raw_ref")
            .and_then(Value::as_str)
            .map(str::to_owned),
    )
    .map_err(|error| RecordError::new(error.to_string()))
}

impl Parser for EnvelopeParser {
    type Record<'a> = Map<String, Value>;
    type Carry = ();
    type Payload = LakeObservation;

    fn parse(
        &self,
        record: &Map<String, Value>,
        _: &(),
        _context: &sinex_source_core::RecordContext<'_>,
    ) -> Result<(Vec<LakeObservation>, ()), RecordError> {
        Ok((vec![envelope(record, self.lake_source)?], ()))
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
fn day_files(root: &Path, source: &LakeSource) -> Vec<PathBuf> {
    let mut out = Vec::new();
    for dir in source.dirs {
        for entry in std::fs::read_dir(root.join(dir))
            .into_iter()
            .flatten()
            .flatten()
        {
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
    out
}

pub fn driver(path: PathBuf, host: String) -> AppendFile {
    AppendFile {
        path,
        host,
        framer: sinex_source_core::Framer::incremental(lines),
    }
}

/// The lake source a binding reads, from its spec name.
fn lake_source_of(b: &sinex_source_core::binding::Binding) -> Result<&'static LakeSource, String> {
    LAKE_SOURCES
        .iter()
        .find(|l| l.spec.parser == b.parser)
        .ok_or_else(|| format!("{}: not a lake source", b.parser))
}

/// `--read sinnix-lake=ROOT`: every lane's day files under the lake root
/// (`/realm`); each lane's directories are its configuration.
pub const FORMAT: sinex_source_core::binding::InputFormat =
    sinex_source_core::binding::InputFormat {
        name: "sinnix-lake",
        discovery: sinex_source_core::discovery::DiscoverySpec {
            owner: sinex_source_core::discovery::Owner::System,
            locations: &[sinex_source_core::discovery::Location {
                root: "subject",
                path: "realm",
            }],
            capabilities: &[],
            credentials: &[],
            streams: &[],
            requests: &[],
            request_only: false,
        },
        discover: crate::discovery::discover_format,
        specs: || LAKE_SOURCES.iter().map(|l| &l.spec).collect(),
        plan: Some(|input, settings| {
            Ok(LAKE_SOURCES
                .iter()
                .flat_map(|source| {
                    day_files(input, source).into_iter().map(|f| {
                        sinex_source_core::binding::Binding::of(&source.spec, &f, settings.clone())
                    })
                })
                .collect())
        }),
        source: |b| Ok(Box::new(source(lake_source_of(b)?))),
        reader: Some(|b| {
            Ok(sinex_source_core::binding::appended(driver(
                b.input.clone(),
                b.settings.host.clone(),
            )))
        }),
        capture: None,
    };

#[cfg(test)]
mod tests;
