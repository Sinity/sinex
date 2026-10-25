//! Sinnix capture-lake JSONL day files become stored material and typed
//! observations under each lane's catalog source. Configuration names the
//! directories relative to the lake root and the accepted envelope lanes. An
//! unbound lane is a record failure. Network-flow records keep their material
//! without emitting per-record events.
use crate::drivers::append_file::{AppendFile, lines};
use crate::registry::{DeclaredSource, Driver, SourceSpec};
use crate::{Parser, RecordError};
use serde_json::{Map, Value};
use sinex_model::event_types::machine::CaptureObserved;
use sinex_model::{Class, Timestamp};
use std::path::{Path, PathBuf};

/// One lane family: a catalog source, the directories it is written to and
/// the `lane` values found in them.
#[derive(Clone, Debug)]
pub struct Lane {
    pub spec: SourceSpec,
    /// Directories relative to the lake root (`/realm`).
    pub dirs: &'static [&'static str],
    /// Accepted `lane` values; renamed lanes list both names.
    pub lanes: &'static [&'static str],
    /// Stored as material only.
    pub firehose: bool,
}

const fn spec(name: &'static str, parser: &'static str, consumer: &'static str) -> SourceSpec {
    SourceSpec {
        name,
        driver: Driver::AppendFile,
        consumer,
        // The class of each lane is the operator's to choose; floors raise it
        // without a code change.
        class: Class::Open,
        parser,
        parser_version: 2,
        event_types: &["capture.observed"],
        framer: crate::Framer::incremental(lines),
    }
}

/// Every envelope lane the reader knows. The non-envelope lanes (keyboard,
/// terminal sessions, scrollback, IRC, media files, transcripts, telemetry,
/// audit, phone) have their own formats and readers.
const LANES: &[Lane] = &[
    Lane {
        spec: spec(
            "desktop.clipboard",
            "sinnix-lake:desktop.clipboard",
            "recall, prompt provenance",
        ),
        dirs: &["activity/desktop/clipboard", "activity/desktop/selection"],
        lanes: &["clipboard", "primary"],
        firehose: false,
    },
    Lane {
        spec: spec(
            "desktop.notification",
            "sinnix-lake:desktop.notification",
            "recall, interruption analysis",
        ),
        dirs: &["activity/desktop/notification"],
        lanes: &["notifications"],
        firehose: false,
    },
    Lane {
        spec: spec(
            "desktop.media",
            "sinnix-lake:desktop.media",
            "listening and attention timeline",
        ),
        dirs: &["activity/desktop/media"],
        lanes: &["mpris"],
        firehose: false,
    },
    Lane {
        spec: spec(
            "desktop.a11y",
            "sinnix-lake:desktop.a11y",
            "what was on screen, recall",
        ),
        dirs: &["activity/desktop/accessibility"],
        lanes: &["a11y"],
        firehose: false,
    },
    Lane {
        spec: spec(
            "desktop.idle-inhibitor",
            "sinnix-lake:desktop.idle-inhibitor",
            "presence and AFK evidence",
        ),
        dirs: &["activity/desktop/idle-inhibitor"],
        lanes: &["idle-inhibitors"],
        firehose: false,
    },
    Lane {
        spec: spec(
            "media.screen",
            "sinnix-lake:media.screen",
            "screen recall and OCR",
        ),
        dirs: &["activity/desktop/frame"],
        lanes: &["screen-frames"],
        firehose: false,
    },
    Lane {
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
        lanes: &["audio-devices", "audio-topology", "audio-index"],
        firehose: false,
    },
    Lane {
        spec: spec(
            "browser.navigation",
            "sinnix-lake:browser.navigation",
            "browsing trails, recall",
        ),
        dirs: &["activity/browser/navigation"],
        lanes: &["browser-nav-edges"],
        firehose: false,
    },
    Lane {
        spec: spec(
            "reading.stack",
            "sinnix-lake:reading.stack",
            "reading queue",
        ),
        dirs: &["activity/browser/reading"],
        lanes: &["reading-stack"],
        firehose: false,
    },
    Lane {
        spec: spec("display", "sinnix-lake:display", "machine state timeline"),
        dirs: &["device/sinnix-prime/monitor"],
        lanes: &["monitor"],
        firehose: false,
    },
    Lane {
        spec: spec(
            "network.netflow",
            "sinnix-lake:network.netflow",
            "per-minute flow summaries (derived)",
        ),
        dirs: &["device/sinnix-prime/netflow"],
        lanes: &["netflow"],
        firehose: true,
    },
    Lane {
        spec: spec(
            "peripheral",
            "sinnix-lake:peripheral",
            "battery and audio device state",
        ),
        dirs: &[
            "device/sinnix-prime/peripheral/bt-audio",
            "device/sinnix-prime/peripheral/bt-battery",
            "device/sinnix-prime/peripheral/logitech",
        ],
        lanes: &["bt-audio", "bt-battery", "logitech"],
        firehose: false,
    },
    Lane {
        spec: spec(
            "environment.awair",
            "sinnix-lake:environment.awair",
            "room air, sleep and focus analysis",
        ),
        dirs: &["health/environment"],
        lanes: &["awair", "environment"],
        firehose: false,
    },
];

pub struct EnvelopeParser {
    lane: &'static Lane,
}

fn envelope(record: &Map<String, Value>, lane: &Lane) -> Result<CaptureObserved, RecordError> {
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
    if !lane.lanes.contains(&name) {
        return Err(RecordError::new(format!(
            "lane {name:?} is not bound to {} (accepted: {})",
            lane.spec.name,
            lane.lanes.join(", ")
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
    CaptureObserved::new(
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
    type Payload = CaptureObserved;

    fn parse(
        &self,
        record: &Map<String, Value>,
        _: &(),
        _context: &crate::RecordContext<'_>,
    ) -> Result<(Vec<CaptureObserved>, ()), RecordError> {
        let observed = envelope(record, self.lane)?;
        if self.lane.firehose {
            return Ok((Vec::new(), ()));
        }
        Ok((vec![observed], ()))
    }
}

pub fn lane(name: &str) -> Option<&'static Lane> {
    LANES.iter().find(|l| l.spec.name == name)
}

pub fn source(lane: &'static Lane) -> DeclaredSource<EnvelopeParser> {
    DeclaredSource {
        spec: lane.spec.clone(),
        parser: EnvelopeParser { lane },
    }
}

/// The lane's day files (`L-YYYYMMDD.jsonl`) under `root`, in name order. The
/// index, sequence, lock and high-water files are not material.
fn day_files(root: &Path, lane: &Lane) -> Vec<PathBuf> {
    let mut out = Vec::new();
    for dir in lane.dirs {
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
        framer: crate::Framer::incremental(lines),
    }
}

/// The lane a binding reads, from its spec name.
fn lane_of(b: &crate::binding::Binding) -> Result<&'static Lane, String> {
    LANES
        .iter()
        .find(|l| l.spec.parser == b.parser)
        .ok_or_else(|| format!("{}: not a lake lane", b.parser))
}

/// `--source sinnix-lake=ROOT`: every lane's day files under the lake root
/// (`/realm`); each lane's directories are its configuration.
pub const KIND: crate::binding::InputKind = crate::binding::InputKind {
    name: "sinnix-lake",
    specs: || LANES.iter().map(|l| &l.spec).collect(),
    plan: Some(|input, settings| {
        Ok(LANES
            .iter()
            .flat_map(|lane| {
                day_files(input, lane)
                    .into_iter()
                    .map(|f| crate::binding::Binding::of(&lane.spec, &f, settings.clone()))
            })
            .collect())
    }),
    source: |b| Ok(Box::new(source(lane_of(b)?))),
    reader: Some(|b| {
        Ok(crate::binding::appended(driver(
            b.input.clone(),
            b.settings.host.clone(),
        )))
    }),
    capture: None,
};

#[cfg(test)]
mod tests;
