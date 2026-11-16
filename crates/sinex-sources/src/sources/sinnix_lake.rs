//! Sinnix capture-lake JSONL day files become stored material and events
//! under each lane's catalog source. Configuration names the directories
//! relative to the lake root and the accepted envelope lanes. An unbound lane
//! is a record failure. Stream-owned lanes keep `lake.observed`; the other
//! lanes produce domain events, one per observed record or listed device.
use serde_json::{Map, Value};
use sinex_model::{Class, Timestamp, json::JsonExt};
use sinex_source_core::discovery::{DiscoverySpec, Location};
use sinex_source_core::drivers::append_file::AppendFile;
use sinex_source_core::registry::{DeclaredSource, Expectation, Origin, Shape, SourceSpec};
use sinex_source_core::{Parser, RecordError, RecordResult, binding};
use std::path::{Path, PathBuf};

/// One catalog source of the lake: the directories it is written to and the
/// `lane` values found in them.
#[derive(Clone, Debug)]
pub struct LakeSource {
    pub spec: SourceSpec,
    /// Directories relative to the lake root; `{host}` stands for the host
    /// the binding reads on.
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
        origin: Origin::Input(Shape::AppendFile),
        // Lanes sampled while their capture runs; event-driven lanes override it.
        expect: Expectation::continuous_days(2),
        purpose,
        // The class of each lane is the operator's to choose; minimum classes raise it
        // without a code change.
        class: Class::Open,
        parser,
        parser_version: 4,
        event_types,
        framer: sinex_source_core::Framer::LINES,
        ..SourceSpec::DEFAULT
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
            &["lake.observed", "capture.gap"],
        ),
        dirs: &["activity/desktop/clipboard", "activity/desktop/selection"],
        lane_values: &["clipboard", "primary"],
    },
    LakeSource {
        spec: spec(
            "desktop.notifications",
            "sinnix-lake:desktop.notifications",
            "recall, interruption analysis",
            &["lake.observed", "capture.gap"],
        ),
        dirs: &["activity/desktop/notification"],
        lane_values: &["notifications"],
    },
    LakeSource {
        spec: spec(
            "desktop.media",
            "sinnix-lake:desktop.media",
            "listening and attention timeline",
            &["lake.observed", "capture.gap"],
        ),
        dirs: &["activity/desktop/media"],
        lane_values: &["mpris"],
    },
    LakeSource {
        spec: spec(
            "desktop.a11y",
            "sinnix-lake:desktop.a11y",
            "what was on screen, recall",
            &["lake.observed", "capture.gap"],
        ),
        dirs: &["activity/desktop/accessibility"],
        lane_values: &["a11y"],
    },
    LakeSource {
        spec: SourceSpec {
            // Written only when it changes, which can be weeks apart.
            expect: Expectation::continuous_days(30),
            ..spec(
                "desktop.idle-inhibitor",
                "sinnix-lake:desktop.idle-inhibitor",
                "presence and AFK state",
                &["lake.observed", "capture.gap"],
            )
        },
        dirs: &["activity/desktop/idle-inhibitor"],
        lane_values: &["idle-inhibitors"],
    },
    LakeSource {
        spec: spec(
            "media.screen",
            "sinnix-lake:media.screen",
            "screen recall and OCR",
            &["lake.observed", "capture.gap"],
        ),
        dirs: &["activity/desktop/frame"],
        lane_values: &["screen-frames"],
    },
    LakeSource {
        spec: spec(
            "media.audio",
            "sinnix-lake:media.audio",
            "audio recall, speech index, device state",
            &["lake.observed", "capture.gap"],
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
            &["link.followed", "capture.gap"],
        ),
        dirs: &["activity/browser/navigation"],
        lane_values: &["browser-nav-edges"],
    },
    LakeSource {
        spec: SourceSpec {
            // Written only when it changes, which can be weeks apart.
            expect: Expectation::continuous_days(30),
            ..spec(
                "reading.stack",
                "sinnix-lake:reading.stack",
                "reading queue",
                &["reading_stack.changed", "capture.gap"],
            )
        },
        dirs: &["activity/browser/reading"],
        lane_values: &["reading-stack"],
    },
    LakeSource {
        spec: spec(
            "desktop.display",
            "sinnix-lake:desktop.display",
            "machine state timeline",
            &["display.sampled", "capture.gap"],
        ),
        dirs: &["device/{host}/monitor"],
        lane_values: &["monitor"],
    },
    LakeSource {
        spec: SourceSpec {
            parser_version: 4,
            ..spec(
                "network.netflow",
                "sinnix-lake:network.netflow",
                "network flows by connection",
                &["lake.observed", "capture.gap"],
            )
        },
        dirs: &["device/{host}/netflow"],
        lane_values: &["netflow"],
    },
    LakeSource {
        spec: spec(
            "machine.peripheral",
            "sinnix-lake:machine.peripheral",
            "battery and audio device state",
            &["battery.sampled", "bluetooth_audio.sampled", "capture.gap"],
        ),
        dirs: &[
            "device/{host}/peripheral/bt-audio",
            "device/{host}/peripheral/bt-battery",
            "device/{host}/peripheral/logitech",
        ],
        lane_values: &["bt-audio", "bt-battery", "logitech"],
    },
    LakeSource {
        spec: spec(
            "environment.awair",
            "sinnix-lake:environment.awair",
            "room air, sleep and focus analysis",
            &["air.sampled", "capture.gap"],
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
    host: String,
    seq: u64,
    boot: Option<String>,
    session: Option<String>,
    observed_at: Timestamp,
    payload: Value,
    /// The path of a file the record describes (a frame, an audio segment).
    raw_ref: Option<String>,
}

fn envelope(record: &Map<String, Value>, source: &LakeSource) -> Result<Envelope, RecordError> {
    match record.str("schema") {
        Some("sinnix-capture-v1") => {}
        other => {
            return Err(RecordError::new(format!(
                "not a sinnix-capture-v1 envelope (schema {other:?})"
            )));
        }
    }
    if record.i64("schema_version") != Some(1) {
        return Err(RecordError::new("envelope schema_version is not 1"));
    }
    let lane = record.str("lane").record("envelope has no lane")?;
    if !source.lane_values.contains(&lane) {
        return Err(RecordError::new(format!(
            "lane {lane:?} is not bound to {} (accepted: {})",
            source.spec.name,
            source.lane_values.join(", ")
        )));
    }
    if record.str("host").is_none_or(str::is_empty) {
        return Err(RecordError::new("envelope host is not a nonempty string"));
    }
    if record.u64("seq").is_none() {
        return Err(RecordError::new("envelope seq is not an unsigned integer"));
    }
    if record
        .get("raw_ref")
        .is_some_and(|value| !value.is_null() && !value.is_string())
    {
        return Err(RecordError::new("envelope raw_ref is not a string or null"));
    }
    let ts = record.f64("ts").record("envelope has no ts")?;
    Ok(Envelope {
        lane: lane.to_owned(),
        host: record["host"].as_str().expect("validated host").to_owned(),
        seq: record["seq"].as_u64().expect("validated sequence"),
        boot: generation(record, "boot_id")?,
        session: generation(record, "session_id")?.or(generation(record, "session")?),
        observed_at: lanes::seconds(ts)?,
        payload: record
            .get("payload")
            .cloned()
            .record("envelope has no payload")?,
        raw_ref: record.str("raw_ref").map(str::to_owned),
    })
}

fn generation(record: &Map<String, Value>, field: &str) -> Result<Option<String>, RecordError> {
    match record.get(field) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(value)) if !value.is_empty() => Ok(Some(value.clone())),
        _ => Err(RecordError::new(format!(
            "envelope {field} is not a nonempty string"
        ))),
    }
}

mod sequence;

impl Parser for EnvelopeParser {
    type Record<'a> = Map<String, Value>;
    type Carry = sequence::Carry;
    type Payload = lanes::LakeEvent;

    fn failed_record(&self, carry: &Self::Carry) -> Self::Carry {
        sequence::failed(carry)
    }

    fn parse(
        &self,
        record: &Map<String, Value>,
        carry: &sequence::Carry,
        _context: &sinex_source_core::RecordContext<'_>,
    ) -> Result<(Vec<lanes::LakeEvent>, sequence::Carry), RecordError> {
        let envelope = envelope(record, self.lake_source)?;
        let mut events = lanes::events(&envelope)?;
        let (gap, carry) = sequence::observe(carry, &envelope);
        if let Some(gap) = gap {
            events.push(lanes::LakeEvent::Gap(gap));
        }
        Ok((events, carry))
    }
}

pub fn lake_source(name: &str) -> Option<&'static LakeSource> {
    LAKE_SOURCES.iter().find(|l| l.spec.name == name)
}

/// The source's day files (`L-YYYYMMDD.jsonl`) under `root` for `host`, in
/// name order. The index, sequence, lock and high-water files are not material.
fn day_files(root: &Path, host: &str, source: &LakeSource) -> Result<Vec<PathBuf>, String> {
    let mut out = Vec::new();
    for dir in source.dirs {
        let path = root.join(dir.replace("{host}", host));
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
        framer: sinex_source_core::Framer::LINES,
    }
}

/// The lake source a binding reads, from its spec name.
fn lake_source_of(b: &sinex_source_core::binding::Binding) -> Result<&'static LakeSource, String> {
    LAKE_SOURCES
        .iter()
        .find(|l| l.spec.parser == b.parser)
        .ok_or_else(|| format!("{}: not a lake source", b.parser))
}

input_format! {
    /// `stage --parser sinnix-lake ROOT`: every lane's day files under the lake root;
    /// each lane's directories are its configuration.
    pub FORMAT = "sinnix-lake" {
    specs: || LAKE_SOURCES.iter().map(|l| &l.spec).collect(),
    source: |b| {
        let lake_source = lake_source_of(b)?;
        Ok(Box::new(DeclaredSource {
            spec: lake_source.spec.clone(),
            parser: EnvelopeParser { lake_source },
        }))
    },
    discovery: DiscoverySpec::system(&[Location::new("declared", "realm")]),
    plan: |input, settings| {
        std::fs::read_dir(input).map_err(|error| format!("{}: {error}", input.display()))?;
        let mut bindings = Vec::new();
        for source in LAKE_SOURCES {
            bindings.extend(
                day_files(input, &settings.host, source)?
                    .into_iter()
                    .map(|file| binding::Binding::of(&source.spec, &file, settings.clone())),
            );
        }
        Ok(bindings)
    },
    reader: appended(driver),
} }

mod lanes;

#[cfg(test)]
mod tests;
