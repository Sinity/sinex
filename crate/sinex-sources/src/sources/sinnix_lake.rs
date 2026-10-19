//! The Sinnix capture lake, read as material (architecture §5.7 phase 1;
//! spec `port-sinnix.md` §2-3). Every envelope lane writes `sinnix-capture-v1`
//! JSONL day files; one parser reads the envelope for every lane, and each
//! lane is declared under its catalog source, so lake history and the native
//! source that later captures the lane share a name and a series.
//!
//! The lake's layout is configuration: each lane names its directories
//! relative to a root and the `lane` values it accepts. A record whose `lane`
//! is not among them is a failure, never a silent skip. Firehose lanes (rule
//! 5: network flows) are stored as material and emit no events; their
//! per-minute summaries are a derivation.
use crate::drivers::append_file::{AppendFile, lines};
use crate::registry::{Declared, Driver, SourceSpec};
use crate::{Parser, RecordError};
use serde_json::{Map, Value};
use sinex_model::kinds::machine::CaptureObserved;
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
    /// Stored as material only (spec `port-sinnix.md` §1 rule 5).
    pub firehose: bool,
}

const fn spec(name: &'static str, parser: &'static str, consumer: &'static str) -> SourceSpec {
    SourceSpec {
        name,
        driver: Driver::AppendFile,
        consumer,
        // The class of each lane is the operator's to choose (sxnext-zpa);
        // floors raise it without a code change.
        class: Class::Open,
        parser,
        parser_version: 1,
        event_types: &["capture.observed"],
        framer: lines,
    }
}

/// Every envelope lane the reader knows. The non-envelope lanes (keyboard,
/// terminal sessions, scrollback, IRC, media files, transcripts, telemetry,
/// audit, phone) have their own formats and readers.
pub const LANES: &[Lane] = &[
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
    Ok(CaptureObserved {
        observed_at,
        lane: name.to_string(),
        host: record
            .get("host")
            .and_then(Value::as_str)
            .ok_or_else(|| RecordError::new("envelope has no host"))?
            .to_string(),
        seq: record
            .get("seq")
            .and_then(Value::as_i64)
            .ok_or_else(|| RecordError::new("envelope has no seq"))?,
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
    type Payload = CaptureObserved;

    fn parse(
        &self,
        record: &Map<String, Value>,
        _: &(),
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

pub fn source(lane: &'static Lane) -> Declared<EnvelopeParser> {
    Declared {
        spec: lane.spec.clone(),
        parser: EnvelopeParser { lane },
    }
}

/// The lane's day files (`L-YYYYMMDD.jsonl`) under `root`, in name order. The
/// index, sequence, lock and high-water files are not material.
pub fn day_files(root: &Path, lane: &Lane) -> Vec<PathBuf> {
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
        framer: lines,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn record(v: Value) -> Map<String, Value> {
        v.as_object().unwrap().clone()
    }

    fn line(lane: &str, seq: i64) -> Map<String, Value> {
        record(
            json!({"schema": "sinnix-capture-v1", "schema_version": 1, "lane": lane,
            "ts": 1_760_000_000.25, "host": "sinnix-prime", "seq": seq,
            "payload": {"app": "kitty"}, "raw_ref": null}),
        )
    }

    #[test]
    fn reads_the_envelope_under_the_catalog_source() {
        let p = EnvelopeParser {
            lane: lane("desktop.clipboard").unwrap(),
        };
        let (out, ()) = p.parse(&line("primary", 7), &()).unwrap();
        assert_eq!(out[0].lane, "primary");
        assert_eq!(out[0].seq, 7);
        assert_eq!(out[0].payload, json!({"app": "kitty"}));
        assert_eq!(
            out[0].observed_at,
            Timestamp::from_unix_ns(1_760_000_000_250_000_000).unwrap()
        );
    }

    #[test]
    fn unbound_lanes_and_bad_envelopes_fail() {
        let p = EnvelopeParser {
            lane: lane("desktop.clipboard").unwrap(),
        };
        assert!(p.parse(&line("mpris", 1), &()).is_err());
        let mut bad = line("clipboard", 1);
        bad.insert("schema".into(), json!("other"));
        assert!(p.parse(&bad, &()).is_err());
        bad = line("clipboard", 1);
        bad.remove("seq");
        assert!(p.parse(&bad, &()).is_err());
    }

    #[test]
    fn firehoses_emit_nothing_but_still_validate() {
        let p = EnvelopeParser {
            lane: lane("network.netflow").unwrap(),
        };
        assert!(p.parse(&line("netflow", 1), &()).unwrap().0.is_empty());
        assert!(p.parse(&line("mpris", 1), &()).is_err());
    }

    #[test]
    fn day_files_skip_index_and_sidecars() {
        let root = std::env::temp_dir().join(format!("sinex-lake-{}", std::process::id()));
        let dir = root.join("activity/desktop/notification");
        std::fs::create_dir_all(&dir).unwrap();
        for f in [
            "notifications-20261001.jsonl",
            "notifications-index.jsonl",
            "notifications-20261001.jsonl.hw",
            "notifications.seq",
        ] {
            std::fs::write(dir.join(f), b"").unwrap();
        }
        let files = day_files(&root, lane("desktop.notification").unwrap());
        assert_eq!(files.len(), 1);
        assert!(files[0].ends_with("notifications-20261001.jsonl"));
    }

    #[test]
    fn source_names_and_parsers_are_unique() {
        let mut names: Vec<_> = LANES.iter().map(|l| l.spec.name).collect();
        names.sort_unstable();
        names.dedup();
        assert_eq!(names.len(), LANES.len());
        assert!(LANES.iter().all(|l| l.spec.parser.ends_with(l.spec.name)));
    }
}
