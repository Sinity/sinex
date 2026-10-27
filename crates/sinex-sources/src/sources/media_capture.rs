//! Screen and audio capture on the `Media` driver, run by `sinex-session`.
//! Screen capture takes a still of each output at an interval (`grim`, JPEG);
//! audio capture records fixed-length segments of the default source
//! (`pw-record`, 16 kHz mono, encoded to Opus with `ffmpeg` when it is
//! present). Each segment is one stored material and one `media.segment` event;
//! reading text from screens and speech from audio are derivations over those
//! events (the `screen-text` derivation in `sinexd`).
//!
//! Private mode stops capture: nothing is recorded while it is on.
use serde_json::{Value, json};
use sinex_model::event_types::session::MediaSegment;
use sinex_model::{Class, InputRef, Timestamp};
use sinex_source_core::drivers::media::{self, Media};
use sinex_source_core::registry::{DeclaredSource, Driver, SourceSpec};
use sinex_source_core::{Parser, RecordContext, RecordError};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

pub const SPEC: SourceSpec = SourceSpec {
    name: "session.media",
    driver: Driver::Media,
    excisable: true,
    purpose: "recall: what was on screen and said; screen text and transcripts",
    // Whatever was on screen or within the microphone's reach.
    class: Class::Guarded,
    parser: "media-segment",
    parser_version: 1,
    event_types: &["media.segment"],
    framer: sinex_source_core::Framer::export(
        media::whole,
        sinex_source_core::drivers::export_archive::Framing::Whole,
    ),
};

pub struct MediaParser;

fn ts(v: &Value) -> Option<Timestamp> {
    let ns: i128 = match v {
        Value::String(s) => s.parse().ok()?,
        Value::Number(n) => n.as_i64()?.into(),
        _ => return None,
    };
    Timestamp::from_unix_ns(ns).ok()
}

impl Parser for MediaParser {
    type Record<'a> = &'a [u8];
    type Carry = ();
    type Payload = MediaSegment;

    fn parse(
        &self,
        record: &&[u8],
        _: &(),
        context: &RecordContext<'_>,
    ) -> Result<(Vec<MediaSegment>, ()), RecordError> {
        let m = &context.manifest.details;
        let started_at = ts(m.get("started_ns").unwrap_or(&Value::Null))
            .ok_or_else(|| RecordError::new("segment has no start"))?;
        let ended_at = ts(m.get("ended_ns").unwrap_or(&Value::Null)).unwrap_or(started_at);
        let s = |k: &str| m.get(k).and_then(Value::as_str).map(str::to_owned);
        let n = |k: &str| {
            m.get(k)
                .and_then(Value::as_u64)
                .and_then(|v| u32::try_from(v).ok())
        };
        Ok((
            vec![MediaSegment {
                kind: s("kind").ok_or_else(|| RecordError::new("segment has no kind"))?,
                started_at,
                ended_at,
                mime: s("mime").unwrap_or_else(|| "application/octet-stream".into()),
                bytes: record.len() as u64,
                target: s("target"),
                width: n("width"),
                height: n("height"),
            }],
            (),
        ))
    }
}

pub fn source() -> DeclaredSource<MediaParser> {
    DeclaredSource {
        spec: SPEC,
        parser: MediaParser,
    }
}

pub fn driver(spool: PathBuf, host: String, kind: &str) -> Media {
    Media {
        spool,
        input: InputRef {
            kind: "media".into(),
            locator: format!("sinex-session:{kind}"),
            host,
        },
    }
}

fn stem(start: Timestamp, end: Timestamp, kind: &str) -> String {
    format!("{:020}-{:020}.{kind}", start.unix_ns(), end.unix_ns())
}

/// Outputs (monitors) and their pixel sizes, from Hyprland.
pub fn outputs() -> Vec<(String, u32, u32)> {
    outputs_until(&sinex_source_core::capture::CaptureStop::new(None))
}

fn outputs_until(stop: &sinex_source_core::capture::CaptureStop) -> Vec<(String, u32, u32)> {
    let Ok(Some(out)) = stop.output(Command::new("hyprctl").args(["monitors", "-j"])) else {
        return Vec::new();
    };
    serde_json::from_slice::<Vec<Value>>(&out)
        .unwrap_or_default()
        .into_iter()
        .filter_map(|m| {
            let name = m["name"].as_str()?.to_owned();
            let w = m["width"].as_u64().and_then(|v| u32::try_from(v).ok());
            let h = m["height"].as_u64().and_then(|v| u32::try_from(v).ok());
            Some((name, w.unwrap_or(0), h.unwrap_or(0)))
        })
        .collect()
}

fn private_on() -> bool {
    sinex_transport::private_mode::PrivateMode::default().on()
}

/// Take a still of every output each `every`, until capture stops. Returns the stills written.
fn capture_screen(
    spool: &Path,
    every: Duration,
    stop: &sinex_source_core::capture::CaptureStop,
) -> std::io::Result<u64> {
    std::fs::create_dir_all(spool)?;
    let mut n = 0;
    while !stop.stopped() {
        let tick = Instant::now();
        if !private_on() {
            // Without Hyprland to name the outputs, one still of everything.
            let mut targets: Vec<(Option<String>, u32, u32)> = outputs_until(stop)
                .into_iter()
                .map(|(n, w, h)| (Some(n), w, h))
                .collect();
            if targets.is_empty() {
                targets.push((None, 0, 0));
            }
            for (output, width, height) in targets {
                let at = Timestamp::now();
                let kind = format!("screen-{}", output.as_deref().unwrap_or("all"));
                let st = stem(at, at, &kind);
                let partial = spool.join(format!("{st}.jpg.partial"));
                let mut grim = Command::new("grim");
                if let Some(o) = &output {
                    grim.args(["-o", o]);
                }
                grim.args(["-t", "jpeg", "-q", "75"])
                    .arg(&partial)
                    .stderr(Stdio::null());
                let ok = stop
                    .status(&mut grim)
                    .is_ok_and(|s| s.is_some_and(|s| s.success()));
                if !ok {
                    let _ = std::fs::remove_file(&partial);
                    continue;
                }
                media::publish_segment(
                    spool,
                    &partial,
                    &st,
                    "jpg",
                    &json!({
                        "kind": "screen", "started_ns": at.unix_ns().to_string(), "ended_ns": at.unix_ns().to_string(),
                        "mime": "image/jpeg", "target": output.as_deref().unwrap_or("all"),
                        "width": (width > 0).then_some(width), "height": (height > 0).then_some(height),
                        "media_type": "image/jpeg",
                    }),
                )?;
                n += 1;
            }
        }
        stop.wait(every.saturating_sub(tick.elapsed()));
    }
    Ok(n)
}

/// Record the default audio source in segments of `segment`, until capture
/// stops. Returns the segments written.
fn capture_audio(
    spool: &Path,
    segment: Duration,
    stop: &sinex_source_core::capture::CaptureStop,
) -> std::io::Result<u64> {
    std::fs::create_dir_all(spool)?;
    let encode = stop
        .status(
            Command::new("ffmpeg")
                .arg("-version")
                .stdout(Stdio::null())
                .stderr(Stdio::null()),
        )
        .is_ok_and(|status| status.is_some_and(|s| s.success()));
    let mut n = 0;
    while !stop.stopped() {
        if private_on() {
            stop.wait(Duration::from_secs(1));
            continue;
        }
        let start = Timestamp::now();
        let wav = spool.join(format!(".{}.wav.partial", start.unix_ns()));
        let mut command = Command::new("pw-record");
        command
            .args(["--rate", "16000", "--channels", "1", "--format", "s16"])
            .arg(&wav)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        let Some(mut child) = stop.spawn(&mut command)? else {
            break;
        };
        stop.wait(segment);
        child.interrupt();
        child.wait()?;
        let end = Timestamp::now();
        if !wav.exists() {
            continue;
        }
        let st = stem(start, end, "audio");
        let (file, ext, mime) = if encode {
            let ogg = spool.join(format!(".{}.ogg.partial", start.unix_ns()));
            let mut command = Command::new("ffmpeg");
            command
                .args(["-loglevel", "error", "-y", "-i"])
                .arg(&wav)
                .args(["-c:a", "libopus", "-b:a", "24k", "-f", "ogg"])
                .arg(&ogg);
            let ok = stop
                .status(&mut command)
                .is_ok_and(|s| s.is_some_and(|s| s.success()));
            if ok {
                std::fs::remove_file(&wav)?;
                (ogg, "ogg", "audio/ogg")
            } else {
                (wav, "wav", "audio/wav")
            }
        } else {
            (wav, "wav", "audio/wav")
        };
        media::publish_segment(
            spool,
            &file,
            &st,
            ext,
            &json!({
                "kind": "audio", "started_ns": start.unix_ns().to_string(), "ended_ns": end.unix_ns().to_string(),
                "mime": mime, "target": "default", "media_type": mime,
            }),
        )?;
        n += 1;
    }
    Ok(n)
}

/// What a media binding captures: the `kind` option, `screen` or `audio`.
fn media(b: &sinex_source_core::binding::Binding) -> Result<&'static str, String> {
    match b.settings.option("kind") {
        Some("screen") => Ok("screen"),
        Some("audio") => Ok("audio"),
        other => Err(format!("media: kind {other:?}; one of screen, audio")),
    }
}

/// `--read media=SPOOL`: screen stills (option `kind: screen`, every
/// `every` seconds, 60 by default) or audio segments (`kind: audio`,
/// `segment` seconds each, 300 by default), captured into SPOOL.
pub const FORMAT: sinex_source_core::binding::InputFormat =
    sinex_source_core::binding::InputFormat {
        name: "media",
        discovery: sinex_source_core::discovery::DiscoverySpec {
            owner: sinex_source_core::discovery::Owner::Session,
            locations: &[sinex_source_core::discovery::Location {
                root: "session",
                path: "media",
            }],
            capabilities: &[],
            credentials: &[],
            streams: &[
                sinex_source_core::discovery::CaptureStream {
                    spool: "screen",
                    options: "{\"kind\":\"screen\"}",
                    capabilities: &["wayland", "grim"],
                },
                sinex_source_core::discovery::CaptureStream {
                    spool: "audio",
                    options: "{\"kind\":\"audio\"}",
                    capabilities: &["pipewire", "pw-record"],
                },
            ],
            requests: &[],
            request_only: false,
        },
        discover: crate::discovery::discover_format,
        specs: || vec![&SPEC],
        plan: Some(|spool, settings| {
            let b = sinex_source_core::binding::Binding::of(&SPEC, spool, settings.clone());
            let kind = media(&b)?;
            Ok(vec![b.named(kind)])
        }),
        source: |_| Ok(Box::new(source())),
        reader: Some(|b| {
            Ok(sinex_source_core::binding::polled(driver(
                b.input.clone(),
                b.settings.host.clone(),
                media(b)?,
            )))
        }),
        capture: Some(|b, stop| {
            let seconds =
                |key, default| Duration::from_secs(b.settings.number(key).unwrap_or(default));
            match media(b)? {
                "screen" => capture_screen(&b.input, seconds("every", 60), stop),
                _ => capture_audio(&b.input, seconds("segment", 300), stop),
            }
            .map_err(|e| e.to_string())
        }),
    };

#[cfg(test)]
mod tests;
