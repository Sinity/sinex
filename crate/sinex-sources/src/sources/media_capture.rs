//! Screen and audio capture on the `Media` driver (architecture §5.3), run by
//! `sinex-session`. Screen capture takes a still of each output at an interval
//! (`grim`, JPEG); audio capture records fixed-length segments of the default
//! source (`pw-record`, 16 kHz mono, encoded to Opus with `ffmpeg` when it is
//! present). Each segment is one stored material and one `media.segment`
//! event; reading text from screens and speech from audio are derivations
//! over those events (the `screen-text` derivation in `sinexd`).
//!
//! Private mode stops capture: nothing is recorded while it is on.
use crate::drivers::media::{self, Media};
use crate::registry::{Declared, Driver, SourceSpec};
use crate::{Parser, RecordContext, RecordError};
use serde_json::{Value, json};
use sinex_model::kinds::session::MediaSegment;
use sinex_model::{Class, InputRef, Timestamp};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

pub const SPEC: SourceSpec = SourceSpec {
    name: "session.media",
    driver: Driver::Media,
    consumer: "recall: what was on screen and said; screen text and transcripts",
    // Whatever was on screen or within the microphone's reach.
    class: Class::Guarded,
    parser: "media-segment",
    parser_version: 1,
    event_types: &["media.segment"],
    framer: media::whole,
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

    fn parse(&self, _: &&[u8], _: &()) -> Result<(Vec<MediaSegment>, ()), RecordError> {
        Err(RecordError::new(
            "a media segment is read with its manifest",
        ))
    }

    fn parse_record(
        &self,
        record: &&[u8],
        _: &(),
        context: &RecordContext<'_>,
    ) -> Result<(Vec<MediaSegment>, ()), RecordError> {
        let m = context.manifest;
        let started_at =
            ts(&m["started_ns"]).ok_or_else(|| RecordError::new("segment has no start"))?;
        let ended_at = ts(&m["ended_ns"]).unwrap_or(started_at);
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

pub fn source() -> Declared<MediaParser> {
    Declared {
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
    let Ok(out) = Command::new("hyprctl")
        .args(["monitors", "-j"])
        .stderr(Stdio::null())
        .output()
    else {
        return Vec::new();
    };
    serde_json::from_slice::<Vec<Value>>(&out.stdout)
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
    sinex_protocol::private_mode::PrivateMode::default().on()
}

/// Take a still of every output each `every`, for `seconds` (forever
/// otherwise). Returns the stills written.
pub fn capture_screen(
    spool: &Path,
    every: Duration,
    seconds: Option<Duration>,
) -> std::io::Result<u64> {
    std::fs::create_dir_all(spool)?;
    let deadline = seconds.map(|s| Instant::now() + s);
    let mut n = 0;
    loop {
        let tick = Instant::now();
        if !private_on() {
            // Without Hyprland to name the outputs, one still of everything.
            let mut targets: Vec<(Option<String>, u32, u32)> = outputs()
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
                let ok = grim
                    .args(["-t", "jpeg", "-q", "75"])
                    .arg(&partial)
                    .stderr(Stdio::null())
                    .status()
                    .is_ok_and(|s| s.success());
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
        if deadline.is_some_and(|d| Instant::now() >= d) {
            return Ok(n);
        }
        std::thread::sleep(every.saturating_sub(tick.elapsed()));
    }
}

/// Record the default audio source in segments of `segment`, for `seconds`
/// (forever otherwise). Returns the segments written.
pub fn capture_audio(
    spool: &Path,
    segment: Duration,
    seconds: Option<Duration>,
) -> std::io::Result<u64> {
    std::fs::create_dir_all(spool)?;
    let deadline = seconds.map(|s| Instant::now() + s);
    let encode = Command::new("ffmpeg")
        .arg("-version")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_ok();
    let mut n = 0;
    while deadline.is_none_or(|d| Instant::now() < d) {
        if private_on() {
            std::thread::sleep(Duration::from_secs(1));
            continue;
        }
        let length = deadline.map_or(segment, |d| {
            segment.min(d.saturating_duration_since(Instant::now()))
        });
        if length < Duration::from_secs(1) {
            break;
        }
        let start = Timestamp::now();
        let wav = spool.join(format!(".{}.wav.partial", start.unix_ns()));
        let mut child = Command::new("pw-record")
            .args(["--rate", "16000", "--channels", "1", "--format", "s16"])
            .arg(&wav)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()?;
        std::thread::sleep(length);
        let _ = Command::new("kill")
            .args(["-INT", &child.id().to_string()])
            .status();
        let _ = child.wait();
        let end = Timestamp::now();
        if !wav.exists() {
            continue;
        }
        let st = stem(start, end, "audio");
        let (file, ext, mime) = if encode {
            let ogg = spool.join(format!(".{}.ogg.partial", start.unix_ns()));
            let ok = Command::new("ffmpeg")
                .args(["-loglevel", "error", "-y", "-i"])
                .arg(&wav)
                .args(["-c:a", "libopus", "-b:a", "24k", "-f", "ogg"])
                .arg(&ogg)
                .status()
                .is_ok_and(|s| s.success());
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

#[cfg(test)]
mod tests;
