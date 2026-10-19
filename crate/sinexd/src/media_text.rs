//! Text read from screen segments (`screen.text`), a derivation over
//! `media.segment` events that reads each segment's stored bytes with a local
//! OCR engine (Tesseract). It runs on demand (`sinexd admin media-text`), not
//! in the derivation driver: its input is the segment's bytes, which the
//! pure per-event and keyed derivations cannot read. Each segment is one key;
//! its output replaces the key's previous output, and a change of engine or
//! version reads every segment again.
//!
//! Speech from audio segments needs a local speech-to-text engine
//! (`whisper.cpp`'s `whisper-cli`); when none is installed, audio segments are
//! left unread and the count says so.
use crate::context::Context;
use crate::writer::Priority;
use serde_json::Value;
use sinex_model::kinds::session::ScreenText;
use sinex_model::{Batch, BatchKind, Class, Event, EventId, Origin, ReadingOrder, RecordOutcome, Timestamp};
use sinex_store::admission::DerivedScope;
use sinex_store::{derived, media, runs};
use std::process::{Command, Stdio};

pub const NAME: &str = "screen-text";
/// Bump when the reading of a segment changes (engine, languages, options).
pub const VERSION: i32 = 1;
const LANGUAGES: &str = "eng+pol";

/// The engine's version line, recorded with each output.
fn engine() -> Result<String, String> {
    let out = Command::new("tesseract")
        .arg("--version")
        .output()
        .map_err(|e| format!("tesseract is not available: {e}"))?;
    let text = String::from_utf8_lossy(&out.stdout).to_string() + &String::from_utf8_lossy(&out.stderr);
    Ok(text.lines().next().unwrap_or("tesseract").trim().to_owned())
}

fn ocr(image: &[u8]) -> Result<String, String> {
    use std::io::Write;
    let mut child = Command::new("tesseract")
        .args(["stdin", "stdout", "-l", LANGUAGES, "--psm", "3"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .map_err(|e| e.to_string())?;
    child.stdin.take().expect("piped").write_all(image).map_err(|e| e.to_string())?;
    let out = child.wait_with_output().map_err(|e| e.to_string())?;
    if !out.status.success() {
        return Err(format!("tesseract exited with {}", out.status));
    }
    Ok(String::from_utf8_lossy(&out.stdout).trim().to_owned())
}

pub struct Totals {
    pub read: u64,
    pub failed: u64,
    pub seconds: f64,
}

/// Read every screen segment not yet read by this version, at most `limit`.
pub async fn run(ctx: &mut Context, host: &str, limit: Option<u64>) -> Result<Totals, String> {
    let e = |e: sinex_store::Error| e.to_string();
    let engine = engine()?;
    let changed = derived::register(
        &mut ctx.aux,
        &[derived::Registered {
            name: NAME.into(),
            kind: "keyed",
            version: VERSION,
            inputs: vec!["media.segment".into()],
            outputs: vec!["screen.text".into()],
        }],
    )
    .await
    .map_err(e)?;
    let all = !changed.is_empty();
    let run = runs::start(
        &mut ctx.aux,
        &runs::Manifest {
            name: NAME,
            kind: "derivation",
            version: &VERSION.to_string(),
            build: option_env!("SINEX_BUILD").unwrap_or("dev"),
            emits: &["screen.text"],
        },
        host,
    )
    .await
    .map_err(e)?;
    let started = std::time::Instant::now();
    let mut totals = Totals { read: 0, failed: 0, seconds: 0.0 };
    let mut after = None;
    loop {
        let remaining = limit.map_or(i64::MAX, |l| (l - totals.read - totals.failed) as i64);
        if remaining <= 0 {
            break;
        }
        let batch = media::pending(&mut ctx.aux, NAME, "screen", all, after, remaining.min(32)).await.map_err(e)?;
        let Some(last) = batch.last() else { break };
        after = Some(last.event_id);
        for segment in batch {
            let text = ctx
                .blobs
                .read(&segment.blob)
                .map_err(|e| e.to_string())
                .and_then(|bytes| ocr(&bytes));
            let text = match text {
                Ok(t) => t,
                Err(reason) => {
                    eprintln!("screen-text: segment {}: {reason}", segment.event_id);
                    totals.failed += 1;
                    continue;
                }
            };
            let started_at = segment
                .payload
                .get("started_at")
                .cloned()
                .and_then(|v| serde_json::from_value::<Timestamp>(v).ok())
                .unwrap_or_else(Timestamp::now);
            let payload = ScreenText {
                segment: segment.event_id.to_string(),
                started_at,
                engine: engine.clone(),
                text,
            };
            let mut hasher = blake3::Hasher::new();
            hasher.update(serde_json::to_string(&payload).expect("serializes").as_bytes());
            hasher.update(segment.event_id.as_bytes());
            let event = Event::build(
                &payload,
                NAME,
                Origin::Derived {
                    parents: vec![EventId(segment.event_id)],
                },
                ReadingOrder(i64::try_from(Timestamp::now().unix_ns()).unwrap_or(i64::MAX)),
                *hasher.finalize().as_bytes(),
                // A segment's text is as private as the segment.
                Class::Guarded,
            )
            .map_err(|e| e.to_string())?;
            let dirty_seq = sinex_store::committed::latest(&mut ctx.aux).await.map_err(e)?;
            ctx.writer
                .submit(
                    Priority::Derived,
                    Batch {
                        run,
                        kind: BatchKind::Derived {
                            derivation: NAME.into(),
                            scope: DerivedScope::Key {
                                key: Value::String(segment.event_id.to_string()),
                                dirty_seq,
                                waiting: false,
                            }
                            .to_value(),
                        },
                        records: vec![RecordOutcome::Events {
                            location: None,
                            events: vec![event],
                        }],
                        archive: vec![],
                    },
                )
                .await
                .map_err(|e| e.to_string())?;
            totals.read += 1;
        }
    }
    runs::end(&mut ctx.aux, run).await.map_err(e)?;
    totals.seconds = started.elapsed().as_secs_f64();
    Ok(totals)
}
