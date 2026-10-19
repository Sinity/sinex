//! Text read from screen segments (`screen.text`): a per-event derivation over
//! `media.segment` events that reads each screen still's stored bytes with a
//! local OCR engine (Tesseract). It runs in the derivation driver like any
//! other derivation; it lives here rather than in `sinex-automata` because it
//! runs a local tool. Without the engine it is not registered, and the daemon
//! says so once.
//!
//! Speech from audio segments needs a local speech-to-text engine; audio
//! segments produce nothing here.
use sinex_automata::{Env, Input, PerEvent};
use sinex_model::kinds::session::ScreenText as ScreenTextPayload;
use sinex_model::{Class, Event, Origin, ReadingOrder, Timestamp};
use std::process::{Command, Stdio};

const LANGUAGES: &str = "eng+pol";

/// The engine's version line, recorded with each output.
pub fn engine() -> Result<String, String> {
    let out = Command::new("tesseract")
        .arg("--version")
        .output()
        .map_err(|e| format!("tesseract is not available: {e}"))?;
    let text =
        String::from_utf8_lossy(&out.stdout).to_string() + &String::from_utf8_lossy(&out.stderr);
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
    child
        .stdin
        .take()
        .expect("piped")
        .write_all(image)
        .map_err(|e| e.to_string())?;
    let out = child.wait_with_output().map_err(|e| e.to_string())?;
    if !out.status.success() {
        return Err(format!("tesseract exited with {}", out.status));
    }
    Ok(String::from_utf8_lossy(&out.stdout).trim().to_owned())
}

/// Screen text, bound to the engine found at start.
pub struct ScreenText {
    pub engine: String,
}

impl PerEvent for ScreenText {
    const NAME: &'static str = "screen-text";
    /// Bump when the reading of a segment changes (engine, languages, options).
    const VERSION: u32 = 2;
    const INPUTS: &'static [&'static str] = &["media.segment"];
    const OUTPUTS: &'static [&'static str] = &["screen.text"];
    const READS_MATERIAL: bool = true;

    fn derive(&self, input: Input<'_>, _: &Env) -> Result<Vec<Event>, String> {
        let segment = input.event;
        if segment.payload.get("kind").and_then(|k| k.as_str()) != Some("screen") {
            return Ok(Vec::new());
        }
        let image = input
            .material
            .ok_or("the screen still's bytes were not read")?;
        let text = ocr(image)?;
        let started_at = segment
            .payload
            .get("started_at")
            .cloned()
            .and_then(|v| serde_json::from_value::<Timestamp>(v).ok())
            .or(segment.ts_orig.at)
            .unwrap_or_else(Timestamp::now);
        let payload = ScreenTextPayload {
            segment: segment.id.to_string(),
            started_at,
            engine: self.engine.clone(),
            text,
        };
        let mut hasher = blake3::Hasher::new();
        hasher.update(
            serde_json::to_string(&payload)
                .expect("serializes")
                .as_bytes(),
        );
        hasher.update(segment.id.0.as_bytes());
        let event = Event::build(
            &payload,
            Self::NAME,
            Origin::Derived {
                parents: vec![segment.id],
            },
            // The driver sets the reading order from the version and the
            // input's commit position.
            ReadingOrder(0),
            *hasher.finalize().as_bytes(),
            // A segment's text is as private as the segment.
            Class::Guarded,
        )
        .map_err(|e| e.to_string())?;
        Ok(vec![event])
    }
}
