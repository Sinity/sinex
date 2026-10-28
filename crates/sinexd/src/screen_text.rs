//! Text read from screen segments (`screen.text`): a per-event derivation over
//! `media.segment` events that reads each screen still's stored bytes with a
//! local OCR engine (Tesseract), at idle priority. It runs in the derivation runner like any
//! other derivation; it lives here rather than in `sinex-derivations` because it
//! runs a local tool. Without the engine it is not registered, and the daemon
//! says so once.
//!
//! Speech from audio segments needs a local speech-to-text engine; audio
//! segments produce nothing here.
use sinex_derivations::{Env, Input, PerEvent, Reads};
use sinex_model::event_types::session::ScreenText as ScreenTextPayload;
use sinex_model::{Class, Event, Origin, ReadingOrder};
use std::process::{Command, Stdio};

const LANGUAGES: &str = "eng+pol";

/// The engine's version line, recorded with each output.
pub(crate) fn engine() -> Result<String, String> {
    let out = Command::new("tesseract")
        .arg("--version")
        .output()
        .map_err(|e| format!("tesseract is not available: {e}"))?;
    if !out.status.success() {
        return Err(format!("tesseract --version exited with {}", out.status));
    }
    let text =
        String::from_utf8_lossy(&out.stdout).to_string() + &String::from_utf8_lossy(&out.stderr);
    Ok(text.lines().next().unwrap_or("tesseract").trim().to_owned())
}

fn ocr(image: &[u8]) -> Result<String, String> {
    use std::io::Write;
    // At idle priority: reading text off stills is background work and must
    // not take the machine from the operator.
    let mut child = Command::new("nice")
        .args([
            "-n",
            "19",
            "tesseract",
            "stdin",
            "stdout",
            "-l",
            LANGUAGES,
            "--psm",
            "3",
        ])
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
    String::from_utf8(out.stdout)
        .map(|text| text.trim().to_owned())
        .map_err(|_| "tesseract returned text that is not UTF-8".into())
}

fn text_for(
    segment: &sinex_model::event_types::session::MediaSegment,
    engine: Option<&str>,
    read_image: impl FnOnce() -> Result<String, String>,
) -> Result<(String, String), String> {
    if let Some(text) = &segment.accessibility_text {
        return Ok(("accessibility".into(), text.clone()));
    }
    let engine = engine.ok_or("accessibility unavailable and no local OCR engine")?;
    Ok((engine.into(), read_image()?))
}

/// Screen text, bound to the engine found at start.
pub(crate) struct ScreenText {
    pub engine: Option<String>,
}

impl PerEvent for ScreenText {
    const NAME: &'static str = "screen-text";
    /// Bump when the reading of a segment changes (engine, languages, options).
    const VERSION: u32 = 3;
    const INPUTS: &'static [&'static str] = &["media.segment"];
    const OUTPUTS: &'static [&'static str] = &["screen.text"];
    const READS: Reads = Reads::Material;

    fn version(&self) -> u32 {
        let definition = format!("{}:{}:{:?}", Self::NAME, Self::VERSION, self.engine);
        let hash = blake3::hash(definition.as_bytes());
        (u32::from_le_bytes(hash.as_bytes()[..4].try_into().expect("four bytes")) >> 2) + 1
    }

    fn reads_for(&self, event: &Event) -> Reads {
        if event.payload.get("kind").and_then(|v| v.as_str()) == Some("screen")
            && event.payload.get("accessibility_text").and_then(|text| text.as_str()).is_none()
            && self.engine.is_some()
        {
            Self::READS
        } else {
            Reads::Nothing
        }
    }

    fn derive(&self, input: Input<'_>, _: &Env) -> Result<Vec<Event>, String> {
        let segment = input.event;
        if segment.payload.get("kind").and_then(|k| k.as_str()) != Some("screen") {
            return Ok(Vec::new());
        }
        let metadata = segment
            .payload_as::<sinex_model::event_types::session::MediaSegment>()
            .ok_or("not a valid media segment")?;
        let (engine, text) = text_for(&metadata, self.engine.as_deref(), || {
            ocr(input.bytes.ok_or("the screen still's bytes were not read")?)
        })?;
        let started_at = metadata.started_at;
        let payload = ScreenTextPayload {
            segment: segment.id.to_string(),
            started_at,
            engine,
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
            // The runner sets the reading order from the version and the
            // input's commit position.
            ReadingOrder(0),
            *hasher.finalize().as_bytes(),
            // A segment's text is as private as the segment.
            segment.class.max(Class::Guarded),
        )
        .map_err(|e| e.to_string())?;
        Ok(vec![event])
    }
}

#[cfg(test)]
mod tests;
