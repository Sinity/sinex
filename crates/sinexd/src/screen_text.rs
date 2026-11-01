//! One current screen-text or speech interpretation per stored media segment.
//! Recorded effects are read from retained evidence; local OCR uses the same
//! material access boundary. This module never constructs a model provider.
use serde_json::{Value, json};
use sinex_derivations::selected::{InputBytes, SelectedInputs, SelectedQuery};
use sinex_derivations::{Computed, Env, InputQuery, KeyState, KeyView, Keyed, Reads};
use sinex_model::event_types::session::{
    ScreenText as ScreenTextPayload, SpeechSpan, TimedSpeechCue,
};
use sinex_model::event_types::work::WorkTimeEvidence;
use sinex_model::media_effects::{
    MediaSelector, MediaUnavailableReason as Reason, RecordedOcr, RecordedSpeech,
    ScreenAvailability, SpeechAvailability, TargetAvailability, UnavailableTarget,
};
use sinex_model::{Event, EventId, TimeQuality};
use std::process::{Command, Stdio};

mod recorded;

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
    accessibility: Option<&str>,
    engine: Option<&str>,
    read_image: impl FnOnce() -> Result<String, String>,
) -> Result<(String, String), String> {
    if let Some(text) = accessibility {
        return Ok(("accessibility".into(), text.into()));
    }
    let engine = engine.ok_or("accessibility unavailable and no local OCR engine")?;
    Ok((engine.into(), read_image()?))
}

pub(crate) struct ScreenText {
    pub engine: Option<String>,
    pub selector: Option<MediaSelector>,
}
pub(crate) struct SpeechSpans {
    pub selector: Option<MediaSelector>,
}

fn segment_id(key: &Value) -> Option<EventId> {
    key["segment"]
        .as_str()?
        .parse::<uuid::Uuid>()
        .ok()
        .map(EventId)
}
fn segment<'a>(key: &Value, inputs: &'a [Event]) -> Option<&'a Event> {
    let id = segment_id(key)?;
    inputs
        .iter()
        .find(|event| event.id == id && event.event_type == "media.segment")
}
fn accessibility(event: &Event) -> Option<&str> {
    event.payload["accessibility_text"].as_str()
}
fn clock(event: Option<&Event>) -> (Option<sinex_model::Timestamp>, WorkTimeEvidence) {
    let known = event.filter(|event| {
        event.ts_orig.quality != TimeQuality::Unknown && event.ts_orig.at.is_some()
    });
    (
        known.and_then(|event| event.ts_orig.at),
        WorkTimeEvidence {
            quality: known.map_or(TimeQuality::Unknown, |event| event.ts_orig.quality),
            precision: known.map(|_| "instant".into()),
            until: None,
        },
    )
}
fn computed<P: sinex_model::EventPayload>(
    payload: &P,
    name: &str,
    inputs: &[Event],
    parents: Option<&[EventId]>,
) -> Computed {
    let inputs: Vec<_> = inputs
        .iter()
        .filter(|event| parents.is_none_or(|parents| parents.contains(&event.id)))
        .collect();
    if inputs.is_empty() {
        return Computed {
            waiting: true,
            ..Default::default()
        };
    }
    Computed {
        outputs: vec![sinex_derivations::derived_event::derived(
            payload, name, inputs,
        )],
        ..Default::default()
    }
}
fn missing(key: &Value) -> Vec<UnavailableTarget> {
    segment_id(key)
        .map(|event| UnavailableTarget {
            event,
            availability: TargetAvailability::Unknown,
        })
        .into_iter()
        .collect()
}

impl ScreenText {
    fn unavailable(
        &self,
        key: &Value,
        inputs: &[Event],
        reason: Reason,
        requested: Vec<UnavailableTarget>,
    ) -> Computed {
        let Some(id) = segment_id(key) else {
            return Computed::default();
        };
        let payload = ScreenTextPayload {
            segment: id.to_string(),
            started_at: None,
            started_at_evidence: Default::default(),
            availability: ScreenAvailability::Unavailable { reason, requested },
            engine: None,
            text: None,
            regions: vec![],
            effect: None,
        };
        computed(&payload, Self::NAME, inputs, None)
    }
}
impl Keyed for ScreenText {
    const NAME: &'static str = "screen-text";
    const VERSION: u32 = 5;
    const INPUTS: &'static [&'static str] = &[
        "media.segment",
        "model.request",
        "model.response",
        "model.policy",
    ];
    const OUTPUTS: &'static [&'static str] = &["screen.text"];
    fn version(&self) -> u32 {
        version(
            Self::NAME,
            Self::VERSION,
            &self.selector,
            self.engine.as_deref(),
        )
    }
    fn input_requirements(&self) -> Option<Vec<sinex_model::InputRequirement>> {
        Some(recorded::requirements("screen"))
    }
    fn reference_inputs(&self) -> Vec<sinex_model::ReferenceInput> {
        vec![recorded::reference(self.selector.as_ref())]
    }
    fn keys_for(&self, input: &KeyView<'_>, _: &Env) -> Vec<Value> {
        media_keys(input)
    }
    fn inputs(&self, _: &Value, _: &Env) -> Option<InputQuery> {
        None
    }
    fn exact_inputs(&self, key: &Value, _: &Env) -> Option<Vec<EventId>> {
        Some(segment_id(key).into_iter().collect())
    }
    fn selected_query(&self, key: &Value, inputs: &[Event], _: &Env) -> Option<SelectedQuery> {
        if let Some(frame) = segment(key, inputs) {
            if frame.record_state==sinex_model::RecordState::Removed || frame.payload["kind"] != "screen" || accessibility(frame).is_some() {
                return None;
            }
        }
        segment_id(key).map(|id| recorded::query(id, "screen-ocr", self.selector.as_ref()))
    }
    fn follow_selected(
        &self,
        _: &Value,
        _: &[Event],
        selected: Option<&SelectedInputs>,
        _: &Env,
    ) -> Vec<EventId> {
        recorded::follow(selected)
    }
    fn reads_for(
        &self,
        key: &Value,
        inputs: &[Event],
        selected: Option<&SelectedInputs>,
        _: &Env,
    ) -> Vec<(EventId, Reads)> {
        let Some(frame) = segment(key, inputs) else {
            return vec![];
        };
        if frame.record_state==sinex_model::RecordState::Removed || frame.payload["kind"] != "screen" || accessibility(frame).is_some() {
            return vec![];
        }
        match recorded::choose(selected, inputs, frame.id, "screen-ocr", self.selector.as_ref()) {
            Ok(Some((_, binding))) => recorded::reads(&binding),
            Ok(None) if self.engine.is_some() => vec![(frame.id, Reads::Material)],
            _ => vec![],
        }
    }
    fn compute_with_bytes(
        &self,
        key: &Value,
        inputs: &[Event],
        selected: Option<&SelectedInputs>,
        bytes: &InputBytes,
        _: &Env,
    ) -> Computed {
        let Some(frame) = segment(key, inputs) else {
            return self.unavailable(key, inputs, Reason::MissingTarget, missing(key));
        };
        if frame.record_state==sinex_model::RecordState::Removed {return self.unavailable(key,inputs,Reason::SourceRemoved,vec![])}
        if frame.payload["kind"] != "screen" {
            return Computed::default();
        }
        let (started_at, started_at_evidence) = clock(Some(frame));
        if let Some(text) = accessibility(frame) {
            let payload = ScreenTextPayload {
                segment: frame.id.to_string(),
                started_at,
                started_at_evidence,
                availability: ScreenAvailability::Available,
                engine: Some("accessibility".into()),
                text: Some(text.into()),
                regions: vec![],
                effect: None,
            };
            return computed(&payload, Self::NAME, inputs, Some(&[frame.id]));
        }
        let binding =
            match recorded::choose(selected, inputs, frame.id, "screen-ocr", self.selector.as_ref()) {
                Ok(binding) => binding,
                Err(error) => return self.unavailable(key, inputs, error.reason, error.requested),
            };
        if let Some((request, binding)) = binding {
            let verified =
                match recorded::verify(&binding, frame, request, inputs, bytes, "screen-ocr") {
                    Ok(verified) => verified,
                    Err(error) => {
                        return self.unavailable(key, inputs, error.reason, error.requested);
                    }
                };
            let interpretation: RecordedOcr =
                match serde_json::from_value(verified.effect.output.clone()) {
                    Ok(output) => output,
                    Err(_) => return self.unavailable(key, inputs, Reason::InvalidReceipt, vec![]),
                };
            let width = frame.payload["width"]
                .as_u64()
                .and_then(|width| u32::try_from(width).ok());
            let height = frame.payload["height"]
                .as_u64()
                .and_then(|height| u32::try_from(height).ok());
            let Some((width,height))=width.zip(height) else {
                return self.unavailable(key,inputs,Reason::RequiredFieldUnavailable,vec![]);
            };
            let Some(length) = frame.payload["bytes"].as_u64() else {
                return self.unavailable(key, inputs, Reason::RequiredFieldUnavailable, vec![]);
            };
            if interpretation
                .validate(&verified.identity.input, length, Some(width), Some(height))
                .is_err()
            {
                return self.unavailable(key, inputs, Reason::InvalidGeometry, vec![]);
            }
            let payload = ScreenTextPayload {
                segment: frame.id.to_string(),
                started_at,
                started_at_evidence,
                availability: ScreenAvailability::Available,
                engine: Some("recorded".into()),
                text: Some(interpretation.text),
                regions: interpretation.regions,
                effect: Some(verified.identity),
            };
            return computed(&payload, Self::NAME, inputs, Some(&verified.parents));
        }
        let Some(image) = bytes.get(&frame.id).and_then(|bytes| bytes.as_deref().ok()) else {
            let reason = if self.engine.is_none() {
                Reason::LocalEngineUnavailable
            } else {
                Reason::MaterialUnavailable
            };
            return self.unavailable(key, inputs, reason, vec![]);
        };
        let (engine, text) = match text_for(None, self.engine.as_deref(), || ocr(image)) {
            Ok(reading) => reading,
            Err(_) => return self.unavailable(key, inputs, Reason::LocalEngineUnavailable, vec![]),
        };
        let payload = ScreenTextPayload {
            segment: frame.id.to_string(),
            started_at,
            started_at_evidence,
            availability: ScreenAvailability::Available,
            engine: Some(engine),
            text: Some(text),
            regions: vec![],
            effect: None,
        };
        computed(&payload, Self::NAME, inputs, Some(&[frame.id]))
    }
    fn compute(&self, key: &Value, inputs: &[Event], env: &Env) -> Computed {
        self.compute_with_bytes(key, inputs, None, &InputBytes::new(), env)
    }
    fn unavailable_inputs(
        &self,
        key: &Value,
        inputs: &[Event],
        _: &[sinex_model::UnavailableInput],
        _: &Env,
    ) -> Computed {
        self.unavailable(key, inputs, Reason::RequiredFieldUnavailable, vec![])
    }
    fn unavailable_selection(&self, key: &Value, inputs: &[Event], _: &str, _: &Env) -> Computed {
        self.unavailable(key, inputs, Reason::RequiredFieldUnavailable, vec![])
    }
    fn key_state(&self, _: &Value, _: &Env) -> KeyState {
        KeyState::Closed
    }
}

impl SpeechSpans {
    fn unavailable(
        &self,
        key: &Value,
        inputs: &[Event],
        reason: Reason,
        requested: Vec<UnavailableTarget>,
    ) -> Computed {
        let Some(id) = segment_id(key) else {
            return Computed::default();
        };
        let payload = SpeechSpan {
            segment: id.to_string(),
            started_at: None,
            started_at_evidence: Default::default(),
            availability: SpeechAvailability::Unavailable { reason, requested },
            spans: None,
            effect: None,
        };
        computed(&payload, Self::NAME, inputs, None)
    }
}
impl Keyed for SpeechSpans {
    const NAME: &'static str = "speech-spans";
    const VERSION: u32 = 1;
    const INPUTS: &'static [&'static str] = ScreenText::INPUTS;
    const OUTPUTS: &'static [&'static str] = &["speech.span"];
    fn version(&self) -> u32 {
        version(Self::NAME, Self::VERSION, &self.selector, None)
    }
    fn input_requirements(&self) -> Option<Vec<sinex_model::InputRequirement>> {
        Some(recorded::requirements("audio"))
    }
    fn reference_inputs(&self) -> Vec<sinex_model::ReferenceInput> {
        vec![recorded::reference(self.selector.as_ref())]
    }
    fn keys_for(&self, input: &KeyView<'_>, _: &Env) -> Vec<Value> {
        media_keys(input)
    }
    fn inputs(&self, _: &Value, _: &Env) -> Option<InputQuery> {
        None
    }
    fn exact_inputs(&self, key: &Value, _: &Env) -> Option<Vec<EventId>> {
        Some(segment_id(key).into_iter().collect())
    }
    fn selected_query(&self, key: &Value, inputs: &[Event], _: &Env) -> Option<SelectedQuery> {
        if segment(key, inputs).is_some_and(|audio| audio.record_state==sinex_model::RecordState::Removed || audio.payload["kind"] != "audio") {
            return None;
        }
        segment_id(key)
            .map(|id| recorded::query(id, "speech-transcription", self.selector.as_ref()))
    }
    fn follow_selected(
        &self,
        _: &Value,
        _: &[Event],
        selected: Option<&SelectedInputs>,
        _: &Env,
    ) -> Vec<EventId> {
        recorded::follow(selected)
    }
    fn reads_for(
        &self,
        key: &Value,
        inputs: &[Event],
        selected: Option<&SelectedInputs>,
        _: &Env,
    ) -> Vec<(EventId, Reads)> {
        let Some(audio) = segment(key, inputs) else {
            return vec![];
        };
        if audio.record_state==sinex_model::RecordState::Removed || audio.payload["kind"] != "audio" {
            return vec![];
        }
        match recorded::choose(
            selected,
            inputs,
            audio.id,
            "speech-transcription",
            self.selector.as_ref(),
        ) {
            Ok(Some((_, binding))) => recorded::reads(&binding),
            _ => vec![],
        }
    }
    fn compute_with_bytes(
        &self,
        key: &Value,
        inputs: &[Event],
        selected: Option<&SelectedInputs>,
        bytes: &InputBytes,
        _: &Env,
    ) -> Computed {
        let Some(audio) = segment(key, inputs) else {
            return self.unavailable(key, inputs, Reason::MissingTarget, missing(key));
        };
        if audio.record_state==sinex_model::RecordState::Removed {return self.unavailable(key,inputs,Reason::SourceRemoved,vec![])}
        if audio.payload["kind"] != "audio" {
            return Computed::default();
        }
        let (request, binding) = match recorded::choose(
            selected,
            inputs,
            audio.id,
            "speech-transcription",
            self.selector.as_ref(),
        ) {
            Ok(Some(binding)) => binding,
            Ok(None) => return self.unavailable(key, inputs, Reason::NoRecordedBinding, vec![]),
            Err(error) => return self.unavailable(key, inputs, error.reason, error.requested),
        };
        let verified = match recorded::verify(
            &binding,
            audio,
            request,
            inputs,
            bytes,
            "speech-transcription",
        ) {
            Ok(verified) => verified,
            Err(error) => return self.unavailable(key, inputs, error.reason, error.requested),
        };
        let interpretation: RecordedSpeech =
            match serde_json::from_value(verified.effect.output.clone()) {
                Ok(output) => output,
                Err(_) => return self.unavailable(key, inputs, Reason::InvalidReceipt, vec![]),
            };
        let instant = |name: &str| {
            audio
                .payload
                .get(name)
                .cloned()
                .and_then(|value| serde_json::from_value::<sinex_model::Timestamp>(value).ok())
        };
        let duration = instant("started_at")
            .zip(instant("ended_at"))
            .and_then(|(start, end)| u64::try_from(end.unix_ns() - start.unix_ns()).ok())
            .filter(|duration| *duration > 0);
        let Some((length, duration)) = audio.payload["bytes"].as_u64().zip(duration) else {
            return self.unavailable(key, inputs, Reason::InvalidAudio, vec![]);
        };
        if interpretation
            .validate(&verified.identity.input, length, duration)
            .is_err()
        {
            return self.unavailable(key, inputs, Reason::InvalidAudio, vec![]);
        }
        let mut spans = Vec::new();
        for cue in interpretation.spans {
            let (start, end) = match sinex_model::media_effects::speech_clock(&cue, audio.ts_orig) {
                Ok(bounds) => bounds,
                Err(_) => return self.unavailable(key, inputs, Reason::InvalidAudio, vec![]),
            };
            spans.push(TimedSpeechCue {
                cue,
                started_at: start.at,
                ended_at: end.at,
                time_quality: start.quality,
            });
        }
        let (started_at, started_at_evidence) = clock(Some(audio));
        let payload = SpeechSpan {
            segment: audio.id.to_string(),
            started_at,
            started_at_evidence,
            availability: SpeechAvailability::Available,
            spans: Some(spans),
            effect: Some(verified.identity),
        };
        computed(&payload, Self::NAME, inputs, Some(&verified.parents))
    }
    fn compute(&self, key: &Value, inputs: &[Event], env: &Env) -> Computed {
        self.compute_with_bytes(key, inputs, None, &InputBytes::new(), env)
    }
    fn unavailable_inputs(
        &self,
        key: &Value,
        inputs: &[Event],
        _: &[sinex_model::UnavailableInput],
        _: &Env,
    ) -> Computed {
        self.unavailable(key, inputs, Reason::RequiredFieldUnavailable, vec![])
    }
    fn unavailable_selection(&self, key: &Value, inputs: &[Event], _: &str, _: &Env) -> Computed {
        self.unavailable(key, inputs, Reason::RequiredFieldUnavailable, vec![])
    }
    fn key_state(&self, _: &Value, _: &Env) -> KeyState {
        KeyState::Closed
    }
}
fn media_keys(input: &KeyView<'_>) -> Vec<Value> {
    if input.event_type == "media.segment" {
        vec![json!({"segment":input.event_id})]
    } else {
        vec![]
    }
}
fn version(
    name: &str,
    version: u32,
    selector: &Option<MediaSelector>,
    engine: Option<&str>,
) -> u32 {
    let definition = serde_json::to_vec(&(name, version, selector, engine, LANGUAGES))
        .expect("media definition serializes");
    (u32::from_le_bytes(
        blake3::hash(&definition).as_bytes()[..4]
            .try_into()
            .expect("four bytes"),
    ) >> 2)
        + 1
}

#[cfg(test)]
mod tests;
