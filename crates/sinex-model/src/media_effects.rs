//! Supplied media interpretations over verified stored bytes. These validators
//! never read a provider or substitute an unavailable geometry or clock.
use crate::event_types::model_call::RecordedEffect;
use crate::model_calls::{InputIdentity, InputSelection, OutputHash};
use crate::{EventId, TimeQuality, Timestamp, TsOrig};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Actual effect provenance, attached only to an available recorded reading.
/// The raw request and response parents retain complete instructions/output.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct MediaEffectIdentity {
    pub policy: EventId,
    pub request: EventId,
    pub response: EventId,
    pub request_id: String,
    pub input: InputIdentity,
    pub provider: String,
    pub provider_account: String,
    pub model: String,
    pub revision: String,
    pub instruction_version: String,
    pub prompt_hash: String,
    pub rubric_hash: String,
    pub settings_version: String,
    pub settings_hash: String,
    pub output_hash: OutputHash,
}

impl MediaEffectIdentity {
    pub fn from_recorded(
        policy: EventId,
        request: EventId,
        response: EventId,
        effect: &RecordedEffect,
        input: &InputIdentity,
    ) -> Result<Self, String> {
        effect.validate()?;
        if !effect
            .request
            .inputs
            .iter()
            .any(|target| &target.identity == input)
        {
            return Err("media interpretation input is absent from the recorded request".into());
        }
        let instructions = &effect.request.instructions;
        Ok(Self {
            policy,
            request,
            response,
            request_id: effect.request_id.clone(),
            input: input.clone(),
            provider: effect.request.provider.clone(),
            provider_account: effect.request.provider_account.clone(),
            model: effect.request.model.clone(),
            revision: effect.request.revision.clone(),
            instruction_version: instructions.version.clone(),
            prompt_hash: instructions.prompt_hash.clone(),
            rubric_hash: instructions.rubric_hash.clone(),
            settings_version: instructions.settings_version.clone(),
            settings_hash: instructions.settings_hash.clone(),
            output_hash: effect.output_hash.clone(),
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ScreenUnavailableReason {
    MissingTarget,
    RequiredFieldUnavailable,
    SelectionAmbiguous,
    PolicyUnavailable,
    ReceiptUnavailable,
    InvalidGeometry,
    InvalidReceipt,
    LocalEngineUnavailable,
}

/// Requested references are explicit unavailable status, not verified evidence
/// or parents. Current verified evidence lives on the output itself.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct UnavailableTarget {
    pub event: EventId,
    pub availability: TargetAvailability,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum TargetAvailability {
    Missing,
    Archived,
    Tombstoned,
    PolicyUnavailable,
    Unknown,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
pub enum ScreenAvailability {
    Available,
    Unavailable {
        reason: ScreenUnavailableReason,
        requested: Vec<UnavailableTarget>,
    },
}

impl ScreenAvailability {
    pub fn validate(
        &self,
        text: Option<&str>,
        engine: Option<&str>,
        regions: &[PixelRegion],
        effect: Option<&MediaEffectIdentity>,
    ) -> Result<(), String> {
        match self {
            Self::Available
                if text.is_none() || engine.is_none_or(|engine| engine.trim().is_empty()) =>
            {
                Err("available screen text needs its observed text and method".into())
            }
            Self::Unavailable { .. }
                if text.is_some()
                    || engine.is_some()
                    || !regions.is_empty()
                    || effect.is_some() =>
            {
                Err("unavailable screen text cannot claim a text, method, region or effect".into())
            }
            _ => Ok(()),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PixelRegion {
    pub left: f64,
    pub top: f64,
    pub width: f64,
    pub height: f64,
    pub text: String,
    /// A probability on [0,1], absent when the provider did not report one.
    pub confidence: Option<f64>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RecordedOcr {
    /// Stable selected-byte identity; exact current reading parents are kept
    /// separately, so an equivalent source replay can reuse this effect.
    pub input: InputIdentity,
    pub frame_width: u32,
    pub frame_height: u32,
    pub text: String,
    pub regions: Vec<PixelRegion>,
}

impl RecordedOcr {
    pub fn validate(
        &self,
        expected: &InputIdentity,
        stored_bytes: u64,
        width: Option<u32>,
        height: Option<u32>,
    ) -> Result<(), String> {
        whole_media(&self.input, expected, stored_bytes)?;
        let (Some(width), Some(height)) = (width, height) else {
            return Err("stored frame geometry is unavailable".into());
        };
        if width == 0 || height == 0 || self.frame_width != width || self.frame_height != height {
            return Err("recorded OCR geometry differs from the stored frame".into());
        }
        for region in &self.regions {
            if ![region.left, region.top, region.width, region.height]
                .iter()
                .all(|value| value.is_finite())
                || region.left < 0.0
                || region.top < 0.0
                || region.width <= 0.0
                || region.height <= 0.0
                || region.left + region.width > f64::from(width)
                || region.top + region.height > f64::from(height)
            {
                return Err("recorded OCR region is outside the stored frame".into());
            }
            if region
                .confidence
                .is_some_and(|value| !value.is_finite() || !(0.0..=1.0).contains(&value))
            {
                return Err(
                    "recorded OCR confidence is outside its declared probability scale".into(),
                );
            }
        }
        Ok(())
    }
}

/// Selection is separate from validation: an observed accessibility reading,
/// including empty text, makes no claim about an unused OCR receipt.
#[derive(Clone, Debug, PartialEq)]
pub enum ScreenReading {
    Accessibility(String),
    Recorded(RecordedOcr),
}

pub fn screen_reading(
    accessibility: Option<&str>,
    recorded: Option<&RecordedOcr>,
    expected: &InputIdentity,
    stored_bytes: u64,
    width: Option<u32>,
    height: Option<u32>,
) -> Result<Option<ScreenReading>, String> {
    if let Some(text) = accessibility {
        return Ok(Some(ScreenReading::Accessibility(text.into())));
    }
    let Some(recorded) = recorded else {
        return Ok(None);
    };
    recorded.validate(expected, stored_bytes, width, height)?;
    Ok(Some(ScreenReading::Recorded(recorded.clone())))
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Speaker {
    pub label: String,
    pub separation_method: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SpeechCue {
    pub start_ns: u64,
    pub end_ns: u64,
    pub text: String,
    pub language: Option<String>,
    pub speaker: Option<Speaker>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RecordedSpeech {
    pub input: InputIdentity,
    pub spans: Vec<SpeechCue>,
}

impl RecordedSpeech {
    pub fn validate(
        &self,
        expected: &InputIdentity,
        stored_bytes: u64,
        duration_ns: u64,
    ) -> Result<(), String> {
        whole_media(&self.input, expected, stored_bytes)?;
        for span in &self.spans {
            if span.start_ns >= span.end_ns || span.end_ns > duration_ns {
                return Err("recorded speech span is outside the stored audio".into());
            }
            if span
                .language
                .as_ref()
                .is_some_and(|language| language.trim().is_empty())
            {
                return Err("supplied speech language is empty".into());
            }
            if span.speaker.as_ref().is_some_and(|speaker| {
                speaker.label.trim().is_empty() || speaker.separation_method.trim().is_empty()
            }) {
                return Err("supplied speech speaker needs a label and separation method".into());
            }
        }
        Ok(())
    }
}

/// Both bounds keep the source clock's quality. A relative cue has no absolute
/// time when its stored audio has none; recording or evaluation time is no base.
pub fn speech_clock(cue: &SpeechCue, audio: TsOrig) -> Result<(TsOrig, TsOrig), String> {
    if cue.start_ns >= cue.end_ns {
        return Err("speech clock needs a positive relative interval".into());
    }
    let Some(start) = audio.at.filter(|_| audio.quality != TimeQuality::Unknown) else {
        return Ok((TsOrig::unknown(), TsOrig::unknown()));
    };
    let bound = |offset: u64| -> Result<TsOrig, String> {
        Timestamp::from_unix_ns(start.unix_ns() + i128::from(offset))
            .map(|at| TsOrig::known(at, audio.quality))
            .map_err(|_| "speech clock is outside the supported timestamp range".into())
    };
    Ok((bound(cue.start_ns)?, bound(cue.end_ns)?))
}

fn whole_media(
    actual: &InputIdentity,
    expected: &InputIdentity,
    stored_bytes: u64,
) -> Result<(), String> {
    if actual != expected {
        return Err("recorded media effect has a different verified input identity".into());
    }
    let InputSelection::Bytes { offset, length, .. } = &actual.selection else {
        return Err("recorded media effect requires selected stored media bytes".into());
    };
    if stored_bytes == 0
        || *offset != 0
        || *length != stored_bytes
        || actual.selected_bytes != stored_bytes
    {
        return Err(
            "recorded media effect does not cover the complete stored frame or audio segment"
                .into(),
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests;
