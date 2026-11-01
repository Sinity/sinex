//! One current text reading per media segment. An unavailable interpretation
//! replaces stale text without fabricating an observed empty result.
use crate::event_types::work::WorkTimeEvidence;
use crate::media_effects::{
    MediaEffectIdentity, PixelRegion, ScreenAvailability, SpeechAvailability, SpeechCue,
};
use crate::{EventPayload, TimeQuality, Timestamp, TsOrig};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Clone, Debug, PartialEq, Serialize, JsonSchema)]
pub struct ScreenText {
    pub segment: String,
    pub started_at: Option<Timestamp>,
    pub started_at_evidence: WorkTimeEvidence,
    pub availability: ScreenAvailability,
    pub engine: Option<String>,
    pub text: Option<String>,
    pub regions: Vec<PixelRegion>,
    pub effect: Option<MediaEffectIdentity>,
}

impl ScreenText {
    pub fn validate(&self) -> Result<(), String> {
        self.availability.validate(
            self.text.as_deref(),
            self.engine.as_deref(),
            &self.regions,
            self.effect.as_ref(),
        )
    }
}

#[derive(Deserialize)]
struct ScreenFields {
    segment: String,
    started_at: Option<Timestamp>,
    started_at_evidence: WorkTimeEvidence,
    availability: ScreenAvailability,
    engine: Option<String>,
    text: Option<String>,
    regions: Vec<PixelRegion>,
    effect: Option<MediaEffectIdentity>,
}
impl<'de> Deserialize<'de> for ScreenText {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let fields = ScreenFields::deserialize(deserializer)?;
        let payload = Self {
            segment: fields.segment,
            started_at: fields.started_at,
            started_at_evidence: fields.started_at_evidence,
            availability: fields.availability,
            engine: fields.engine,
            text: fields.text,
            regions: fields.regions,
            effect: fields.effect,
        };
        payload.validate().map_err(serde::de::Error::custom)?;
        Ok(payload)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct TimedSpeechCue {
    #[serde(flatten)]
    pub cue: SpeechCue,
    pub started_at: Option<Timestamp>,
    pub ended_at: Option<Timestamp>,
    pub time_quality: TimeQuality,
}

#[derive(Clone, Debug, PartialEq, Serialize, JsonSchema)]
pub struct SpeechSpan {
    pub segment: String,
    pub started_at: Option<Timestamp>,
    pub started_at_evidence: WorkTimeEvidence,
    pub availability: SpeechAvailability,
    /// Some([]) is a verified transcript with no observed speech. None is
    /// unavailable, and never supplies a fake cue or zero-length interval.
    pub spans: Option<Vec<TimedSpeechCue>>,
    pub effect: Option<MediaEffectIdentity>,
}
impl SpeechSpan {
    pub fn validate(&self) -> Result<(), String> {
        match &self.availability {
            SpeechAvailability::Available if self.spans.is_none()||self.effect.is_none()=>Err("available speech needs its retained interpretation, including an observed empty cue list".into()),
            SpeechAvailability::Unavailable{..} if self.spans.is_some()||self.effect.is_some()=>Err("unavailable speech cannot claim cues or a verified effect".into()),
            _=>Ok(()),
        }
    }
}
#[derive(Deserialize)]
struct SpeechFields {
    segment: String,
    started_at: Option<Timestamp>,
    started_at_evidence: WorkTimeEvidence,
    availability: SpeechAvailability,
    spans: Option<Vec<TimedSpeechCue>>,
    effect: Option<MediaEffectIdentity>,
}
impl<'de> Deserialize<'de> for SpeechSpan {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let fields = SpeechFields::deserialize(deserializer)?;
        let payload = Self {
            segment: fields.segment,
            started_at: fields.started_at,
            started_at_evidence: fields.started_at_evidence,
            availability: fields.availability,
            spans: fields.spans,
            effect: fields.effect,
        };
        payload.validate().map_err(serde::de::Error::custom)?;
        Ok(payload)
    }
}

fn declaration<P: EventPayload>() -> crate::KindDeclaration {
    let mut declaration = crate::KindDeclaration::schemaless(
        crate::KindRef {
            name: P::EVENT_TYPE.into(),
            version: P::VERSION,
        },
        "sinex".into(),
    );
    declaration.normalization = crate::kinds::Normalization::Rust;
    declaration.payload_schema = Some(P::payload_schema());
    declaration.key_rule = Some(crate::kinds::KeyRule::Fields {
        fields: vec!["segment".into()],
    });
    declaration.replace_rule = Some(P::REPLACE);
    declaration.time_rule = Some(crate::kinds::TimeRule::InstantEvidence {
        field: "started_at".into(),
        evidence_field: "started_at_evidence".into(),
    });
    declaration.search = P::SEARCH;
    declaration
}
impl EventPayload for ScreenText {
    const VERSION: u64 = 3;
    const EVENT_TYPE: &'static str = "screen.text";
    const KEY: crate::KeyKind = crate::KeyKind::Fields(&["segment"]);
    const REPLACE: crate::ReplaceRule = crate::ReplaceRule::ReplaceWhenNewer;
    fn ts_orig(&self) -> TsOrig {
        TsOrig::maybe(
            self.started_at_evidence.instant(self.started_at),
            self.started_at_evidence.quality,
        )
    }
    fn payload_schema() -> Value {
        serde_json::to_value(schemars::schema_for!(Self)).expect("screen text schema serializes")
    }
    fn declaration() -> crate::KindDeclaration {
        declaration::<Self>()
    }
}
impl EventPayload for SpeechSpan {
    const VERSION: u64 = 2;
    const EVENT_TYPE: &'static str = "speech.span";
    const KEY: crate::KeyKind = crate::KeyKind::Fields(&["segment"]);
    const REPLACE: crate::ReplaceRule = crate::ReplaceRule::ReplaceWhenNewer;
    fn ts_orig(&self) -> TsOrig {
        TsOrig::maybe(
            self.started_at_evidence.instant(self.started_at),
            self.started_at_evidence.quality,
        )
    }
    fn payload_schema() -> Value {
        serde_json::to_value(schemars::schema_for!(Self)).expect("speech span schema serializes")
    }
    fn declaration() -> crate::KindDeclaration {
        declaration::<Self>()
    }
}

#[cfg(test)]
mod tests;
