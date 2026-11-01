use super::*;
use crate::media_effects::MediaUnavailableReason;

#[test]
fn screen_available_empty_and_unavailable_have_distinct_validated_payloads() {
    let mut text = ScreenText {
        segment: crate::EventId::new().to_string(),
        started_at: None,
        started_at_evidence: Default::default(),
        availability: ScreenAvailability::Available,
        engine: Some("accessibility".into()),
        text: Some(String::new()),
        regions: vec![],
        effect: None,
    };
    let encoded = serde_json::to_value(&text).unwrap();
    assert_eq!(serde_json::from_value::<ScreenText>(encoded).unwrap(), text);
    assert_eq!(text.ts_orig(), TsOrig::unknown());
    text.availability = ScreenAvailability::Unavailable {
        reason: MediaUnavailableReason::RequiredFieldUnavailable,
        requested: vec![],
    };
    assert!(serde_json::from_value::<ScreenText>(serde_json::to_value(&text).unwrap()).is_err());
    text.text = None;
    text.engine = None;
    serde_json::from_value::<ScreenText>(serde_json::to_value(&text).unwrap()).unwrap();
    assert_eq!(ScreenText::VERSION, 3);
}

#[test]
fn speech_unavailable_cannot_claim_known_empty_cues_or_effect() {
    let mut span = SpeechSpan {
        segment: crate::EventId::new().to_string(),
        started_at: None,
        started_at_evidence: Default::default(),
        availability: SpeechAvailability::Unavailable {
            reason: MediaUnavailableReason::NoRecordedBinding,
            requested: vec![],
        },
        spans: None,
        effect: None,
    };
    serde_json::from_value::<SpeechSpan>(serde_json::to_value(&span).unwrap()).unwrap();
    span.spans = Some(vec![]);
    assert!(serde_json::from_value::<SpeechSpan>(serde_json::to_value(&span).unwrap()).is_err());
    span.availability = SpeechAvailability::Available;
    assert!(
        span.validate().is_err(),
        "known-empty speech still needs its actual retained effect"
    );
}

#[test]
fn speech_cue_roundtrip_keeps_source_clock_separate_from_relative_nanoseconds() {
    let cue=TimedSpeechCue {cue:SpeechCue{start_ns:1,end_ns:20,text:"neutral speech".into(),language:Some("en".into()),speaker:None},
        started_at:None,ended_at:None,time_quality:TimeQuality::Unknown};
    let value=serde_json::to_value(&cue).unwrap();
    assert_eq!(value["start_ns"],1);
    assert_eq!(serde_json::from_value::<TimedSpeechCue>(value).unwrap(),cue);
}
