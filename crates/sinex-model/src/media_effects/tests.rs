use super::*;
use crate::model_calls::{InputHash, InputKind, RecordIdentity};

fn identity() -> InputIdentity {
    InputIdentity {
        source: "synthetic.media".into(),
        kind: InputKind {
            name: "media.segment".into(),
            version: 2,
        },
        record: RecordIdentity::Material {
            material: crate::MaterialId::new(),
            offset: 0,
            output: 0,
        },
        reading_hash: InputHash::of(b"fixture media record").0,
        selection: InputSelection::Bytes {
            field: "material".into(),
            blob: format!("blake3:{}", blake3::hash(b"fixture media bytes")),
            offset: 0,
            length: 19,
        },
        input_hash: InputHash::of(b"fixture media bytes"),
        selected_bytes: 19,
    }
}
fn ocr() -> RecordedOcr {
    RecordedOcr {
        input: identity(),
        frame_width: 100,
        frame_height: 80,
        text: "neutral screen text".into(),
        regions: vec![PixelRegion {
            left: 1.5,
            top: 2.0,
            width: 98.5,
            height: 78.0,
            text: "neutral screen text".into(),
            confidence: Some(0.8),
        }],
    }
}
fn cue() -> SpeechCue {
    SpeechCue {
        start_ns: 1,
        end_ns: 10,
        text: "neutral speech".into(),
        language: None,
        speaker: None,
    }
}

#[test]
fn recorded_ocr_requires_exact_frame_identity_and_bounded_finite_geometry() {
    let mut reading = ocr();
    let expected = reading.input.clone();
    reading
        .validate(&expected, 19, Some(100), Some(80))
        .unwrap();
    reading.regions[0].width += 0.5;
    assert!(
        reading
            .validate(&expected, 19, Some(100), Some(80))
            .is_err()
    );
    reading.regions[0].width = f64::NAN;
    assert!(
        reading
            .validate(&expected, 19, Some(100), Some(80))
            .is_err()
    );
    reading.regions.clear();
    assert!(reading.validate(&expected, 19, None, Some(80)).is_err());
    let mut other = expected.clone();
    other.reading_hash = InputHash::of(b"other raw record").0;
    assert!(reading.validate(&other, 19, Some(100), Some(80)).is_err());
    assert!(
        reading
            .validate(&expected, 20, Some(100), Some(80))
            .is_err()
    );
}

#[test]
fn observed_empty_accessibility_wins_without_validating_or_claiming_unused_ocr() {
    let mut reading = ocr();
    let expected = reading.input.clone();
    reading.frame_width = 0;
    assert_eq!(
        screen_reading(Some(""), Some(&reading), &expected, 19, None, None).unwrap(),
        Some(ScreenReading::Accessibility(String::new()))
    );
    assert!(screen_reading(None, Some(&reading), &expected, 19, None, None).is_err());
    assert_eq!(
        screen_reading(None, None, &expected, 19, None, None).unwrap(),
        None
    );
}

#[test]
fn recorded_speech_keeps_exact_nanosecond_bounds_and_requires_a_speaker_method() {
    let input = identity();
    let mut speech = RecordedSpeech {
        input: input.clone(),
        spans: vec![cue()],
    };
    speech.validate(&input, 19, 10).unwrap();
    assert!(speech.validate(&input, 19, 9).is_err());
    speech.spans[0].speaker = Some(Speaker {
        label: "speaker A".into(),
        separation_method: String::new(),
    });
    assert!(speech.validate(&input, 19, 10).is_err());
    speech.spans[0].speaker.as_mut().unwrap().separation_method =
        "supplied fixture diarization".into();
    speech.validate(&input, 19, 10).unwrap();
    speech.spans[0].start_ns = 10;
    assert!(speech.validate(&input, 19, 10).is_err());
}

#[test]
fn speech_relative_time_never_invents_an_absolute_clock() {
    let cue = cue();
    assert_eq!(
        speech_clock(&cue, TsOrig::unknown()).unwrap(),
        (TsOrig::unknown(), TsOrig::unknown())
    );
    let start = Timestamp::from_unix_ns(1_234_567_890).unwrap();
    let (first, last) = speech_clock(&cue, TsOrig::known(start, TimeQuality::Captured)).unwrap();
    assert_eq!(first.at.unwrap().unix_ns(), 1_234_567_891);
    assert_eq!(last.at.unwrap().unix_ns(), 1_234_567_900);
    assert_eq!(
        (first.quality, last.quality),
        (TimeQuality::Captured, TimeQuality::Captured)
    );
    assert_eq!(
        speech_clock(
            &cue,
            TsOrig {
                at: Some(start),
                quality: TimeQuality::Unknown
            }
        )
        .unwrap(),
        (TsOrig::unknown(), TsOrig::unknown())
    );
}

#[test]
fn unavailable_and_observed_empty_screen_text_are_distinct_states() {
    ScreenAvailability::Available
        .validate(Some(""), Some("accessibility"), &[], None)
        .unwrap();
    assert!(
        ScreenAvailability::Available
            .validate(None, Some("accessibility"), &[], None)
            .is_err()
    );
    let unavailable = ScreenAvailability::Unavailable {
        reason: ScreenUnavailableReason::RequiredFieldUnavailable,
        requested: vec![UnavailableTarget {
            event: crate::EventId::new(),
            availability: TargetAvailability::PolicyUnavailable,
        }],
    };
    unavailable.validate(None, None, &[], None).unwrap();
    assert!(unavailable.validate(Some(""), None, &[], None).is_err());
    assert!(
        unavailable
            .validate(None, Some("accessibility"), &[], None)
            .is_err()
    );
    let encoded = serde_json::to_value(&unavailable).unwrap();
    assert_eq!(encoded["state"], "unavailable");
    assert_eq!(
        encoded["requested"][0]["availability"],
        "policy_unavailable"
    );
    assert!(encoded.get("evidence").is_none());
}
