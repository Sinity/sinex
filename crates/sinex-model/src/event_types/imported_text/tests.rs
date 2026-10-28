use super::*;
use crate::{Class, Event, MaterialId, Origin, ReadingOrder, RecordLocation};
fn build(payload: &TranscriptCue) -> Event {
    Event::build(
        payload,
        "media.transcripts",
        Origin::Record {
            location: RecordLocation {
                material: MaterialId::new(),
                anchor: 0,
            },
            output_index: 0,
        },
        ReadingOrder(0),
        [0; 32],
        Class::Guarded,
    )
    .unwrap()
}
#[devkit::test]
fn provider_ids_and_model_runs_remain_observations_at_their_record_locations() {
    let mut p = TranscriptCue {
        input: "example-input".into(),
        provider_id: Some("a".into()),
        text: "same".into(),
        external_run: Some("run-a".into()),
        model: Some("model-a".into()),
        ..Default::default()
    };
    let a = build(&p);
    assert!(a.equivalence_key.is_none());
    assert_ne!(a.origin, build(&p).origin);
    p.external_run = Some("run-b".into());
    assert!(build(&p).equivalence_key.is_none());
    p.external_run = Some("run-a".into());
    p.model = Some("model-b".into());
    assert!(build(&p).equivalence_key.is_none());
    p.provider_id = None;
    assert!(build(&p).equivalence_key.is_none());
    assert!(build(&p).ts_orig.at.is_none());
}
