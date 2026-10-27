use super::*;
use crate::{Class, Event, MaterialId, Origin, ReadingOrder, RecordLocation};
fn build(payload: &TranscriptSegment) -> Event {
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
fn stable_ids_are_scoped_to_external_runs_and_models() {
    let mut p = TranscriptSegment {
        input: "example-input".into(),
        provider_segment_id: Some("a".into()),
        text: "same".into(),
        external_run: Some("run-a".into()),
        model: Some("model-a".into()),
        ..Default::default()
    };
    let a = build(&p);
    assert_eq!(a.equivalence_key, build(&p).equivalence_key);
    p.external_run = Some("run-b".into());
    assert_ne!(a.equivalence_key, build(&p).equivalence_key);
    p.external_run = Some("run-a".into());
    p.model = Some("model-b".into());
    assert_ne!(a.equivalence_key, build(&p).equivalence_key);
    p.provider_segment_id = None;
    assert!(build(&p).equivalence_key.is_none());
    assert!(build(&p).ts_orig.at.is_none());
}
