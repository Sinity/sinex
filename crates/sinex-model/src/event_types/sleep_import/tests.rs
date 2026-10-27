use super::*;
use crate::{
    Class, Event, EventPayload, MaterialId, Origin, ReadingOrder, RecordLocation, ReplaceRule,
};

#[devkit::test]
fn corrected_imports_replace_by_dataset_and_provider_id() {
    let value = serde_json::json!({"dataset":"example", "provider_session_id":"example-1", "start_local":"2030-10-27T02:10:00", "end_local":"2030-10-27T02:40:00", "zone":"Europe/Warsaw", "start_unresolved":"ambiguous local time", "end_unresolved":"ambiguous local time", "hr_avg":70.0, "events_hr":0});
    let payload: SleepSessionImported = serde_json::from_value(value).unwrap();
    let build = |p: &SleepSessionImported| {
        Event::build(
            p,
            "health.sleep-csv",
            Origin::Record {
                location: RecordLocation {
                    material: MaterialId::new(),
                    anchor: 0,
                },
                output_index: 0,
            },
            ReadingOrder(1),
            [1; 32],
            Class::Guarded,
        )
        .unwrap()
    };
    let a = build(&payload);
    let mut corrected = payload.clone();
    corrected.hr_avg = Some(71.0);
    let b = build(&corrected);
    assert_eq!(a.equivalence_key, b.equivalence_key);
    assert_eq!(SleepSessionImported::REPLACE, ReplaceRule::ReplaceWhenNewer);
    assert_eq!(a.ts_orig.at, None);
    corrected.dataset = "second-dataset".into();
    assert_ne!(a.equivalence_key, build(&corrected).equivalence_key);
    assert_eq!(payload.sh_duration_minutes, None);
    assert_eq!(payload.events_hr, Some(0));
    assert!(SleepSessionImported::payload_schema().is_object());
}
