use super::*;
use crate::{DrainInput, Limits, Source};
use sinex_model::{Class, ReadingOrder, Timestamp};

#[test]
fn gap_uses_the_normal_material_and_parser_with_the_stream_floor() {
    let gap = Gap {
        stream: "example.input".into(),
        start: None,
        end: Some(Timestamp::from_unix_secs(100).unwrap()),
        reason: "broker disconnected".into(),
        sequence: None,
    };
    let acquired = material(&gap, "example", Timestamp::from_unix_secs(101).unwrap());
    let raw = acquired.material.unwrap();
    let source = source(Class::Guarded);
    let output = source
        .drain(DrainInput {
            source: "capture.gap",
            class: Class::Guarded,
            material: raw.id,
            bytes: crate::drain::RecordBytes::Slice(&raw.bytes),
            records: &raw.records,
            carry: serde_json::Value::Null,
            reading_order: &|_| ReadingOrder(0),
            limits: Limits::default(),
            manifest: &raw.manifest,
        })
        .unwrap();
    let events: Vec<_> = output
        .iter()
        .flat_map(|o| o.outcomes.iter())
        .flat_map(|o| match o {
            sinex_model::RecordOutcome::Events { events, .. } => events.as_slice(),
            _ => &[],
        })
        .collect();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].class, Class::Guarded);
    assert!(events[0].ts_orig.at.is_none());
    assert_eq!(events[0].payload_as::<Gap>(), Some(gap));
}
