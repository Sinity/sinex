use super::*;
use crate::{DrainInput, Limits, Source};
use sinex_model::{MaterialId, ReadingOrder, RecordOutcome};

#[test]
fn a_stored_segment_drains_to_one_event_from_its_manifest() {
    let dir = std::env::temp_dir().join(format!("sinex-mc-{}", uuid::Uuid::now_v7()));
    std::fs::create_dir_all(&dir).unwrap();
    let partial = dir.join("x.partial");
    std::fs::write(&partial, b"\xff\xd8jpeg").unwrap();
    media::publish_segment(
        &dir,
        &partial,
        "00000000001000000000-00000000001000000000.screen-DP-3",
        "jpg",
        &json!({"kind": "screen", "started_ns": "1000000000", "ended_ns": "1000000000", "mime": "image/jpeg",
                "target": "DP-3", "width": 3840, "height": 2160}),
    )
    .unwrap();
    let m = driver(dir.clone(), "h".into(), "screen")
        .poll(&Value::Null)
        .unwrap()
        .material
        .unwrap();
    let chunks = source()
        .drain(DrainInput {
            erased: None,
            source: SPEC.name,
            class: SPEC.class,
            material: MaterialId::new(),
            bytes: m.bytes.as_slice().into(),
            records: &m.records,
            carry: Value::Null,
            reading_order: &|a| ReadingOrder(a as i64),
            limits: Limits::default(),
            manifest: &m.manifest,
        })
        .unwrap();
    let events: Vec<_> = chunks
        .iter()
        .flat_map(|c| &c.outcomes)
        .flat_map(|o| match o {
            RecordOutcome::Events { events, .. } => events.clone(),
            RecordOutcome::Failure { reason, .. } => panic!("{reason}"),
        })
        .collect();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].event_type, "media.segment");
    assert_eq!(events[0].payload["width"], 3840);
    assert_eq!(events[0].payload["bytes"], 6);
    let _ = std::fs::remove_dir_all(dir);
}
