use super::*;
use crate::{
    Class, EquivalenceKey, Event, EventPayload, KeyKind, MaterialId, Origin, ReadingOrder,
    RecordLocation, TimeQuality,
};
use serde_json::json;

fn origin() -> Origin {
    Origin::Record {
        location: RecordLocation {
            material: MaterialId::new(),
            anchor: 7,
        },
        output_index: 0,
    }
}

#[devkit::test]
fn timed_imports_keep_the_original_wire_key() {
    let old = json!({"command":"pwd", "hostname":"h", "started_at":"2023-11-14T22:13:20Z", "header_elapsed":0, "n":1});
    let payload: CommandImported = serde_json::from_value(old.clone()).unwrap();
    assert_eq!(serde_json::to_value(&payload).unwrap(), old);
    let event = Event::build(
        &payload,
        "zsh",
        origin(),
        ReadingOrder(0),
        [0; 32],
        Class::Open,
    )
    .unwrap();
    assert_eq!(
        event.equivalence_key,
        Some(EquivalenceKey::of(
            "command.imported",
            &["hostname", "started_at", "command", "n"],
            &old
        ))
    );
    assert_eq!(event.ts_orig.quality, TimeQuality::Intrinsic);
}

#[devkit::test]
fn untimed_ingress_uses_locations_and_unknown_time() {
    let kind = crate::event_types::find("command.imported").unwrap();
    assert_eq!(kind.key, CommandImported::KEY);
    let payload = json!({"command":"pwd", "hostname":"h", "n":0});
    let build = || {
        (kind.build)(
            &payload,
            "zsh",
            origin(),
            ReadingOrder(0),
            [0; 32],
            Class::Open,
        )
        .unwrap()
    };
    let a = build();
    let b = build();
    assert_eq!(a.equivalence_key, None);
    assert_eq!(b.equivalence_key, None);
    assert_ne!(a.location(), b.location());
    assert_eq!(a.ts_orig.at, None);
    assert_eq!(a.ts_orig.quality, TimeQuality::Unknown);
    assert_eq!(
        a.payload_as::<CommandImported>().unwrap().key_kind(),
        KeyKind::Location
    );
    let required = CommandImported::payload_schema()["required"]
        .as_array()
        .unwrap()
        .clone();
    assert!(required.contains(&json!("n")));
    assert!(!required.contains(&json!("started_at")));
    assert!(!required.contains(&json!("header_elapsed")));
}
