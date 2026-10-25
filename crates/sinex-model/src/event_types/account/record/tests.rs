use super::*;
use serde_json::json;

fn wire(values: Value) -> Value {
    json!({"service":"samsungcloud","table":"Synthetic Table","native_id":"synthetic-id","at":null,"values":values})
}

#[devkit::test]
fn rows_keep_every_value_and_gain_a_computed_untyped_marker() {
    for values in [
        json!({}),
        json!({"text":"0042","number":9007199254740993_i64,"real":1.5,"null":null,"array":[false,{}],"nested":{"value":true}}),
    ] {
        let original = wire(values);
        let record: AccountRow = serde_json::from_value(original.clone()).unwrap();
        let output = serde_json::to_value(&record).unwrap();
        assert_eq!(output["values"], original["values"]);
        for field in ["service", "table", "native_id", "at"] {
            assert_eq!(output[field], original[field]);
        }
        assert_eq!(output["untyped"], true);
        assert_eq!(
            serde_json::from_value::<AccountRow>(output).unwrap(),
            record
        );
    }
}

#[devkit::test]
fn an_untyped_marker_in_the_input_cannot_change_the_fallback_kind() {
    for marker in [json!(true), json!(false)] {
        let mut original = wire(json!({"raw":true}));
        original["untyped"] = marker;
        let output =
            serde_json::to_value(serde_json::from_value::<AccountRow>(original).unwrap()).unwrap();
        assert_eq!(output["untyped"], true);
        assert_eq!(output["values"], json!({"raw":true}));
    }
}

#[devkit::test]
fn registered_fallback_keeps_identity_time_and_rows_without_the_marker() {
    use crate::{EventPayload, KeyKind, ReplaceRule, TimeQuality};
    assert_eq!(
        AccountRow::KEY,
        KeyKind::Fields(&["service", "table", "native_id"])
    );
    assert_eq!(AccountRow::REPLACE, ReplaceRule::ReplaceWhenNewer);
    let mut original = wire(json!({"raw":true}));
    original["at"] = json!("2026-01-01T00:00:00Z");
    let record: AccountRow = serde_json::from_value(original.clone()).unwrap();
    assert_eq!(record.ts_orig().at, record.at);
    assert_eq!(record.ts_orig().quality, TimeQuality::Intrinsic);
    let kind = crate::event_types::find("account.row").unwrap();
    let schema = (kind.json_schema)();
    assert_eq!(schema["properties"]["untyped"]["default"], true);
    assert!(
        !schema["required"]
            .as_array()
            .unwrap()
            .iter()
            .any(|field| field == "untyped")
    );
    let origin = crate::Origin::Record {
        location: crate::RecordLocation {
            material: crate::MaterialId::new(),
            anchor: 7,
        },
        output_index: 0,
    };
    let event = (kind.build)(
        &original,
        "producer:synthetic",
        origin,
        crate::ReadingOrder(1),
        [0; 32],
        crate::Class::Open,
    )
    .unwrap();
    assert_eq!(event.payload["values"], original["values"]);
    assert_eq!(event.payload["untyped"], true);
}
