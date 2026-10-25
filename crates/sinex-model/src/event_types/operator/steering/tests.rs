use super::*;
use crate::{
    Class, EquivalenceKey, EventPayload, KeyKind, MaterialId, Origin, ReadingOrder, RecordLocation,
    ReplaceRule, TimeQuality,
};
use serde_json::json;

#[devkit::test]
fn known_rows_keep_the_existing_wire_identity_and_exact_values() {
    let cases = [
        (
            "commitments",
            json!({"id":"c1","text":"finish the draft","created_at":"2026-10-12T08:00:00+02:00","window_start":"morning","window_end":null,"forecast_p":0.7,"status":"done","outcome_at":"2026-10-12T09:05:00+02:00","outcome_note":"written","activity_id":"a1","review_id":null}),
        ),
        (
            "activities",
            json!({"id":"a1","name":"walking","kind":"practice","est_minutes":9007199254740993_i64,"energy_tier":"low","standing_notes":"","hypothesis":null,"prereg_prediction":null,"metric_ref":null,"experiment_status":null}),
        ),
        (
            "reviews",
            json!({"id":"r1","ts":"2026-10-12T19:00:00Z","window":"evening","generated_summary":"model-written review","operator_notes":null}),
        ),
    ];
    assert_eq!(SteeringRow::KEY, KeyKind::Fields(&["table", "record_id"]));
    assert_eq!(SteeringRow::REPLACE, ReplaceRule::ReplaceWhenNewer);
    for (table, values) in cases {
        let record = SteeringRow::from_row(table, values.clone()).unwrap();
        let wire = serde_json::to_value(&record).unwrap();
        assert_eq!(wire["table"], table);
        assert_eq!(wire["record_id"], values["id"]);
        assert_eq!(wire["values"], values);
        let again: SteeringRow = serde_json::from_value(wire.clone()).unwrap();
        assert_eq!(again, record);
        let location = RecordLocation {
            material: MaterialId::new(),
            anchor: 57,
        };
        let event = crate::Event::build(
            &record,
            "operator.steering",
            Origin::Record {
                location,
                output_index: 0,
            },
            ReadingOrder(19),
            [7; 32],
            Class::Open,
        )
        .unwrap();
        assert_eq!(
            event.origin,
            Origin::Record {
                location,
                output_index: 0
            }
        );
        assert_eq!(event.reading_order, ReadingOrder(19));
        assert_eq!(event.record_hash, [7; 32]);
        assert_eq!(
            event.equivalence_key,
            Some(EquivalenceKey::of(
                "steering.row",
                &["table", "record_id"],
                &wire
            ))
        );
        assert_eq!(
            event.ts_orig.quality,
            if table == "activities" {
                TimeQuality::Unknown
            } else {
                TimeQuality::Intrinsic
            }
        );
    }
}

#[devkit::test]
fn missing_null_empty_and_numeric_representations_stay_distinct() {
    for values in [
        json!({"id":"c1"}),
        json!({"id":"c1","text":null,"created_at":null,"forecast_p":null}),
        json!({"id":"c1","text":"","created_at":"","forecast_p":0}),
        json!({"id":"c1","text":"","created_at":"","forecast_p":0.0}),
        json!({"id":"c1","forecast_p":9007199254740993_i64}),
    ] {
        let record = SteeringRow::from_row("commitments", values.clone()).unwrap();
        let wire = serde_json::to_value(&record).unwrap();
        assert_eq!(wire["values"], values);
        assert!(record.ts_orig().at.is_none());
        assert_eq!(serde_json::from_value::<SteeringRow>(wire).unwrap(), record);
    }
    let absent = SteeringRow::from_row("commitments", json!({"id":"c1"})).unwrap();
    let null = SteeringRow::from_row("commitments", json!({"id":"c1","text":null})).unwrap();
    assert_ne!(absent, null);
}

#[devkit::test]
fn known_table_selects_its_type_and_unknown_shapes_are_refused() {
    for (table, values) in [
        ("commitments", json!({"id":"c1","forecast_p":"0.7"})),
        ("commitments", json!({"id":"c1","status":"future-status"})),
        ("commitments", json!({"id":"c1","created_at":17})),
        ("commitments", json!({"id":"c1","created_at":"bad time"})),
        ("commitments", json!({"id":"c1","extra_column":true})),
        ("activities", json!({"id":"a1","text":"wrong table"})),
        ("activities", json!({"id":"a1","est_minutes":1.5})),
        ("activities", json!({"id":"a1","kind":"future-kind"})),
        ("activities", json!({"id":"a1","energy_tier":"future-tier"})),
        ("reviews", json!({"id":"r1","operator_notes":false})),
        ("unknown", json!({"id":"u1"})),
        ("commitments", json!({"id":null})),
    ] {
        assert!(
            SteeringRow::from_row(table, values.clone()).is_err(),
            "{table}: {values}"
        );
    }
    let record =
        SteeringRow::from_row("commitments", json!({"id":"c1","text":"original"})).unwrap();
    let wire = serde_json::to_value(record).unwrap();
    for (field, value) in [
        ("table", json!("activities")),
        ("record_id", json!("other")),
        ("at", json!("2026-10-12T00:00:00Z")),
    ] {
        let mut changed = wire.clone();
        changed[field] = value;
        assert!(
            serde_json::from_value::<SteeringRow>(changed).is_err(),
            "{field}"
        );
    }
}

#[devkit::test]
fn registration_exposes_known_fields_and_builds_through_the_validated_type() {
    let kind = crate::event_types::find("steering.row").unwrap();
    let schema = (kind.json_schema)();
    let table = &schema["$defs"]["Table"]["enum"];
    assert_eq!(table, &json!(["commitments", "activities", "reviews"]));
    for (row, field) in [
        ("Commitment", "forecast_p"),
        ("Activity", "est_minutes"),
        ("Review", "generated_summary"),
    ] {
        assert!(
            schema["$defs"][row]["properties"].get(field).is_some(),
            "{schema}"
        );
        assert_eq!(schema["$defs"][row]["additionalProperties"], false);
    }
    let wire = serde_json::to_value(
        SteeringRow::from_row("reviews", json!({"id":"r1","ts":null,"operator_notes":""})).unwrap(),
    )
    .unwrap();
    let origin = Origin::Record {
        location: RecordLocation {
            material: MaterialId::new(),
            anchor: 0,
        },
        output_index: 0,
    };
    let event = (kind.build)(
        &wire,
        "producer:steering",
        origin.clone(),
        ReadingOrder(1),
        [0; 32],
        Class::Open,
    )
    .unwrap();
    assert_eq!(event.payload, wire);
    let mut changed = wire;
    changed["table"] = json!("commitments");
    assert!(
        (kind.build)(
            &changed,
            "producer:steering",
            origin,
            ReadingOrder(1),
            [0; 32],
            Class::Open
        )
        .is_err()
    );
}
