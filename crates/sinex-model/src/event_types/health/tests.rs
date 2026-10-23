use super::*;
use serde_json::json;

fn row(provider: &str, category: &str, values: Value) -> Value {
    json!({"provider":provider,"category":category,"native_id":"synthetic-1",
        "start":null,"end":null,"offset_minutes":null,"values":values})
}

#[test]
fn known_fields_keep_missing_null_numeric_precision_and_text() {
    let wire = row(
        "samsung-health",
        "step_daily_trend",
        json!({"count":9007199254740993_i64,"distance":null,"speed":"0042"}),
    );
    let decoded: HealthRecord = serde_json::from_value(wire.clone()).unwrap();
    let HealthValues::Steps(steps) = &decoded.values else {
        panic!("wrong category")
    };
    assert_eq!(
        steps.count,
        Some(Some(Scalar::Number(Number::from(9007199254740993_i64))))
    );
    assert_eq!(steps.distance, Some(None));
    assert_eq!(steps.speed, Some(Some(Scalar::Text("0042".into()))));
    assert_eq!(serde_json::to_value(decoded).unwrap(), wire);
}

#[test]
fn known_field_type_errors_do_not_become_unknown_rows() {
    for value in [
        json!({"count":[]}),
        json!({"count":{}}),
        json!({"count":true}),
    ] {
        assert!(
            serde_json::from_value::<HealthRecord>(row(
                "samsung-health",
                "step_daily_trend",
                value
            ))
            .is_err()
        );
    }
    assert!(
        serde_json::from_value::<HealthRecord>(row(
            "samsung-health-cloud",
            "Heart Rate",
            json!({"binning_data":[{"heart_rate":[]}]})
        ))
        .is_err()
    );
}

#[test]
fn unknown_categories_and_columns_are_preserved_and_identified() {
    let original = row(
        "samsung-health",
        "new-category",
        json!({"anything":[null,{},9007199254740993_i64]}),
    );
    let decoded: HealthRecord = serde_json::from_value(original.clone()).unwrap();
    assert!(matches!(decoded.values, HealthValues::Unknown(_)));
    let output = serde_json::to_value(decoded).unwrap();
    assert_eq!(output["values"], original["values"]);
    assert_eq!(output["uninterpreted"], true);
    let original = row(
        "samsung-health",
        "hrv",
        json!({"score":7,"new/field":false}),
    );
    let decoded: HealthRecord = serde_json::from_value(original.clone()).unwrap();
    let output = serde_json::to_value(decoded).unwrap();
    assert_eq!(output["values"], original["values"]);
    assert_eq!(
        output["uninterpreted_fields"],
        json!(["/values/new~1field"])
    );
    assert_eq!(
        serde_json::to_value(serde_json::from_value::<HealthRecord>(output.clone()).unwrap())
            .unwrap(),
        output
    );
}

#[test]
fn sleep_samples_and_event_numbers_keep_the_existing_wire_shape() {
    let mut original = row(
        "sleep-as-android",
        "sleep",
        json!({"snore":null,"cycles":3.0,"actigraphy":[[null,1.0],["00:10",null]]}),
    );
    original["events"] = json!([{"kind":"HR","at":"2026-01-01T00:00:00Z","value":88.0}]);
    let decoded: HealthRecord = serde_json::from_value(original.clone()).unwrap();
    assert!(matches!(decoded.values, HealthValues::SleepSession(_)));
    assert_eq!(serde_json::to_value(decoded).unwrap(), original);
}

#[test]
fn fit_direct_nested_and_map_values_are_concrete_and_lossless() {
    let original = row(
        "google-fit",
        "synthetic.steps",
        json!({"fitValue":[{"intVal":9007199254740993_i64},{"value":{"intVal":1}},{"mapVal":[{"key":"synthetic","value":{"fpVal":1.5}}]}],"modifiedTimeMillis":1}),
    );
    let decoded: HealthRecord = serde_json::from_value(original.clone()).unwrap();
    let HealthValues::Fit(fit) = &decoded.values else {
        panic!("not fit")
    };
    assert_eq!(fit.fit_value.as_ref().unwrap().as_ref().unwrap().len(), 3);
    assert_eq!(serde_json::to_value(decoded).unwrap(), original);
    assert!(
        serde_json::from_value::<HealthRecord>(row(
            "google-fit",
            "synthetic.steps",
            json!({"fitValue":[{"intVal":"1"}]})
        ))
        .is_err()
    );
}

#[test]
fn decoded_bins_preserve_extensions_without_hiding_bad_known_fields() {
    let original = row(
        "samsung-health-cloud",
        "Health HRV",
        json!({"binning_data":[{"start_time":1,"sdnn":2.5,"extra":true}]}),
    );
    let decoded: HealthRecord = serde_json::from_value(original.clone()).unwrap();
    let output = serde_json::to_value(decoded).unwrap();
    assert_eq!(output["values"], original["values"]);
    assert_eq!(
        output["uninterpreted_fields"],
        json!(["/values/binning_data"])
    );
}

#[test]
fn declared_samsung_categories_share_source_metadata_without_losing_fields() {
    for category in [
        "Stress Internal Data",
        "Step Count",
        "Health HRV",
        "Heart Rate",
        "Oxygen Saturation (SpO2)",
        "Health Respiratory Rate",
        "Samsung Health Skin Temperature",
        "Sleep",
        "Sleep Combined",
        "Sleep Stage",
        "Sleep Snoring",
        "Health Movement",
        "Shealth Vitality Nap Data",
        "Health Vitality Score",
        "Weight",
        "Floors Climbed",
        "Shealth Mood",
        "Activity Day Summary",
        "Calories Burned",
        "Electrocardiogram 2",
    ] {
        let original = row(
            "samsung-health-cloud",
            category,
            json!({"datauuid":"synthetic-id","start_time":123,"time_offset":3600000,"new_column":{"raw":true}}),
        );
        let decoded: HealthRecord = serde_json::from_value(original.clone()).unwrap();
        assert!(
            !matches!(decoded.values, HealthValues::Unknown(_)),
            "{category}"
        );
        let output = serde_json::to_value(decoded).unwrap();
        assert_eq!(output["values"], original["values"], "{category}");
        assert_eq!(
            output["uninterpreted_fields"],
            json!(["/values/new_column"]),
            "{category}"
        );
    }
}

#[test]
fn interpretation_markers_are_computed_from_values_not_input_claims() {
    let mut original = row("samsung-health", "hrv", json!({"score":7}));
    original["uninterpreted"] = json!(true);
    original["uninterpreted_fields"] = json!(["/values/score"]);
    let output =
        serde_json::to_value(serde_json::from_value::<HealthRecord>(original).unwrap()).unwrap();
    assert!(output.get("uninterpreted").is_none());
    assert!(output.get("uninterpreted_fields").is_none());
    let schema = serde_json::to_value(schemars::schema_for!(HealthRecord)).unwrap();
    assert!(schema["properties"].get("uninterpreted").is_some());
    assert!(schema["properties"].get("uninterpreted_fields").is_some());
}
