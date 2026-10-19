use super::*;
use serde_json::json;

#[test]
fn points_are_records_of_their_data_source() {
    let manifest = json!({"path": "Takeout/Fit/All Data/derived_com.google.active_minutes_x.json"});
    let context = RecordContext {
        manifest: &manifest,
        member: None,
    };
    let point = json!({"fitValue": [{"value": {"intVal": 1}}],
        "startTimeNanos": 1_627_859_880_000_000_000_i64, "endTimeNanos": 1_627_859_940_000_000_000_i64,
        "dataTypeName": "com.google.active_minutes", "modifiedTimeMillis": 1});
    let (out, ()) = FitParser
        .parse_record(point.as_object().unwrap(), &(), &context)
        .unwrap();
    let r = &out[0];
    assert_eq!(r.category, "com.google.active_minutes");
    assert_eq!(
        r.native_id,
        "derived_com.google.active_minutes_x:1627859880000000000:1627859940000000000"
    );
    assert_eq!(r.values["fitValue"][0]["value"]["intVal"], json!(1));
    assert!(!r.values.contains_key("startTimeNanos"));
}
