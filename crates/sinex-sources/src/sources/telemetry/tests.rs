use super::*;
use sinex_model::Timestamp;
use sinex_source_core::drivers::live_stream::frame;

#[devkit::test]
fn a_spooled_sample_is_one_telemetry_event_at_its_sampling_time() {
    let at = Timestamp::from_unix_secs(1_800_000_000).unwrap();
    let line =
        br#"{"component":"derivation","name":"command.fusion","measures":{"dirty_keys":3.0}}"#;
    let spooled = frame(at, line);
    let record: Map<String, Value> = serde_json::from_slice(&spooled).unwrap();
    let (out, ()) = TelemetryParser
        .parse(&record, &(), &sinex_source_core::RecordContext::none())
        .unwrap();
    assert_eq!(
        out,
        vec![SinexTelemetry {
            component: "derivation".into(),
            name: "command.fusion".into(),
            sampled_at: at,
            measures: BTreeMap::from([("dirty_keys".into(), 3.0)]),
        }]
    );
    let bad = frame(at, b"not a sample");
    let record: Map<String, Value> = serde_json::from_slice(&bad).unwrap();
    assert!(
        TelemetryParser
            .parse(&record, &(), &sinex_source_core::RecordContext::none())
            .is_err()
    );
}
