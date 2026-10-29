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

#[devkit::test]
fn telemetry_discovery_reads_the_supplied_spool_without_starting_a_capture() {
    use sinex_source_core::discovery::{DiscoveryContext, SourceConfig};
    let mut context = DiscoveryContext::default();
    context.host = "fixture".into();
    let initial = (FORMAT.discover)(&FORMAT, &context);
    assert!(
        initial
            .iter()
            .all(|input| input.bindings.is_empty() && !input.capture)
    );
    let spool = tempfile::tempdir().unwrap();
    context.sources.insert(
        SPEC.name.into(),
        SourceConfig {
            paths: vec![spool.path().display().to_string()],
            ..Default::default()
        },
    );
    let discovered = (FORMAT.discover)(&FORMAT, &context);
    let bindings: Vec<_> = discovered
        .iter()
        .flat_map(|input| &input.bindings)
        .collect();
    assert_eq!(bindings.len(), 1);
    assert_eq!(bindings[0].input, spool.path());
    assert!(discovered.iter().all(|input| !input.capture));
    assert!(FORMAT.capture.is_none(), "sampling belongs to the daemon");
}
