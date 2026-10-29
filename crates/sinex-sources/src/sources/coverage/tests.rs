use super::*;
use sinex_model::event_types::coverage::CoverageCompleteness;
use sinex_model::{RecordOutcome, Timestamp};
use sinex_source_core::{DrainInput, Limits, drain};

fn observation() -> SourceCoverage {
    SourceCoverage {
        observed_run: None,
        observed_class: None,
        observation: "synthetic-read".into(),
        observed_source: "terminal.synthetic".into(),
        binding: "synthetic-binding".into(),
        host: "synthetic-host".into(),
        started_at: Timestamp::from_unix_secs(10).unwrap(),
        ended_at: Timestamp::from_unix_secs(11).unwrap(),
        state: CoverageState::RecordedUnknownCoverage,
        reason: Some("unchanged input read successfully".into()),
        completeness: CoverageCompleteness::Unknown,
        bounded_by: "read attempt".into(),
        events: 0,
        failures: 0,
    }
}

fn parse(observation: SourceCoverage) -> Vec<sinex_source_core::DrainOutput> {
    let material = observation_material(observation).material.unwrap();
    drain(
        &ObservationParser,
        DrainInput {
            source: OBSERVATION.name,
            class: OBSERVATION.class,
            material: material.id,
            bytes: material.bytes.as_slice().into(),
            records: &material.records,
            carry: Value::Null,
            reading_order: &*material.reading_order,
            limits: Limits::default(),
            manifest: &material.manifest,
        },
    )
    .unwrap()
}

#[devkit::test]
fn unchanged_source_read_keeps_material_provenance_and_unknown_coverage() {
    let out = parse(observation());
    let RecordOutcome::Events { events, .. } = &out[0].outcomes[0] else {
        panic!("coverage evidence did not parse");
    };
    assert_eq!(
        events[0].payload_as::<SourceCoverage>().unwrap(),
        observation()
    );
    assert!(events[0].location().is_some());
}

#[devkit::test]
fn failed_source_read_cannot_claim_observed_coverage() {
    let mut evidence = observation();
    evidence.state = CoverageState::Observed;
    evidence.failures = 1;
    let out = parse(evidence);
    assert!(matches!(out[0].outcomes[0], RecordOutcome::Failure { .. }));
}
