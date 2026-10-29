use super::*;
use serde_json::json;

fn record(host: &str, lane: &str, sequence: u64, boot: Option<&str>) -> Envelope {
    Envelope {
        host: host.into(),
        lane: lane.into(),
        seq: sequence,
        boot: boot.map(str::to_owned),
        session: None,
        observed_at: Timestamp::from_unix_secs(sequence as i64).unwrap(),
        payload: json!({}),
        raw_ref: None,
    }
}

#[devkit::test]
fn exact_gaps_survive_carry_restart_and_ignore_other_streams_and_generations() {
    let (_, carry) = observe(
        &Carry::default(),
        &record("a", "clipboard", 1, Some("boot-a")),
    );
    let carry: Carry = serde_json::from_value(serde_json::to_value(carry).unwrap()).unwrap();
    let (gap, carry) = observe(&carry, &record("a", "clipboard", 4, Some("boot-a")));
    let gap = gap.unwrap();
    assert_eq!(gap.reason, "sequence");
    assert_eq!(
        gap.sequence,
        Some(SequenceGap {
            first_missing: 2,
            last_missing: 3
        })
    );
    assert_eq!(gap.start, Some(Timestamp::from_unix_secs(1).unwrap()));
    assert_eq!(gap.end, Some(Timestamp::from_unix_secs(4).unwrap()));
    let (gap, carry) = observe(&carry, &record("a", "primary", 100, Some("boot-a")));
    assert!(gap.is_none());
    let (gap, carry) = observe(&carry, &record("b", "clipboard", 100, Some("boot-a")));
    assert!(gap.is_none());
    let (gap, carry) = observe(&carry, &record("a", "clipboard", 1, Some("boot-b")));
    assert!(gap.is_none(), "a new boot establishes a new baseline");
    let (gap, carry) = observe(&carry, &record("a", "clipboard", 2, Some("boot-b")));
    assert!(gap.is_none());
    let (gap, _) = observe(&carry, &record("a", "clipboard", 2, Some("boot-b")));
    assert!(
        gap.is_none(),
        "duplicate delivery is not a missing sequence"
    );
}

#[devkit::test]
fn unqualified_resets_and_reordering_leave_continuity_unknown() {
    let (_, carry) = observe(&Carry::default(), &record("a", "clipboard", 10, None));
    let (gap, carry) = observe(&carry, &record("a", "clipboard", 2, None));
    assert!(gap.is_none());
    let (gap, carry) = observe(&carry, &record("a", "clipboard", 20, None));
    assert!(
        gap.is_none(),
        "a lower number might be a reset or reordered input"
    );
    let (gap, _) = observe(&carry, &record("a", "clipboard", 22, None));
    assert!(
        gap.is_none(),
        "unknown generation is never reported as proven loss"
    );
    let carry: Carry = serde_json::from_value(serde_json::Value::Null).unwrap();
    assert!(
        observe(&carry, &record("a", "clipboard", 100, None))
            .0
            .is_none(),
        "legacy carry and a new rotated binding have no previous sequence"
    );
}

#[devkit::test]
fn reversed_source_clocks_keep_exact_sequence_bounds_and_unknown_gap_times() {
    let (_, carry) = observe(
        &Carry::default(),
        &record("a", "clipboard", 1, Some("boot")),
    );
    let mut later = record("a", "clipboard", 3, Some("boot"));
    later.observed_at = Timestamp::from_unix_secs(0).unwrap();
    let gap = observe(&carry, &later).0.unwrap();
    assert_eq!(
        gap.sequence,
        Some(SequenceGap {
            first_missing: 2,
            last_missing: 2
        })
    );
    assert!(gap.start.is_none() && gap.end.is_none());
}

#[devkit::test]
fn an_unreadable_record_invalidates_continuity_through_the_real_drainer() {
    use sinex_model::{Class, MaterialId, ReadingOrder, RecordOutcome};
    use sinex_source_core::{DrainInput, Limits, Source};
    let line = |sequence| {
        serde_json::json!({"schema":"sinnix-capture-v1","schema_version":1,"lane":"clipboard","host":"fixture","seq":sequence,"ts":1700000000,"payload":{},"raw_ref":null}).to_string() + "\n"
    };
    let bytes = (line(1) + "unreadable JSON\n" + &line(4)).into_bytes();
    let records = sinex_source_core::drivers::append_file::lines(&bytes).0;
    let source = super::super::source(super::super::lake_source("desktop.clipboard").unwrap());
    let manifest = sinex_model::Manifest::default();
    let output = source
        .drain(DrainInput {
            source: "desktop.clipboard",
            class: Class::Open,
            material: MaterialId::new(),
            bytes: sinex_source_core::drain::RecordBytes::Slice(&bytes),
            records: &records,
            carry: serde_json::Value::Null,
            reading_order: &|_| ReadingOrder(0),
            limits: Limits::default(),
            manifest: &manifest,
        })
        .unwrap();
    let outcomes: Vec<_> = output.iter().flat_map(|chunk| &chunk.outcomes).collect();
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| matches!(outcome, RecordOutcome::Failure { .. }))
            .count(),
        1
    );
    let kinds: Vec<_> = outcomes
        .iter()
        .flat_map(|outcome| match outcome {
            RecordOutcome::Events { events, .. } => events
                .iter()
                .map(|event| event.event_type.as_str())
                .collect(),
            _ => Vec::new(),
        })
        .collect();
    assert_eq!(
        kinds,
        ["lake.observed", "lake.observed"],
        "a parse failure is not proof of lost capture bytes"
    );
    let carry: Carry = serde_json::from_value(output.last().unwrap().carry.clone()).unwrap();
    let gap = observe(&carry, &record("fixture", "clipboard", 8, None))
        .0
        .unwrap();
    assert_eq!(
        gap.sequence,
        Some(SequenceGap {
            first_missing: 5,
            last_missing: 7
        }),
        "the valid record after a failure is a fresh positive anchor"
    );
}
