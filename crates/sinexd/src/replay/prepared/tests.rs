use super::*;
use sinex_model::{
    ArchiveRequest, Class, Event, MaterialId, OperationId, Origin, ReadingOrder, RecordLocation,
    ReplayScope, TsOrig, event_types::telemetry::HardwareState,
};

#[test]
fn preparation_preserves_valid_deep_payloads_and_unknown_source_time() {
    let directory = tempfile::tempdir().unwrap();
    let operation = OperationId::new();
    let material = MaterialId::new();
    let location = RecordLocation {
        material,
        anchor: 0,
    };
    let state = serde_json::from_str(&format!("{}0{}", "[".repeat(125), "]".repeat(125))).unwrap();
    let event = Event::build(
        &HardwareState {
            row_id: 1,
            captured_at: None,
            host: Some("synthetic".into()),
            boot_id: None,
            state,
            values: Default::default(),
        },
        "synthetic",
        Origin::Record {
            location,
            output_index: 0,
        },
        ReadingOrder(1),
        [7; 32],
        Class::Open,
    )
    .unwrap();
    assert_eq!(event.ts_orig, TsOrig::unknown());
    let payload = serde_json::to_vec(&event.payload).unwrap();
    assert!(serde_json::from_slice::<serde_json::Value>(&payload).is_ok());
    let records = vec![RecordOutcome::Events {
        location: Some(location),
        events: vec![event.clone()],
    }];
    let batch = ReadBatch {
        source: None,
        host: None,
        records: records.clone(),
    };
    let encoded = serde_json::to_vec(&batch).unwrap();
    assert!(
        serde_json::from_slice::<ReadBatch>(&encoded).is_err(),
        "the default preparation decoder rejects this otherwise valid payload"
    );
    let mut preparation = Builder::new(directory.path()).unwrap();
    preparation
        .push(Batch {
            run: RunId(operation.0),
            kind: BatchKind::Replay {
                operation,
                source: None,
                host: None,
            },
            records: vec![],
            archive: vec![ArchiveRequest::Replay {
                operation,
                scope: ReplayScope {
                    materials: vec![material],
                    ..ReplayScope::default()
                },
            }],
        })
        .unwrap();
    preparation
        .push(Batch {
            run: RunId(operation.0),
            kind: BatchKind::Replay {
                operation,
                source: None,
                host: None,
            },
            records,
            archive: vec![],
        })
        .unwrap();
    let mut prepared = preparation.finish().unwrap();
    prepared.next().unwrap().unwrap();
    let recovered = prepared.next().unwrap().unwrap();
    let RecordOutcome::Events { events, .. } = &recovered.records[0] else {
        panic!("preparation changed an event into a failure")
    };
    assert_eq!(events, &[event]);
    assert!(prepared.next().is_none());
}
