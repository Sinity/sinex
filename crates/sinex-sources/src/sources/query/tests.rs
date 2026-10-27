use super::*;
use sinex_source_core::{DrainInput, Limits, drain};
use sinex_model::RecordOutcome;

#[devkit::test]
fn query_requests_keep_bytes_and_use_manifest_authorship_without_changing_the_marked_time() {
    let at = Timestamp::from_unix_secs(100).unwrap();
    let marked = Timestamp::from_unix_secs(50).unwrap();
    let record = QueryRecord::Mark(Mark {
        host: "spoofed".into(),
        at: marked,
        text: Some("synthetic note".into()),
        cwd: None,
        command: None,
        app: None,
        title: None,
        by: "spoofed".into(),
        written_at: marked,
    });
    let acquired = request_material(record, "operator", "synthetic", at);
    let material = acquired.material.unwrap();
    assert!(String::from_utf8_lossy(&material.bytes).contains("spoofed"));
    let result = drain(
        &RequestParser,
        DrainInput {
            source: REQUEST.name,
            class: REQUEST.class,
            material: material.id,
            bytes: material.bytes.as_slice().into(),
            records: &material.records,
            carry: Value::Null,
            reading_order: &*material.reading_order,
            limits: Limits::default(),
            manifest: &material.manifest,
        },
    )
    .unwrap();
    let RecordOutcome::Events { events, .. } = &result[0].outcomes[0] else {
        panic!("request failed");
    };
    let payload: Mark = events[0].payload_as().unwrap();
    assert_eq!(payload.at, marked);
    assert_eq!(payload.by, "operator");
    assert_eq!(payload.written_at, at);
    assert_eq!(events[0].class, Class::Guarded);
    assert_eq!(events[0].location().unwrap().0.material, material.id);
}
