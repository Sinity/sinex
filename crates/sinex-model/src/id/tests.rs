use super::*;

#[devkit::test]
fn ts_coided_is_the_minting_time() {
    let before = time::OffsetDateTime::now_utc();
    let id = EventId::new();
    let after = time::OffsetDateTime::now_utc();
    let coided = id.ts_coided().unwrap().0;
    // UUIDv7 keeps milliseconds.
    assert!(coided >= before - time::Duration::milliseconds(1));
    assert!(coided <= after);
}

#[devkit::test]
fn ids_serialize_as_bare_uuids() {
    let id = RunId::new();
    assert_eq!(
        serde_json::to_value(id).unwrap(),
        serde_json::json!(id.0.to_string())
    );
}
