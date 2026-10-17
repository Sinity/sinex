use super::*;
fn visit() -> Value {
    json!({"table":"visits","visit":{"id":7,"url":2,"visit_time":WINDOWS_EPOCH_US+1_700_000_000_123_456i64,"visit_duration":0,"transition":0,"from_visit":0},"url":{"id":2,"url":"https://example.test/","title":"Synthetic"},"referrer_url":null})
}
#[test]
fn preserves_provider_microseconds_and_zero_duration_without_running_claim() {
    let row = parse(&visit(), "Default", Uuid::now_v7()).unwrap();
    assert_eq!(row.unix_us, Some(1_700_000_000_123_456));
    assert!(row.unfinished);
    assert_eq!(row.payload["duration_us"], 0);
    assert!(row.payload.get("running").is_none());
}
#[test]
fn missing_id_never_becomes_zero_or_an_anonymous_occurrence() {
    let mut row = visit();
    row["visit"].as_object_mut().unwrap().remove("id");
    assert!(parse(&row, "Default", Uuid::now_v7()).is_err());
}
#[test]
fn redirects_are_flagged_without_dropping_source_fields() {
    let mut row = visit();
    row["visit"]["transition"] = json!(0x8000_0001u64);
    let parsed = parse(&row, "Default", Uuid::now_v7()).unwrap();
    assert_eq!(parsed.payload["transition"], 0x8000_0001u64);
    assert_eq!(parsed.payload["redirect"], true);
}
#[test]
fn download_guid_is_identity_and_zero_start_is_unknown_time() {
    let id = Uuid::now_v7();
    let row = json!({"table":"downloads","download":{"id":1,"guid":id.to_string(),"state":0,"start_time":0,"end_time":0,"received_bytes":5,"total_bytes":10,"target_path":"/synthetic/file","current_path":"/synthetic/file.part"},"url_chain":[{"id":1,"chain_index":0,"url":"https://example.test/a"},{"id":1,"chain_index":1,"url":"https://example.test/b"}]});
    let parsed = parse(&row, "Default", Uuid::now_v7()).unwrap();
    assert_eq!(parsed.key, id.to_string());
    assert_eq!(parsed.unix_us, None);
    assert!(parsed.unfinished);
    assert_eq!(parsed.payload["url"], "https://example.test/b");
    let mut missing = row;
    missing["download"]["guid"] = json!("");
    assert!(parse(&missing, "Default", Uuid::now_v7()).is_err());
}
