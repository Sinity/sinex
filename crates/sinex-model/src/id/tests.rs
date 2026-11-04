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

#[devkit::test]
fn a_token_lives_from_its_mint_time_to_its_holders_end() {
    let token = "01a1998e-447b-7001-8000-000000000001";
    let minted = 0x01a1_998e_447b_i64;
    assert_eq!(uuid7_millisecond(token), Some(minted));
    assert_eq!(
        uuid7_millisecond("01a1998e-447b-4001-8000-000000000001"),
        None
    );
    assert_eq!(uuid7_millisecond("not a token"), None);
    let start = i128::from(minted) * 1_000_000;
    let lifetime = TokenLifetime {
        length: Some("duration_ns"),
        slack_ns: 2_000_000_000,
    };
    // The report, timed at the start, lasts its duration.
    let report = serde_json::json!({"duration_ns": 3_600_000_000_000_i64});
    assert_eq!(
        lifetime.bounds(token, start + 400_000, &report),
        (
            start - 2_000_000_000,
            start + 3_600_000_000_000 + 2_000_000_000
        )
    );
    // The command's end, with no length, reaches back to the mint time.
    let end = start + 7_000_000_000;
    assert_eq!(
        lifetime.bounds(token, end, &serde_json::json!({})),
        (start - 2_000_000_000, end + 2_000_000_000)
    );
}
