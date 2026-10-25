use super::*;

#[devkit::test]
fn timestamp_round_trips_nanoseconds() {
    let t = Timestamp::from_unix_ns(1_792_339_243_093_191_950).unwrap();
    let text = serde_json::to_string(&t).unwrap();
    assert_eq!(text, "\"2026-10-18T16:00:43.09319195Z\"");
    let back: Timestamp = serde_json::from_str(&text).unwrap();
    assert_eq!(back, t);
}

#[devkit::test]
fn unknown_has_no_instant() {
    assert_eq!(
        TsOrig::maybe(None, TimeQuality::Intrinsic),
        TsOrig::unknown()
    );
}
