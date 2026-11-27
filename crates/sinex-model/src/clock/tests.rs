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

#[devkit::test]
fn an_interval_reaches_its_end_or_the_bound_of_an_unknown_end() {
    let at = |seconds: i64| Timestamp::from_unix_secs(1_800_000_000 + seconds).unwrap();
    let state = IntervalKind::of("state.interval").unwrap();
    for (payload, reaches) in [
        (
            serde_json::json!({"ended_at": at(50), "fenced_at": at(90)}),
            Some(at(50)),
        ),
        (
            serde_json::json!({"ended_at": null, "fenced_at": at(90)}),
            Some(at(90)),
        ),
        (
            serde_json::json!({"ended_at": null, "fenced_at": null}),
            None,
        ),
        (serde_json::json!({}), None),
    ] {
        assert_eq!(state.reaches_until(at(0), &payload), reaches, "{payload}");
    }
    // A kind that declares no bound ignores a field of that name.
    let span = IntervalKind::of("window.span").unwrap();
    assert_eq!(
        span.reaches_until(at(0), &serde_json::json!({"fenced_at": at(90)})),
        None
    );
}
