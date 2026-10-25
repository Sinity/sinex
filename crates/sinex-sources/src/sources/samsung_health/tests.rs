use super::*;

#[devkit::test]
fn type_line_header_and_rows() {
    let manifest = json!({"path": "com.samsung.health.hrv.20260807162074.csv"});
    let ctx = RecordContext {
        manifest: &manifest,
        member: None,
    };
    let (_, c) = SamsungParser
        .parse(
            &&b"\xef\xbb\xbfcom.samsung.health.hrv,7006003,1\n"[..],
            &Carry::default(),
            &ctx,
        )
        .unwrap();
    assert_eq!(c.category, "hrv");
    let (_, c) = SamsungParser
        .parse(
            &&b"com.samsung.health.hrv.start_time,time_offset,datauuid,end_time,score\n"[..],
            &c,
            &ctx,
        )
        .unwrap();
    assert_eq!(c.header[0], "start_time");
    let (out, _) = SamsungParser
        .parse(
            &&b"2025-05-21 04:00:00.000,UTC+0200,a291,2025-05-21 05:00:00.000,7\n"[..],
            &c,
            &ctx,
        )
        .unwrap();
    let r = &out[0];
    assert_eq!(r.native_id, "a291");
    assert_eq!(
        r.start,
        fields::rfc3339("2025-05-21T04:00:00Z").ok(),
        "the text is UTC"
    );
    assert_eq!(r.offset_minutes, Some(120));
    assert_eq!(serde_json::to_value(r.values()).unwrap()["score"], json!(7));
}

#[devkit::test]
fn categories_and_values() {
    assert_eq!(
        category_of("com.samsung.shealth.tracker.heart_rate"),
        "tracker.heart_rate"
    );
    assert_eq!(typed("12"), json!(12));
    assert_eq!(typed("0.5"), json!(0.5));
    assert_eq!(typed("gOU20c3N2O"), json!("gOU20c3N2O"));
    assert_eq!(typed("0042"), json!("0042"), "zero-padded stays text");
    assert_eq!(typed("0"), json!(0));
    assert_eq!(typed("-1.5"), json!(-1.5));
    assert_eq!(offset("UTC-0130"), Some(-90));
}
