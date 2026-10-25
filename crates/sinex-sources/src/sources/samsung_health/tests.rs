use super::*;

#[devkit::test]
fn type_line_header_and_rows() {
    let manifest = sinex_model::Manifest::from_json(
        json!({"path": "com.samsung.health.hrv.20260807162074.csv"}),
    );
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

#[devkit::test]
fn attachment_bins_become_one_record_each() {
    let manifest = sinex_model::Manifest::from_json(json!({"path": "export.zip"}));
    let member = serde_json::from_value::<sinex_model::Member>(
        json!({"path": "jsons/com.samsung.shealth.stress/0/a291.binning_data.json"}),
    )
    .unwrap();
    let ctx = RecordContext {
        manifest: &manifest,
        member: Some(&member),
    };
    let bins = br#"[{"score":14,"start_time":1736636340000,"end_time":1736636399000},{"score":10,"start_time":1736636280000,"end_time":1736636339000}]"#;
    let (out, ()) = AttachmentParser.parse(&&bins[..], &(), &ctx).unwrap();
    assert_eq!(out.len(), 2);
    assert_eq!(out[0].category(), "stress.binning_data");
    assert_eq!(out[1].native_id, "a291:binning_data:1");
    assert_eq!(
        out[0].start,
        Timestamp::from_unix_ns(1_736_636_340_000_000_000).ok()
    );
    assert_eq!(
        serde_json::to_value(out[0].values()).unwrap()["score"],
        json!(14)
    );

    let member = serde_json::from_value::<sinex_model::Member>(
        json!({"path": "jsons/com.samsung.shealth.sleep/0/b17.extra_data.json"}),
    )
    .unwrap();
    let ctx = RecordContext {
        manifest: &manifest,
        member: Some(&member),
    };
    let (out, ()) = AttachmentParser
        .parse(&&br#"{"sleep_result_type":"STAGE"}"#[..], &(), &ctx)
        .unwrap();
    assert_eq!(out[0].native_id, "b17:extra_data");
    assert_eq!(out[0].category(), "sleep.extra_data");
    assert_eq!(out[0].start, None);
}

#[devkit::test]
fn attachment_paths_strip_the_column_prefix() {
    assert_eq!(
        attachment_of(
            "jsons/com.samsung.shealth.tracker.heart_rate/f/x1.com.samsung.health.heart_rate.binning_data.json"
        ),
        Some(("tracker.heart_rate".to_owned(), "x1", "binning_data"))
    );
    assert!(select_attachments(
        "jsons/com.samsung.shealth.sleep/0/b.extra_data.json"
    ));
    assert!(!select(
        "jsons/com.samsung.shealth.sleep/0/b.extra_data.json"
    ));
}
