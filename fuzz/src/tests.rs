use super::*;
#[test]
fn synthetic_regressions_exercise_valid_and_invalid_inputs() {
    for bytes in [
        b"{}".as_slice(),
        b"{invalid",
        b"{\"path\":\"tabs\\tand\\nlines\",\"members\":[]}",
        b"\xff\x00",
    ] {
        manifest_roundtrip(bytes);
        producer_roundtrip(bytes);
        query(bytes);
    }
    for text in [
        "events",
        "events where source = 'synthetic'",
        "sinex:ev/00000000-0000-0000-0000-000000000000",
        "events where (",
        "events | count",
    ] {
        query(text.as_bytes());
    }
}

#[test]
fn copy_delimiters_and_unicode_roundtrip_without_changing_rows() {
    use sqlx::Connection;
    let database = devkit::pg::test_database().unwrap();
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async {
        let mut connection = sqlx::PgConnection::connect(&database.url(None))
            .await
            .unwrap();
        sqlx::Executor::execute(&mut connection, "CREATE TEMP TABLE copy_input (value text)")
            .await
            .unwrap();
        for text in [
            "",
            "nul\0byte",
            "tab\tline\nreturn\rslash\\",
            "\\N",
            "zażółć\t🍋",
        ] {
            copy_escape_in(&mut connection, text).await;
        }
    });
}
#[test]
fn producer_frames_roundtrip_valid_events_and_all_existing_classes() {
    for class in ["open", "guarded", "sealed"] {
        let bytes = format!(
            r#"{{"kind":"events","frame":"synthetic","events":[{{"event_type":"note.document","payload":{{"text":"synthetic"}},"class":"{class}"}}]}}"#
        );
        producer_roundtrip(bytes.as_bytes());
    }
    producer_roundtrip(br#"{"kind":"chunk","frame":"synthetic","for_frame":"f","blob":"b","offset":0,"total":3,"data":"YWJj"}"#);
}

#[test]
fn manifest_details_keep_overflowing_integer_roundtrip() {
    let input = include_bytes!("../seeds/manifest_roundtrip/overflowing-integer.json");
    let manifest: sinex_model::Manifest = serde_json::from_slice(input).unwrap();
    assert!(manifest.details["me$i"][0]["end"].is_f64());
    manifest_roundtrip(input);
}
