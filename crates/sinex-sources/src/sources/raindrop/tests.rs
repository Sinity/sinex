use super::*;
use serde_json::json;

#[devkit::test]
fn csv_header_is_carried_then_rows_read() {
    let (none, header) = RaindropParser
        .parse(
            &&b"\xef\xbb\xbfid,title,note,excerpt,url,folder,tags,created,cover,highlights,favorite\n"[..],
            &vec![], &crate::RecordContext::none())
        .unwrap();
    assert!(none.is_empty());
    assert_eq!(header[0], "id");
    let row = b"1302755715,\"Save, the World\",,In theory,https://a/,Unsorted,\"ai, ea\",2025-08-17T13:19:33.851Z,,,false\n";
    let (p, _) = RaindropParser
        .parse(&&row[..], &header, &crate::RecordContext::none())
        .unwrap();
    assert_eq!(p[0].native_id, "1302755715");
    assert_eq!(p[0].title.as_deref(), Some("Save, the World"));
    assert_eq!(p[0].tags, vec!["ai", "ea"]);
    assert_eq!(p[0].favorite, Some(false));
    assert_eq!(p[0].note, None);
}

#[devkit::test]
fn api_lines_and_the_trash() {
    let row = json!({"_id": 1302755715, "link": "https://a/", "title": "A", "excerpt": "", "note": "",
        "tags": ["ai"], "important": false, "removed": false, "created": "2026-10-11T06:10:25.715Z",
        "lastUpdate": "2026-10-11T06:10:29.856Z", "collection": {"$id": 39498970}, "highlights": [],
        "domain": "a"});
    let line = format!("{row}\n");
    let (p, carried) = RaindropParser
        .parse(
            &line.as_bytes(),
            &vec!["id".into()],
            &crate::RecordContext::none(),
        )
        .unwrap();
    assert_eq!(
        carried,
        vec!["id"],
        "a JSON line leaves the CSV header alone"
    );
    assert_eq!(
        p[0].native_id, "1302755715",
        "the same ID as the CSV row above"
    );
    assert_eq!(p[0].highlights, None);
    assert_eq!(p[0].trashed_at, None);
    assert!(p[0].extra.contains_key("collection"));
    let mut trashed = row.as_object().unwrap().clone();
    trashed.insert("collection".into(), json!({"$id": -99}));
    let line = format!("{}\n", Value::Object(trashed));
    let (p, _) = RaindropParser
        .parse(&line.as_bytes(), &vec![], &crate::RecordContext::none())
        .unwrap();
    assert!(p[0].trashed_at.is_some());
    assert_eq!(
        sinex_model::EventPayload::record_state(&p[0]),
        sinex_model::RecordState::Removed
    );
}

#[devkit::test]
fn every_export_file_name_is_planned_and_nothing_else() {
    let dir = tempfile::tempdir().unwrap();
    for name in [
        "raindrop_bookmarks_16_04_2025.csv",
        "raindrop-03-01-2026-00000000-0000-0000-0000-000000000000.csv",
        "Renamed-by-hand.csv",
        "raindrops-all.jsonl",
        "raindrops-trash.jsonl",
        "highlights.jsonl",
        "collections-root.json",
        "tags.json",
        "EXPORT-MANIFEST.json",
        "MANIFEST.json",
    ] {
        std::fs::write(dir.path().join(name), "").unwrap();
    }
    let plan = (FORMAT.plan.unwrap())(dir.path(), &crate::binding::Settings::default()).unwrap();
    let mut names: Vec<String> = plan
        .iter()
        .map(|b| b.input.file_name().unwrap().to_string_lossy().into_owned())
        .collect();
    names.sort();
    assert_eq!(
        names,
        [
            "Renamed-by-hand.csv",
            "raindrop-03-01-2026-00000000-0000-0000-0000-000000000000.csv",
            "raindrop_bookmarks_16_04_2025.csv",
            "raindrops-all.jsonl",
            "raindrops-trash.jsonl",
        ]
    );
}
