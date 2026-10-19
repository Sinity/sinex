use super::*;
use serde_json::json;

#[test]
fn gallery_dl_booru_metadata() {
    let s = gallery_dl(&json!({
        "category": "danbooru",
        "id": 123,
        "md5": "ABCDEF",
        "rating": "s",
        "source": "https://example.org/art/1",
        "tag_string_artist": "some_artist",
        "tag_string_general": "1girl solo",
        "tags": ["1girl", "solo"],
    }));
    let tags: Vec<&str> = s
        .values
        .iter()
        .filter(|v| v.field == "tag")
        .map(|v| v.value.as_str())
        .collect();
    assert!(tags.contains(&"artist:some_artist"));
    assert!(tags.contains(&"general:1girl"));
    assert!(tags.contains(&"solo"));
    assert!(s.values.contains(&Imported {
        field: "content_rating".into(),
        value: "general".into()
    }));
    assert_eq!(s.md5.as_deref(), Some("abcdef"));
    assert_eq!(s.source_ref.as_deref(), Some("danbooru:123"));
}

#[test]
fn reddit_and_twitter_metadata_keep_text_and_urls() {
    let s = gallery_dl(&json!({
        "category": "reddit", "title": "A title", "url": "https://i.redd.it/x.jpg"
    }));
    assert!(s.values.contains(&Imported {
        field: "title".into(),
        value: "A title".into()
    }));
    assert_eq!(s.source_ref.as_deref(), Some("https://i.redd.it/x.jpg"));
}

#[test]
fn hydrus_tag_lines() {
    let s = tag_lines("creator:someone\n\nseries:thing\nplain tag\n");
    assert_eq!(s.values.len(), 3);
    assert_eq!(s.values[2].value, "plain tag");
}

#[test]
fn sidecar_names() {
    let names: Vec<String> = sidecars_of(std::path::Path::new("/r/a.jpg"))
        .into_iter()
        .map(|(p, _)| p.display().to_string())
        .collect();
    assert_eq!(names, ["/r/a.jpg.json", "/r/a.json", "/r/a.jpg.txt"]);
}
