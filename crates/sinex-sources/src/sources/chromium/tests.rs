use super::*;
use crate::sources::fields::WEBKIT_EPOCH_OFFSET_US;
use rusqlite::Connection;
use serde_json::json;
use sinex_source_core::drivers::sqlite_rows::RowState;

const NOW_WEBKIT: i64 = 13_370_000_000_000_000;

fn history() -> PathBuf {
    let dir = std::env::temp_dir().join(format!("sinex-chromium-{}", uuid::Uuid::now_v7()));
    std::fs::create_dir_all(dir.join("Default")).unwrap();
    std::fs::create_dir_all(dir.join("Profile 7")).unwrap();
    std::fs::create_dir_all(dir.join("Crashpad")).unwrap();
    let path = dir.join("Default/History");
    let c = Connection::open(&path).unwrap();
    c.execute_batch(&format!(
        "CREATE TABLE urls (id INTEGER PRIMARY KEY, url TEXT, title TEXT, visit_count INTEGER, typed_count INTEGER, last_visit_time INTEGER, hidden INTEGER);
         CREATE TABLE visits (id INTEGER PRIMARY KEY, url INTEGER, visit_time INTEGER, from_visit INTEGER, transition INTEGER, segment_id INTEGER, visit_duration INTEGER, opener_visit INTEGER);
         INSERT INTO urls VALUES (1, 'https://a.example/', 'A', 2, 1, {NOW_WEBKIT}, 0), (2, 'https://b.example/', '', 0, 0, 0, 1);
         INSERT INTO visits VALUES (10, 1, {NOW_WEBKIT}, 0, 805306369, 0, 0, 0), (11, 1, {}, 10, 0, 0, 1500000, 3);",
        NOW_WEBKIT + 1
    ))
    .unwrap();
    std::fs::copy(&path, dir.join("Profile 7/History")).unwrap();
    path
}

fn rows(d: &SqliteRows) -> (Vec<Map<String, Value>>, sinex_model::Manifest) {
    let a = d.poll(&Value::Null, &mut RowState::default()).unwrap();
    let m = a.material.unwrap();
    let sinex_source_core::drivers::StoredBytes::FramedFile(file) = m.stored.as_ref().unwrap()
    else {
        panic!("rows use staged bytes")
    };
    use std::os::unix::fs::FileExt;
    let rows = m
        .records
        .iter()
        .map(|(s, e)| {
            let mut bytes = vec![0; e - s];
            file.read_exact_at(&mut bytes, *s as u64).unwrap();
            serde_json::from_slice(&bytes).unwrap()
        })
        .collect();
    (rows, m.manifest)
}

fn context(manifest: &sinex_model::Manifest) -> RecordContext<'_> {
    RecordContext {
        manifest,
        member: None,
    }
}

#[devkit::test]
fn visits_carry_their_url_and_unknown_times_stay_unknown() {
    let path = history();
    let (rows, manifest) = rows(&visits_driver(path, "h".into()));
    let cx = context(&manifest);
    let (open, ()) = VisitParser.parse(&rows[0], &(), &cx).unwrap();
    assert!(open[0].profile.ends_with("/Default"));
    assert_eq!(open[0].url.as_deref(), Some("https://a.example/"));
    assert_eq!(open[0].title.as_deref(), Some("A"));
    assert_eq!((open[0].from_visit, open[0].duration_us), (None, None));
    assert_eq!(
        open[0].visited_at.unix_ns(),
        i128::from(NOW_WEBKIT - WEBKIT_EPOCH_OFFSET_US) * 1000
    );
    let (closed, ()) = VisitParser.parse(&rows[1], &(), &cx).unwrap();
    assert_eq!(
        (closed[0].from_visit, closed[0].duration_us),
        (Some(10), Some(1_500_000))
    );
    assert_eq!(closed[0].extra.get("opener_visit"), Some(&json!(3)));
    assert!(!closed[0].extra.contains_key("rowid"));
}

#[devkit::test]
fn urls_read_counts_and_never_visited_is_unknown() {
    let path = history();
    let (rows, manifest) = rows(&urls_driver(path, "h".into()));
    let (b, ()) = UrlParser.parse(&rows[1], &(), &context(&manifest)).unwrap();
    assert_eq!(b[0].last_visited_at, None);
    assert_eq!(b[0].title, None);
    assert!(b[0].hidden);
}

#[devkit::test]
fn profiles_are_named_from_their_path() {
    assert_eq!(
        profile_of("/home/u/.config/google-chrome/Profile 7/History"),
        "google-chrome/Profile 7"
    );
    let path = history();
    let root = path.parent().unwrap().parent().unwrap();
    let names: Vec<String> = profiles(root).into_iter().map(|(n, _)| n).collect();
    let browser = root.file_name().unwrap().to_string_lossy();
    assert_eq!(
        names,
        vec![format!("{browser}/Default"), format!("{browser}/Profile 7")]
    );
}
