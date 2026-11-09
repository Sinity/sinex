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

/// Words of the query language, so generated queries reach past the first
/// token far more often than random text does.
const QUERY_WORDS: &[&str] = &[
    "events",
    "commands",
    "where",
    "not",
    "and",
    "or",
    "(",
    ")",
    "source",
    "event_type",
    "text",
    "=",
    "!=",
    ">",
    "<",
    "'synthetic'",
    "\"phrase\"",
    "since:7d",
    "|",
    "count",
    "by",
    "sort",
    "limit",
    "10",
    "group",
    "fields",
    "as",
    "of",
    "-",
    "!",
    "near:",
    "sinex:ev/",
    "2026-11-09",
    "1d",
    "len(",
    "exists",
    "in",
];

fn seed_texts() -> Vec<Vec<u8>> {
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("seeds/query");
    let mut seeds: Vec<Vec<u8>> = std::fs::read_dir(dir)
        .unwrap()
        .map(|entry| std::fs::read(entry.unwrap().path()).unwrap())
        .collect();
    seeds.sort();
    seeds
}

// Each run tries new inputs: queries assembled from the language's words and
// the seed queries with bytes changed, inserted or removed. A pull request
// runs the default number of cases; the nightly run raises PROPTEST_CASES.
proptest::proptest! {
    #[test]
    fn generated_queries_keep_the_query_properties(
        words in proptest::collection::vec(proptest::sample::select(QUERY_WORDS), 0..40),
    ) {
        query(words.join(" ").as_bytes());
    }

    #[test]
    fn changed_seed_queries_keep_the_query_properties(
        seed in proptest::prelude::any::<proptest::sample::Index>(),
        edits in proptest::collection::vec((proptest::prelude::any::<proptest::sample::Index>(), proptest::num::u8::ANY, 0u8..3), 1..16),
    ) {
        let seeds = seed_texts();
        let mut bytes = seeds[seed.index(seeds.len())].clone();
        for (at, byte, kind) in edits {
            let position = at.index(bytes.len() + 1);
            match kind {
                0 if position < bytes.len() => bytes[position] = byte,
                1 => bytes.insert(position, byte),
                _ if position < bytes.len() => {
                    bytes.remove(position);
                }
                _ => bytes.push(byte),
            }
        }
        query(&bytes);
    }

    #[test]
    fn arbitrary_text_keeps_the_query_properties(text in "\\PC{0,200}") {
        query(text.as_bytes());
    }
}
