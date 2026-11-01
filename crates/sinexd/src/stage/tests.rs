use super::*;
use std::io::Write;
use std::path::Path;

fn time_settings() -> sinex_model::time_settings::TimeSettings {
    sinex_model::time_settings::TimeSettings::new("UTC", 4, 0).expect("fixture day setting")
}

fn request(items: Vec<StagedItem>) -> Request {
    Request {
        items,
        account: Some("owner@example.test".into()),
        export_date: Some("2030-01-02".into()),
        host: "fixture-host".into(),
        zone: Some("UTC".into()),
    }
}

fn detected(path: &Path) -> StagedItem {
    StagedItem {
        path: path.to_path_buf(),
        parser: None,
    }
}

fn named(path: &Path, parser: &str) -> StagedItem {
    StagedItem {
        path: path.to_path_buf(),
        parser: Some(parser.into()),
    }
}

async fn stage(db: &devkit::pg::TestDatabase, store: &Path, items: Vec<StagedItem>) -> Outcome {
    let outcome = local(&db.url(None), store, request(items), time_settings())
        .await
        .unwrap();
    assert!(outcome.failures.is_empty(), "{:?}", outcome.failures);
    outcome
}

fn zip(path: &Path, members: &[(&str, String)]) {
    let mut writer = zip::ZipWriter::new(std::fs::File::create(path).unwrap());
    for (name, body) in members {
        writer
            .start_file(*name, zip::write::SimpleFileOptions::default())
            .unwrap();
        writer.write_all(body.as_bytes()).unwrap();
    }
    writer.finish().unwrap();
}

fn note(title: &str, created: i64) -> String {
    json!({"title": title, "textContent": "synthetic", "createdTimestampUsec": created,
        "userEditedTimestampUsec": created + 1, "isTrashed": false, "isPinned": false,
        "isArchived": false})
    .to_string()
}

fn chrome_history() -> String {
    json!({"Browser History": [
        {"url": "https://example.test/a", "title": "A", "time_usec": 1_565_000_000_000_001i64,
         "page_transition": "LINK"},
        {"url": "https://example.test/b", "title": "B", "time_usec": 1_565_000_000_000_002i64,
         "page_transition": "TYPED"}
    ]})
    .to_string()
}

/// A Takeout export in three parts: Keep notes in the first two (one of
/// them unreadable), Chrome history, members no parser reads, and a part
/// holding only those.
fn multipart_takeout(dir: &Path) -> [PathBuf; 3] {
    let parts = [
        dir.join("takeout-20300101T000000Z-001.zip"),
        dir.join("takeout-20300101T000000Z-002.zip"),
        dir.join("takeout-20300101T000000Z-003.zip"),
    ];
    zip(
        &parts[0],
        &[
            ("Takeout/archive_browser.html", "<html></html>".into()),
            (
                "Takeout/Keep/First.json",
                note("first", 1_739_887_192_500_000),
            ),
            ("Takeout/Keep/Broken.json", r#"{"title":"no time"}"#.into()),
            ("Takeout/Chrome/History.json", chrome_history()),
            ("Takeout/Travel/Trips.json", "{}".into()),
        ],
    );
    zip(
        &parts[1],
        &[(
            "Takeout/Keep/Second.json",
            note("second", 1_739_887_292_500_000),
        )],
    );
    zip(
        &parts[2],
        &[("Takeout/Drive/Document.pdf", "%PDF-1.7 synthetic".into())],
    );
    parts
}

fn files(outcome: &Outcome) -> &Vec<Value> {
    outcome.report["inventory"]["files"].as_array().unwrap()
}

fn accounting(file: &Value, source: &str) -> Accounting {
    serde_json::from_value(file["products"][source]["accounting"].clone())
        .unwrap_or_else(|e| panic!("{source}: {e}: {file}"))
}

/// law: sx.stage.inventory-total
/// law: sx.acct.identity
#[devkit::test]
async fn a_multipart_takeout_is_inventoried_and_accounted(db: devkit::pg::TestDatabase) {
    let dir = tempfile::tempdir().unwrap();
    let export = dir.path().join("export");
    std::fs::create_dir(&export).unwrap();
    let parts = multipart_takeout(&export);
    let store = dir.path().join("store");
    let outcome = stage(&db, &store, vec![detected(&export)]).await;
    assert_eq!(outcome.result, OperationResult::Ok);
    let files = files(&outcome);
    assert_eq!(files.len(), 3);

    // Every member of every part appears exactly once.
    for (file, part) in files.iter().zip(&parts) {
        assert_eq!(file["path"], json!(part));
        let mut archive = zip::ZipArchive::new(std::fs::File::open(part).unwrap()).unwrap();
        let listed: Vec<String> = (0..archive.len())
            .map(|i| archive.by_index(i).unwrap().name().to_owned())
            .collect();
        let inventoried: Vec<String> = file["members"]
            .as_array()
            .unwrap()
            .iter()
            .map(|m| m["name"].as_str().unwrap().to_owned())
            .collect();
        assert_eq!(inventoried, listed);
    }
    let disposition = |file: &Value, name: &str| {
        file["members"]
            .as_array()
            .unwrap()
            .iter()
            .find(|m| m["name"] == name)
            .unwrap()["disposition"]
            .clone()
    };
    assert_eq!(disposition(&files[0], "Takeout/Keep/First.json"), "read");
    assert_eq!(
        disposition(&files[0], "Takeout/Travel/Trips.json"),
        "stored_not_interpreted"
    );
    assert_eq!(files[2]["reading"]["as"], "unparsed");

    // Each product's accounting balances and says what became of its records.
    let keep = accounting(&files[0], "google.keep");
    assert_eq!(keep.balanced(), Ok(()));
    assert_eq!((keep.framed, keep.read, keep.failures), (2, 1, 1));
    assert_eq!((keep.readings, keep.new), (1, 1));
    assert!(keep.first_failure.is_some());
    let chrome = accounting(&files[0], "browser.takeout-chrome-history");
    assert_eq!(chrome.balanced(), Ok(()));
    assert_eq!((chrome.framed, chrome.read, chrome.new), (2, 2, 2));
    let second = accounting(&files[1], "google.keep");
    assert_eq!((second.framed, second.new), (1, 1));
    let total: Accounting =
        serde_json::from_value(outcome.report["counts"]["accounting"].clone()).unwrap();
    assert_eq!(total.balanced(), Ok(()));
    assert_eq!(total.new, 4);
    assert_eq!(outcome.report["counts"]["stored_without_parser"], 1);

    let mut conn = <PgConnection as sqlx::Connection>::connect(&db.url(None))
        .await
        .unwrap();
    // The part no parser reads is kept whole, with no parser.
    let (parser, manifest): (Option<String>, Value) = sqlx::query_as(
        "SELECT m.parser, m.manifest FROM raw.materials m JOIN raw.inputs i ON i.id = m.input_id WHERE i.locator = $1",
    )
    .bind(parts[2].display().to_string())
    .fetch_one(&mut conn)
    .await
    .unwrap();
    assert_eq!(parser, None);
    assert_eq!(manifest["candidates"], json!([]));
    assert_eq!(manifest["account"], "owner@example.test");
    assert_eq!(manifest["export_date"], "2030-01-02");
    // The parts parsers read are kept whole too, as their original archives.
    let originals: i64 = sqlx::query_scalar(
        "SELECT count(DISTINCT m.blob) FROM raw.materials m WHERE m.manifest->>'role' = 'original-archive'",
    )
    .fetch_one(&mut conn)
    .await
    .unwrap();
    assert_eq!(originals, 2);
    // The stage is one operation that ended ok, with its scope as asked.
    let stored = sinex_store::stages::show(&mut conn, outcome.operation)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.result.as_deref(), Some("ok"));
    assert_eq!(stored.actor, "operator");
    assert_eq!(stored.scope["account"], "owner@example.test");
    assert_eq!(stored.inventory, outcome.report["inventory"]);
    assert!(stored.end_seq >= stored.start_seq);
}

/// law: sx.stage.restage-two-materials-one-live-reading
#[devkit::test]
async fn identical_bytes_from_two_paths_are_two_materials_and_one_live_reading(
    db: devkit::pg::TestDatabase,
) {
    let dir = tempfile::tempdir().unwrap();
    let first = dir.path().join("a.zip");
    zip(
        &first,
        &[
            ("Takeout/Keep/One.json", note("one", 1_739_887_192_500_000)),
            ("Takeout/Keep/Two.json", note("two", 1_739_887_192_600_000)),
        ],
    );
    let second = dir.path().join("b.zip");
    std::fs::copy(&first, &second).unwrap();
    let store = dir.path().join("store");
    let outcome = stage(&db, &store, vec![detected(&first)]).await;
    assert_eq!(accounting(&files(&outcome)[0], "google.keep").new, 2);
    let outcome = stage(&db, &store, vec![detected(&second)]).await;
    let again = accounting(&files(&outcome)[0], "google.keep");
    assert_eq!((again.new, again.same), (0, 2));

    let mut conn = <PgConnection as sqlx::Connection>::connect(&db.url(None))
        .await
        .unwrap();
    let originals: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM raw.materials WHERE manifest->>'role' = 'original-archive'",
    )
    .fetch_one(&mut conn)
    .await
    .unwrap();
    assert_eq!(originals, 2);
    let live: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM core.events e WHERE e.event_type <> 'source.coverage' AND e.id IN (SELECT event_id FROM core.live_equivalence UNION ALL SELECT event_id FROM core.live_locations)",
    )
    .fetch_one(&mut conn)
    .await
    .unwrap();
    assert_eq!(live, 2);

    // The same unchanged path staged again is finished: nothing new is read.
    let outcome = stage(&db, &store, vec![detected(&first)]).await;
    let finished = accounting(&files(&outcome)[0], "google.keep");
    assert_eq!((finished.framed, finished.readings), (0, 0));
    let after: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM raw.materials WHERE manifest->>'role' = 'original-archive'",
    )
    .fetch_one(&mut conn)
    .await
    .unwrap();
    assert_eq!(after, 2);
}

/// law: sx.stage.without-parser
#[devkit::test]
async fn a_file_no_parser_recognises_is_kept_byte_for_byte(db: devkit::pg::TestDatabase) {
    let dir = tempfile::tempdir().unwrap();
    let unknown = dir.path().join("unknown.bin");
    let bytes: Vec<u8> = (0..=255u8).cycle().take(70_000).collect();
    std::fs::write(&unknown, &bytes).unwrap();
    let store = dir.path().join("store");
    let first = stage(&db, &store, vec![detected(&unknown)]).await;
    assert_eq!(files(&first)[0]["reading"]["as"], "unparsed");
    let material: uuid::Uuid =
        serde_json::from_value(files(&first)[0]["material"].clone()).unwrap();
    // Staged again, it is a new material of the same input.
    let second = stage(&db, &store, vec![detected(&unknown)]).await;
    let again: uuid::Uuid = serde_json::from_value(files(&second)[0]["material"].clone()).unwrap();
    assert_ne!(material, again);

    let mut conn = <PgConnection as sqlx::Connection>::connect(&db.url(None))
        .await
        .unwrap();
    let rows: Vec<(Option<String>, Option<i32>, String, i64)> = sqlx::query_as(
        "SELECT m.parser, m.parser_version, m.blob, m.length FROM raw.materials m JOIN raw.inputs i ON i.id = m.input_id WHERE i.locator = $1 ORDER BY m.id",
    )
    .bind(unknown.display().to_string())
    .fetch_all(&mut conn)
    .await
    .unwrap();
    assert_eq!(rows.len(), 2);
    for (parser, version, blob, length) in &rows {
        assert_eq!((parser, version), (&None, &None));
        assert_eq!(*length, bytes.len() as i64);
        let blobs = Blobs::open(&store).unwrap();
        assert_eq!(blobs.read(blob).unwrap(), bytes);
    }
    // Replay has nothing to read it with until a parser is assigned.
    let error = sinex_store::material_bytes::reference(&mut conn, MaterialId(material))
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("stored without a parser"),
        "{error}"
    );
}

/// law: sx.stage.shapes
#[devkit::test]
async fn each_staged_shape_makes_its_material_an_operation_and_a_run(db: devkit::pg::TestDatabase) {
    let dir = tempfile::tempdir().unwrap();
    let history = dir.path().join("history");
    std::fs::write(
        &history,
        ": 1600000000:0;synthetic one\n: 1600000001:0;synthetic two\n",
    )
    .unwrap();
    let database = dir.path().join("atuin.db");
    let connection = rusqlite::Connection::open(&database).unwrap();
    connection
        .execute_batch(
            "CREATE TABLE history (id TEXT PRIMARY KEY, timestamp INTEGER, duration INTEGER, exit INTEGER, command TEXT, cwd TEXT, session TEXT, hostname TEXT, deleted_at INTEGER);
             INSERT INTO history VALUES ('a1', 1600000000000000000, 1, 0, 'synthetic', '/', 's', 'h', NULL);",
        )
        .unwrap();
    drop(connection);
    let vault = dir.path().join("vault");
    std::fs::create_dir(&vault).unwrap();
    std::fs::write(vault.join("note.md"), "# Synthetic\n").unwrap();
    let archive = dir.path().join("takeout.zip");
    zip(
        &archive,
        &[("Takeout/Keep/One.json", note("one", 1_739_887_192_500_000))],
    );
    let store = dir.path().join("store");

    let mut operations = Vec::new();
    for (item, source) in [
        (detected(&history), "terminal.zsh-history"),
        (detected(&database), "terminal.atuin-history"),
        (named(&vault, "notes"), "knowledge.notes-vault"),
        (detected(&archive), "google.keep"),
    ] {
        let outcome = stage(&db, &store, vec![item]).await;
        let file = &files(&outcome)[0];
        assert_eq!(file["products"][source]["state"], "read", "{file}");
        assert!(accounting(file, source).new > 0, "{file}");
        operations.push((outcome.operation, source));
    }
    let mut conn = <PgConnection as sqlx::Connection>::connect(&db.url(None))
        .await
        .unwrap();
    for (operation, source) in operations {
        let kind: String = sqlx::query_scalar("SELECT kind FROM core.operations WHERE id = $1")
            .bind(operation.0)
            .fetch_one(&mut conn)
            .await
            .unwrap();
        assert_eq!(kind, "stage");
        let runs: i64 = sqlx::query_scalar("SELECT count(*) FROM core.runs WHERE name = $1")
            .bind(source)
            .fetch_one(&mut conn)
            .await
            .unwrap();
        assert!(runs >= 1, "{source} has no run");
    }
    // A live database is read as rows, never as an image of the file.
    let rows = manifests(&db, &database).await;
    assert!(!rows.is_empty());
    assert!(
        rows.iter()
            .all(|m| m["role"] == "records" && m["table"] == "history"),
        "{rows:?}"
    );
    // A directory is one tar material.
    let tars = manifests(&db, &vault).await;
    assert!(!tars.is_empty());
    assert!(
        tars.iter().all(|m| m["media_type"] == "application/x-tar"),
        "{tars:?}"
    );
    // An archive is kept whole beside its record parts.
    let kept = manifests(&db, &archive).await;
    assert!(
        kept.iter().any(|m| m["role"] == "original-archive"),
        "{kept:?}"
    );
    assert!(kept.iter().any(|m| m["role"] == "records"), "{kept:?}");
}

/// The manifests of the materials staged from `locator`.
async fn manifests(db: &devkit::pg::TestDatabase, locator: &Path) -> Vec<Value> {
    let mut conn = <PgConnection as sqlx::Connection>::connect(&db.url(None))
        .await
        .unwrap();
    sqlx::query_scalar(
        "SELECT m.manifest FROM raw.materials m JOIN raw.inputs i ON i.id = m.input_id
         WHERE i.locator = $1 AND i.kind <> 'source-observation'",
    )
    .bind(locator.display().to_string())
    .fetch_all(&mut conn)
    .await
    .unwrap()
}

#[devkit::test]
fn parsers_pair_with_paths_once_or_one_each() {
    let paths = ["/a".to_owned(), "/b".to_owned()];
    let none = items(&paths, &[]).unwrap();
    assert!(none.iter().all(|i| i.parser.is_none()));
    let one = items(&paths, &["zsh".into()]).unwrap();
    assert!(one.iter().all(|i| i.parser.as_deref() == Some("zsh")));
    let each = items(&paths, &["zsh".into(), "atuin".into()]).unwrap();
    assert_eq!(each[1].parser.as_deref(), Some("atuin"));
    let error = items(&paths, &["a".into(), "b".into(), "c".into()]).unwrap_err();
    assert!(error.contains("3 parsers for 2 paths"), "{error}");
}

#[devkit::test]
fn a_relative_path_is_refused() {
    let error = request(vec![detected(Path::new("relative/export.zip"))])
        .plan()
        .unwrap_err();
    assert!(error.contains("absolute"), "{error}");
}
