//! The media library end to end, against a real database and the real writer:
//! intake, curation, the selection policy, ranking, opening, previews, imports,
//! and item continuity across renames and edits.
use crate::library::{Library, dispatch};
use crate::writer;
use serde_json::{Value, json};
use sinex_protocol::Request;
use sinex_store::Blobs;
use sqlx::PgConnection;
use std::path::{Path, PathBuf};
use std::sync::Arc;

struct World {
    _db: devkit::pg::TestDatabase,
    conn: PgConnection,
    writer: writer::WriterHandle,
    blobs: Blobs,
    root: PathBuf,
}

async fn world(db: devkit::pg::TestDatabase) -> World {
    let url = db.url(None);
    let mut connection = <sqlx::PgConnection as sqlx::Connection>::connect(&url)
        .await
        .unwrap();
    let base = std::env::temp_dir().join(format!("sinexd-library-{}", uuid::Uuid::now_v7()));
    let blobs = Blobs::open(&base.join("store")).unwrap();
    let (handle, queues) = writer::channels();
    writer::claim(&mut connection).await.unwrap();
    tokio::spawn(writer::run(
        url.clone(),
        connection,
        queues,
        Arc::new(crate::dirty_keys::Registered::new(
            sinex_derivations::keyed(),
        )),
        Some(blobs.clone()),
    ));
    let conn = <PgConnection as sqlx::Connection>::connect(&url)
        .await
        .unwrap();
    let root = base.join("library");
    std::fs::create_dir_all(root.join("sub")).unwrap();
    // Intake records resolved paths; TMPDIR may be a borrowed /proc/fd alias.
    let root = std::fs::canonicalize(root).unwrap();
    World {
        _db: db,
        conn,
        writer: handle,
        blobs,
        root,
    }
}

impl World {
    async fn run(&mut self, request: Request) -> Result<Value, String> {
        dispatch(
            &mut self.conn,
            Some(&self.writer),
            &self.blobs,
            "h",
            &[],
            request,
            "operator",
        )
        .await
    }

    async fn ok(&mut self, request: Request) -> Value {
        let is_add = matches!(request, Request::LibraryAdd { .. });
        let before = self.derived_count().await;
        let answer = self.run(request).await.unwrap_or_else(|e| panic!("{e}"));
        if is_add {
            assert_eq!(
                answer["library_records_written"],
                self.derived_count().await - before
            );
        }
        answer
    }

    async fn derived_count(&mut self) -> i64 {
        sqlx::query_scalar("SELECT count(*) FROM core.events WHERE source = 'library.items'")
            .fetch_one(&mut self.conn)
            .await
            .unwrap()
    }

    async fn item_at(&mut self, path: &Path) -> String {
        let key = sinex_library::copy_key("h", &path.display().to_string());
        let holders = sinex_library::reads::items_at(&mut self.conn, &[key])
            .await
            .unwrap();
        assert_eq!(holders.len(), 1, "one item holds {}", path.display());
        holders[0].item.clone()
    }
}

/// How many rows a query answers.
async fn rows(conn: &mut PgConnection, text: &str) -> usize {
    let ctx = sinex_model::query::Context::new(
        jiff::Timestamp::now(),
        jiff::tz::TimeZone::UTC,
        jiff::civil::Time::constant(4, 0, 0, 0),
    );
    let bound = sinex_model::query::bind(text, &ctx).unwrap_or_else(|e| panic!("{text}: {e:?}"));
    let page = sinex_store::query::Page {
        size: Some(100),
        cursor: None,
    };
    let answer = sinex_store::query::run(conn, text, &bound, &ctx, &page)
        .await
        .unwrap_or_else(|e| panic!("{text}: {e}"));
    serde_json::to_value(answer).unwrap()["rows"]
        .as_array()
        .unwrap()
        .len()
}

fn ffmpeg() -> bool {
    std::process::Command::new("ffmpeg")
        .arg("-version")
        .output()
        .is_ok_and(|o| o.status.success())
}

/// A real image when ffmpeg is installed (so previews can be made), a bare
/// PNG header otherwise.
fn image(path: &Path, colour: &str) {
    if ffmpeg() {
        let ok = std::process::Command::new("ffmpeg")
            .args(["-v", "error", "-y", "-f", "lavfi", "-i"])
            .arg(format!("color=c={colour}:s=320x200"))
            .args(["-frames:v", "1"])
            .arg(path)
            .status()
            .unwrap()
            .success();
        assert!(ok);
    } else {
        let mut b = b"\x89PNG\r\n\x1a\n\0\0\0\rIHDR".to_vec();
        b.extend_from_slice(&320u32.to_be_bytes());
        b.extend_from_slice(&200u32.to_be_bytes());
        b.extend_from_slice(colour.as_bytes());
        std::fs::write(path, b).unwrap();
    }
}

#[devkit::test]
async fn the_library_end_to_end(db: devkit::pg::TestDatabase) {
    let mut w = world(db).await;
    let a = w.root.join("a.png");
    let b = w.root.join("sub/b.png");
    image(&a, "red");
    image(&b, "blue");
    std::fs::write(
        w.root.join("notes.txt"),
        "plain text is stored and described too\n",
    )
    .unwrap();
    // A gallery-dl sidecar beside a.png: read by import, never an item.
    std::fs::write(
        w.root.join("a.png.json"),
        json!({"category": "danbooru", "id": 7, "rating": "s",
               "tag_string_artist": "someone", "tags": ["red", "square"]})
        .to_string(),
    )
    .unwrap();

    let added = w
        .ok(Request::LibraryAdd {
            path: w.root.display().to_string(),
            reference: None,
        })
        .await;
    assert_eq!(added["items_new"], 3, "{added}");
    assert_eq!(added["files_described"], 3);
    let ia = w.item_at(&a).await;
    let ib = w.item_at(&b).await;

    // An unchanged scan keeps the items without writing copy events.
    let again = w
        .ok(Request::LibraryAdd {
            path: w.root.display().to_string(),
            reference: None,
        })
        .await;
    assert_eq!(again["items_new"], 0, "{again}");
    assert_eq!(again["library_records_written"], 0);

    // Curation.
    w.ok(Request::LibraryTag {
        item: ia.clone(),
        tag: "colour:red".into(),
    })
    .await;
    w.ok(Request::LibraryTag {
        item: ia.clone(),
        tag: "temp".into(),
    })
    .await;
    w.ok(Request::LibraryUntag {
        item: ia.clone(),
        tag: "temp".into(),
    })
    .await;
    assert!(
        w.run(Request::LibraryUntag {
            item: ia.clone(),
            tag: "never".into()
        })
        .await
        .is_err()
    );
    w.ok(Request::LibrarySet {
        item: ia.clone(),
        field: "medium".into(),
        value: Some("digital".into()),
        empty: None,
    })
    .await;
    w.ok(Request::LibrarySet {
        item: ia.clone(),
        field: "medium".into(),
        value: Some("render".into()),
        empty: None,
    })
    .await;
    w.ok(Request::LibrarySet {
        item: ib.clone(),
        field: "medium".into(),
        value: None,
        empty: Some(true),
    })
    .await;
    w.ok(Request::LibraryCollect {
        collection: "favourites".into(),
        item: ia.clone(),
    })
    .await;
    w.ok(Request::LibraryCollect {
        collection: "favourites".into(),
        item: ib.clone(),
    })
    .await;
    // The same item twice is two entries.
    w.ok(Request::LibraryCollect {
        collection: "favourites".into(),
        item: ia.clone(),
    })
    .await;
    let fav = w
        .ok(Request::LibraryCollection {
            collection: Some("favourites".into()),
        })
        .await;
    let order: Vec<&str> = fav["entries"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["item"].as_str().unwrap())
        .collect();
    assert_eq!(order, [ia.as_str(), ib.as_str(), ia.as_str()]);
    w.ok(Request::LibraryUncollect {
        collection: "favourites".into(),
        item: ib.clone(),
    })
    .await;
    let fav = w
        .ok(Request::LibraryCollection {
            collection: Some("favourites".into()),
        })
        .await;
    assert_eq!(fav["entries"].as_array().unwrap().len(), 2);

    w.ok(Request::LibraryRate {
        item: ia.clone(),
        value: "4".into(),
        scale: None,
        of: None,
    })
    .await;
    assert!(
        w.run(Request::LibraryRate {
            item: ia.clone(),
            value: "9".into(),
            scale: None,
            of: None
        })
        .await
        .is_err()
    );
    w.ok(Request::LibraryCompare {
        winner: ia.clone(),
        loser: ib.clone(),
        scale: None,
    })
    .await;
    w.ok(Request::LibraryCompare {
        winner: ia.clone(),
        loser: ib.clone(),
        scale: None,
    })
    .await;
    let rank = w.ok(Request::LibraryRank { scale: None }).await;
    assert_eq!(rank["ranking"][0]["item"], json!(ia));
    assert_eq!(rank["comparisons"], 2);

    // Import: the sidecar's values arrive at the import level; the
    // operator's `-tag` hides one of them.
    let imported = w
        .ok(Request::LibraryImport {
            path: w.root.display().to_string(),
        })
        .await;
    assert_eq!(imported["files_with_sidecars_matched"], 1, "{imported}");
    w.ok(Request::LibraryTag {
        item: ia.clone(),
        tag: "-square".into(),
    })
    .await;

    let view = w.ok(Request::LibraryItem { item: ia.clone() }).await;
    let tags: Vec<(String, String)> = view["tags"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| {
            (
                t["tag"].as_str().unwrap().to_owned(),
                t["level"].as_str().unwrap().to_owned(),
            )
        })
        .collect();
    assert!(
        tags.contains(&("colour:red".into(), "override".into())),
        "{tags:?}"
    );
    assert!(
        tags.contains(&("artist:someone".into(), "import".into())),
        "{tags:?}"
    );
    assert!(tags.contains(&("red".into(), "import".into())));
    assert!(!tags.iter().any(|(t, _)| t == "square" || t == "temp"));
    assert_eq!(view["properties"]["medium"]["value"], "render");
    assert_eq!(view["properties"]["content_rating"]["value"], "general");
    assert_eq!(view["ratings"][0]["value"], 4.0);
    assert_eq!(view["collections"].as_array().unwrap().len(), 2);
    assert_eq!(view["copies"][0]["present"], true);
    let vb = w.ok(Request::LibraryItem { item: ib.clone() }).await;
    assert_eq!(vb["properties"]["medium"]["state"], "empty");

    // Items are derived once; media keeps both scans of each image.
    assert_eq!(rows(&mut w.conn, "items").await, 3);
    assert_eq!(
        rows(
            &mut w.conn,
            &format!(
                "items where revision = \"{}\"",
                view["revision"].as_str().unwrap()
            )
        )
        .await,
        1
    );
    assert_eq!(
        rows(&mut w.conn, "media where media_type = \"image/png\"").await,
        2
    );
    assert!(rows(&mut w.conn, "assignments where level = import").await >= 4);

    // Opening: a verified copy on disk; then the view.
    let open = w
        .ok(Request::LibraryOpen {
            item: ia.clone(),
            rendition: None,
        })
        .await;
    assert_eq!(open["path"], json!(a.display().to_string()));
    assert_eq!(open["verified_copy_on_disk"], true);
    w.ok(Request::LibraryViewed {
        item: ia.clone(),
        seconds: Some("12.5".into()),
        player: Some("imv".into()),
    })
    .await;

    if ffmpeg() {
        let p = w.ok(Request::LibraryPreview { item: ia.clone() }).await;
        let preview = p["content"].as_str().unwrap();
        assert!(w.blobs.contains(preview).unwrap());
        let citations: i64 =
            sqlx::query_scalar("SELECT count(*) FROM core.event_blobs WHERE blob=$1")
                .bind(preview)
                .fetch_one(&mut w.conn)
                .await
                .unwrap();
        assert_eq!(citations, 1, "rendered preview is cited once");
        assert!(p["width"].as_u64().unwrap() <= 512);
        let view = w.ok(Request::LibraryItem { item: ia.clone() }).await;
        assert!(
            view["renditions"]
                .as_array()
                .unwrap()
                .iter()
                .any(|r| r["role"] == "preview")
        );
    }

    // Continuity: a rename keeps the item and its curation; an edit in place
    // is a new revision; a copy elsewhere is a different item.
    // The sidecar has been imported; without a.png beside it, it would be
    // an ordinary file of its own.
    std::fs::remove_file(w.root.join("a.png.json")).unwrap();
    let moved = w.root.join("sub/a-renamed.png");
    std::fs::rename(&a, &moved).unwrap();
    {
        use std::io::Write;
        let mut f = std::fs::OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(&b)
            .unwrap();
        f.write_all(b"\x89PNG\r\n\x1a\n edited in place").unwrap();
    }
    std::fs::copy(&moved, w.root.join("copy.png")).unwrap();
    let third = w
        .ok(Request::LibraryAdd {
            path: w.root.display().to_string(),
            reference: None,
        })
        .await;
    assert_eq!(third["items_moved"], 1, "{third}");
    assert_eq!(third["item_revisions"], 1, "{third}");
    assert_eq!(third["items_new"], 1, "{third}");
    assert_eq!(w.item_at(&moved).await, ia);
    assert_ne!(w.item_at(&w.root.join("copy.png")).await, ia);
    let view = w.ok(Request::LibraryItem { item: ia.clone() }).await;
    assert!(
        view["tags"]
            .as_array()
            .unwrap()
            .iter()
            .any(|t| t["tag"] == "colour:red")
    );
    let vb = w.ok(Request::LibraryItem { item: ib.clone() }).await;
    assert_eq!(vb["revision"], vb["content"]);
    let old = vb["renditions"]
        .as_array()
        .unwrap()
        .iter()
        .find(|rendition| {
            rendition["role"] == "original" && rendition["revision"] != vb["revision"]
        })
        .unwrap();
    let opened = w
        .ok(Request::LibraryOpen {
            item: ib.clone(),
            rendition: Some(old["rendition"].as_str().unwrap().into()),
        })
        .await;
    assert_eq!(opened["revision"], old["revision"]);
    assert_eq!(opened["content"], old["content"]);
    let original_bytes = std::fs::read(opened["path"].as_str().unwrap()).unwrap();
    std::fs::write(&b, original_bytes).unwrap();
    let returned = w
        .ok(Request::LibraryAdd {
            path: w.root.display().to_string(),
            reference: None,
        })
        .await;
    assert_eq!(returned["item_revisions"], 1);
    let returned = w.ok(Request::LibraryItem { item: ib.clone() }).await;
    let history = returned["revisions"].as_array().unwrap();
    assert_eq!(history.len(), 3);
    assert_eq!(history[0]["revision"], history[2]["revision"]);
    assert_eq!(history[2]["replaces"], history[1]["revision"]);
    assert_eq!(returned["revision"], old["revision"]);
    std::fs::remove_file(opened["path"].as_str().unwrap()).unwrap();

    // Bytes gone from disk are still openable from the content store.
    std::fs::remove_file(&moved).unwrap();
    std::fs::remove_file(w.root.join("copy.png")).unwrap();
    let open = w
        .ok(Request::LibraryOpen {
            item: ia.clone(),
            rendition: None,
        })
        .await;
    assert_eq!(open["verified_copy_on_disk"], false);
    let path = PathBuf::from(open["path"].as_str().unwrap());
    let bytes = std::fs::read(&path).unwrap();
    assert_eq!(
        format!("blake3:{}", blake3::hash(&bytes).to_hex()),
        open["content"].as_str().unwrap()
    );
    let _ = std::fs::remove_file(path);

    // Indexing by reference stores no bytes.
    let other = w.root.parent().unwrap().join("by-reference");
    std::fs::create_dir_all(&other).unwrap();
    std::fs::write(other.join("big.bin"), vec![7u8; 4096]).unwrap();
    w.ok(Request::LibraryAdd {
        path: other.display().to_string(),
        reference: Some(true),
    })
    .await;
    let item = w.item_at(&other.join("big.bin")).await;
    let it = sinex_library::reads::item(&mut w.conn, &item)
        .await
        .unwrap()
        .unwrap();
    assert!(!w.blobs.contains(&it.content).unwrap());
    let _ = Library {
        writer: &w.writer,
        blobs: &w.blobs,
        host: "h",
        roots: &[],
    };
    std::fs::remove_dir_all(w.root.parent().unwrap()).unwrap();
}

/// The live library copy at `path`: its event and the bytes it names.
async fn copy(conn: &mut PgConnection, path: &Path) -> (uuid::Uuid, String) {
    let row: (uuid::Uuid, String) = sqlx::query_as(
        "SELECT e.id, e.payload->>'content' FROM core.events e
         JOIN core.live_equivalence l ON l.event_id = e.id
         WHERE e.event_type = 'library.copy' AND e.payload->>'path' = $1 ORDER BY e.commit_seq DESC LIMIT 1",
    )
    .bind(path.display().to_string())
    .fetch_one(conn)
    .await
    .unwrap();
    row
}

async fn blob_row(conn: &mut PgConnection, blob: &str) -> bool {
    sqlx::query_scalar::<_, bool>("SELECT EXISTS (SELECT 1 FROM core.blobs WHERE hash = $1)")
        .bind(blob)
        .fetch_one(conn)
        .await
        .unwrap()
}

/// A library scan reads its files again, so excising one of its records is
/// refused before anything changes.
#[devkit::test]
async fn excision_of_a_scanned_file_is_refused(db: devkit::pg::TestDatabase) {
    let mut w = world(db).await;
    let file = w.root.join("c.png");
    image(&file, "blue");
    w.ok(Request::LibraryAdd {
        path: w.root.display().to_string(),
        reference: None,
    })
    .await;
    let (event, bytes) = copy(&mut w.conn, &file).await;
    let refused = crate::excision::excise(
        &mut w.conn,
        &w.blobs.clone(),
        &w.writer,
        sinex_store::excision::Target::Event(sinex_model::EventId(event)),
        "operator",
    )
    .await
    .unwrap_err();
    assert!(refused.contains("cannot be excised"), "{refused}");
    assert!(w.blobs.contains(&bytes).unwrap());
    assert!(blob_row(&mut w.conn, &bytes).await);
    let left: i64 = sqlx::query_scalar("SELECT count(*) FROM core.events WHERE id = $1")
        .bind(event)
        .fetch_one(&mut w.conn)
        .await
        .unwrap();
    assert_eq!(left, 1);
}

#[devkit::test]
async fn library_add_through_an_alias_keeps_one_copy(db: devkit::pg::TestDatabase) {
    let mut w = world(db).await;
    let file = w.root.join("note.txt");
    std::fs::write(&file, "synthetic library content").unwrap();
    let alias = w.root.parent().unwrap().join("alias");
    std::os::unix::fs::symlink(&w.root, &alias).unwrap();
    let added = w
        .ok(Request::LibraryAdd {
            path: alias.display().to_string(),
            reference: None,
        })
        .await;
    assert_eq!(added["items_new"], 1);
    assert_eq!(added["files_described"], 1);
    assert_eq!(added["root"], w.root.display().to_string());
    let first_item = w.item_at(&file).await;
    let (_, blob) = copy(&mut w.conn, &file).await;
    assert_eq!(w.blobs.read(&blob).unwrap(), b"synthetic library content");
    for _ in 0..3 {
        let again = w
            .ok(Request::LibraryAdd {
                path: w.root.display().to_string(),
                reference: None,
            })
            .await;
        assert_eq!(again["items_new"], 0);
        assert_eq!(again["files_described"], 0);
        assert_eq!(again["copies_admitted"], 0);
        assert_eq!(again["library_records_written"], 0);
        assert_eq!(w.item_at(&file).await, first_item);
    }
    let count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM core.live_equivalence WHERE event_type='library.copy'",
    )
    .fetch_one(&mut w.conn)
    .await
    .unwrap();
    assert_eq!(count, 1);
    std::fs::remove_dir_all(w.root.parent().unwrap()).unwrap();
}

#[devkit::test]
async fn hiding_scans_recomputes_items_and_preview_renditions(db: devkit::pg::TestDatabase) {
    use sinex_model::{ArchiveRequest, Batch, BatchKind, EventId, OperationId, RunId};
    let mut w = world(db).await;
    let file = w.root.join("a.png");
    image(&file, "red");
    let add = Request::LibraryAdd {
        path: w.root.display().to_string(),
        reference: None,
    };
    w.ok(add.clone()).await;
    let item = w.item_at(&file).await;
    let (first, _) = copy(&mut w.conn, &file).await;
    // Changed metadata supplies a second observation of the same revision.
    let bytes = std::fs::read(&file).unwrap();
    std::fs::remove_file(&file).unwrap();
    std::fs::write(&file, bytes).unwrap();
    w.ok(add).await;
    let (second, _) = copy(&mut w.conn, &file).await;
    assert_ne!(first, second, "changed file facts supply new evidence");
    if ffmpeg() {
        w.ok(Request::LibraryPreview { item: item.clone() }).await;
    }
    let read = <PgConnection as sqlx::Connection>::connect(&w._db.url(None))
        .await
        .unwrap();
    let mut runner = crate::derivation_runner::DerivationRunner::start(
        read,
        w.writer.clone(),
        vec![Box::new(sinex_derivations::library::LibraryItems)],
        vec![],
        "h",
    )
    .await
    .unwrap();
    runner.catch_up().await.unwrap();
    for (id, visible) in [(first, true), (second, false)] {
        w.writer
            .submit(
                writer::Priority::Live,
                Batch {
                    run: RunId::new(),
                    kind: BatchKind::Api {
                        actor: "operator".into(),
                    },
                    records: vec![],
                    archive: vec![ArchiveRequest::Hide {
                        target: EventId(id),
                        operation: OperationId::new(),
                    }],
                },
            )
            .await
            .unwrap();
        runner.catch_up().await.unwrap();
        assert_eq!(
            sinex_library::reads::item(&mut w.conn, &item)
                .await
                .unwrap()
                .is_some(),
            visible
        );
        let renditions = sinex_library::reads::renditions(&mut w.conn, &item)
            .await
            .unwrap();
        assert_eq!(!renditions.is_empty(), visible);
        if visible && ffmpeg() {
            assert!(renditions.iter().any(|rendition| rendition.role
                == sinex_model::event_types::library::RenditionRole::Preview));
        }
    }
    w.writer
        .submit(
            writer::Priority::Live,
            Batch {
                run: RunId::new(),
                kind: BatchKind::Api {
                    actor: "operator".into(),
                },
                records: vec![],
                archive: vec![ArchiveRequest::Unhide {
                    target: EventId(second),
                    operation: OperationId::new(),
                }],
            },
        )
        .await
        .unwrap();
    runner.catch_up().await.unwrap();
    assert!(
        sinex_library::reads::item(&mut w.conn, &item)
            .await
            .unwrap()
            .is_some()
    );
    std::fs::remove_dir_all(w.root.parent().unwrap()).unwrap();
}

#[devkit::test]
async fn a_returning_path_continues_its_item_and_curation(db: devkit::pg::TestDatabase) {
    let mut w = world(db).await;
    let file = w.root.join("a.txt");
    std::fs::write(&file, "first revision").unwrap();
    let add = Request::LibraryAdd {
        path: w.root.display().to_string(),
        reference: None,
    };
    w.ok(add.clone()).await;
    let item = w.item_at(&file).await;
    w.ok(Request::LibraryTag {
        item: item.clone(),
        tag: "kept".into(),
    })
    .await;
    let moved = w.root.join("b.txt");
    std::fs::rename(&file, &moved).unwrap();
    w.ok(add.clone()).await;
    std::fs::write(&moved, "second revision").unwrap();
    w.ok(add.clone()).await;
    std::fs::remove_file(&moved).unwrap();
    w.ok(add.clone()).await;
    std::fs::write(&file, "first revision").unwrap();
    let returned = w.ok(add).await;
    assert_eq!(returned["items_new"], 0);
    assert_eq!(returned["item_revisions"], 1);
    assert_eq!(returned["items_moved"], 0);
    assert_eq!(w.item_at(&file).await, item);
    let view = w.ok(Request::LibraryItem { item }).await;
    assert_eq!(view["revision"], view["content"]);
    assert_eq!(view["revisions"].as_array().unwrap().len(), 3);
    let revisions = view["revisions"].as_array().unwrap();
    assert_eq!(revisions[0]["revision"], revisions[2]["revision"]);
    assert_eq!(revisions[2]["replaces"], revisions[1]["revision"]);
    assert!(
        view["tags"]
            .as_array()
            .unwrap()
            .iter()
            .any(|tag| tag["tag"] == "kept")
    );
    std::fs::remove_dir_all(w.root.parent().unwrap()).unwrap();
}

#[devkit::test]
async fn ambiguous_moves_preserve_both_items_curation(db: devkit::pg::TestDatabase) {
    let mut w = world(db).await;
    let a = w.root.join("a.txt");
    let b = w.root.join("b.txt");
    for path in [&a, &b] {
        std::fs::write(path, "equal bytes").unwrap();
    }
    let add = Request::LibraryAdd {
        path: w.root.display().to_string(),
        reference: None,
    };
    w.ok(add.clone()).await;
    let first = w.item_at(&a).await;
    let second = w.item_at(&b).await;
    for (item, tag) in [(&first, "first"), (&second, "second")] {
        w.ok(Request::LibraryTag {
            item: item.clone(),
            tag: tag.into(),
        })
        .await;
    }
    // Create both replacements before removing the originals to ensure
    // their device and inode cannot match either original.
    let c = w.root.join("c.txt");
    let d = w.root.join("d.txt");
    std::fs::copy(&a, &c).unwrap();
    std::fs::copy(&b, &d).unwrap();
    std::fs::remove_file(a).unwrap();
    std::fs::remove_file(b).unwrap();
    let added = w.ok(add).await;
    assert_eq!(added["items_new"], 2);
    assert_eq!(added["items_moved"], 0);
    for path in [&c, &d] {
        let item = w.item_at(path).await;
        assert_ne!(item, first);
        assert_ne!(item, second);
        let view = w.ok(Request::LibraryItem { item }).await;
        assert!(view["tags"].as_array().unwrap().is_empty());
    }
    for (item, tag) in [(first, "first"), (second, "second")] {
        let view = w.ok(Request::LibraryItem { item }).await;
        assert!(
            view["tags"]
                .as_array()
                .unwrap()
                .iter()
                .any(|value| value["tag"] == tag)
        );
        assert!(view["copies"].as_array().unwrap().is_empty());
    }
    std::fs::remove_dir_all(w.root.parent().unwrap()).unwrap();
}

#[devkit::test]
async fn hiding_the_latest_change_falls_back_to_the_previous_copy(db: devkit::pg::TestDatabase) {
    use sinex_model::{ArchiveRequest, Batch, BatchKind, EventId, OperationId, RunId};
    let mut w = world(db).await;
    let file = w.root.join("a.txt");
    let add = Request::LibraryAdd {
        path: w.root.display().to_string(),
        reference: None,
    };
    std::fs::write(&file, "first bytes").unwrap();
    w.ok(add.clone()).await;
    let item = w.item_at(&file).await;
    let (_, first_content) = copy(&mut w.conn, &file).await;
    std::fs::write(&file, "second, changed bytes").unwrap();
    w.ok(add).await;
    let (latest, second_content) = copy(&mut w.conn, &file).await;
    assert_ne!(first_content, second_content);
    assert!(
        sinex_library::reads::copies_of(&mut w.conn, &first_content)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        sinex_library::reads::copies_of(&mut w.conn, &second_content)
            .await
            .unwrap()
            .len(),
        1
    );
    let read = <PgConnection as sqlx::Connection>::connect(&w._db.url(None))
        .await
        .unwrap();
    let mut runner = crate::derivation_runner::DerivationRunner::start(
        read,
        w.writer.clone(),
        vec![Box::new(sinex_derivations::library::LibraryItems)],
        vec![],
        "h",
    )
    .await
    .unwrap();
    runner.catch_up().await.unwrap();
    for (change, expected) in [
        (
            ArchiveRequest::Hide {
                target: EventId(latest),
                operation: OperationId::new(),
            },
            first_content,
        ),
        (
            ArchiveRequest::Unhide {
                target: EventId(latest),
                operation: OperationId::new(),
            },
            second_content,
        ),
    ] {
        w.writer
            .submit(
                writer::Priority::Live,
                Batch {
                    run: RunId::new(),
                    kind: BatchKind::Api {
                        actor: "operator".into(),
                    },
                    records: vec![],
                    archive: vec![change],
                },
            )
            .await
            .unwrap();
        runner.catch_up().await.unwrap();
        let view = w.ok(Request::LibraryItem { item: item.clone() }).await;
        assert_eq!(view["content"], expected);
        assert_eq!(view["revision"], expected);
    }
    std::fs::remove_dir_all(w.root.parent().unwrap()).unwrap();
}

#[devkit::test]
async fn an_unchanged_rescan_counts_each_item_once_after_hiding_a_move_removal(
    db: devkit::pg::TestDatabase,
) {
    use sinex_model::{ArchiveRequest, Batch, BatchKind, EventId, OperationId, RunId};
    let mut w = world(db).await;
    let original = w.root.join("a.txt");
    let moved = w.root.join("b.txt");
    let add = Request::LibraryAdd {
        path: w.root.display().to_string(),
        reference: None,
    };
    std::fs::write(&original, "first bytes").unwrap();
    w.ok(add.clone()).await;
    let item = w.item_at(&original).await;
    std::fs::rename(&original, &moved).unwrap();
    w.ok(add.clone()).await;
    std::fs::write(&moved, "second changed bytes").unwrap();
    w.ok(add.clone()).await;
    let removed: uuid::Uuid = sqlx::query_scalar(
        "SELECT id FROM core.events WHERE event_type = 'library.copy'
         AND payload->>'path' = $1 AND record_state = 'removed' ORDER BY commit_seq DESC LIMIT 1",
    )
    .bind(original.display().to_string())
    .fetch_one(&mut w.conn)
    .await
    .unwrap();
    w.writer
        .submit(
            writer::Priority::Live,
            Batch {
                run: RunId::new(),
                kind: BatchKind::Api {
                    actor: "operator".into(),
                },
                records: vec![],
                archive: vec![ArchiveRequest::Hide {
                    target: EventId(removed),
                    operation: OperationId::new(),
                }],
            },
        )
        .await
        .unwrap();
    let unchanged = w.ok(add).await;
    for count in [
        "files_described",
        "copies_admitted",
        "items_new",
        "items_moved",
        "item_revisions",
    ] {
        assert_eq!(unchanged[count], 0, "{count}: {unchanged}");
    }
    let view = w.ok(Request::LibraryItem { item }).await;
    assert_eq!(view["copies"].as_array().unwrap().len(), 2);
    assert_eq!(view["revisions"].as_array().unwrap().len(), 2);
    std::fs::remove_dir_all(w.root.parent().unwrap()).unwrap();
}

#[devkit::test]
async fn a_new_file_at_a_moved_items_old_path_keeps_curation_separate(
    db: devkit::pg::TestDatabase,
) {
    let mut w = world(db).await;
    let a = w.root.join("a.txt");
    let b = w.root.join("b.txt");
    std::fs::write(&a, "first item").unwrap();
    let add = Request::LibraryAdd {
        path: w.root.display().to_string(),
        reference: None,
    };
    w.ok(add.clone()).await;
    let original = w.item_at(&a).await;
    w.ok(Request::LibraryTag {
        item: original.clone(),
        tag: "original".into(),
    })
    .await;
    std::fs::rename(&a, &b).unwrap();
    w.ok(add.clone()).await;
    assert_eq!(w.item_at(&b).await, original);
    std::fs::write(&a, "different item").unwrap();
    let added = w.ok(add.clone()).await;
    assert_eq!(added["items_new"], 1);
    let new = w.item_at(&a).await;
    assert_ne!(original, new);
    w.ok(Request::LibraryTag {
        item: new.clone(),
        tag: "new".into(),
    })
    .await;
    let old_view = w
        .ok(Request::LibraryItem {
            item: original.clone(),
        })
        .await;
    let new_view = w.ok(Request::LibraryItem { item: new.clone() }).await;
    assert_eq!(old_view["tags"][0]["tag"], "original");
    assert_eq!(new_view["tags"][0]["tag"], "new");
    assert_eq!(old_view["tags"].as_array().unwrap().len(), 1);
    assert_eq!(new_view["tags"].as_array().unwrap().len(), 1);
    assert_eq!(w.ok(add).await["items_new"], 0);
    assert_eq!(w.item_at(&a).await, new);
    assert_eq!(w.item_at(&b).await, original);
}

#[devkit::test]
async fn selections_inside_a_configured_root_use_one_item_and_preserve_other_paths(
    db: devkit::pg::TestDatabase,
) {
    let mut w = world(db).await;
    let selected = w.root.join("sub/x.txt");
    let outside = w.root.join("outside.txt");
    std::fs::write(&selected, "selected").unwrap();
    std::fs::write(&outside, "outside").unwrap();
    let roots = [w.root.clone()];
    let result = dispatch(
        &mut w.conn,
        Some(&w.writer),
        &w.blobs,
        "h",
        &roots,
        Request::LibraryAdd {
            path: selected.display().to_string(),
            reference: None,
        },
        "operator",
    )
    .await
    .unwrap();
    assert_eq!(result["root"], w.root.display().to_string());
    assert_eq!(result["files_described"], 1);
    let item = w.item_at(&selected).await;
    let full = w
        .ok(Request::LibraryAdd {
            path: w.root.display().to_string(),
            reference: None,
        })
        .await;
    assert_eq!(full["items_new"], 1, "only the outside file is new");
    assert_eq!(w.item_at(&selected).await, item);
    let other = w.item_at(&outside).await;
    // Stored bindings also keep the root for later command selections.
    std::fs::write(w.root.join("sub/another.txt"), "another").unwrap();
    let partial = w
        .ok(Request::LibraryAdd {
            path: w.root.join("sub").display().to_string(),
            reference: None,
        })
        .await;
    assert_eq!(partial["root"], w.root.display().to_string());
    assert_eq!(partial["files_described"], 1);
    assert_eq!(w.item_at(&selected).await, item);
    assert_eq!(w.item_at(&outside).await, other);
    assert_eq!(rows(&mut w.conn, "items").await, 3);
}

#[devkit::test]
async fn one_to_many_move_ambiguity_is_visible_without_transferring_curation(
    db: devkit::pg::TestDatabase,
) {
    let mut w = world(db).await;
    let old_path = w.root.join("old.txt");
    std::fs::write(&old_path, "equal bytes").unwrap();
    let add = Request::LibraryAdd {
        path: w.root.display().to_string(),
        reference: None,
    };
    w.ok(add.clone()).await;
    let old = w.item_at(&old_path).await;
    w.ok(Request::LibraryTag {
        item: old.clone(),
        tag: "kept".into(),
    })
    .await;
    for path in ["a.txt", "b.txt"] {
        std::fs::copy(&old_path, w.root.join(path)).unwrap();
    }
    std::fs::remove_file(&old_path).unwrap();
    let added = w.ok(add).await;
    assert_eq!(added["items_new"], 2);
    assert_eq!(added["items_moved"], 0);
    for path in ["a.txt", "b.txt"] {
        let item = w.item_at(&w.root.join(path)).await;
        assert_ne!(item, old);
        let view = w.ok(Request::LibraryItem { item }).await;
        assert!(view["tags"].as_array().unwrap().is_empty());
        assert_eq!(
            view["copies"][0]["move_candidates"]
                .as_array()
                .unwrap()
                .len(),
            3
        );
    }
    let view = w.ok(Request::LibraryItem { item: old }).await;
    assert_eq!(view["tags"][0]["tag"], "kept");
}

#[devkit::test]
async fn a_partial_library_add_reports_failures_and_admits_readable_files(
    db: devkit::pg::TestDatabase,
) {
    use std::os::unix::fs::PermissionsExt;
    let mut w = world(db).await;
    let blocked = w.root.join("sub");
    std::fs::write(blocked.join("known.txt"), "known").unwrap();
    let add = Request::LibraryAdd {
        path: w.root.display().to_string(),
        reference: None,
    };
    w.ok(add.clone()).await;
    let known = w.item_at(&blocked.join("known.txt")).await;
    std::fs::write(w.root.join("new.txt"), "new").unwrap();
    std::fs::set_permissions(&blocked, std::fs::Permissions::from_mode(0o000)).unwrap();
    let result = w.run(add).await;
    std::fs::set_permissions(&blocked, std::fs::Permissions::from_mode(0o700)).unwrap();
    let result = result.unwrap();
    assert_eq!(result["scan_failures"], 1);
    assert_eq!(result["items_new"], 1);
    assert_eq!(w.item_at(&blocked.join("known.txt")).await, known);
}

#[devkit::test]
#[ignore = "paired 10,000-file library-add measurement"]
async fn library_add_ten_thousand_files(db: devkit::pg::TestDatabase) {
    let mut w = world(db).await;
    for index in 0..10_000 {
        std::fs::write(
            w.root.join(format!("file-{index:05}.txt")),
            format!("synthetic file {index:05}\n"),
        )
        .unwrap();
    }
    let started = std::time::Instant::now();
    let result = w
        .ok(Request::LibraryAdd {
            path: w.root.display().to_string(),
            reference: Some(true),
        })
        .await;
    let seconds = started.elapsed().as_secs_f64();
    assert_eq!(result["items_new"], 10_000);
    assert_eq!(result["copies_admitted"], 10_000);
    assert_eq!(result["library_records_written"], 20_000);
    eprintln!(
        "LIBRARY_ADD_MEASUREMENT {}",
        json!({"files": 10_000, "reference": true, "seconds": seconds, "result": result})
    );
}
