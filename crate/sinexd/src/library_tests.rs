//! The media library end to end, against a real database and the real
//! writer: intake, curation, the selection policy, ranking, handoff,
//! previews, imports, and item continuity across renames and edits.
use crate::library::{Lib, dispatch};
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

async fn world() -> World {
    let (db, url, connection) = crate::testdb::fresh().await;
    let (handle, queues) = writer::channels();
    tokio::spawn(writer::run(
        url.clone(),
        connection,
        queues,
        Arc::new(sinex_store::NoDerivations),
    ));
    let conn = <PgConnection as sqlx::Connection>::connect(&url)
        .await
        .unwrap();
    let base = std::env::temp_dir().join(format!("sinexd-library-{}", uuid::Uuid::now_v7()));
    let root = base.join("library");
    std::fs::create_dir_all(root.join("sub")).unwrap();
    World {
        _db: db,
        conn,
        writer: handle,
        blobs: Blobs::open(&base.join("store")).unwrap(),
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
            request,
            "operator",
        )
        .await
    }

    async fn ok(&mut self, request: Request) -> Value {
        self.run(request).await.unwrap_or_else(|e| panic!("{e}"))
    }

    async fn item_at(&mut self, path: &Path) -> String {
        let key = sinex_library::key("h", &path.display().to_string());
        let owners = sinex_library::db::owners(&mut self.conn, &[key])
            .await
            .unwrap();
        assert_eq!(owners.len(), 1, "one item adopts {}", path.display());
        owners[0].item.clone()
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

#[tokio::test]
async fn the_library_end_to_end() {
    let mut w = world().await;
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

    // Adding again changes nothing.
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

    // The library through the query language.
    assert_eq!(rows(&mut w.conn, "items").await, 3);
    assert_eq!(
        rows(&mut w.conn, "media where media_type = \"image/png\"").await,
        2
    );
    assert!(rows(&mut w.conn, "assignments where level = import").await >= 4);

    // Handoff: a verified copy on disk; then the view.
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
    assert_eq!(vb["revision"], 2);
    assert_eq!(vb["revisions"].as_array().unwrap().len(), 2);

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
    let it = sinex_library::db::item(&mut w.conn, &item)
        .await
        .unwrap()
        .unwrap();
    assert!(!w.blobs.contains(&it.content).unwrap());
    let _ = Lib {
        writer: &w.writer,
        blobs: &w.blobs,
        host: "h",
    };
    std::fs::remove_dir_all(w.root.parent().unwrap()).unwrap();
}
