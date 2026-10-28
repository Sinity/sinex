//! Excision as the API runs it: planned and checked here, applied by the
//! writer, on a real database.
use super::*;
use crate::reader_context::ReaderContext;
use crate::sealing::Sink;
use crate::writer::Priority;
use serde_json::Value;
use sinex_model::event_types::machine::FileChanged;
use sinex_model::{
    Batch, BatchKind, Class, Event, EventId, MaterialId, Origin, ReadingOrder, RecordLocation,
    RecordOutcome, RunId, Timestamp,
};
use sinex_source_core::drivers::{self, live_stream};
use sinex_source_core::{Binding, Limits, Settings, SourceSpec};
use sinex_sources::sources::clipboard;
use sinex_store::positions;
use std::io::Write;

/// Register `bytes` as material `id` of a binding of `spec`: the rows a
/// source batch leaves, for tests that admit events directly.
pub(crate) async fn register(
    conn: &mut PgConnection,
    blobs: &Blobs,
    spec: &SourceSpec,
    id: MaterialId,
    bytes: &[u8],
) {
    let blob = blobs.put(bytes).unwrap();
    let binding = format!("{}:synthetic@h", spec.name);
    sqlx::query(
        "INSERT INTO core.blobs (hash, size, status) VALUES ($1, $2, 'present') ON CONFLICT DO NOTHING",
    )
    .bind(&blob)
    .bind(bytes.len() as i64)
    .execute(&mut *conn)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO raw.bindings (binding, source, input, settings) VALUES ($1, $2, 'synthetic', '{}') ON CONFLICT DO NOTHING",
    )
    .bind(&binding)
    .bind(spec.name)
    .execute(&mut *conn)
    .await
    .unwrap();
    let input: uuid::Uuid = sqlx::query_scalar(
        "INSERT INTO raw.inputs (id, binding, kind, locator, host) VALUES ($1, $2, 'file', 'synthetic', 'h')
         ON CONFLICT (binding, locator, host) DO UPDATE SET kind = EXCLUDED.kind RETURNING id",
    )
    .bind(uuid::Uuid::now_v7())
    .bind(&binding)
    .fetch_one(&mut *conn)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO raw.materials (id, input_id, blob, length, manifest, parser, parser_version)
         VALUES ($1, $2, $3, $4, '{}', $5, $6)",
    )
    .bind(id.0)
    .bind(input)
    .bind(&blob)
    .bind(bytes.len() as i64)
    .bind(spec.parser)
    .bind(spec.parser_version as i32)
    .execute(&mut *conn)
    .await
    .unwrap();
}

/// Excise `target` in one transaction on `conn`, as the writer does, for
/// tests of what consumers see afterwards.
pub(crate) async fn excise_directly(
    conn: &mut PgConnection,
    blobs: &Blobs,
    target: Target,
) -> OperationId {
    use sqlx::Connection;
    let plan = plan(conn, blobs, target).await.unwrap();
    let operation = OperationId::new();
    let mut tx = conn.begin().await.unwrap();
    excision::apply(
        &mut tx,
        target,
        &plan,
        operation,
        "operator",
        &sinex_store::NoDerivations,
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
    excision::finish_deletions(conn, blobs).await.unwrap();
    operation
}

/// A clipboard spool read through its live-stream driver.
struct Clipboard {
    _db: devkit::pg::TestDatabase,
    _dir: tempfile::TempDir,
    ctx: ReaderContext,
    binding: Binding,
    copies: i64,
}

impl Clipboard {
    async fn new(db: devkit::pg::TestDatabase) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let mut ctx = ReaderContext::open(&db.url(None), &dir.path().join("store"), sinex_model::time_settings::TimeSettings::operator_default())
            .await
            .unwrap();
        let spool = dir.path().join("spool");
        std::fs::create_dir_all(&spool).unwrap();
        let binding = Binding::of(&clipboard::SPEC, &spool, Settings::new("h"));
        crate::backfill::record(&mut ctx.read_conn, &binding)
            .await
            .unwrap();
        Self {
            _db: db,
            _dir: dir,
            ctx,
            binding,
            copies: 0,
        }
    }

    /// Capture `texts` as copies and read them in.
    async fn copy(&mut self, texts: &[&str]) {
        let mut segment = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.binding.input.join("000000000000.ndjson"))
            .unwrap();
        for text in texts {
            self.copies += 1;
            let line = clipboard::frame("clipboard", &["text/plain".into()], text.as_bytes());
            let received = Timestamp::from_unix_secs(1_700_000_000 + self.copies).unwrap();
            segment
                .write_all(&live_stream::frame(received, &line))
                .unwrap();
        }
        let driver = clipboard::driver(self.binding.input.clone(), "h".into(), "clipboard");
        let source = clipboard::source();
        let sink = Sink(self.ctx.blobs.clone());
        loop {
            let (cursor, carry) = positions::load(&mut self.ctx.read_conn, &self.binding.name)
                .await
                .unwrap()
                .unwrap_or((Value::Null, Value::Null));
            let acquired = driver.poll(&cursor).unwrap();
            let more = acquired.material.is_some();
            for batch in drivers::batches(
                &source,
                &self.binding.name,
                RunId::new(),
                acquired,
                carry,
                &sink,
                Limits::default(),
            )
            .unwrap()
            {
                self.ctx.writer.submit(Priority::Bulk, batch).await.unwrap();
            }
            if !more {
                break;
            }
        }
    }

    /// The copies whose reading is live, by text, in the order copied.
    async fn live(&mut self) -> Vec<String> {
        sqlx::query_scalar(
            "SELECT e.payload->>'text' FROM core.live_locations l JOIN core.events e ON e.id = l.event_id
             WHERE e.source = $1 ORDER BY e.payload->>'received_at'",
        )
        .bind(clipboard::SPEC.name)
        .fetch_all(&mut self.ctx.read_conn)
        .await
        .unwrap()
    }

    async fn event(&mut self, text: &str, nth: i64) -> EventId {
        EventId(
            sqlx::query_scalar(
                "SELECT id FROM core.events WHERE payload->>'text' = $1 ORDER BY payload->>'received_at' OFFSET $2 LIMIT 1",
            )
            .bind(text)
            .bind(nth)
            .fetch_one(&mut self.ctx.read_conn)
            .await
            .unwrap(),
        )
    }

    /// Every stored material's bytes, in part order.
    async fn stored(&mut self) -> String {
        let blobs: Vec<String> =
            sqlx::query_scalar("SELECT blob FROM raw.materials ORDER BY part_index")
                .fetch_all(&mut self.ctx.read_conn)
                .await
                .unwrap();
        blobs
            .iter()
            .map(|blob| String::from_utf8(self.ctx.blobs.read(blob).unwrap()).unwrap())
            .collect()
    }
}

/// A copy alone in its material is excised with its material; equal copies
/// elsewhere stay, and a copy that shares a material with others is refused
/// without changes.
#[devkit::test]
async fn a_copy_alone_in_its_material_goes_and_nothing_else_changes(db: devkit::pg::TestDatabase) {
    let mut w = Clipboard::new(db).await;
    w.copy(&["git status"]).await;
    w.copy(&["git status", "synthetic other"]).await;
    let alone = w.event("git status", 0).await;
    let shared = w.event("synthetic other", 0).await;
    let before = w.stored().await;

    let refused = excise(
        &mut w.ctx.read_conn,
        &w.ctx.blobs,
        &w.ctx.writer,
        Target::Event(shared),
        "operator",
    )
    .await
    .unwrap_err();
    assert!(refused.contains("holds other records too"), "{refused}");
    assert_eq!(w.stored().await, before);

    let dry = plan(&mut w.ctx.read_conn, &w.ctx.blobs, Target::Event(alone))
        .await
        .unwrap();
    assert_eq!(describe(&dry), "events 1 | materials removed 1");
    let done = excise(
        &mut w.ctx.read_conn,
        &w.ctx.blobs,
        &w.ctx.writer,
        Target::Event(alone),
        "operator",
    )
    .await
    .unwrap();
    assert!(
        done.starts_with("excised events 1 | materials removed 1 | inputs removed 0"),
        "{done}"
    );
    assert_eq!(w.live().await, ["git status", "synthetic other"]);
    let stored = w.stored().await;
    assert_eq!(stored.matches("git status").count(), 1, "{stored}");
    assert!(stored.contains("synthetic other"));
    let kept: i64 = sqlx::query_scalar("SELECT count(*) FROM core.excised_materials")
        .fetch_one(&mut w.ctx.read_conn)
        .await
        .unwrap();
    assert_eq!(kept, 1, "the material's digest stays");

    w.copy(&["git status"]).await;
    assert_eq!(
        w.live().await,
        ["git status", "synthetic other", "git status"]
    );
    crate::replay::run(
        &mut w.ctx.read_conn,
        &w.ctx.blobs,
        &w.ctx.writer,
        crate::replay::Options {
            scope: crate::replay::Scope {
                binding: Some(w.binding.name.clone()),
                ..Default::default()
            },
            compare: false,
            resume: None,
            actor: "operator".into(),
            unlock: None,
        },
    )
    .await
    .unwrap();
    assert_eq!(
        w.live().await,
        ["git status", "synthetic other", "git status"]
    );
    let tombstones: i64 = sqlx::query_scalar("SELECT count(*) FROM core.event_tombstones")
        .fetch_one(&mut w.ctx.read_conn)
        .await
        .unwrap();
    assert_eq!(tombstones, 1);
}

/// Sources that read a record again, or can keep another copy of it, are
/// refused before anything changes: an append-only file, SQLite rows, a
/// directory snapshot, an export archive, an API cursor and API requests
/// that can repeat a record.
#[devkit::test]
async fn records_of_sources_that_read_again_are_refused_without_changes(
    db: devkit::pg::TestDatabase,
) {
    use sinex_sources::sources::{atuin, library, mail_imap, notes, takeout_chrome, zsh};
    let dir = tempfile::tempdir().unwrap();
    let mut ctx = ReaderContext::open(&db.url(None), &dir.path().join("store"), sinex_model::time_settings::TimeSettings::operator_default())
        .await
        .unwrap();
    let bytes = b"synthetic record\n";
    for spec in [
        &zsh::SPEC,
        &atuin::SPEC,
        &notes::SPEC,
        &takeout_chrome::SPEC,
        &mail_imap::SPEC,
        &library::REQUEST,
    ] {
        let material = MaterialId::new();
        register(&mut ctx.read_conn, &ctx.blobs, spec, material, bytes).await;
        let event = Event::build(
            &FileChanged {
                received_at: Timestamp::from_unix_secs(1_700_000_000).unwrap(),
                change: "synthetic record".into(),
                path: None,
                path_hex: None,
                to: None,
                is_dir: None,
                note: None,
            },
            spec.name,
            Origin::Record {
                location: RecordLocation {
                    material,
                    anchor: 0,
                },
                output_index: 0,
            },
            ReadingOrder(0),
            [0; 32],
            Class::Open,
        )
        .unwrap();
        let id = event.id;
        ctx.writer
            .submit(
                Priority::Live,
                Batch {
                    run: RunId::new(),
                    kind: BatchKind::Api {
                        actor: "test".into(),
                    },
                    records: vec![RecordOutcome::Events {
                        location: None,
                        events: vec![event],
                    }],
                    archive: vec![],
                },
            )
            .await
            .unwrap();
        for target in [Target::Event(id), Target::Material(material)] {
            let refused = excise(
                &mut ctx.read_conn,
                &ctx.blobs,
                &ctx.writer,
                target,
                "operator",
            )
            .await
            .unwrap_err();
            assert!(
                refused.contains(&format!("records of {} cannot be excised", spec.name)),
                "{refused}"
            );
        }
        let blob: String = sqlx::query_scalar("SELECT blob FROM raw.materials WHERE id = $1")
            .bind(material.0)
            .fetch_one(&mut ctx.read_conn)
            .await
            .unwrap();
        assert_eq!(ctx.blobs.read(&blob).unwrap(), bytes);
    }
    let counts: (i64, i64) = sqlx::query_as(
        "SELECT (SELECT count(*) FROM core.events), (SELECT count(*) FROM core.operations WHERE kind = 'excise')",
    )
    .fetch_one(&mut ctx.read_conn)
    .await
    .unwrap();
    assert_eq!(counts, (6, 0));
    ctx.close().await.unwrap();
}
