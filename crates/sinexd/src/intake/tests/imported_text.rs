use crate::binding_reader::{BindingReader, once};
use crate::reader_context::ReaderContext;
use crate::replay::{Options, Scope};
use serde_json::{Value, json};
use sinex_source_core::{Binding, InputFormat, Settings};
use sinex_sources::sources::{documents, transcript};

async fn ingest(
    ctx: &mut ReaderContext,
    format: &InputFormat,
    path: &std::path::Path,
    options: Value,
) -> Binding {
    let mut settings = Settings::new("example-host");
    settings.options = options;
    let binding = (format.plan.unwrap())(path, &settings).unwrap().remove(0);
    let reader = BindingReader::new(format, binding.clone()).unwrap();
    once(
        &mut ctx.read_conn,
        &ctx.blobs,
        &ctx.writer,
        "example-host",
        vec![reader],
    )
    .await
    .unwrap();
    binding
}
async fn live(ctx: &mut ReaderContext, event_type: &str) -> Vec<Value> {
    sqlx::query_scalar("SELECT payload FROM core.events WHERE event_type=$1 AND id IN (SELECT event_id FROM core.live_locations UNION ALL SELECT event_id FROM core.live_equivalence) ORDER BY payload->>'provider_id'")
        .bind(event_type).fetch_all(&mut ctx.read_conn).await.unwrap()
}
async fn replay(ctx: &mut ReaderContext, binding: &Binding) {
    let result = crate::replay::run(
        &mut ctx.read_conn,
        &ctx.blobs,
        &ctx.writer,
        Options {
            scope: Scope {
                binding: Some(binding.name.clone()),
                ..Scope::default()
            },
            compare: false,
            resume: None,
            actor: "synthetic".into(),
            unlock: None,
        },
    )
    .await
    .unwrap();
    assert_eq!(result.failures, 0);
    assert!(result.admitted > 0);
}

#[devkit::test]
async fn binary_document_is_admitted_guarded_and_replayed_without_the_root(
    db: devkit::pg::TestDatabase,
) {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("documents");
    std::fs::create_dir(&root).unwrap();
    let bytes = vec![0xff; (2 << 20) + 1];
    std::fs::write(root.join("example.bin"), &bytes).unwrap();
    let mut ctx = ReaderContext::open(&db.url(None), &dir.path().join("store"))
        .await
        .unwrap();
    let binding = ingest(&mut ctx, &documents::FORMAT, &root, json!({})).await;
    let before = live(&mut ctx, "document.captured").await;
    assert_eq!(before.len(), 1);
    assert_eq!(
        before[0]["content"],
        format!("blake3:{}", blake3::hash(&bytes).to_hex())
    );
    let row: (uuid::Uuid,i64,String,Value) = sqlx::query_as("SELECT e.material_id,e.anchor,e.class::text,e.payload FROM core.events e WHERE e.event_type='document.captured'")
        .fetch_one(&mut ctx.read_conn).await.unwrap();
    assert_eq!(row.2, "guarded");
    let (blob, manifest): (String, Value) =
        sqlx::query_as("SELECT blob,manifest FROM raw.materials WHERE id=$1")
            .bind(row.0)
            .fetch_one(&mut ctx.read_conn)
            .await
            .unwrap();
    let manifest: sinex_model::Manifest = serde_json::from_value(manifest).unwrap();
    let member = manifest.member_at(row.1 as usize).unwrap();
    assert_eq!(
        ctx.blobs
            .read_range(&blob, member.stream_offset, member.length)
            .unwrap(),
        bytes
    );
    std::fs::remove_dir_all(&root).unwrap();
    replay(&mut ctx, &binding).await;
    assert_eq!(live(&mut ctx, "document.captured").await, before);
    ctx.close().await.unwrap();
}

#[devkit::test]
async fn imported_json_metadata_survives_writer_restart_and_replay(db: devkit::pg::TestDatabase) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("whisper.json");
    std::fs::write(&path,br#"{"segments":[{"id":0,"text":"same","start":0,"end":1},{"id":1,"text":"same","start":2,"end":3}],"language":"pl","model":"example-model","external_run":"example-run"}"#).unwrap();
    let store = dir.path().join("store");
    let mut ctx = ReaderContext::open(&db.url(None), &store).await.unwrap();
    let binding = ingest(&mut ctx, &transcript::WHISPER_FORMAT, &path, json!({})).await;
    let before = live(&mut ctx, "transcript.cue").await;
    assert_eq!(before.len(), 2);
    assert!(before.iter().all(|p| p["language"] == "pl"
        && p["model"] == "example-model"
        && p["external_run"] == "example-run"));
    let classes: Vec<String> =
        sqlx::query_scalar("SELECT class::text FROM core.events WHERE event_type='transcript.cue'")
            .fetch_all(&mut ctx.read_conn)
            .await
            .unwrap();
    assert!(classes.iter().all(|c| c == "guarded"));
    let corrected = br#"{"segments":[{"id":0,"text":"same","start":0,"end":1},{"id":1,"text":"same","start":2,"end":3}],"language":"en","model":"example-model","external_run":"example-run"}"#;
    std::fs::write(&path, corrected).unwrap();
    ingest(&mut ctx, &transcript::WHISPER_FORMAT, &path, json!({})).await;
    let before = live(&mut ctx, "transcript.cue").await;
    assert_eq!(before.len(), 2);
    assert!(before.iter().all(|p| p["language"] == "en"));
    ctx.close().await.unwrap();
    std::fs::remove_file(&path).unwrap();
    let mut ctx = ReaderContext::open(&db.url(None), &store).await.unwrap();
    replay(&mut ctx, &binding).await;
    assert_eq!(live(&mut ctx, "transcript.cue").await, before);
    ctx.close().await.unwrap();
}
