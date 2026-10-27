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
async fn sparse_binary_document_is_admitted_guarded_and_replayed_without_the_root(
    db: devkit::pg::TestDatabase,
) {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("documents");
    std::fs::create_dir(&root).unwrap();
    use std::os::unix::fs::FileExt;
    let mut bytes = vec![0; (2 << 20) + 1];
    bytes[..4].copy_from_slice(b"head");
    let tail = bytes.len() - 4;
    bytes[tail..].copy_from_slice(b"tail");
    let file = std::fs::File::create(root.join("example.bin")).unwrap();
    file.set_len(bytes.len() as u64).unwrap();
    file.write_all_at(b"head", 0).unwrap();
    file.write_all_at(b"tail", tail as u64).unwrap();
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
    let node = member.node.as_ref().unwrap();
    assert_eq!(node.size, bytes.len() as u64);
    assert!(node.sparse.is_some());
    assert!(member.length < node.size);
    let stored = ctx
        .blobs
        .read_range(&blob, member.stream_offset, member.length)
        .unwrap();
    assert!(stored.starts_with(b"head"));
    assert!(stored.ends_with(b"tail"));

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

#[devkit::test]
async fn document_metadata_changes_replace_readings_without_changing_bytes(
    db: devkit::pg::TestDatabase,
) {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("documents");
    std::fs::create_dir(&root).unwrap();
    let path = root.join("example.txt");
    std::fs::write(&path, b"same bytes").unwrap();
    let mut ctx = ReaderContext::open(&db.url(None), &dir.path().join("store"))
        .await
        .unwrap();
    let binding = ingest(&mut ctx, &documents::FORMAT, &root, json!({})).await;
    let before = live(&mut ctx, "document.captured").await;
    assert_eq!(before.len(), 1);
    let file = std::fs::File::open(&path).unwrap();
    file.set_modified(std::time::UNIX_EPOCH + std::time::Duration::from_secs(1_700_000_000))
        .unwrap();
    ingest(&mut ctx, &documents::FORMAT, &root, json!({})).await;
    let after = live(&mut ctx, "document.captured").await;
    assert_eq!(after.len(), 1);
    assert_eq!(after[0]["content"], before[0]["content"]);
    assert_ne!(after[0]["modified_at"], before[0]["modified_at"]);
    std::fs::remove_dir_all(&root).unwrap();
    replay(&mut ctx, &binding).await;
    assert_eq!(live(&mut ctx, "document.captured").await, after);
    ctx.close().await.unwrap();
}

#[devkit::test]
async fn imported_text_refusal_keeps_bytes_and_advances_before_the_next_revision(
    db: devkit::pg::TestDatabase,
) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("transcript.json");
    let malformed = b"[{malformed JSON";
    std::fs::write(&path, malformed).unwrap();
    let mut ctx = ReaderContext::open(&db.url(None), &dir.path().join("store"))
        .await
        .unwrap();
    let binding = ingest(&mut ctx, &transcript::JSON_FORMAT, &path, json!({})).await;
    let (cursor, _) = sinex_store::positions::load(&mut ctx.read_conn, &binding.name)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(cursor["done"], true);
    let (blob, length): (String, i64) = sqlx::query_as(
        "SELECT m.blob, m.length FROM raw.failures f JOIN raw.materials m ON m.id=f.material_id",
    )
    .fetch_one(&mut ctx.read_conn)
    .await
    .unwrap();
    assert_eq!(
        ctx.blobs.read_range(&blob, 0, length as u64).unwrap(),
        malformed
    );
    assert!(live(&mut ctx, "transcript.cue").await.is_empty());
    std::fs::write(&path, br#"[{"text":"after the refusal"}]"#).unwrap();
    ingest(&mut ctx, &transcript::JSON_FORMAT, &path, json!({})).await;
    let after = live(&mut ctx, "transcript.cue").await;
    assert_eq!(after.len(), 1);
    assert_eq!(after[0]["text"], "after the refusal");
    let failures: i64 = sqlx::query_scalar("SELECT count(*) FROM raw.failures")
        .fetch_one(&mut ctx.read_conn)
        .await
        .unwrap();
    assert_eq!(failures, 1);
    ctx.close().await.unwrap();
}
