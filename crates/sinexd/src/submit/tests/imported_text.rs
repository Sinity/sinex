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
    once(&mut ctx.read_conn, &ctx.blobs, &ctx.writer, vec![reader])
        .await
        .unwrap();
    binding
}
async fn live(ctx: &mut ReaderContext, event_type: &str) -> Vec<Value> {
    sqlx::query_scalar("SELECT payload FROM core.events WHERE event_type=$1 AND id IN (SELECT event_id FROM core.live_locations UNION ALL SELECT event_id FROM core.live_equivalence) ORDER BY payload->>'provider_id', payload::text")
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
    let mut ctx = ReaderContext::open(
        &db.url(None),
        &dir.path().join("store"),
        sinex_model::time_settings::TimeSettings::new("Europe/Warsaw", 4, 0)
            .expect("fixture day setting"),
    )
    .await
    .unwrap();
    let binding = ingest(&mut ctx, &documents::FORMAT, &root, json!({})).await;
    let before = live(&mut ctx, "document.captured").await;
    assert_eq!(before.len(), 1);
    assert_eq!(
        before[0]["content"],
        format!("blake3:{}", blake3::hash(&bytes).to_hex())
    );
    let row: (uuid::Uuid,i64,String,Value) = sqlx::query_as("SELECT e.material_id,e.record_offset,e.class::text,e.payload FROM core.events e WHERE e.event_type='document.captured'")
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
        .read_range(
            &ctx.blobs.locate(&mut ctx.read_conn, &blob).await.unwrap(),
            member.stream_offset,
            member.length,
        )
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
    let mut ctx = ReaderContext::open(
        &db.url(None),
        &store,
        sinex_model::time_settings::TimeSettings::new("Europe/Warsaw", 4, 0)
            .expect("fixture day setting"),
    )
    .await
    .unwrap();
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
    assert_eq!(before.len(), 4);
    for language in ["pl", "en"] {
        assert_eq!(
            before.iter().filter(|p| p["language"] == language).count(),
            2
        );
    }
    ctx.close().await.unwrap();
    std::fs::remove_file(&path).unwrap();
    let mut ctx = ReaderContext::open(
        &db.url(None),
        &store,
        sinex_model::time_settings::TimeSettings::new("Europe/Warsaw", 4, 0)
            .expect("fixture day setting"),
    )
    .await
    .unwrap();
    replay(&mut ctx, &binding).await;
    assert_eq!(live(&mut ctx, "transcript.cue").await, before);
    ctx.close().await.unwrap();
}

#[devkit::test]
async fn malformed_transcript_cue_leaves_neighboring_cues_at_their_original_offsets(
    db: devkit::pg::TestDatabase,
) {
    async fn readings(
        conn: &mut sqlx::PgConnection,
        locator: &str,
    ) -> Vec<(uuid::Uuid, i64, Value)> {
        sqlx::query_as(
            "SELECT e.material_id,e.record_offset,e.payload FROM core.events e \
             JOIN raw.materials m ON m.id=e.material_id \
             JOIN core.live_locations l ON l.event_id=e.id \
             WHERE e.event_type='transcript.cue' AND m.manifest->>'path'=$1 \
             ORDER BY e.record_offset",
        )
        .bind(locator)
        .fetch_all(conn)
        .await
        .unwrap()
    }
    let dir = tempfile::tempdir().unwrap();
    let mut ctx = ReaderContext::open(
        &db.url(None),
        &dir.path().join("store"),
        sinex_model::time_settings::TimeSettings::new("Europe/Warsaw", 4, 0).unwrap(),
    )
    .await
    .unwrap();
    for (format, document, language) in [
        (
            &transcript::JSON_FORMAT,
            br#"[ {"text":"before"}, {"text":42}, {"text":"after"} ]"#.as_slice(),
            None,
        ),
        (
            &transcript::WHISPER_FORMAT,
            br#"{"segments":[ {"text":"before"}, {"text":42}, {"text":"after"} ],"language":"pl"}"#
                .as_slice(),
            Some("pl"),
        ),
    ] {
        let path = dir.path().join(format!("{}.json", format.name));
        std::fs::write(&path, document).unwrap();
        let binding = ingest(&mut ctx, format, &path, json!({})).await;
        let locator = path.to_str().unwrap();
        let start = |cue: &[u8]| document.windows(cue.len()).position(|w| w == cue).unwrap();
        let before = readings(&mut ctx.read_conn, locator).await;
        assert_eq!(before.len(), 2, "{}", format.name);
        for ((_, offset, payload), (cue, word)) in before.iter().zip([
            (br#"{"text":"before"}"#.as_slice(), "before"),
            (br#"{"text":"after"}"#.as_slice(), "after"),
        ]) {
            assert_eq!(*offset, start(cue) as i64);
            assert_eq!(payload["text"], word);
            assert_eq!(payload["language"].as_str(), language);
        }
        let malformed = br#"{"text":42}"#;
        let (bad, reason, blob, manifest): (i64, String, String, Value) = sqlx::query_as(
            "SELECT f.record_offset,f.reason,m.blob,m.manifest FROM raw.failures f \
             JOIN raw.materials m ON m.id=f.material_id WHERE m.manifest->>'path'=$1",
        )
        .bind(locator)
        .fetch_one(&mut ctx.read_conn)
        .await
        .unwrap();
        assert_eq!(bad, start(malformed) as i64);
        assert!(reason.contains("text"), "{reason}");
        let manifest: sinex_model::Manifest = serde_json::from_value(manifest).unwrap();
        let range = (bad as usize, bad as usize + malformed.len());
        assert!(manifest.records.unwrap().contains(&range));
        let stored = ctx.blobs.locate(&mut ctx.read_conn, &blob).await.unwrap();
        assert_eq!(
            ctx.blobs
                .read_range(&stored, 0, document.len() as u64)
                .unwrap(),
            document
        );

        std::fs::remove_file(&path).unwrap();
        let report = crate::replay::run(
            &mut ctx.read_conn,
            &ctx.blobs,
            &ctx.writer,
            Options {
                scope: Scope {
                    binding: Some(binding.name),
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
        assert_eq!(report.events_read, 2);
        assert_eq!(report.failures, 1);
        assert_eq!(readings(&mut ctx.read_conn, locator).await, before);
        let failed_offsets: Vec<i64> = sqlx::query_scalar(
            "SELECT DISTINCT f.record_offset FROM raw.failures f \
             JOIN raw.materials m ON m.id=f.material_id WHERE m.manifest->>'path'=$1",
        )
        .bind(locator)
        .fetch_all(&mut ctx.read_conn)
        .await
        .unwrap();
        assert_eq!(failed_offsets, [bad]);
    }
    ctx.close().await.unwrap();
}

#[devkit::test]
async fn document_metadata_changes_keep_distinct_observations_of_the_same_bytes(
    db: devkit::pg::TestDatabase,
) {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("documents");
    std::fs::create_dir(&root).unwrap();
    let path = root.join("example.txt");
    std::fs::write(&path, b"same bytes").unwrap();
    let mut ctx = ReaderContext::open(
        &db.url(None),
        &dir.path().join("store"),
        sinex_model::time_settings::TimeSettings::new("Europe/Warsaw", 4, 0)
            .expect("fixture day setting"),
    )
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
    assert_eq!(after.len(), 2);
    assert!(
        after
            .iter()
            .all(|payload| payload["content"] == before[0]["content"])
    );
    assert_ne!(after[0]["modified_at"], after[1]["modified_at"]);
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
    let mut ctx = ReaderContext::open(
        &db.url(None),
        &dir.path().join("store"),
        sinex_model::time_settings::TimeSettings::new("Europe/Warsaw", 4, 0)
            .expect("fixture day setting"),
    )
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
        ctx.blobs
            .read_range(
                &ctx.blobs.locate(&mut ctx.read_conn, &blob).await.unwrap(),
                0,
                length as u64
            )
            .unwrap(),
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

#[devkit::test]
async fn located_document_listing_is_admitted_and_replayed_without_external_media(
    db: devkit::pg::TestDatabase,
) {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("downloads");
    std::fs::create_dir(&root).unwrap();
    let mut media = b"\0\0\0\x20ftypisom".to_vec();
    media.resize((2 << 20) + 7, 0x93);
    std::fs::write(root.join("movie.mp4"), &media).unwrap();
    std::fs::write(root.join("note.txt"), b"ordinary stored bytes").unwrap();
    let mut ctx = ReaderContext::open(
        &db.url(None),
        &dir.path().join("store"),
        sinex_model::time_settings::TimeSettings::new("Europe/Warsaw", 4, 0).unwrap(),
    )
    .await
    .unwrap();
    let binding = ingest(
        &mut ctx,
        &documents::FORMAT,
        &root,
        json!({"locate_media":true}),
    )
    .await;
    assert!(binding.name.contains("#located@"));
    let before = live(&mut ctx, "document.captured").await;
    assert_eq!(before.len(), 2);
    let media_event = before
        .iter()
        .find(|payload| payload["located"] == true)
        .unwrap();
    assert_eq!(
        media_event["content"],
        format!("blake3:{}", blake3::hash(&media).to_hex())
    );
    assert_eq!(media_event["size"], media.len());
    let rows: Vec<(String,i64,Value)> = sqlx::query_as("SELECT m.blob,m.length,m.manifest FROM raw.materials m JOIN raw.inputs i ON i.id=m.input_id WHERE i.binding=$1")
        .bind(&binding.name).fetch_all(&mut ctx.read_conn).await.unwrap();
    assert_eq!(rows.len(), 1);
    let (blob, length, manifest) = &rows[0];
    assert!(*length < 16384, "media bytes entered the committed store");
    let manifest: sinex_model::Manifest = serde_json::from_value(manifest.clone()).unwrap();
    let listed = manifest
        .members
        .iter()
        .find(|member| member.path == "movie.mp4")
        .unwrap();
    assert_eq!(listed.length, 0);
    assert_eq!(
        ctx.blobs
            .read_range(
                &ctx.blobs.locate(&mut ctx.read_conn, blob).await.unwrap(),
                listed.stream_offset,
                listed.length
            )
            .unwrap(),
        Vec::<u8>::new()
    );
    let note = manifest
        .members
        .iter()
        .find(|member| member.path == "note.txt")
        .unwrap();
    assert_eq!(
        ctx.blobs
            .read_range(
                &ctx.blobs.locate(&mut ctx.read_conn, blob).await.unwrap(),
                note.stream_offset,
                note.length
            )
            .unwrap(),
        b"ordinary stored bytes"
    );
    std::fs::remove_dir_all(&root).unwrap();
    replay(&mut ctx, &binding).await;
    assert_eq!(live(&mut ctx, "document.captured").await, before);
    ctx.close().await.unwrap();
}
