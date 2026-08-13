use super::*;
use crate::runtime::content_store::{ContentStoreConfig, MaterialContentStore};
use crate::sources::dispatch::test_parser_dispatch;
use camino::Utf8PathBuf;
use sinex_db::repositories::source_materials::SourceMaterial as SourceMaterialRegistration;
use sinex_primitives::{ByteRange, Id, MaterialManifestV1, Uuid};
use tempfile::TempDir;
use xtask::sandbox::prelude::*;

/// Build a local-BLAKE3-CAS content store backed by a temp dir (no
/// git-annex). The `TempDir` guard must be kept alive for the test body.
fn test_content_store(ctx: &TestContext) -> TestResult<(Arc<ContentStoreManager>, TempDir)> {
    let tmp = TempDir::new()?;
    let root = Utf8PathBuf::from_path_buf(tmp.path().join("cas"))
        .map_err(|_| eyre!("content-store path must be valid UTF-8"))?;
    let config = ContentStoreConfig {
        root_path: root,
        ..Default::default()
    };
    let manager = ContentStoreManager::new(config, ctx.pool().clone(), None)?;
    Ok((Arc::new(manager), tmp))
}

/// Stage real bytes into the CAS and register a source material that
/// references the resulting blob. Returns the material id the listener
/// would receive in a parse command.
async fn stage_material(
    ctx: &TestContext,
    content_store: &ContentStoreManager,
    filename: &str,
    payload: &[u8],
) -> TestResult<Uuid> {
    let blob = content_store
        .ingest_from_bytes(payload, filename, "text/plain")
        .await?;
    let material = ctx
        .pool()
        .source_materials()
        .register_material(SourceMaterialRegistration::blob_text(filename).with_blob_id(blob.id))
        .await?;
    Ok(material.id)
}

fn parse_command(source_id: &str, material_id: Option<Uuid>) -> SourceParseCommand {
    SourceParseCommand {
        operation_id: Uuid::now_v7(),
        source_id: source_id.to_string(),
        source_material_id: material_id,
        source_version: None,
        executor: "test".to_string(),
    }
}

#[sinex_test]
async fn load_material_bytes_returns_real_content(ctx: TestContext) -> TestResult<()> {
    let (content_store, _tmp) = test_content_store(&ctx)?;
    let payload = b"weechat: <nick> a real line of history\n";
    let material_id = stage_material(&ctx, &content_store, "weechat.log", payload).await?;

    let bytes = load_material_bytes(ctx.pool(), &content_store, material_id)
        .await
        .map_err(|e| eyre!(e))?;

    assert_eq!(bytes, payload, "listener must load the real material bytes");
    Ok(())
}

#[sinex_test]
async fn load_material_bytes_uses_and_validates_manifest_authority(
    ctx: TestContext,
) -> TestResult<()> {
    let (content_store, _tmp) = test_content_store(&ctx)?;
    let payload = b"manifest-authoritative bytes";
    let blob = content_store
        .ingest_from_bytes(payload, "manifest.log", "text/plain")
        .await?;
    let material = ctx
        .pool()
        .source_materials()
        .register_material(
            SourceMaterialRegistration::blob_text("manifest.log").with_blob_id(blob.id),
        )
        .await?;
    let manifest = MaterialManifestV1::from_capture(
        material.id,
        "manifest.log",
        "local_cas",
        blake3::hash(payload).to_hex().to_string(),
        payload.len() as u64,
        serde_json::json!({"logical_source_identifier": "test.manifest"}),
        "2026-08-12T00:00:00Z",
        "2026-08-12T00:00:01Z",
    );
    let manifest_blob = content_store
        .ingest_from_bytes(
            &manifest.canonical_bytes()?,
            "material-manifest.json",
            "application/json",
        )
        .await?;
    ctx.pool()
        .source_materials()
        .update_metadata(
            Id::from_uuid(material.id),
            serde_json::json!({
                "material_manifest": {
                    "manifest_type": sinex_primitives::MATERIAL_MANIFEST_V1,
                    "content_key": manifest_blob.content_key(),
                }
            }),
        )
        .await?;

    let bytes = load_material_bytes(ctx.pool(), &content_store, material.id)
        .await
        .map_err(|e| eyre!(e))?;
    assert_eq!(bytes, payload);
    Ok(())
}

#[sinex_test]
async fn load_material_bytes_replays_from_manifest_when_blob_route_is_absent(
    ctx: TestContext,
) -> TestResult<()> {
    let (content_store, tmp) = test_content_store(&ctx)?;
    let root = Utf8PathBuf::from_path_buf(tmp.path().join("cas"))
        .map_err(|_| eyre!("content-store path must be valid UTF-8"))?;
    let raw_store = MaterialContentStore::new(ContentStoreConfig {
        root_path: root.clone(),
        ..Default::default()
    })?;
    let payload = b"source removal replay has no dependency on the original path";
    let payload_path = root.join("removed-source.bin");
    tokio::fs::write(&payload_path, payload).await?;
    let payload_key = raw_store.store_file(&payload_path).await?;

    let material_id = Uuid::now_v7();
    let manifest = MaterialManifestV1::from_capture(
        material_id,
        "removed-source.bin",
        "chunk",
        payload_key.digest.clone(),
        payload.len() as u64,
        serde_json::json!({
            "material_type": "chunk",
            "pack_member_key": "observed-member-7",
            "logical_source_identifier": "test.pack",
        }),
        "2026-08-12T00:00:00Z",
        "2026-08-12T00:00:01Z",
    );
    let mut manifest = manifest;
    let replay_range = ByteRange { start: 0, end: 24 };
    manifest.bytes.parser_ranges = vec![replay_range];
    let manifest_path = root.join("removed-source-manifest.json");
    tokio::fs::write(&manifest_path, manifest.canonical_bytes()?).await?;
    let manifest_key = raw_store.store_file(&manifest_path).await?;
    tokio::fs::remove_file(&payload_path).await?;

    ctx.pool
        .source_materials()
        .register_external_in_flight(
            material_id,
            "chunk",
            Some("chunk://test.pack#7"),
            serde_json::json!({"material_manifest": {"content_key": manifest_key.key}}),
            sinex_primitives::Timestamp::now(),
        )
        .await?;

    let authority = load_material_authority(ctx.pool(), &content_store, material_id)
        .await
        .map_err(|error| eyre!(error))?;
    assert_eq!(&authority.bytes, payload);
    let range = authority
        .exact_range(material_id, replay_range)
        .map_err(|error| eyre!(error))?;
    assert_eq!(range, payload[..24]);
    let second_range = authority
        .exact_range(material_id, ByteRange { start: 24, end: 47 })
        .map_err(|error| eyre!(error))?;
    assert_eq!(second_range, payload[24..47]);
    Ok(())
}

#[sinex_test]
async fn load_material_bytes_rejects_noncanonical_manifest_encoding(
    ctx: TestContext,
) -> TestResult<()> {
    let (content_store, _tmp) = test_content_store(&ctx)?;
    let payload = b"canonical manifest bytes";
    let blob = content_store
        .ingest_from_bytes(payload, "canonical.log", "text/plain")
        .await?;
    let material = ctx
        .pool()
        .source_materials()
        .register_material(
            SourceMaterialRegistration::blob_text("canonical.log").with_blob_id(blob.id),
        )
        .await?;
    let manifest = MaterialManifestV1::from_capture(
        material.id,
        "canonical.log",
        "local_cas",
        blake3::hash(payload).to_hex().to_string(),
        payload.len() as u64,
        serde_json::json!({"logical_source_identifier": "test.canonical"}),
        "2026-08-12T00:00:00Z",
        "2026-08-12T00:00:01Z",
    );
    let canonical_bytes = manifest.canonical_bytes()?;
    let noncanonical_bytes = serde_json::to_vec_pretty(&manifest)?;
    assert_ne!(
        canonical_bytes, noncanonical_bytes,
        "fixture must exercise the canonical encoding boundary"
    );
    let manifest_blob = content_store
        .ingest_from_bytes(
            &noncanonical_bytes,
            "material-manifest.json",
            "application/json",
        )
        .await?;
    ctx.pool()
        .source_materials()
        .update_metadata(
            Id::from_uuid(material.id),
            serde_json::json!({
                "material_manifest": {
                    "manifest_type": sinex_primitives::MATERIAL_MANIFEST_V1,
                    "content_key": manifest_blob.content_key(),
                }
            }),
        )
        .await?;

    let error = load_material_bytes(ctx.pool(), &content_store, material.id)
        .await
        .expect_err("manifest reads must require the canonical CAS representation");
    assert!(error.contains("not in canonical encoding"), "got: {error}");
    Ok(())
}

#[sinex_test]
async fn load_material_bytes_rejects_manifest_with_invalid_discriminator(
    ctx: TestContext,
) -> TestResult<()> {
    let (content_store, _tmp) = test_content_store(&ctx)?;
    let payload = b"manifest validation bytes";
    let blob = content_store
        .ingest_from_bytes(payload, "invalid-manifest.log", "text/plain")
        .await?;
    let material = ctx
        .pool()
        .source_materials()
        .register_material(
            SourceMaterialRegistration::blob_text("invalid-manifest.log").with_blob_id(blob.id),
        )
        .await?;
    let mut manifest = MaterialManifestV1::from_capture(
        material.id,
        "invalid-manifest.log",
        "local_cas",
        blake3::hash(payload).to_hex().to_string(),
        payload.len() as u64,
        json!({}),
        "2026-08-12T00:00:00Z",
        "2026-08-12T00:00:01Z",
    );
    manifest.manifest_type = sinex_primitives::MaterialManifestType::LegacyV0;
    let manifest_blob = content_store
        .ingest_from_bytes(
            &manifest.canonical_bytes()?,
            "invalid-material-manifest.json",
            "application/json",
        )
        .await?;
    ctx.pool()
        .source_materials()
        .update_metadata(
            Id::from_uuid(material.id),
            json!({
                "material_manifest": {
                    "manifest_type": sinex_primitives::MATERIAL_MANIFEST_V1,
                    "content_key": manifest_blob.content_key(),
                }
            }),
        )
        .await?;

    let err = load_material_bytes(ctx.pool(), &content_store, material.id)
        .await
        .expect_err("loader must validate the manifest discriminator before reading bytes");
    assert!(err.contains("manifest validation failed"), "got: {err}");
    Ok(())
}

#[sinex_test]
async fn load_material_bytes_fails_closed_on_missing_material(ctx: TestContext) -> TestResult<()> {
    let (content_store, _tmp) = test_content_store(&ctx)?;
    let err = load_material_bytes(ctx.pool(), &content_store, Uuid::now_v7())
        .await
        .expect_err("missing material must fail closed, never return empty bytes");
    assert!(err.contains("not found"), "got: {err}");
    Ok(())
}

#[sinex_test]
async fn load_material_bytes_fails_closed_when_material_has_no_blob(
    ctx: TestContext,
) -> TestResult<()> {
    let (content_store, _tmp) = test_content_store(&ctx)?;
    // A material with no associated blob has no bytes to load.
    let material = ctx
        .pool()
        .source_materials()
        .register_material(SourceMaterialRegistration::blob_text("blobless.log"))
        .await?;

    let err = load_material_bytes(ctx.pool(), &content_store, material.id)
        .await
        .expect_err("material without a blob must fail closed");
    assert!(err.contains("no associated blob"), "got: {err}");
    Ok(())
}

#[sinex_test]
async fn run_parse_dispatches_loaded_bytes_on_happy_path(ctx: TestContext) -> TestResult<()> {
    let (content_store, _tmp) = test_content_store(&ctx)?;
    let (dispatch, calls) = test_parser_dispatch();
    let payload = b"weechat: real bytes reach the parser";
    let material_id = stage_material(&ctx, &content_store, "weechat.log", payload).await?;

    let cmd = parse_command("weechat", Some(material_id));
    let ack = run_parse("weechat", &cmd, &dispatch, ctx.pool(), &content_store).await;

    assert!(ack.accepted, "happy-path parse should be accepted: {ack:?}");
    let recorded = calls.lock().unwrap();
    assert_eq!(recorded.len(), 1, "dispatch must be invoked exactly once");
    assert_eq!(
        recorded[0].1, payload,
        "dispatch must receive the real loaded material bytes, not empty bytes"
    );
    Ok(())
}

#[sinex_test]
async fn run_parse_rejects_mismatched_source(ctx: TestContext) -> TestResult<()> {
    let (content_store, _tmp) = test_content_store(&ctx)?;
    let (dispatch, calls) = test_parser_dispatch();
    let cmd = parse_command("desktop", Some(Uuid::now_v7()));

    let ack = run_parse("weechat", &cmd, &dispatch, ctx.pool(), &content_store).await;

    assert!(!ack.accepted);
    assert!(ack.error.unwrap().contains("does not match"));
    assert_eq!(
        calls.lock().unwrap().len(),
        0,
        "dispatch must not run for a mismatched source"
    );
    Ok(())
}

#[sinex_test]
async fn run_parse_rejects_missing_material_id(ctx: TestContext) -> TestResult<()> {
    let (content_store, _tmp) = test_content_store(&ctx)?;
    let (dispatch, calls) = test_parser_dispatch();
    let cmd = parse_command("weechat", None);

    let ack = run_parse("weechat", &cmd, &dispatch, ctx.pool(), &content_store).await;

    assert!(!ack.accepted);
    assert!(ack.error.unwrap().contains("source_material_id"));
    assert_eq!(
        calls.lock().unwrap().len(),
        0,
        "dispatch must not run without a material to load"
    );
    Ok(())
}

#[sinex_test]
async fn default_dispatch_rejects_unknown_source() -> TestResult<()> {
    // The default registry-driven dispatch rejects unregistered sources.
    let default_dispatch = crate::sources::dispatch::default_parser_dispatch();
    let result = default_dispatch("completely-unknown-source-xyz", b"data", None);
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("unknown source_id"));
    Ok(())
}
