use super::*;
use crate::runtime::content_store::ContentStoreConfig;
use crate::sources::dispatch::test_parser_dispatch;
use camino::Utf8PathBuf;
use sinex_db::repositories::source_materials::SourceMaterial as SourceMaterialRegistration;
use sinex_primitives::Uuid;
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
        .register_material(
            SourceMaterialRegistration::blob_text(filename).with_blob_id(blob.id),
        )
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
async fn load_material_bytes_fails_closed_on_missing_material(
    ctx: TestContext,
) -> TestResult<()> {
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
