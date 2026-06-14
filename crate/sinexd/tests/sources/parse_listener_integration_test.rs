//! Integration test: source parse listener receives parse commands via NATS,
//! loads real source-material bytes, dispatches to the parser, and returns acks.
//!
//! Replaces the fake-DB-write scan-runtime tests referenced in #1132. Guards the
//! #1768 regression: the listener must load real material bytes (registry → blob
//! → CAS) and fail closed when material cannot be loaded, never silently parse
//! empty bytes and report a successful zero-event replay.

use camino::Utf8PathBuf;
use color_eyre::eyre::eyre;
use sinex_db::DbPoolExt;
use sinex_db::repositories::source_materials::SourceMaterial as SourceMaterialRegistration;
use sinex_primitives::Uuid;
use sinexd::runtime::content_store::{ContentStoreConfig, ContentStoreManager};
use sinexd::sources::dispatch::{ParserDispatchFn, default_parser_dispatch, test_parser_dispatch};
use sinexd::sources::parse_listener::{SourceParseAck, SourceParseCommand, spawn_parse_listener};
use std::sync::Arc;
use std::sync::Mutex;
use tempfile::TempDir;
use xtask::sandbox::prelude::*;

type DispatchCalls = Arc<Mutex<Vec<(String, Vec<u8>, Option<Uuid>)>>>;

/// Build a local-BLAKE3-CAS content store backed by a temp dir (no git-annex).
/// The returned `TempDir` guard must outlive the test body.
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

/// Stage real bytes into the CAS and register a source material referencing the
/// resulting blob; returns the material id the gateway would send.
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

async fn request_parse_ack(
    client: &async_nats::Client,
    subject: &str,
    cmd: &SourceParseCommand,
) -> TestResult<SourceParseAck> {
    let response = client
        .request(subject.to_string(), serde_json::to_vec(cmd)?.into())
        .await
        .map_err(|e| eyre!("NATS request failed: {e}"))?;
    Ok(serde_json::from_slice(&response.payload)?)
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

/// Spawn a parse listener wired to the given dispatch + a temp CAS, returning
/// the join handle, the NATS subject, and the guards that must stay alive for
/// the duration of the test.
async fn spawn_listener(
    ctx: &TestContext,
    source_id: &str,
    dispatch: ParserDispatchFn,
) -> TestResult<(
    tokio::task::JoinHandle<()>,
    String,
    Arc<ContentStoreManager>,
    TempDir,
)> {
    let (content_store, tmp) = test_content_store(ctx)?;
    let client = ctx.nats_client();
    let handle = spawn_parse_listener(
        client.clone(),
        source_id,
        dispatch,
        ctx.pool().clone(),
        content_store.clone(),
    )
    .await
    .map_err(|e| eyre!("spawn failed: {e}"))?;
    let subject = format!("sinex.control.sources.{source_id}.parse");
    Ok((handle, subject, content_store, tmp))
}

/// Spawn a listener wired to a recording test dispatch; also returns the call
/// log so a test can assert exactly what bytes reached the parser.
async fn spawn_recording_listener(
    ctx: &TestContext,
    source_id: &str,
) -> TestResult<(
    tokio::task::JoinHandle<()>,
    DispatchCalls,
    String,
    Arc<ContentStoreManager>,
    TempDir,
)> {
    let (dispatch, calls): (ParserDispatchFn, DispatchCalls) = test_parser_dispatch();
    let (handle, subject, content_store, tmp) = spawn_listener(ctx, source_id, dispatch).await?;
    Ok((handle, calls, subject, content_store, tmp))
}

/// End-to-end happy path: a parse command for staged material loads the real
/// bytes over the registry → blob → CAS path and delivers them to the parser.
#[sinex_test]
async fn parse_listener_loads_and_dispatches_real_bytes(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let source_id = "weechat";
    let (handle, calls, subject, content_store, _tmp) = spawn_recording_listener(&ctx, source_id).await?;
    let client = ctx.nats_client();

    let payload = b"weechat: <nick> real history line\n";
    let material_id = stage_material(&ctx, &content_store, "weechat.log", payload).await?;

    let cmd = parse_command(source_id, Some(material_id));
    let ack = request_parse_ack(&client, &subject, &cmd).await?;

    assert!(ack.accepted, "parse should be accepted: {:?}", ack.error);
    assert!(ack.error.is_none(), "no error expected: {:?}", ack.error);

    let recorded = calls.lock().unwrap();
    assert_eq!(recorded.len(), 1, "dispatch should be invoked once");
    assert_eq!(recorded[0].0, source_id);
    assert_eq!(
        recorded[0].1, payload,
        "dispatch must receive the real loaded material bytes, not empty bytes"
    );
    assert_eq!(recorded[0].2, Some(material_id));

    handle.abort();
    Ok(())
}

/// #1768 AC #3: a parse-replay command on registered material, dispatched
/// through the *real* registry parser, yields the expected non-empty parsed
/// events — proving real bytes loaded from the registry/CAS path produce real
/// output, not a zero-event success on empty bytes.
#[sinex_test]
async fn parse_listener_real_parser_emits_events_from_loaded_material(
    ctx: TestContext,
) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let source_id = "weechat";
    let (handle, subject, content_store, _tmp) =
        spawn_listener(&ctx, source_id, default_parser_dispatch()).await?;
    let client = ctx.nats_client();

    // A well-formed WeeChat log line the registered parser turns into one
    // irc.message intent (see registry_dispatch_test).
    let payload = b"2024-01-15 14:23:45\tsinity\thello world";
    let material_id = stage_material(&ctx, &content_store, "weechat.log", payload).await?;

    let cmd = parse_command(source_id, Some(material_id));
    let ack = request_parse_ack(&client, &subject, &cmd).await?;

    assert!(ack.accepted, "parse should be accepted: {:?}", ack.error);
    assert_eq!(
        ack.event_count,
        Some(1),
        "the real parser must emit one event from the loaded material bytes, not zero on empty bytes"
    );

    handle.abort();
    Ok(())
}

/// #1768 regression guard: a command for material that does not exist must fail
/// closed (accepted=false) instead of silently parsing empty bytes and acking a
/// successful zero-event replay.
#[sinex_test]
async fn parse_listener_fails_closed_on_missing_material(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let source_id = "weechat";
    let (handle, calls, subject, _content_store, _tmp) =
        spawn_recording_listener(&ctx, source_id).await?;
    let client = ctx.nats_client();

    let cmd = parse_command(source_id, Some(Uuid::now_v7()));
    let ack = request_parse_ack(&client, &subject, &cmd).await?;

    assert!(
        !ack.accepted,
        "missing material must fail closed, not report success"
    );
    let err = ack.error.expect("a diagnostic error is required");
    assert!(err.contains("not found"), "got: {err}");
    assert_eq!(
        calls.lock().unwrap().len(),
        0,
        "the parser must not run when material bytes cannot be loaded"
    );

    handle.abort();
    Ok(())
}

/// A command whose source_id does not match the listener is rejected without
/// invoking the parser.
#[sinex_test]
async fn parse_listener_rejects_mismatched_source(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let source_id = "weechat";
    let (handle, calls, subject, _content_store, _tmp) =
        spawn_recording_listener(&ctx, source_id).await?;
    let client = ctx.nats_client();

    let cmd = parse_command("desktop", Some(Uuid::now_v7()));
    let ack = request_parse_ack(&client, &subject, &cmd).await?;

    assert!(!ack.accepted, "mismatched source should be rejected");
    assert!(
        ack.error.unwrap().contains("does not match"),
        "error should explain the mismatch"
    );
    assert_eq!(
        calls.lock().unwrap().len(),
        0,
        "dispatch should not run for a mismatched source"
    );

    handle.abort();
    Ok(())
}
