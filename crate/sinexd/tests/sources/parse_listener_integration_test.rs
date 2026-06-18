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
use sinex_primitives::environment::environment;
use sinex_primitives::{ControlSubject, Uuid};
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
    // Address the listener on the exact subject the gateway replay engine
    // publishes to: the environment-namespaced control subject (#1780). The
    // bare `sinex.control.sources.{id}.parse` would never reach the listener
    // in any real environment.
    let subject = environment().nats_subject(&ControlSubject::source_parse(source_id));
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
    let (handle, calls, subject, content_store, _tmp) =
        spawn_recording_listener(&ctx, source_id).await?;
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

/// #1780 regression guard: the listener must bind the *environment-namespaced*
/// control subject the gateway replay engine publishes to, not the bare one.
///
/// The gateway sends parse-replay commands to
/// `env.nats_subject(ControlSubject::source_parse(id))` (e.g.
/// `dev.sinex.control.sources.weechat.parse`). A listener bound to the bare
/// `sinex.control.sources.weechat.parse` receives nothing and the gateway
/// request times out — the exact failure #1780 exists to eliminate. This test
/// fails on the pre-fix wiring: the bare subject would answer and the
/// namespaced subject would have no responder.
#[sinex_test]
async fn parse_listener_binds_gateway_namespaced_subject_not_bare(
    ctx: TestContext,
) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let source_id = "weechat";
    let (handle, _calls, namespaced_subject, content_store, _tmp) =
        spawn_recording_listener(&ctx, source_id).await?;
    let client = ctx.nats_client();

    let bare_subject = format!("sinex.control.sources.{source_id}.parse");
    assert_ne!(
        bare_subject, namespaced_subject,
        "the environment must namespace control subjects; otherwise this test is vacuous"
    );

    let payload = b"2024-01-15 14:23:45\tsinity\thello world";
    let material_id = stage_material(&ctx, &content_store, "weechat.log", payload).await?;

    // The bare subject must have no responder: the listener is not bound there.
    let bare_result = client
        .request(
            bare_subject.clone(),
            serde_json::to_vec(&parse_command(source_id, Some(material_id)))?.into(),
        )
        .await;
    assert!(
        bare_result.is_err(),
        "bare subject '{bare_subject}' must have no responder; the listener binds the namespaced subject"
    );

    // The gateway's namespaced subject must reach the live listener and ack.
    let ack = request_parse_ack(
        &client,
        &namespaced_subject,
        &parse_command(source_id, Some(material_id)),
    )
    .await?;
    assert!(
        ack.accepted,
        "namespaced gateway subject must reach a live subscriber: {:?}",
        ack.error
    );

    handle.abort();
    Ok(())
}

/// #1780 AC: the parse ack reports parse *acceptance and outcome*, not durable
/// completion of event application. The listener dispatches the parser and acks
/// with the parsed-intent count; it does not itself persist events. Durable
/// completion is observed separately by the replay path, which acks and then
/// polls operation state (`replay_control::execution::replay_writer`,
/// `dispatch_staged_source_replay`). This pins that an accepted ack leaves
/// `core.events` untouched for the parsed material.
#[sinex_test]
async fn parse_listener_ack_is_parse_outcome_not_durable_completion(
    ctx: TestContext,
) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let source_id = "weechat";
    let (handle, subject, content_store, _tmp) =
        spawn_listener(&ctx, source_id, default_parser_dispatch()).await?;
    let client = ctx.nats_client();

    let payload = b"2024-01-15 14:23:45\tsinity\thello world";
    let material_id = stage_material(&ctx, &content_store, "weechat.log", payload).await?;

    let cmd = parse_command(source_id, Some(material_id));
    let ack = request_parse_ack(&client, &subject, &cmd).await?;

    assert!(ack.accepted, "parse should be accepted: {:?}", ack.error);
    assert_eq!(
        ack.event_count,
        Some(1),
        "ack carries the parse outcome (intent count), the parse-acceptance signal"
    );

    // The ack is not durable event application: the listener discards the
    // parsed intents (it only counts them), so nothing reaches the event engine
    // and no row is persisted for this material.
    let persisted: i64 = sqlx::query_scalar!(
        "SELECT COUNT(*) FROM core.events WHERE source_material_id = $1",
        material_id
    )
    .fetch_one(ctx.pool())
    .await?
    .unwrap_or(0);
    assert_eq!(
        persisted, 0,
        "an accepted parse ack must not durably apply events; completion is observed separately by the replay path"
    );

    handle.abort();
    Ok(())
}
