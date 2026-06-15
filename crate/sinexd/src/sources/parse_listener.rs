//! NATS listener for parse commands dispatched by the gateway replay engine.
//!
//! The gateway's replay execution publishes `SourceParseCommand` to the
//! environment-namespaced `{env}.sinex.control.sources.{source_id}.parse`
//! subject (`env.nats_subject(ControlSubject::source_parse(..))`) for
//! staged-source replay (#1060). This listener subscribes to that same
//! namespaced subject and dispatches to the appropriate parser capability via
//! the provided dispatch function. Binding the bare, un-namespaced subject
//! silently misses every gateway request (the environment name defaults to
//! `dev` and is never empty), which is the subscriber-timeout this listener
//! exists to eliminate (#1780).

use async_nats::Client;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use sinex_db::{Blob, DbPoolExt, SourceMaterialRecord};
use sinex_primitives::{ControlSubject, Id, Uuid};
use sqlx::PgPool;
use std::sync::Arc;
use tracing::{error, info, warn};

use crate::runtime::content_store::ContentStoreManager;
use crate::sources::dispatch::ParserDispatchFn;

/// Command dispatched by the gateway replay engine to request a source parse.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceParseCommand {
    pub operation_id: Uuid,
    pub source_id: String,
    pub source_material_id: Option<Uuid>,
    pub source_version: Option<String>,
    pub executor: String,
}

/// Response sent back to the gateway after accepting or rejecting a parse command.
#[derive(Debug, Serialize, Deserialize)]
pub struct SourceParseAck {
    pub accepted: bool,
    pub error: Option<String>,
    pub event_count: Option<usize>,
}

/// Subscribe to parse commands for a source and handle them.
///
/// Runs as a background task alongside the source's normal continuous
/// operation. Each source gets its own subscription so parse commands
/// for `source_id = "desktop"` only reach the desktop source.
///
/// `pool` and `content_store` are required so the listener can load the real
/// source-material bytes for each command from the registry → blob → CAS path;
/// a command for material that cannot be loaded fails closed (see
/// [`load_material_bytes`]) rather than parsing empty bytes.
pub async fn spawn_parse_listener(
    client: Client,
    source_id: &str,
    dispatch: ParserDispatchFn,
    pool: PgPool,
    content_store: Arc<ContentStoreManager>,
) -> Result<tokio::task::JoinHandle<()>, async_nats::SubscribeError> {
    // Subscribe on the environment-namespaced control subject so the gateway
    // replay engine — which publishes to
    // `env.nats_subject(ControlSubject::source_parse(..))` — actually reaches
    // this listener. The scan control path namespaces on both ends; parse must
    // match it or every gateway parse-replay request times out (#1780).
    let subject = sinex_primitives::environment::environment()
        .nats_subject(&ControlSubject::source_parse(source_id));
    let mut subscription = client.subscribe(subject.clone()).await?;

    let source_id = source_id.to_string();
    let client_clone = client.clone();

    Ok(tokio::spawn(async move {
        info!(
            source_id = %source_id,
            subject = %subject,
            "Parse listener started"
        );

        while let Some(message) = subscription.next().await {
            let client = client_clone.clone();
            let source_id = source_id.clone();
            let dispatch = dispatch.clone();
            let pool = pool.clone();
            let content_store = content_store.clone();

            tokio::spawn(async move {
                match handle_parse_command(
                    &client,
                    &source_id,
                    &message,
                    &dispatch,
                    &pool,
                    &content_store,
                )
                .await
                {
                    Ok(()) => {}
                    Err(e) => {
                        warn!(
                            target: "sinex_metrics",
                            metric = "source.parse_command_failures_total",
                            source_id = %source_id,
                            error = %e,
                            "Parse command handling failed"
                        );
                    }
                }
            });
        }

        warn!(
            source_id = %source_id,
            "Parse listener subscription closed"
        );
    }))
}

/// Load the raw bytes of a source material via registry → blob → CAS.
///
/// Fails closed: a material that is absent, has no associated blob, whose blob
/// row is missing, or whose content cannot be retrieved returns `Err`. The
/// listener turns any `Err` into `SourceParseAck { accepted: false, .. }` so a
/// parse-replay never silently "succeeds" with zero events on missing material.
async fn load_material_bytes(
    pool: &PgPool,
    content_store: &ContentStoreManager,
    material_id: Uuid,
) -> Result<Vec<u8>, String> {
    let material = pool
        .source_materials()
        .get_by_id(Id::<SourceMaterialRecord>::from_uuid(material_id))
        .await
        .map_err(|e| format!("failed to load source material {material_id}: {e}"))?
        .ok_or_else(|| format!("source material {material_id} not found in registry"))?;

    let blob_id = material.optional_blob_id.ok_or_else(|| {
        format!("source material {material_id} has no associated blob; cannot load bytes")
    })?;

    let blob = pool
        .blobs()
        .get_by_id(Id::<Blob>::from_uuid(blob_id))
        .await
        .map_err(|e| format!("failed to load blob {blob_id} for material {material_id}: {e}"))?
        .ok_or_else(|| format!("blob {blob_id} for material {material_id} not found"))?;

    content_store
        .retrieve_content(&blob.content_key())
        .await
        .map_err(|e| format!("failed to retrieve content for material {material_id}: {e}"))
}

/// Resolve a parse command into an ack: validate the source id, load real
/// material bytes, dispatch the parser, and report the outcome. All failure
/// paths return `accepted: false` with a diagnostic error.
async fn run_parse(
    listener_source_id: &str,
    cmd: &SourceParseCommand,
    dispatch: &ParserDispatchFn,
    pool: &PgPool,
    content_store: &ContentStoreManager,
) -> SourceParseAck {
    if cmd.source_id != listener_source_id {
        return SourceParseAck {
            accepted: false,
            error: Some(format!(
                "Parse command source_id '{}' does not match listener '{}'",
                cmd.source_id, listener_source_id
            )),
            event_count: None,
        };
    }

    let Some(material_id) = cmd.source_material_id else {
        return SourceParseAck {
            accepted: false,
            error: Some(
                "parse command has no source_material_id; cannot load material bytes".to_string(),
            ),
            event_count: None,
        };
    };

    let bytes = match load_material_bytes(pool, content_store, material_id).await {
        Ok(bytes) => bytes,
        Err(e) => {
            warn!(
                target: "sinex_metrics",
                metric = "source.parse_material_load_failures_total",
                operation_id = %cmd.operation_id,
                source_id = %cmd.source_id,
                material_id = %material_id,
                error = %e,
                "Parse material load failed"
            );
            return SourceParseAck {
                accepted: false,
                error: Some(e),
                event_count: None,
            };
        }
    };

    match dispatch(&cmd.source_id, &bytes, cmd.source_material_id) {
        Ok(outcome) => {
            info!(
                operation_id = %cmd.operation_id,
                parser = %outcome.parser_id,
                version = %outcome.parser_version,
                event_count = outcome.events.len(),
                material_bytes = bytes.len(),
                "Parse completed"
            );
            SourceParseAck {
                accepted: true,
                error: None,
                event_count: Some(outcome.events.len()),
            }
        }
        Err(e) => {
            warn!(
                target: "sinex_metrics",
                metric = "source.parse_dispatch_failures_total",
                operation_id = %cmd.operation_id,
                source_id = %cmd.source_id,
                error = %e,
                "Parse dispatch failed"
            );
            SourceParseAck {
                accepted: false,
                error: Some(e),
                event_count: None,
            }
        }
    }
}

async fn handle_parse_command(
    client: &Client,
    source_id: &str,
    message: &async_nats::Message,
    dispatch: &ParserDispatchFn,
    pool: &PgPool,
    content_store: &ContentStoreManager,
) -> Result<(), Box<dyn std::error::Error>> {
    let reply_subject = message.reply.clone();

    let ack = match serde_json::from_slice::<SourceParseCommand>(&message.payload) {
        Ok(cmd) => {
            info!(
                operation_id = %cmd.operation_id,
                source_id = %cmd.source_id,
                material_id = ?cmd.source_material_id,
                "Received parse command"
            );

            run_parse(source_id, &cmd, dispatch, pool, content_store).await
        }
        Err(e) => {
            warn!(
                target: "sinex_metrics",
                metric = "source.parse_command_deser_failures_total",
                error = %e,
                "Failed to deserialize parse command"
            );
            SourceParseAck {
                accepted: false,
                error: Some(format!("Invalid parse command payload: {e}")),
                event_count: None,
            }
        }
    };

    if let Some(reply) = reply_subject {
        let payload = serde_json::to_vec(&ack)?;
        if let Err(e) = client.publish(reply, payload.into()).await {
            error!(
                target: "sinex_metrics",
                metric = "source.parse_ack_failures_total",
                error = %e,
                "Failed to send parse command ack"
            );
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
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
}
