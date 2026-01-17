// Ingestd test utilities for integration testing
// Provides test handles for ingestd instances

use std::sync::Arc;
use std::time::Duration;

use crate::TestResult;
use camino::Utf8PathBuf;
use sinex_core::types::{error::SinexError, Seconds, Ulid};
use sinex_ingestd::{config::IngestdConfig, service::IngestService};
use tokio::process::Child;
use tokio::sync::Mutex as AsyncMutex;
use tokio::task::JoinHandle;

// Re-export StreamMessage for convenience

/// Configuration for test ingestd instance
#[derive(Debug, Clone)]
pub struct TestIngestdConfig {
    /// NATS connection configuration (includes TLS settings if needed)
    pub nats: sinex_core::nats::NatsConnectionConfig,
    pub database_url: String,
    pub work_dir: Option<std::path::PathBuf>,
    pub batch_size: usize,
    pub batch_timeout_secs: Seconds,
    pub consumer_fetch_timeout_ms: u64,
    pub consumer_fetch_max_messages: usize,
    pub namespace: Option<String>,
}

impl TestIngestdConfig {
    /// Create config with a simple NATS URL (no TLS)
    pub fn with_nats_url(nats_url: impl Into<String>) -> Self {
        Self {
            nats: sinex_core::nats::NatsConnectionConfig::builder()
                .url(nats_url.into())
                .build(),
            ..Default::default()
        }
    }
}

impl Default for TestIngestdConfig {
    fn default() -> Self {
        Self {
            nats: sinex_core::nats::NatsConnectionConfig::default(),
            database_url: "postgresql:///sinex_test?host=/run/postgresql".to_string(),
            work_dir: None,
            batch_size: 1,
            batch_timeout_secs: Seconds::from_secs(1),
            consumer_fetch_timeout_ms: 1_000,
            consumer_fetch_max_messages: 100,
            namespace: None,
        }
    }
}

/// Handle for a test ingestd process
pub struct TestIngestdHandle {
    pub stream_name: String,
    process: Option<Child>,
    service: Option<IngestService>,
    join_handle: Arc<AsyncMutex<Option<JoinHandle<()>>>>,
    _work_dir: Option<tempfile::TempDir>,
    nats_config: sinex_core::nats::NatsConnectionConfig,
    cleanup_streams: Vec<String>,
}

impl TestIngestdHandle {
    /// Stop the ingestd process
    pub async fn stop(&mut self) -> TestResult<()> {
        tracing::info!(stream = %self.stream_name, "Stopping test ingestd");
        if let Some(service) = self.service.as_mut() {
            service.shutdown().await?;
        }

        if let Some(mut process) = self.process.take() {
            let _ = process.kill().await;
        }

        if let Some(join) = self.join_handle.lock().await.take() {
            if let Err(join_err) = join.await {
                return Err(
                    SinexError::service(format!("ingestd task join error: {join_err}")).into(),
                );
            }
        }

        if !self.cleanup_streams.is_empty() {
            match self.nats_config.connect().await {
                Ok(client) => {
                    let js = async_nats::jetstream::new(client);
                    for stream in &self.cleanup_streams {
                        if let Err(err) = js.delete_stream(stream).await {
                            tracing::debug!(
                                stream = stream.as_str(),
                                error = %err,
                                "Failed to delete JetStream stream during ingestd cleanup"
                            );
                        }
                    }
                }
                Err(err) => {
                    tracing::warn!(
                        url = %self.nats_config.url,
                        error = %err,
                        "Failed to connect to NATS for ingestd cleanup"
                    );
                }
            }
        }
        tracing::info!(stream = %self.stream_name, "Test ingestd stopped");
        Ok(())
    }
}

impl Drop for TestIngestdHandle {
    fn drop(&mut self) {
        // Best effort cleanup
        if let Some(mut process) = self.process.take() {
            let _ = process.start_kill();
        }
    }
}

/// Start a test ingestd instance with custom configuration
pub async fn start_test_ingestd_with_config(
    config: TestIngestdConfig,
    ctx: Option<&crate::TestContext>,
) -> TestResult<TestIngestdHandle> {
    let work_dir_temp = match &config.work_dir {
        Some(_existing) => None,
        None => Some(
            tempfile::tempdir()
                .map_err(|e| SinexError::service(format!("failed to create temp work dir: {e}")))?,
        ),
    };

    let work_dir_path = config
        .work_dir
        .clone()
        .or_else(|| work_dir_temp.as_ref().map(|d| d.path().to_path_buf()))
        .ok_or_else(|| SinexError::service("failed to resolve ingestd work dir"))?;

    let work_dir = Utf8PathBuf::try_from(work_dir_path)
        .map_err(|e| SinexError::configuration(e.to_string()))?;

    let namespace = config
        .namespace
        .clone()
        .unwrap_or_else(|| format!("ingestd-{}", Ulid::new()));

    let events_consumer = namespaced_consumer_name(&namespace, "ingestd");
    let env = sinex_core::environment::environment();
    let stream_name = env.nats_stream_name_with_namespace(Some(&namespace), "SINEX_RAW_EVENTS");
    let material_begin_stream =
        env.nats_stream_name_with_namespace(Some(&namespace), "SOURCE_MATERIAL_BEGIN");
    let material_slices_stream =
        env.nats_stream_name_with_namespace(Some(&namespace), "SOURCE_MATERIAL_SLICES");
    let material_end_stream =
        env.nats_stream_name_with_namespace(Some(&namespace), "SOURCE_MATERIAL_END");
    let confirmations_stream = format!("{stream_name}_CONFIRMATIONS");
    let dlq_stream = format!("{stream_name}_DLQ");

    tracing::debug!("Starting ingestd with NATS {}", config.nats.url);
    eprintln!("ingestd connecting to {}", config.nats.url);
    let mut attempts = 0;
    let nats_client = loop {
        attempts += 1;
        // Extract host:port for TCP check, handling both nats:// and tls:// schemes
        let host_port = config
            .nats
            .url
            .trim_start_matches("nats://")
            .trim_start_matches("tls://");
        if let Err(err) = tokio::net::TcpStream::connect(host_port).await {
            eprintln!("tcp connect failed (attempt {attempts}): {err}");
        }
        match config.nats.connect().await {
            Ok(client) => break client,
            Err(err) if attempts < 10 => {
                eprintln!("retrying ingestd NATS connect (attempt {attempts}): {err}");
                tokio::time::sleep(Duration::from_millis(50)).await;
                continue;
            }
            Err(err) => {
                return Err(SinexError::network(format!("Failed to connect to NATS: {err}")).into())
            }
        }
    };
    let jetstream = async_nats::jetstream::new(nats_client.clone());
    let _ = jetstream.delete_stream(&stream_name).await;
    let _ = jetstream.delete_stream(&material_begin_stream).await;
    let _ = jetstream.delete_stream(&material_slices_stream).await;
    let _ = jetstream.delete_stream(&material_end_stream).await;
    let _ = jetstream.delete_stream(&confirmations_stream).await;
    let _ = jetstream.delete_stream(&dlq_stream).await;

    let annex_path = work_dir.join("annex");
    let state_dir = work_dir.join("assembler_state");

    let mut ingest_config = IngestdConfig::builder()
        .database_url(config.database_url.clone())
        .nats(config.nats.clone())
        .batch_size(config.batch_size)
        .batch_timeout_secs(config.batch_timeout_secs)
        .consumer_fetch_timeout_ms(config.consumer_fetch_timeout_ms.into())
        .consumer_fetch_max_messages(config.consumer_fetch_max_messages)
        .validate_schemas(false)
        .skip_schema_sync(true)
        .work_dir(work_dir)
        .annex_repo_path(annex_path)
        .assembler_state_dir(state_dir)
        .nats_stream_name(stream_name.to_string())
        .build();

    ingest_config.nats_namespace = config.namespace.clone();
    ingest_config.nats_consumer_name = events_consumer.clone();

    let cleanup_streams = vec![
        ingest_config.nats_stream_name.clone(),
        confirmations_stream.clone(),
        dlq_stream.clone(),
        material_begin_stream.clone(),
        material_slices_stream.clone(),
        material_end_stream.clone(),
    ];

    let service = IngestService::new(ingest_config.clone()).await?;

    let mut service_runner = service.clone();
    let join_handle = tokio::spawn(async move {
        if let Err(err) = service_runner.run().await {
            tracing::warn!(error = %err, "ingestd service runner exited with error");
        }
    });

    // Verify service is ready by checking the events stream + consumer exist.
    // Material streams/consumers are created by MaterialAssembler and may lag; tests that rely on
    // material handling already wait for completion.
    tokio::time::timeout(std::time::Duration::from_secs(15), async {
        loop {
            if join_handle.is_finished() {
                return Err(SinexError::service(
                    "ingestd runner exited before becoming ready",
                ));
            }

            let events_stream = match jetstream.get_stream(&ingest_config.nats_stream_name).await {
                Ok(stream) => stream,
                Err(_) => {
                    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                    continue;
                }
            };

            match events_stream
                .get_consumer::<async_nats::jetstream::consumer::pull::Config>(
                    &ingest_config.nats_consumer_name,
                )
                .await
            {
                Ok(_) => return Ok(()),
                Err(_) => {
                    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                    continue;
                }
            }
        }
    })
    .await
    .map_err(|_| SinexError::service("ingestd consumer did not become ready"))??;

    let handle = TestIngestdHandle {
        stream_name: ingest_config.nats_stream_name,
        process: None,
        service: Some(service),
        join_handle: Arc::new(AsyncMutex::new(Some(join_handle))),
        _work_dir: work_dir_temp,
        nats_config: config.nats.clone(),
        cleanup_streams,
    };

    if let Some(ctx) = ctx {
        let join_arc = handle.join_handle.clone();
        ctx.register_background_task(
            "ingestd-runner",
            tokio::spawn(async move {
                if let Some(join) = join_arc.lock().await.take() {
                    let _ = join.await;
                }
            }),
        )
        .await;
    }

    Ok(handle)
}

fn namespaced_consumer_name(namespace: &str, base: &str) -> String {
    format!("{}_{}", sanitize_namespace_token(namespace), base)
}

fn sanitize_namespace_token(namespace: &str) -> String {
    namespace
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() {
                c.to_ascii_uppercase()
            } else {
                '_'
            }
        })
        .collect()
}

// Comprehensive node management tests
#[cfg(test)]
mod tests {
    use super::*;
    use crate::prelude::*;
    use crate::sinex_test;
    use crate::SinexError;

    #[sinex_test]
    async fn test_ingestd_config_default() -> TestResult<()> {
        let config = TestIngestdConfig::default();

        assert_eq!(config.nats.url, "nats://localhost:4222");
        assert_eq!(
            config.database_url,
            "postgresql:///sinex_test?host=/run/postgresql"
        );

        Ok(())
    }

    #[sinex_test]
    async fn test_ingestd_handle_creation(ctx: TestContext) -> TestResult<()> {
        use crate::nats::EphemeralNats;

        let nats = EphemeralNats::start().await?;
        let work_dir = tempfile::tempdir()
            .map_err(|e| SinexError::service(format!("failed to create temp work dir: {e}")))?;

        let config = TestIngestdConfig {
            nats: nats.connection_config(),
            database_url: ctx.database_url().to_string(),
            work_dir: Some(work_dir.path().to_path_buf()),
            ..Default::default()
        };

        let mut handle = start_test_ingestd_with_config(config.clone(), Some(&ctx)).await?;

        assert!(!handle.stream_name.is_empty());
        handle.stop().await?;

        Ok(())
    }

    #[sinex_test]
    async fn test_ingestd_handle_stop(ctx: TestContext) -> TestResult<()> {
        use crate::nats::EphemeralNats;

        let nats = EphemeralNats::start().await?;
        let work_dir = tempfile::tempdir()
            .map_err(|e| SinexError::service(format!("failed to create temp work dir: {e}")))?;

        let config = TestIngestdConfig {
            nats: nats.connection_config(),
            database_url: ctx.database_url().to_string(),
            work_dir: Some(work_dir.path().to_path_buf()),
            ..Default::default()
        };
        let mut handle = start_test_ingestd_with_config(config, Some(&ctx)).await?;

        // Should be able to stop without error
        handle.stop().await?;

        // Multiple stops should be ok
        handle.stop().await?;

        Ok(())
    }

    #[sinex_test]
    fn test_ingestd_handle_drop() -> TestResult<()> {
        // Test that drop doesn't panic even with no process
        let handle = TestIngestdHandle {
            stream_name: "test-stream".to_string(),
            process: None,
            service: None,
            join_handle: Arc::new(AsyncMutex::new(None)),
            _work_dir: None,
            nats_config: sinex_core::nats::NatsConnectionConfig::default(),
            cleanup_streams: Vec::new(),
        };

        drop(handle); // Should not panic
        Ok(())
    }
}
