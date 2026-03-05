use camino::Utf8PathBuf;
use serde_json::json;
use sinex_ingestd::{JetStreamTopology, config::IngestdConfig, service::IngestService};
use sinex_primitives::nats::NatsConnectionConfig;
use sinex_primitives::{
    Event, EventSource, EventType, HostName, Id, OffsetKind, Provenance, SourceMaterial,
};
use tempfile::TempDir;
use tokio::time::{Duration, timeout};
use xtask::sandbox::prelude::*;
use xtask::sandbox::timing::{Timeouts, WaitHelpers};

#[sinex_test]
async fn ingestd_processes_backlog_after_downtime(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().await?;
    let nats = ctx.nats_handle()?;
    let js = ctx.jetstream().await?;
    let env = ctx.env();

    let namespace = ctx.pipeline_namespace().prefix().to_string();
    let base_stream = env.nats_stream_name_with_namespace(Some(&namespace), "SINEX_RAW_EVENTS");
    let consumer_name = format!("ingestd-backlog-{namespace}");
    let topology = JetStreamTopology::new(
        env,
        base_stream.clone(),
        consumer_name.clone(),
        Some(&namespace),
    );

    let work_dir = TempDir::new()?;
    let work_dir_utf8 = Utf8PathBuf::from_path_buf(work_dir.path().to_path_buf())
        .unwrap_or_else(|_| Utf8PathBuf::from("/tmp"));
    let annex_path = work_dir_utf8.join("annex");
    let assembler_state_dir = work_dir_utf8.join("assembler_state");
    tokio::fs::create_dir_all(annex_path.as_std_path()).await?;
    tokio::fs::create_dir_all(assembler_state_dir.as_std_path()).await?;

    let config = IngestdConfig::builder()
        .database_url(ctx.database_url().to_string())
        .nats(
            NatsConnectionConfig::builder()
                .url(nats.client_url().to_string())
                .build(),
        )
        .nats_stream_name(base_stream)
        .nats_consumer_name(consumer_name)
        .nats_namespace(namespace)
        .consumer_fetch_max_messages(32)
        .consumer_fetch_timeout_ms(50.into())
        .validate_schemas(false)
        .skip_schema_sync(true)
        .work_dir(work_dir_utf8.clone())
        .annex_repo_path(annex_path)
        .assembler_state_dir(assembler_state_dir)
        .build();

    // Create the JetStream stream directly (instead of starting+stopping ingestd just for this)
    let stream_config = async_nats::jetstream::stream::Config {
        name: topology.events_stream.clone(),
        subjects: vec![topology.events_subject.clone()],
        ..Default::default()
    };
    js.get_or_create_stream(stream_config).await?;

    // Publish events to JetStream while ingestd is offline (the "backlog")
    let subject_prefix = topology.events_subject.trim_end_matches(".>");
    let subject = format!("{subject_prefix}.backlog.event");

    // Pre-register a source material for FK constraints
    let material_id = Id::<SourceMaterial>::new();
    let identifier = format!("backlog-source-{material_id}");
    sqlx::query!(
        r#"
        INSERT INTO raw.source_material_registry
            (id, material_kind, source_identifier, status, timing_info_type)
        VALUES ($1::uuid, 'annex', $2, 'completed', 'realtime')
        ON CONFLICT (id) DO NOTHING
        "#,
        material_id.to_uuid(),
        identifier,
    )
    .execute(&ctx.pool)
    .await?;

    for idx in 0..3 {
        let event = Event::<serde_json::Value> {
            id: Some(Id::new()),
            source: EventSource::new("backlog-source").expect("valid source"),
            event_type: EventType::new("backlog.event").expect("valid event type"),
            payload: json!({"seq": idx}),
            ts_orig: Some(sinex_primitives::Timestamp::now()),
            host: HostName::new("test-host"),
            node_version: Some("test".to_string()),
            payload_schema_id: None,
            provenance: Provenance::Material {
                id: material_id,
                anchor_byte: 0,
                offset_start: None,
                offset_end: None,
                offset_kind: OffsetKind::Byte,
            },
            associated_blob_ids: None,
        };
        let payload = serde_json::to_vec(&event)?;
        js.publish(subject.clone(), payload.into()).await?.await?;
    }

    let mut service = IngestService::new(config).await?;
    let mut runner = service.clone();
    let handle = tokio::spawn(async move { runner.run().await });

    WaitHelpers::wait_for_event_count(&ctx.pool, 3, Timeouts::STANDARD).await?;

    service.shutdown().await?;
    let join_result = timeout(Duration::from_secs(Timeouts::QUICK), handle)
        .await
        .map_err(|_| color_eyre::eyre::eyre!("ingestd runner shutdown timed out"))?;
    join_result??;

    Ok(())
}
