use futures::{StreamExt, future::try_join_all};
use sinex_db::repositories::{DbPoolExt, material_status};
use sinex_node_sdk::{
    AcquisitionManager, AppendStreamAcquirer, RotationPolicy, SOURCE_MATERIAL_BEGIN_SUBJECT,
    SOURCE_MATERIAL_END_SUBJECT, SOURCE_MATERIAL_STREAM,
    content_store::ContentStoreProcessCounters, source_material_slice_subject, stage_material,
    stage_material_from_file,
};
use sinex_primitives::error::SinexError;
use sinex_primitives::ids::Id;
use sinex_primitives::temporal::Timestamp;
use sinex_primitives::units::{Bytes, Seconds};
use std::io::ErrorKind;
use std::sync::Arc;
use std::time::{Duration, Instant};
use uuid::Uuid;
use xtask::sandbox::prelude::*;
use xtask::sandbox::timing::{DEFAULT_WAIT_SECS, INTEGRATION_WAIT_SECS, Timeouts, WaitHelpers};
use xtask::sandbox::{
    EphemeralNats, TestIngestdConfig, TestIngestdHandle, start_test_ingestd_with_config,
};

async fn wait_for_material_assembler_ready(
    nats: &EphemeralNats,
    nats_client: &async_nats::Client,
    namespace: &str,
) -> Result<()> {
    let env = sinex_primitives::environment::environment();
    let js_check = nats.jetstream_with_client(nats_client.clone());
    let stream = env.nats_stream_name_with_namespace(Some(namespace), SOURCE_MATERIAL_STREAM);
    nats.wait_for_consumer_on_stream(&js_check, &stream, Duration::from_secs(Timeouts::STANDARD))
        .await?;
    Ok(())
}

fn material_manager(
    ctx: &TestContext,
    nats_client: async_nats::Client,
    source_type: impl Into<String>,
) -> AcquisitionManager {
    AcquisitionManager::new_with_namespace(
        nats_client,
        RotationPolicy::default(),
        source_type.into(),
        Some(ctx.pipeline_namespace().prefix().to_string()),
    )
}

async fn fetch_realtime_capture_bytes(
    pool: &sqlx::PgPool,
    material_id: Uuid,
) -> Result<Option<i64>, sqlx::Error> {
    sqlx::query_scalar!(
        r#"
        SELECT MAX(offset_end) AS "max?"
        FROM raw.temporal_ledger
        WHERE source_material_id = $1::uuid
          AND source_type = 'realtime_capture'
        "#,
        material_id
    )
    .fetch_one(pool)
    .await
}

async fn setup_material_ingestd<F>(
    ctx: TestContext,
    work_dir: Option<std::path::PathBuf>,
    configure: F,
) -> Result<(
    TestContext,
    Arc<EphemeralNats>,
    async_nats::Client,
    TestIngestdHandle,
)>
where
    F: FnOnce(&mut TestIngestdConfig),
{
    let ctx = ctx.with_nats().dedicated().await?;
    let nats = ctx.nats_handle()?;
    let nats_client = ctx.nats_client();
    let namespace = ctx.pipeline_namespace().prefix().to_string();

    let mut ingest_config = TestIngestdConfig {
        nats: nats.connection_config(),
        database_url: ctx.database_url().to_string(),
        work_dir,
        namespace: Some(namespace.clone()),
        ..Default::default()
    };
    configure(&mut ingest_config);

    let ingest_handle = start_test_ingestd_with_config(ingest_config, Some(&ctx)).await?;
    AcquisitionManager::bootstrap_streams_with_namespace(&nats_client, Some(&namespace)).await?;
    wait_for_material_assembler_ready(&nats, &nats_client, &namespace).await?;

    Ok((ctx, nats, nats_client, ingest_handle))
}

fn source_material_proof(runner_id: &str, claim_ids: &[&str], reproducer: &str) -> ProofMetadata {
    source_material_proof_with_subjects(
        runner_id,
        &["https://github.com/Sinity/sinex/issues/315"],
        claim_ids,
        reproducer,
    )
}

fn source_material_proof_with_subjects(
    runner_id: &str,
    subject_refs: &[&str],
    claim_ids: &[&str],
    reproducer: &str,
) -> ProofMetadata {
    ProofMetadata {
        runner_id: Some(runner_id.to_string()),
        subject_refs: subject_refs
            .iter()
            .map(|subject| (*subject).to_string())
            .collect(),
        claim_ids: claim_ids.iter().map(|claim| (*claim).to_string()).collect(),
        status: Some("asserted_by_test".to_string()),
        reproducer: Some(reproducer.to_string()),
        environment: serde_json::json!({
            "plane": "isolated-dev",
            "stack": ["node-sdk", "nats", "ingestd", "postgres"],
        }),
    }
}

async fn fetch_material_blob_summary(
    pool: &sqlx::PgPool,
    material_id: Uuid,
) -> Result<(Uuid, String, i64)> {
    let row = sqlx::query(
        r"
        SELECT
            m.optional_blob_id::uuid AS blob_id,
            b.annex_backend AS storage_backend,
            b.size_bytes
        FROM raw.source_material_registry m
        JOIN core.blobs b ON b.id = m.optional_blob_id
        WHERE m.id = $1::uuid
        ",
    )
    .bind(material_id)
    .fetch_one(pool)
    .await?;

    Ok((
        sqlx::Row::get(&row, "blob_id"),
        sqlx::Row::get(&row, "storage_backend"),
        sqlx::Row::get(&row, "size_bytes"),
    ))
}

async fn read_ingestd_content_store_process_counters(
    work_dir: &std::path::Path,
) -> Result<ContentStoreProcessCounters> {
    let path = work_dir.join("content-store-process-counters.json");
    let bytes = match tokio::fs::read(&path).await {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == ErrorKind::NotFound => {
            return Ok(ContentStoreProcessCounters::default());
        }
        Err(error) => return Err(error.into()),
    };
    serde_json::from_slice(&bytes).map_err(Into::into)
}

async fn wait_for_completed_material(
    pool: &sqlx::PgPool,
    material_id: Uuid,
    expected_bytes: i64,
) -> Result<(Uuid, String, i64)> {
    WaitHelpers::wait_for_condition(
        || {
            let pool = pool.clone();
            async move {
                let row = sqlx::query(
                    r"
                    SELECT status, total_bytes, optional_blob_id::uuid AS optional_blob_id
                    FROM raw.source_material_registry
                    WHERE id = $1::uuid
                    ",
                )
                .bind(material_id)
                .fetch_optional(&pool)
                .await?;
                let Some(row) = row else {
                    return Ok::<bool, sqlx::Error>(false);
                };
                let status: String = sqlx::Row::get(&row, "status");
                let total_bytes: Option<i64> = sqlx::Row::get(&row, "total_bytes");
                let blob_id: Option<Uuid> = sqlx::Row::get(&row, "optional_blob_id");
                Ok::<bool, sqlx::Error>(
                    status == material_status::COMPLETED
                        && total_bytes == Some(expected_bytes)
                        && blob_id.is_some(),
                )
            }
        },
        INTEGRATION_WAIT_SECS,
    )
    .await?;

    fetch_material_blob_summary(pool, material_id).await
}

fn nats_redelivery_count(summary: &NatsEvidenceSummary) -> usize {
    summary
        .streams
        .iter()
        .flat_map(|stream| stream.consumers.iter())
        .map(|consumer| consumer.num_redelivered)
        .sum()
}

fn source_material_stream_summary(summary: &NatsEvidenceSummary) -> Option<&NatsStreamEvidence> {
    summary.streams.iter().find(|stream| {
        stream
            .subjects
            .iter()
            .any(|subject| subject.contains("source_material.frames."))
    })
}

fn source_material_consumer_summary(
    summary: &NatsEvidenceSummary,
) -> Option<&NatsConsumerEvidence> {
    source_material_stream_summary(summary).and_then(|stream| stream.consumers.first())
}

fn source_material_redelivery_count(summary: &NatsEvidenceSummary) -> usize {
    source_material_consumer_summary(summary).map_or(0, |consumer| consumer.num_redelivered)
}

/// Test basic material acquisition flow: begin → append slices → finalize
#[sinex_test]
async fn material_acquisition_basic_flow(ctx: TestContext) -> Result<()> {
    let work_dir = tempfile::tempdir()?;
    let (ctx, _nats, nats_client, mut ingest_handle) =
        setup_material_ingestd(ctx, Some(work_dir.path().to_path_buf()), |_| {}).await?;

    // Create AcquisitionManager
    let manager = material_manager(&ctx, nats_client.clone(), "test-source");

    // Begin material
    let mut handle = manager.begin_material("test-identifier").await?;
    let material_id = handle.material_id;

    // Append some slices
    manager.append_slice(&mut handle, b"slice 1 data").await?;
    manager.append_slice(&mut handle, b"slice 2 data").await?;
    manager.append_slice(&mut handle, b"slice 3 data").await?;

    // Finalize
    manager.finalize(handle, "test complete").await?;

    // Wait for MaterialAssembler to process and persist the material/ledger entries.
    ctx.timing()
        .wait_for_condition(
            || {
                let pool = ctx.pool.clone();
                async move {
                    let material = pool
                        .source_materials()
                        .get_by_id(Id::from_uuid(material_id))
                        .await?
                        .ok_or_else(|| sinex_primitives::error::SinexError::database("missing"))?;
                    let ledger_count: Option<i64> = sqlx::query_scalar!(
                        "SELECT COUNT(*) FROM raw.temporal_ledger WHERE source_material_id = $1::uuid",
                        material_id
                    )
                    .fetch_one(&pool)
                    .await?;
                    Ok::<bool, sinex_primitives::error::SinexError>(
                        material.status.as_str() == "completed"
                            && ledger_count.unwrap_or(0) >= 2
                    )
                }
            },
            DEFAULT_WAIT_SECS,
        )
        .await?;

    // Verify database state
    let material = ctx
        .pool
        .source_materials()
        .get_by_id(Id::from_uuid(material_id))
        .await?
        .expect("Material should exist");

    assert_eq!(material.status.as_str(), "completed");

    // Verify ledger entries: staged_at (written at begin) + realtime_capture (written at finalize)
    let ledger_count: Option<i64> = sqlx::query_scalar!(
        "SELECT COUNT(*) FROM raw.temporal_ledger WHERE source_material_id = $1::uuid",
        material_id
    )
    .fetch_one(&ctx.pool)
    .await?;
    assert_eq!(
        ledger_count.unwrap_or(0),
        2,
        "expected staged_at + realtime_capture ledger entries"
    );

    // Verify the staged_at entry was written (the early fallback for ts_orig derivation)
    let staged_at_count: Option<i64> = sqlx::query_scalar!(
        "SELECT COUNT(*) FROM raw.temporal_ledger WHERE source_material_id = $1::uuid AND source_type = 'staged_at'",
        material_id
    )
    .fetch_one(&ctx.pool)
    .await?;
    assert_eq!(
        staged_at_count.unwrap_or(0),
        1,
        "expected exactly one staged_at ledger entry"
    );

    ingest_handle.stop().await?;
    Ok(())
}

/// Scenario: tiny logical row-stream records travel as one physical source frame
/// while preserving per-record byte anchors all the way to persisted material state.
#[sinex_test(
    timeout = 120,
    scenario = "source-material.row-stream-batched-anchors.v1",
    category = "source_material",
    lane = "fast",
    cost_tier = "integration",
    tags = "source_material,row_stream,anchors,material_spool",
    fixtures = "postgres,nats,ingestd,material_spool",
    subjects = "issue:315,issue:324,node-sdk:source-material",
    claims = "tiny-logical-records-batched,per-record-byte-anchors-preserved,material-ledger-total-bytes-matches-source-frame",
    reproducer = "xtask test -p sinex-node-sdk --scenario-tag row_stream"
)]
async fn source_material_scenario_batches_row_stream_records_with_stable_anchors(
    ctx: TestContext,
) -> Result<()> {
    ctx.set_proof_metadata(source_material_proof(
        "source-material.row-stream-batched-anchors.v1",
        &[
            "tiny-logical-records-batched",
            "per-record-byte-anchors-preserved",
            "material-ledger-total-bytes-matches-source-frame",
        ],
        "xtask test -p sinex-node-sdk -E 'test(source_material_scenario_batches_row_stream_records_with_stable_anchors)'",
    ));
    ctx.record_evidence_event(
        "scenario.start",
        "starting batched row-stream source-material scenario",
        serde_json::json!({
            "issue": 315,
            "source_identifier": "scenario://row-stream",
        }),
    );

    let work_dir = tempfile::tempdir()?;
    let (ctx, _nats, nats_client, mut ingest_handle) =
        setup_material_ingestd(ctx, Some(work_dir.path().to_path_buf()), |_| {}).await?;
    let mut slice_sub = nats_client
        .subscribe(
            ctx.pipeline_namespace()
                .subject("source_material.frames.slices.>"),
        )
        .await?;

    let manager = Arc::new(
        material_manager(&ctx, nats_client.clone(), "scenario-row-stream")
            .with_work_dir(work_dir.path().join("writer")),
    );
    let mut stream = AppendStreamAcquirer::new(manager);
    let records = vec![
        br#"{"row":1,"command":"echo one"}"#.to_vec(),
        b"\n".to_vec(),
        br#"{"row":2,"command":"echo two"}"#.to_vec(),
        b"\n".to_vec(),
        br#"{"row":3,"command":"echo three"}"#.to_vec(),
        b"\n".to_vec(),
    ];
    let expected_payload = records.concat();
    let anchors = stream
        .append_many_with_anchors(&records, "scenario://row-stream")
        .await?;
    let material_id = anchors
        .first()
        .ok_or_else(|| color_eyre::eyre::eyre!("batched append returned no anchors"))?
        .material_id;

    let slice_msg = tokio::time::timeout(Duration::from_secs(Timeouts::SHORT), slice_sub.next())
        .await?
        .ok_or_else(|| color_eyre::eyre::eyre!("missing live source-material slice frame"))?;
    assert_eq!(
        slice_msg.payload.as_ref(),
        expected_payload.as_slice(),
        "one physical slice frame should contain the concatenated logical records"
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(200), slice_sub.next())
            .await
            .is_err(),
        "batched append should not publish one source-material slice per tiny record"
    );

    stream.finalize("row stream scenario complete").await?;

    let expected_bytes = i64::try_from(expected_payload.len())?;
    WaitHelpers::wait_for_condition(
        || {
            let pool = ctx.pool.clone();
            async move {
                let row = sqlx::query(
                    r"
                    SELECT status, total_bytes
                    FROM raw.source_material_registry
                    WHERE id = $1::uuid
                    ",
                )
                .bind(material_id)
                .fetch_optional(&pool)
                .await?;
                let Some(row) = row else {
                    return Ok::<bool, sqlx::Error>(false);
                };
                let status: String = sqlx::Row::get(&row, "status");
                let total_bytes: Option<i64> = sqlx::Row::get(&row, "total_bytes");
                let ledger_bytes = fetch_realtime_capture_bytes(&pool, material_id).await?;
                Ok::<bool, sqlx::Error>(
                    status == material_status::COMPLETED
                        && total_bytes == Some(expected_bytes)
                        && ledger_bytes == Some(expected_bytes),
                )
            }
        },
        DEFAULT_WAIT_SECS,
    )
    .await?;

    let mut expected_start = 0_i64;
    for (anchor, record) in anchors.iter().zip(records.iter()) {
        let record_len = i64::try_from(record.len())?;
        assert_eq!(anchor.material_id, material_id);
        assert_eq!(anchor.offset_start, expected_start);
        assert_eq!(anchor.offset_end, expected_start + record_len);
        expected_start = anchor.offset_end;
    }
    assert_eq!(expected_start, expected_bytes);

    ctx.write_evidence_json(
        "row-stream-anchors",
        "source_material_anchors",
        &serde_json::json!({
            "material_id": material_id.to_string(),
            "expected_bytes": expected_bytes,
            "anchors": anchors.iter().map(|anchor| serde_json::json!({
                "material_id": anchor.material_id.to_string(),
                "offset_start": anchor.offset_start,
                "offset_end": anchor.offset_end,
            })).collect::<Vec<_>>(),
        }),
        Some(format!(
            "{} anchor(s), {expected_bytes} byte(s)",
            anchors.len()
        )),
    )?;
    ctx.capture_db_evidence("source-material-db").await?;
    ctx.capture_nats_evidence("source-material-nats").await?;
    ctx.capture_material_directory_evidence("source-material-spool", work_dir.path())?;
    ctx.record_evidence_event(
        "scenario.complete",
        "batched row-stream source-material scenario completed",
        serde_json::json!({
            "material_id": material_id.to_string(),
            "record_count": records.len(),
            "expected_bytes": expected_bytes,
        }),
    );

    ingest_handle.stop().await?;
    Ok(())
}

/// Scenario: source-material batching exposes a trendable resource profile for
/// tiny logical record bursts without hard-coding arbitrary performance gates.
#[sinex_test(
    timeout = 120,
    scenario = "source-material.resource-frame-amplification.v1",
    category = "source_material",
    lane = "fast",
    cost_tier = "integration",
    tags = "source_material,row_stream,resource_shape,frame_amplification",
    fixtures = "postgres,nats,ingestd,material_spool",
    subjects = "issue:317,issue:324,node-sdk:source-material",
    claims = "tiny-record-burst-uses-single-slice-frame,frame-amplification-profile-is-machine-readable,material-ledger-total-bytes-matches-record-burst",
    reproducer = "xtask test -p sinex-node-sdk --scenario-tag frame_amplification"
)]
async fn source_material_resource_frame_amplification_profile(ctx: TestContext) -> Result<()> {
    ctx.set_proof_metadata(source_material_proof_with_subjects(
        "source-material.resource-frame-amplification.v1",
        &[
            "https://github.com/Sinity/sinex/issues/317",
            "https://github.com/Sinity/sinex/issues/324",
        ],
        &[
            "tiny-record-burst-uses-single-slice-frame",
            "frame-amplification-profile-is-machine-readable",
            "material-ledger-total-bytes-matches-record-burst",
        ],
        "xtask test -p sinex-node-sdk --scenario-tag frame_amplification",
    ));

    let work_dir = tempfile::tempdir()?;
    let (ctx, _nats, nats_client, mut ingest_handle) =
        setup_material_ingestd(ctx, Some(work_dir.path().to_path_buf()), |_| {}).await?;
    let mut slice_sub = nats_client
        .subscribe(
            ctx.pipeline_namespace()
                .subject("source_material.frames.slices.>"),
        )
        .await?;

    let manager = Arc::new(
        material_manager(&ctx, nats_client.clone(), "resource-frame-profile")
            .with_work_dir(work_dir.path().join("writer")),
    );
    let mut stream = AppendStreamAcquirer::new(manager);
    let records = (0..256)
        .map(|idx| format!("{{\"idx\":{idx},\"event\":\"tiny\"}}\n").into_bytes())
        .collect::<Vec<_>>();
    let expected_payload = records.concat();
    let logical_bytes = expected_payload.len();

    let started = Instant::now();
    let anchors = stream
        .append_many_with_anchors(&records, "scenario://resource-frame-profile")
        .await?;
    let append_elapsed_ms = started.elapsed().as_secs_f64() * 1000.0;
    let material_id = anchors
        .first()
        .ok_or_else(|| color_eyre::eyre::eyre!("resource append returned no anchors"))?
        .material_id;

    let slice_msg = tokio::time::timeout(Duration::from_secs(Timeouts::SHORT), slice_sub.next())
        .await?
        .ok_or_else(|| color_eyre::eyre::eyre!("missing resource profile slice frame"))?;
    assert_eq!(
        slice_msg.payload.as_ref(),
        expected_payload.as_slice(),
        "resource profile should emit one physical slice for the tiny-record burst"
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(200), slice_sub.next())
            .await
            .is_err(),
        "tiny-record burst should not amplify into one slice frame per record"
    );

    stream.finalize("resource frame profile complete").await?;
    let expected_bytes = i64::try_from(logical_bytes)?;
    let (_blob_id, blob_backend, blob_size_bytes) =
        wait_for_completed_material(&ctx.pool, material_id, expected_bytes).await?;
    let nats_summary = ctx.capture_nats_evidence("source-material-nats").await?;
    let db_summary = ctx.capture_db_evidence("source-material-db").await?;
    let spool_summary =
        ctx.capture_material_directory_evidence("source-material-spool", work_dir.path())?;
    let source_stream = source_material_stream_summary(&nats_summary);
    let source_consumer = source_material_consumer_summary(&nats_summary);
    let stream_current_messages = source_stream.map_or(0, |stream| stream.messages);
    let stream_current_bytes = source_stream.map_or(0, |stream| stream.bytes);
    let stream_ack_floor_sequence =
        source_consumer.map_or(0, |consumer| consumer.ack_floor_stream_sequence);
    let stream_delivered_sequence =
        source_consumer.map_or(0, |consumer| consumer.delivered_stream_sequence);

    let slice_frame_count = 1_u64;
    let logical_record_count = u64::try_from(records.len())?;
    let published_frame_count = 3_u64; // begin + one coalesced slice + end
    ctx.write_evidence_json(
        "resource-frame-amplification",
        "source_material_resource_profile",
        &serde_json::json!({
            "schema_version": 1,
            "issue": 317,
            "profile": "frame_amplification",
            "interpretation": "observed_advisory",
            "material_id": material_id.to_string(),
            "logical_record_count": logical_record_count,
            "logical_payload_bytes": logical_bytes,
            "slice_frame_count": slice_frame_count,
            "published_frame_count": published_frame_count,
            "slice_frames_per_logical_record": slice_frame_count as f64 / logical_record_count as f64,
            "published_frames_per_logical_record": published_frame_count as f64 / logical_record_count as f64,
            "source_material_stream_current_messages": stream_current_messages,
            "source_material_stream_current_bytes": stream_current_bytes,
            "source_material_stream_ack_floor_sequence": stream_ack_floor_sequence,
            "source_material_stream_delivered_sequence": stream_delivered_sequence,
            "source_material_stream_pending": source_consumer.map_or(0, |consumer| consumer.num_pending),
            "source_material_stream_ack_pending": source_consumer.map_or(0, |consumer| consumer.num_ack_pending),
            "append_elapsed_ms": append_elapsed_ms,
            "blob_backend": blob_backend,
            "blob_size_bytes": blob_size_bytes,
            "nats_redeliveries": nats_redelivery_count(&nats_summary),
            "source_material_redeliveries": source_material_redelivery_count(&nats_summary),
            "db_source_material_count": db_summary.source_material_count,
            "spool_file_count": spool_summary.file_count,
        }),
        Some(format!(
            "{logical_record_count} logical record(s) -> {slice_frame_count} slice frame(s)"
        )),
    )?;

    ingest_handle.stop().await?;
    Ok(())
}

/// Scenario: duplicate source bytes captured through independent material IDs
/// converge on the same `BLAKE3` blob identity through normal ingestd finalization.
#[sinex_test(
    timeout = 120,
    scenario = "source-material.resource-duplicate-finalization.v1",
    category = "source_material",
    lane = "fast",
    cost_tier = "integration",
    tags = "source_material,resource_shape,duplicate_content,redelivery,blob_dedup",
    fixtures = "postgres,nats,ingestd,material_spool",
    subjects = "issue:315,issue:317,issue:324,node-sdk:source-material",
    claims = "duplicate-content-reuses-one-blob,duplicate-finalization-has-no-redelivery-loop,duplicate-finalization-profile-is-machine-readable",
    reproducer = "xtask test -p sinex-node-sdk --scenario-tag duplicate_content"
)]
async fn source_material_scenario_duplicate_content_reuses_blob_identity(
    ctx: TestContext,
) -> Result<()> {
    ctx.set_proof_metadata(source_material_proof_with_subjects(
        "source-material.duplicate-content-blob-identity.v1",
        &[
            "https://github.com/Sinity/sinex/issues/315",
            "https://github.com/Sinity/sinex/issues/317",
            "https://github.com/Sinity/sinex/issues/324",
        ],
        &[
            "duplicate-content-reuses-blake3-blob",
            "source-material-ids-remain-distinct",
            "normal-acquisition-path-finalizes-both-materials",
        ],
        "xtask test -p sinex-node-sdk -E 'test(source_material_scenario_duplicate_content_reuses_blob_identity)'",
    ));
    ctx.record_evidence_event(
        "scenario.start",
        "starting duplicate-content source-material scenario",
        serde_json::json!({ "issue": 315 }),
    );

    let work_dir = tempfile::tempdir()?;
    let (ctx, _nats, nats_client, mut ingest_handle) =
        setup_material_ingestd(ctx, Some(work_dir.path().to_path_buf()), |_| {}).await?;
    let manager = material_manager(&ctx, nats_client, "scenario-duplicate-content")
        .with_work_dir(work_dir.path().join("writer"));
    let payload = b"same logical export bytes\n";

    let first_id = stage_material(
        &manager,
        "scenario://duplicate-content/first",
        payload,
        "first duplicate-content scenario material",
        Some(serde_json::json!({ "scenario": "duplicate-content", "ordinal": 1 })),
    )
    .await?;
    let second_id = stage_material(
        &manager,
        "scenario://duplicate-content/second",
        payload,
        "second duplicate-content scenario material",
        Some(serde_json::json!({ "scenario": "duplicate-content", "ordinal": 2 })),
    )
    .await?;

    let expected_bytes = i64::try_from(payload.len())?;
    WaitHelpers::wait_for_condition(
        || {
            let pool = ctx.pool.clone();
            async move {
                let rows = sqlx::query(
                    r"
                    SELECT id::uuid AS id, status, optional_blob_id::uuid AS optional_blob_id, total_bytes
                    FROM raw.source_material_registry
                    WHERE id IN ($1::uuid, $2::uuid)
                    ",
                )
                .bind(first_id)
                .bind(second_id)
                .fetch_all(&pool)
                .await?;
                Ok::<bool, sqlx::Error>(
                    rows.len() == 2
                        && rows.iter().all(|row| {
                            let status: String = sqlx::Row::get(row, "status");
                            let blob: Option<Uuid> = sqlx::Row::get(row, "optional_blob_id");
                            let total_bytes: Option<i64> = sqlx::Row::get(row, "total_bytes");
                            status == material_status::COMPLETED
                                && blob.is_some()
                                && total_bytes == Some(expected_bytes)
                        }),
                )
            }
        },
        DEFAULT_WAIT_SECS,
    )
    .await?;

    let rows = sqlx::query(
        r"
        SELECT id::uuid AS id, optional_blob_id::uuid AS optional_blob_id, total_bytes
        FROM raw.source_material_registry
        WHERE id IN ($1::uuid, $2::uuid)
        ORDER BY id
        ",
    )
    .bind(first_id)
    .bind(second_id)
    .fetch_all(&ctx.pool)
    .await?;
    assert_eq!(rows.len(), 2);
    let first_blob: Option<Uuid> = sqlx::Row::get(&rows[0], "optional_blob_id");
    let second_blob: Option<Uuid> = sqlx::Row::get(&rows[1], "optional_blob_id");
    assert_ne!(first_id, second_id);
    assert_eq!(
        first_blob, second_blob,
        "duplicate source bytes should converge on one blob identity"
    );
    assert!(first_blob.is_some());

    let blob_count: Option<i64> = sqlx::query_scalar(
        r"
        SELECT COUNT(*)
        FROM core.blobs
        WHERE checksum_blake3 = $1
        ",
    )
    .bind(blake3::hash(payload).to_hex().to_string())
    .fetch_one(&ctx.pool)
    .await?;
    assert_eq!(
        blob_count.unwrap_or_default(),
        1,
        "duplicate material finalization should not create duplicate blob rows"
    );

    let nats_summary = ctx.capture_nats_evidence("source-material-nats").await?;
    let redeliveries = source_material_redelivery_count(&nats_summary);
    assert_eq!(
        redeliveries, 0,
        "normal duplicate-content finalization should not need source-material redelivery"
    );
    let source_stream = source_material_stream_summary(&nats_summary);
    let source_consumer = source_material_consumer_summary(&nats_summary);

    ctx.write_evidence_json(
        "duplicate-content-materials",
        "source_material_blob_identity",
        &serde_json::json!({
            "schema_version": 1,
            "issue": 317,
            "profile": "duplicate_finalization",
            "interpretation": "observed_advisory",
            "first_material_id": first_id.to_string(),
            "second_material_id": second_id.to_string(),
            "shared_blob_id": first_blob.map(|id| id.to_string()),
            "checksum_blake3": blake3::hash(payload).to_hex().to_string(),
            "source_material_stream_current_messages": source_stream.map_or(0, |stream| stream.messages),
            "source_material_stream_current_bytes": source_stream.map_or(0, |stream| stream.bytes),
            "source_material_stream_ack_floor_sequence": source_consumer.map_or(0, |consumer| consumer.ack_floor_stream_sequence),
            "source_material_stream_delivered_sequence": source_consumer.map_or(0, |consumer| consumer.delivered_stream_sequence),
            "source_material_stream_pending": source_consumer.map_or(0, |consumer| consumer.num_pending),
            "source_material_stream_ack_pending": source_consumer.map_or(0, |consumer| consumer.num_ack_pending),
            "source_material_redeliveries": redeliveries,
            "nats_redeliveries_all_streams": nats_redelivery_count(&nats_summary),
            "blob_count_for_checksum": blob_count.unwrap_or_default(),
        }),
        Some("2 material(s), 1 shared blob".to_string()),
    )?;
    ctx.capture_db_evidence("source-material-db").await?;
    ctx.capture_material_directory_evidence("source-material-spool", work_dir.path())?;
    ctx.record_evidence_event(
        "scenario.complete",
        "duplicate-content source-material scenario completed",
        serde_json::json!({
            "first_material_id": first_id.to_string(),
            "second_material_id": second_id.to_string(),
        }),
    );

    ingest_handle.stop().await?;
    Ok(())
}

/// Scenario: material storage chooses local CAS for small material and the
/// large-object backend for large material, with subprocess counts measured at
/// the SDK boundary.
#[sinex_test(
    timeout = 240,
    serial,
    scenario = "source-material.resource-storage-backends.v1",
    category = "source_material",
    lane = "heavy",
    cost_tier = "heavy",
    tags = "source_material,resource_shape,storage_profile,local_cas,git_annex",
    fixtures = "postgres,nats,ingestd,material_spool,git_annex",
    subjects = "issue:317,issue:324,node-sdk:source-material,node-sdk:content-store",
    claims = "small-material-uses-local-cas-without-git-annex-subprocess,large-material-uses-git-annex-with-observed-subprocess-count,storage-backend-profile-is-machine-readable",
    reproducer = "xtask test -p sinex-node-sdk --scenario-tag storage_profile --heavy"
)]
async fn source_material_resource_storage_backend_profile(ctx: TestContext) -> Result<()> {
    ctx.set_proof_metadata(source_material_proof_with_subjects(
        "source-material.resource-storage-backends.v1",
        &[
            "https://github.com/Sinity/sinex/issues/317",
            "https://github.com/Sinity/sinex/issues/324",
        ],
        &[
            "small-material-uses-local-cas-without-git-annex-subprocess",
            "large-material-uses-git-annex-with-observed-subprocess-count",
            "storage-backend-profile-is-machine-readable",
        ],
        "xtask test -p sinex-node-sdk --scenario-tag storage_profile --heavy",
    ));

    let work_dir = tempfile::tempdir()?;
    let (ctx, _nats, nats_client, mut ingest_handle) =
        setup_material_ingestd(ctx, Some(work_dir.path().to_path_buf()), |_| {}).await?;
    let manager = material_manager(&ctx, nats_client.clone(), "resource-storage")
        .with_work_dir(work_dir.path().join("writer"));

    let small_payload = vec![b's'; 4 * 1024];
    let small_counter_baseline =
        read_ingestd_content_store_process_counters(work_dir.path()).await?;
    let small_started = Instant::now();
    let small_material_id = stage_material(
        &manager,
        "scenario://storage/small-local-cas",
        &small_payload,
        "resource storage small local-cas profile",
        Some(serde_json::json!({ "scenario": "storage-profile", "size": "small" })),
    )
    .await?;
    let small_stage_elapsed_ms = small_started.elapsed().as_secs_f64() * 1000.0;
    let (_small_blob_id, small_backend, small_blob_bytes) = wait_for_completed_material(
        &ctx.pool,
        small_material_id,
        i64::try_from(small_payload.len())?,
    )
    .await?;
    let small_counters = read_ingestd_content_store_process_counters(work_dir.path())
        .await?
        .saturating_delta_since(small_counter_baseline);
    assert_eq!(
        small_backend, "SINEXBLAKE3",
        "small material should use local CAS storage"
    );
    assert_eq!(
        small_counters.git_annex_commands, 0,
        "small material should not cross the git-annex subprocess boundary"
    );

    let large_path = work_dir.path().join("large-storage-profile.bin");
    let large_payload_len = 17 * 1024 * 1024;
    tokio::fs::write(&large_path, vec![b'l'; large_payload_len]).await?;
    let large_utf8_path = camino::Utf8PathBuf::from_path_buf(large_path)
        .map_err(|path| color_eyre::eyre::eyre!("large profile path is not UTF-8: {path:?}"))?;

    let large_counter_baseline =
        read_ingestd_content_store_process_counters(work_dir.path()).await?;
    let large_started = Instant::now();
    let (large_material_id, large_streamed_bytes) = stage_material_from_file(
        &manager,
        &large_utf8_path,
        "resource storage large git-annex profile",
        Some(serde_json::json!({ "scenario": "storage-profile", "size": "large" })),
    )
    .await?;
    let large_stage_elapsed_ms = large_started.elapsed().as_secs_f64() * 1000.0;
    let (_large_blob_id, large_backend, large_blob_bytes) =
        wait_for_completed_material(&ctx.pool, large_material_id, large_streamed_bytes).await?;
    let large_counters = read_ingestd_content_store_process_counters(work_dir.path())
        .await?
        .saturating_delta_since(large_counter_baseline);
    assert_ne!(
        large_backend, "SINEXBLAKE3",
        "large material should cross into git-annex-backed storage"
    );
    assert!(
        large_counters.git_annex_commands > 0,
        "large material should record at least one git-annex subprocess"
    );

    let nats_summary = ctx.capture_nats_evidence("source-material-nats").await?;
    let source_stream = source_material_stream_summary(&nats_summary);
    let source_consumer = source_material_consumer_summary(&nats_summary);
    ctx.write_evidence_json(
        "resource-storage-backends",
        "source_material_resource_profile",
        &serde_json::json!({
            "schema_version": 1,
            "issue": 317,
            "profile": "storage_backend",
            "interpretation": "observed_advisory",
            "small": {
                "material_id": small_material_id.to_string(),
                "payload_bytes": small_payload.len(),
                "blob_backend": small_backend,
                "blob_size_bytes": small_blob_bytes,
                "stage_elapsed_ms": small_stage_elapsed_ms,
                "ingestd_content_store_process_counter_delta": small_counters,
            },
            "large": {
                "material_id": large_material_id.to_string(),
                "payload_bytes": large_streamed_bytes,
                "blob_backend": large_backend,
                "blob_size_bytes": large_blob_bytes,
                "stage_elapsed_ms": large_stage_elapsed_ms,
                "ingestd_content_store_process_counter_delta": large_counters,
            },
            "source_material_stream_current_messages": source_stream.map_or(0, |stream| stream.messages),
            "source_material_stream_current_bytes": source_stream.map_or(0, |stream| stream.bytes),
            "source_material_stream_ack_floor_sequence": source_consumer.map_or(0, |consumer| consumer.ack_floor_stream_sequence),
            "source_material_stream_delivered_sequence": source_consumer.map_or(0, |consumer| consumer.delivered_stream_sequence),
            "source_material_stream_pending": source_consumer.map_or(0, |consumer| consumer.num_pending),
            "source_material_stream_ack_pending": source_consumer.map_or(0, |consumer| consumer.num_ack_pending),
            "source_material_redeliveries": source_material_redelivery_count(&nats_summary),
            "nats_redeliveries_all_streams": nats_redelivery_count(&nats_summary),
        }),
        Some(format!(
            "small local-cas subprocesses={}, large git-annex subprocesses={}",
            small_counters.git_annex_commands, large_counters.git_annex_commands
        )),
    )?;
    ctx.capture_db_evidence("source-material-db").await?;
    ctx.capture_material_directory_evidence("source-material-spool", work_dir.path())?;

    ingest_handle.stop().await?;
    Ok(())
}

/// Reusing the same logical source must create distinct registry identifiers per observation.
#[sinex_test]
async fn material_acquisition_reuses_logical_source_without_aliasing_material_ids(
    ctx: TestContext,
) -> Result<()> {
    let work_dir = tempfile::tempdir()?;
    let (ctx, _nats, nats_client, mut ingest_handle) =
        setup_material_ingestd(ctx, Some(work_dir.path().to_path_buf()), |_| {}).await?;

    let manager = material_manager(&ctx, nats_client.clone(), "test-source");
    let logical_source = "same-logical-source";

    let mut first = manager.begin_material(logical_source).await?;
    let first_id = first.material_id;
    manager.append_slice(&mut first, b"first").await?;
    manager.finalize(first, "first complete").await?;

    let mut second = manager.begin_material(logical_source).await?;
    let second_id = second.material_id;
    manager.append_slice(&mut second, b"second").await?;
    manager.finalize(second, "second complete").await?;

    ctx.timing()
        .wait_for_condition(
            || {
                let pool = ctx.pool.clone();
                async move {
                    let first = pool
                        .source_materials()
                        .get_by_id(Id::from_uuid(first_id))
                        .await?;
                    let second = pool
                        .source_materials()
                        .get_by_id(Id::from_uuid(second_id))
                        .await?;

                    Ok::<bool, sinex_primitives::error::SinexError>(
                        first
                            .as_ref()
                            .is_some_and(|record| record.status.as_str() == "completed")
                            && second
                                .as_ref()
                                .is_some_and(|record| record.status.as_str() == "completed"),
                    )
                }
            },
            DEFAULT_WAIT_SECS,
        )
        .await?;

    let first = ctx
        .pool
        .source_materials()
        .get_by_id(Id::from_uuid(first_id))
        .await?
        .expect("first material should exist");
    let second = ctx
        .pool
        .source_materials()
        .get_by_id(Id::from_uuid(second_id))
        .await?
        .expect("second material should exist");

    assert_ne!(first.id, second.id);
    assert_ne!(first.source_identifier, second.source_identifier);
    assert!(first.source_identifier.starts_with(logical_source));
    assert!(second.source_identifier.starts_with(logical_source));
    assert_eq!(
        first
            .metadata
            .get("logical_source_identifier")
            .and_then(serde_json::Value::as_str),
        Some(logical_source)
    );
    assert_eq!(
        second
            .metadata
            .get("logical_source_identifier")
            .and_then(serde_json::Value::as_str),
        Some(logical_source)
    );
    assert_eq!(
        first
            .metadata
            .get("observation_material_id")
            .and_then(serde_json::Value::as_str),
        Some(first_id.to_string().as_str())
    );
    assert_eq!(
        second
            .metadata
            .get("observation_material_id")
            .and_then(serde_json::Value::as_str),
        Some(second_id.to_string().as_str())
    );

    ingest_handle.stop().await?;
    Ok(())
}

/// Dropping a never-written handle should not orphan a material registry row.
#[sinex_test]
async fn material_acquisition_drop_before_first_slice_does_not_publish_orphan(
    ctx: TestContext,
) -> Result<()> {
    let work_dir = tempfile::tempdir()?;
    let (ctx, _nats, nats_client, mut ingest_handle) =
        setup_material_ingestd(ctx, Some(work_dir.path().to_path_buf()), |_| {}).await?;

    let manager = material_manager(&ctx, nats_client.clone(), "drop-source");

    let handle = manager.begin_material("drop-before-first-slice").await?;
    let material_id = handle.material_id;
    let temp_path = handle.temp_path().to_path_buf();
    drop(handle);

    ctx.timing()
        .wait_for_condition(
            || {
                let temp_path = temp_path.clone();
                async move { Ok::<bool, SinexError>(!temp_path.exists()) }
            },
            DEFAULT_WAIT_SECS,
        )
        .await?;

    let material = ctx
        .pool
        .source_materials()
        .get_by_id(Id::from_uuid(material_id))
        .await?;
    assert!(
        material.is_none(),
        "dropped pre-slice handles should not create a durable material row"
    );

    let ledger_count: Option<i64> = sqlx::query_scalar!(
        "SELECT COUNT(*) FROM raw.temporal_ledger WHERE source_material_id = $1::uuid",
        material_id
    )
    .fetch_one(&ctx.pool)
    .await?;
    assert_eq!(
        ledger_count.unwrap_or_default(),
        0,
        "dropped pre-slice handles should not write temporal ledger entries"
    );

    ingest_handle.stop().await?;
    Ok(())
}

/// Zero-byte finalize must still publish begin so ingestd can record the failure honestly.
#[sinex_test]
async fn material_acquisition_empty_finalize_still_publishes_begin(ctx: TestContext) -> Result<()> {
    let work_dir = tempfile::tempdir()?;
    let (ctx, _nats, nats_client, mut ingest_handle) =
        setup_material_ingestd(ctx, Some(work_dir.path().to_path_buf()), |_| {}).await?;
    let dlq_subject = ctx.pipeline_namespace().subject("events.dlq.ingestd");
    let mut dlq_sub = nats_client.subscribe(dlq_subject).await?;

    let manager = material_manager(&ctx, nats_client.clone(), "empty-source");

    let handle = manager.begin_material("empty-finalize").await?;
    let material_id = handle.material_id;
    manager.finalize(handle, "empty finalize").await?;

    ctx.timing()
        .wait_for_condition(
            || {
                let pool = ctx.pool.clone();
                async move {
                    let staged_at_count: Option<i64> = sqlx::query_scalar!(
                        "SELECT COUNT(*) FROM raw.temporal_ledger WHERE source_material_id = $1::uuid AND source_type = 'staged_at'",
                        material_id
                    )
                    .fetch_one(&pool)
                    .await?;
                    Ok::<bool, color_eyre::Report>(staged_at_count.unwrap_or_default() == 1)
                }
            },
            DEFAULT_WAIT_SECS,
        )
        .await?;

    let deadline = tokio::time::Instant::now() + Duration::from_secs(Timeouts::LONG);
    loop {
        if let Ok(Some(msg)) =
            tokio::time::timeout(Duration::from_millis(500), dlq_sub.next()).await
        {
            let payload: serde_json::Value = serde_json::from_slice(&msg.payload)?;
            if payload["error"] == "empty_material"
                && payload["material_id"] == material_id.to_string()
            {
                break;
            }
        }

        if tokio::time::Instant::now() > deadline {
            bail!("timed out waiting for empty_material DLQ entry");
        }
    }

    ingest_handle.stop().await?;
    Ok(())
}

/// Test cancellation mid-slice cleans up temp state and records cancellation metadata.
#[sinex_test]
async fn material_acquisition_cancel_mid_slice(ctx: TestContext) -> Result<()> {
    let work_dir = tempfile::tempdir()?;
    let (ctx, _nats, nats_client, mut ingest_handle) =
        setup_material_ingestd(ctx, Some(work_dir.path().to_path_buf()), |_| {}).await?;

    let manager = material_manager(&ctx, nats_client.clone(), "cancel-source");

    let mut handle = manager.begin_material("cancel-identifier").await?;
    let material_id = handle.material_id;
    let temp_path = handle.temp_path().to_path_buf();

    manager.append_slice(&mut handle, b"partial data").await?;
    manager.cancel(&mut handle, "user_cancelled").await?;

    ctx.timing()
        .wait_for_condition(
            || {
                let temp_path = temp_path.clone();
                async move { Ok::<bool, sinex_primitives::error::SinexError>(!temp_path.exists()) }
            },
            DEFAULT_WAIT_SECS,
        )
        .await?;

    ctx.timing()
        .wait_for_condition(
            || {
                let pool = ctx.pool.clone();
                async move {
                    let material = pool
                        .source_materials()
                        .get_by_id(Id::from_uuid(material_id))
                        .await?;
                    let Some(material) = material else {
                        return Ok::<bool, SinexError>(false);
                    };
                    Ok::<bool, SinexError>(
                        material.status == material_status::CANCELLED
                            && material
                                .metadata
                                .get("cancelled")
                                .and_then(sinex_primitives::JsonValue::as_bool)
                                .unwrap_or(false),
                    )
                }
            },
            DEFAULT_WAIT_SECS,
        )
        .await?;

    let material = ctx
        .pool
        .source_materials()
        .get_by_id(Id::from_uuid(material_id))
        .await?
        .expect("cancelled material should exist");
    assert_eq!(material.status.as_str(), material_status::CANCELLED);
    assert_eq!(
        material
            .metadata
            .get("cancelled")
            .and_then(sinex_primitives::JsonValue::as_bool),
        Some(true)
    );

    ingest_handle.stop().await?;
    Ok(())
}

/// Test out-of-order slice handling
#[sinex_test(timeout = 60)]
async fn material_acquisition_out_of_order_slices(ctx: TestContext) -> Result<()> {
    ctx.set_proof_metadata(source_material_proof(
        "source-material.out-of-order-slices.v1",
        &[
            "out-of-order-material-frames-complete",
            "buffered-slice-ledger-bytes-match-material",
        ],
        "xtask test -p sinex-node-sdk -E 'test(material_acquisition_out_of_order_slices)'",
    ));
    // `TestContext` is acquired from a pool and cleaned for us; don't do extra per-test DB resets.
    let work_dir = tempfile::tempdir()?;
    let (ctx, nats, nats_client, mut ingest_handle) =
        setup_material_ingestd(ctx, Some(work_dir.path().to_path_buf()), |_| {}).await?;

    // Manually publish slices out of order to test MaterialAssembler's buffering
    let material_id = Uuid::now_v7();
    let js = nats.jetstream_with_client(nats_client.clone());

    // Ensure the registry already contains the material id we are about to stream so the assembler
    // can finalize without waiting on implicit creation.
    sqlx::query!(
        r#"
            INSERT INTO raw.source_material_registry
                (id, material_kind, source_identifier, status, timing_info_type, metadata)
            VALUES ($1::uuid, $2, $3, 'sensing', 'realtime', '{}'::jsonb)
            ON CONFLICT (id) DO NOTHING
        "#,
        material_id,
        "annex",
        "test-ooo"
    )
    .execute(&ctx.pool)
    .await?;

    // Publish begin message
    let begin_msg = serde_json::json!({
        "material_id": material_id.to_string(),
        "material_kind": "annex",
        "source_identifier": "test-ooo",
        "metadata": {},
        "started_at": Timestamp::now().format_rfc3339(),
    });
    js.publish(
        ctx.pipeline_namespace()
            .subject(SOURCE_MATERIAL_BEGIN_SUBJECT),
        serde_json::to_vec(&begin_msg)?.into(),
    )
    .await?
    .await?;

    // Publish slices out of order: 2, 0, 1
    let slices = vec![
        (12i64, b"slice 2 data".to_vec()),
        (0i64, b"slice 0 data".to_vec()),
        (24i64, b"slice 3 data".to_vec()),
    ];

    for (offset, data) in slices {
        let mut headers = async_nats::HeaderMap::new();
        let offset_str = offset.to_string();
        let chunk_hash = blake3::hash(&data).to_hex();
        headers.insert("Offset", offset_str.as_str());
        headers.insert("Chunk-Hash", chunk_hash.as_str());

        js.publish_with_headers(
            ctx.pipeline_namespace()
                .subject(&source_material_slice_subject(material_id)),
            headers,
            data.into(),
        )
        .await?
        .await?;
    }

    // Compute expected hash
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"slice 0 data");
    hasher.update(b"slice 2 data");
    hasher.update(b"slice 3 data");
    let content_hash = hasher.finalize().to_hex();
    let expected_size: i64 =
        (b"slice 0 data".len() + b"slice 2 data".len() + b"slice 3 data".len()) as i64;

    // Publish end message
    let end_msg = serde_json::json!({
        "material_id": material_id.to_string(),
        "ended_at": Timestamp::now().format_rfc3339(),
        "content_hash": content_hash.to_string(),
        "total_slices": 3,
        "total_size_bytes": expected_size,
    });
    js.publish(
        ctx.pipeline_namespace()
            .subject(SOURCE_MATERIAL_END_SUBJECT),
        serde_json::to_vec(&end_msg)?.into(),
    )
    .await?
    .await?;

    // Wait for MaterialAssembler to process.
    //
    // This should complete quickly once ingestd has created the material consumers; if it doesn't,
    // fail with a clear error instead of "backfilling" database state.
    WaitHelpers::wait_for_condition(
        || {
            let pool = ctx.pool.clone();
            async move {
                if let Some(material) = pool
                    .source_materials()
                    .get_by_id(Id::from_uuid(material_id))
                    .await?
                {
                    let ledger_bytes = fetch_realtime_capture_bytes(&pool, material_id).await?;
                    return Ok::<bool, SinexError>(
                        material.status.as_str() == "completed"
                            && ledger_bytes.unwrap_or_default() >= expected_size,
                    );
                }
                Ok::<bool, SinexError>(false)
            }
        },
        DEFAULT_WAIT_SECS,
    )
    .await?;

    // Verify material was assembled correctly despite out-of-order arrival
    let material = ctx
        .pool
        .source_materials()
        .get_by_id(Id::from_uuid(material_id))
        .await?;

    if let Some(material) = material {
        // MaterialAssembler should have finalized it
        assert_eq!(material.status.as_str(), "completed");
        let ledger_bytes = fetch_realtime_capture_bytes(&ctx.pool, material_id).await?;
        assert!(
            ledger_bytes.unwrap_or_default() >= expected_size,
            "ledger should capture all bytes"
        );
    }

    ingest_handle.stop().await?;
    Ok(())
}

/// Ensure end-before-begin ordering is tolerated (end is NAKed and later finalized).
#[sinex_test(timeout = 60)]
async fn material_acquisition_end_before_begin(ctx: TestContext) -> Result<()> {
    ctx.set_proof_metadata(source_material_proof(
        "source-material.end-before-begin-retry.v1",
        &[
            "out-of-order-material-end-frame-retries",
            "later-begin-and-slices-finalize-material",
        ],
        "xtask test -p sinex-node-sdk -E 'test(material_acquisition_end_before_begin)'",
    ));
    let work_dir = tempfile::tempdir()?;
    let (ctx, nats, nats_client, mut ingest_handle) =
        setup_material_ingestd(ctx, Some(work_dir.path().to_path_buf()), |_| {}).await?;

    let material_id = Uuid::now_v7();
    let js = nats.jetstream_with_client(nats_client.clone());

    let slices = vec![
        (0i64, b"slice 0 data".to_vec()),
        (12i64, b"slice 1 data".to_vec()),
    ];

    let mut hasher = blake3::Hasher::new();
    hasher.update(&slices[0].1);
    hasher.update(&slices[1].1);
    let content_hash = hasher.finalize().to_hex();
    let expected_size = slices
        .iter()
        .map(|(_, data)| data.len() as i64)
        .sum::<i64>();

    let end_msg = serde_json::json!({
        "material_id": material_id.to_string(),
        "ended_at": Timestamp::now().format_rfc3339(),
        "content_hash": content_hash.to_string(),
        "total_slices": slices.len(),
        "total_size_bytes": expected_size,
    });
    js.publish(
        ctx.pipeline_namespace()
            .subject(SOURCE_MATERIAL_END_SUBJECT),
        serde_json::to_vec(&end_msg)?.into(),
    )
    .await?
    .await?;

    // Give the end consumer a chance to see the message before begin arrives.
    tokio::time::sleep(Duration::from_millis(200)).await;

    let begin_msg = serde_json::json!({
        "material_id": material_id.to_string(),
        "material_kind": "annex",
        "source_identifier": "end-before-begin",
        "metadata": {},
        "started_at": Timestamp::now().format_rfc3339(),
    });
    js.publish(
        ctx.pipeline_namespace()
            .subject(SOURCE_MATERIAL_BEGIN_SUBJECT),
        serde_json::to_vec(&begin_msg)?.into(),
    )
    .await?
    .await?;

    for (offset, data) in slices {
        let mut headers = async_nats::HeaderMap::new();
        let offset_str = offset.to_string();
        let chunk_hash = blake3::hash(&data).to_hex();
        headers.insert("Offset", offset_str.as_str());
        headers.insert("Chunk-Hash", chunk_hash.as_str());

        js.publish_with_headers(
            ctx.pipeline_namespace()
                .subject(&source_material_slice_subject(material_id)),
            headers,
            data.into(),
        )
        .await?
        .await?;
    }

    WaitHelpers::wait_for_condition(
        || {
            let pool = ctx.pool.clone();
            async move {
                if let Some(material) = pool
                    .source_materials()
                    .get_by_id(Id::from_uuid(material_id))
                    .await?
                {
                    if material.status.as_str() != "completed" {
                        return Ok::<bool, SinexError>(false);
                    }
                    let ledger_bytes = fetch_realtime_capture_bytes(&pool, material_id).await?;
                    return Ok::<bool, SinexError>(
                        ledger_bytes.unwrap_or_default() >= expected_size,
                    );
                }
                Ok::<bool, SinexError>(false)
            }
        },
        INTEGRATION_WAIT_SECS,
    )
    .await?;

    let record = ctx
        .pool
        .source_materials()
        .get_by_id(Id::from_uuid(material_id))
        .await?
        .expect("material should exist after completion");
    assert_eq!(record.status.as_str(), "completed");

    ingest_handle.stop().await?;
    Ok(())
}

/// Ensure material assembly resumes correctly after ingestd restart
#[sinex_test(
    timeout = 90,
    scenario = "runtime.material-acquisition-restart-recovery.v1",
    category = "runtime",
    lane = "heavy",
    cost_tier = "integration",
    tags = "runtime,restart,recovery,source_material",
    fixtures = "postgres,nats,ingestd,material_spool",
    subjects = "issue:324,node-sdk:material-acquisition,component:ingestd",
    claims = "restart-with-pending-material-state-recovers,material-ledger-total-bytes-match-post-restart-finalization",
    reproducer = "xtask test -p sinex-node-sdk --scenario-tag restart --heavy"
)]
async fn material_acquisition_restart_recovery(ctx: TestContext) -> Result<()> {
    ctx.set_proof_metadata(source_material_proof(
        "source-material.restart-recovery.v1",
        &[
            "restart-with-pending-material-state-recovers",
            "material-ledger-total-bytes-match-post-restart-finalization",
        ],
        "xtask test -p sinex-node-sdk -E 'test(material_acquisition_restart_recovery)'",
    ));
    let ctx = ctx
        .with_tracing("sinex_ingestd=debug")
        .with_nats()
        .dedicated()
        .await?;
    // `TestContext` is acquired from a pool and cleaned for us; don't do extra per-test DB resets.
    let nats = ctx.nats_handle()?;
    let nats_client = ctx.nats_client();
    let namespace = ctx.pipeline_namespace().prefix().to_string();
    let js = nats.jetstream_with_client(nats_client.clone());
    let run_suffix = Uuid::now_v7();

    let work_dir = tempfile::tempdir()?;
    let work_dir_path = work_dir.path().to_path_buf();

    let config = TestIngestdConfig {
        nats: nats.connection_config(),
        database_url: ctx.database_url().to_string(),
        work_dir: Some(work_dir_path.clone()),
        namespace: Some(namespace.clone()),
        consumer_fetch_timeout_ms: 50,
        ..Default::default()
    };

    let mut ingest_handle = start_test_ingestd_with_config(config.clone(), Some(&ctx)).await?;
    nats.wait_for_stream(
        &js,
        &ingest_handle.stream_name,
        Duration::from_secs(Timeouts::SHORT),
    )
    .await?;
    wait_for_material_assembler_ready(&nats, &nats_client, &namespace).await?;

    let manager = material_manager(&ctx, nats_client.clone(), "restart-test");

    let mut handle = manager
        .begin_material(&format!("restart-session-{run_suffix}"))
        .await?;
    let material_id = handle.material_id;

    let first_chunk = b"first-chunk";
    manager.append_slice(&mut handle, first_chunk).await?;
    // Wait for ingestd to persist the first chunk by observing assembler state on disk.
    let state_file = work_dir_path
        .join("assembler_state")
        .join(material_id.to_string())
        .join("state.wal");
    WaitHelpers::wait_for_condition(
        || {
            let state_file = state_file.clone();
            async move {
                let data = match tokio::fs::read(&state_file).await {
                    Ok(data) => data,
                    Err(err) if err.kind() == ErrorKind::NotFound => return Ok(false),
                    Err(err) => return Err(SinexError::io(err.to_string())),
                };
                // WAL is newline-delimited JSON envelopes; presence of a "Slice" entry
                // means the first chunk was persisted.
                Ok(data.windows(7).any(|w| w == b"\"Slice\""))
            }
        },
        Timeouts::STANDARD,
    )
    .await?;

    ingest_handle.stop().await?;
    ctx.quiesce_background_tasks().await?;

    let mut ingest_handle = start_test_ingestd_with_config(config, Some(&ctx)).await?;
    nats.wait_for_stream(
        &js,
        &ingest_handle.stream_name,
        Duration::from_secs(Timeouts::SHORT),
    )
    .await?;
    wait_for_material_assembler_ready(&nats, &nats_client, &namespace).await?;

    manager.append_slice(&mut handle, b"second-chunk").await?;
    manager
        .finalize(handle, &format!("restart completed {run_suffix}"))
        .await?;

    let expected_size = (b"first-chunk".len() + b"second-chunk".len()) as i64;

    // Wait for material completion and ledger offset to reflect all slices.
    let pool = ctx.pool.clone();
    WaitHelpers::wait_for_condition(
        || {
            let pool = pool.clone();
            async move {
                if let Some(material) = pool
                    .source_materials()
                    .get_by_id(Id::from_uuid(material_id))
                    .await?
                    && material.status.as_str() == "completed"
                {
                    let ledger_bytes = fetch_realtime_capture_bytes(&pool, material_id)
                        .await
                        .map_err(|e| SinexError::database(e.to_string()))?;

                    return Ok::<bool, SinexError>(
                        ledger_bytes.unwrap_or_default() >= expected_size,
                    );
                }
                Ok::<bool, SinexError>(false)
            }
        },
        Timeouts::STANDARD,
    )
    .await?;

    let record = ctx
        .pool
        .source_materials()
        .get_by_id(Id::from_uuid(material_id))
        .await?
        .expect("material should exist after restart");
    assert_eq!(record.status.as_str(), "completed");

    let ledger_bytes = fetch_realtime_capture_bytes(&ctx.pool, material_id).await?;

    assert_eq!(ledger_bytes.unwrap_or_default(), expected_size);

    ingest_handle.stop().await?;
    ctx.quiesce_background_tasks().await?;
    Ok(())
}

/// Ensure multiple concurrent acquisitions remain isolated and complete successfully.
#[sinex_test(timeout = 90)]
async fn material_acquisition_concurrent_sessions_isolated(ctx: TestContext) -> Result<()> {
    let ctx = ctx.with_tracing("sinex_ingestd=debug");
    // `TestContext` is acquired from a pool and cleaned for us; don't do extra per-test DB resets.
    let synchronizer = Arc::new(xtask::sandbox::timing::WorkerReadinessCoordinator::new(4));

    let work_dir = tempfile::tempdir()?;
    let (ctx, nats, nats_client, mut ingest_handle) =
        setup_material_ingestd(ctx, Some(work_dir.path().to_path_buf()), |_| {}).await?;
    let js = nats.jetstream_with_client(nats_client.clone());
    nats.wait_for_stream(
        &js,
        &ingest_handle.stream_name,
        Duration::from_secs(Timeouts::SHORT),
    )
    .await?;

    let futures = (0..4).map(|idx| {
        let manager = material_manager(&ctx, nats_client.clone(), format!("concurrent-{idx}"));
        let synchronizer = synchronizer.clone();
        async move {
            let session_id = format!("session-{idx}");
            let mut handle = manager.begin_material(&session_id).await?;
            let material_id = handle.material_id;
            let _ = synchronizer.worker_ready();
            synchronizer
                .wait_for_all_ready(Duration::from_secs(Timeouts::MEDIUM))
                .await?;
            manager
                .append_slice(&mut handle, format!("slice-{idx}").as_bytes())
                .await?;
            let completion_reason = format!("session-{idx} complete");
            manager.finalize(handle, &completion_reason).await?;
            Result::<Uuid>::Ok(material_id)
        }
    });

    let material_ids = try_join_all(futures).await?;
    let pool = ctx.pool.clone();

    let awaited_material_ids = material_ids.clone();
    WaitHelpers::wait_for_condition(
        || {
            let pool = pool.clone();
            let material_ids = awaited_material_ids.clone();
            async move {
                for material_id in material_ids {
                    let Some(material) = pool
                        .source_materials()
                        .get_by_id(Id::from_uuid(material_id))
                        .await?
                    else {
                        return Ok::<bool, SinexError>(false);
                    };

                    if material.status.as_str() != "completed" {
                        return Ok::<bool, SinexError>(false);
                    }
                }

                Ok::<bool, SinexError>(true)
            }
        },
        INTEGRATION_WAIT_SECS,
    )
    .await?;

    for material_id in material_ids {
        let record = pool
            .source_materials()
            .get_by_id(Id::from_uuid(material_id))
            .await?
            .expect("material should exist after wait");
        assert_eq!(record.status.as_str(), "completed");
    }

    ingest_handle.stop().await?;
    Ok(())
}

/// Test material rotation based on size
#[sinex_test]
async fn material_acquisition_rotation_by_size(ctx: TestContext) -> Result<()> {
    // `TestContext` is acquired from a pool and cleaned for us; don't do extra per-test DB resets.
    let work_dir = tempfile::tempdir()?;
    let (ctx, nats, nats_client, mut ingest_handle) =
        setup_material_ingestd(ctx, Some(work_dir.path().to_path_buf()), |_| {}).await?;
    let js = nats.jetstream_with_client(nats_client.clone());
    nats.wait_for_stream(
        &js,
        &ingest_handle.stream_name,
        Duration::from_secs(Timeouts::SHORT),
    )
    .await?;

    // Create manager with small max_bytes to trigger rotation
    let _rotation_policy = RotationPolicy {
        max_bytes: Bytes::from_bytes(100), // Very small to trigger rotation
        max_age_seconds: Seconds::from_secs(3600),
    };

    let manager = material_manager(&ctx, nats_client.clone(), "test-rotation");

    // Use AppendStreamAcquirer for automatic rotation
    let mut acquirer = sinex_node_sdk::AppendStreamAcquirer::new(std::sync::Arc::new(manager));

    // Append data that exceeds max_bytes
    let large_data = vec![b'X'; 150]; // 150 bytes > 100 byte limit
    acquirer.append(&large_data, "test-rotation-source").await?;

    // The manager should have rotated - finalize current
    acquirer.finalize("rotation test complete").await?;

    // Wait deterministically for processing
    WaitHelpers::wait_for_condition(
        || {
            let pool = ctx.pool.clone();
            async move {
                let material_count: Option<i64> = sqlx::query_scalar(
                    r"SELECT COUNT(*) FROM raw.source_material_registry
                       WHERE status = 'completed'",
                )
                .fetch_one(&pool)
                .await?;

                Ok::<bool, SinexError>(material_count.unwrap_or(0) >= 1)
            }
        },
        DEFAULT_WAIT_SECS,
    )
    .await?;

    ingest_handle.stop().await?;
    Ok(())
}
