//! Concurrency and ledger assertions for the material assembler.

use async_nats::jetstream;
use blake3::Hasher;
use futures::future::join_all;
use serde_json::json;
use sinex_ingestd::{IngestdResult, MaterialAssembler, MaterialReadySet};
use sinex_node_sdk::annex::{AnnexConfig, GitAnnex};
use sinex_primitives::{Uuid, temporal};
use sqlx::Row;
use std::sync::Arc;
use std::time::Duration;
use xtask::sandbox::prelude::*;
use xtask::sandbox::timing::{DEFAULT_WAIT_SECS, WaitHelpers};

async fn fake_annex() -> TestResult<(Arc<GitAnnex>, tempfile::TempDir)> {
    let dir = tempfile::tempdir()?;
    let repo_path = camino::Utf8PathBuf::from_path_buf(dir.path().to_path_buf())
        .map_err(|_| color_eyre::eyre::eyre!("tempdir not utf8"))?;
    GitAnnex::init(&repo_path, Some("assembler-concurrency")).await?;
    let annex = GitAnnex::new(AnnexConfig {
        repo_path,
        num_copies: None,
        large_files: None,
    })?;
    Ok((Arc::new(annex), dir))
}

async fn start_assembler(
    ctx: &TestContext,
    namespace: &str,
) -> TestResult<(
    tokio::task::JoinHandle<IngestdResult<()>>,
    jetstream::Context,
    tempfile::TempDir,
    tempfile::TempDir,
)> {
    let nats = ctx.nats_handle()?;
    let nats_client = ctx.nats_client();
    let js = nats.jetstream_with_client(nats_client.clone());
    let (annex, annex_dir) = fake_annex().await?;
    let state_dir = tempfile::tempdir()?;
    let assembler = MaterialAssembler::new(
        nats_client.clone(),
        ctx.pool.clone(),
        annex,
        state_dir.path().to_path_buf(),
        Some(namespace.to_string()),
        1_000,
        Some(MaterialReadySet::default()),
        100,
        512 * 1024 * 1024,
        300,
        3600,
        90,
    )?;

    let handle = tokio::spawn(async move { assembler.run().await });
    Ok((handle, js, annex_dir, state_dir))
}

#[sinex_test(timeout = 120, trace = true)]
async fn assembler_handles_concurrent_materials_and_records_ledger(
    ctx: TestContext,
) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let _nats_client = ctx.nats_client();
    let namespace = ctx.pipeline_namespace().prefix().to_string();
    let (handle, js, _annex_guard, _state_guard) = start_assembler(&ctx, &namespace).await?;
    let material_stream = ctx.pipeline_namespace().stream("SOURCE_MATERIAL");

    // Prepare three materials with predictable hashes/offsets.
    let material_ids: Vec<_> = (0..3).map(|_| uuid::Uuid::now_v7()).collect();
    let mut material_plans = Vec::new();
    for (idx, material_id) in material_ids.iter().enumerate() {
        let mut slices = Vec::new();
        let mut offset = 0i64;
        let mut hasher = Hasher::new();
        for slice_idx in 0..3 {
            let payload = format!("payload-{idx}-{slice_idx}").into_bytes();
            hasher.update(&payload);
            let current_offset = offset;
            offset += payload.len() as i64;
            slices.push((slice_idx, current_offset, payload));
        }
        let hash = hasher.finalize().to_hex().to_string();
        material_plans.push((*material_id, slices, offset, hash));
    }

    // Seed source_material_registry rows for the material IDs the assembler will finalize.
    for (material_id, _, _, _) in &material_plans {
        sqlx::query(
            r"
            INSERT INTO raw.source_material_registry
                (id, material_kind, source_identifier, status, timing_info_type, metadata, staged_at, start_time)
            VALUES ($1::uuid, 'annex', $2, 'sensing', 'realtime', '{}'::jsonb, NOW(), NOW())
            ON CONFLICT (id) DO NOTHING
            ",
        )
        .bind(*material_id)
        .bind(format!("test://concurrent/{material_id}"))
        .execute(&ctx.pool)
        .await?;
    }

    WaitHelpers::wait_for_condition(
        || {
            let js = js.clone();
            let material_stream = material_stream.clone();
            async move {
                let mut stream_handle = js
                    .get_stream(&material_stream)
                    .await
                    .map_err(|e| sinex_primitives::error::SinexError::network(e.to_string()))?;
                let info = stream_handle
                    .info()
                    .await
                    .map_err(|e| sinex_primitives::error::SinexError::network(e.to_string()))?;
                Ok::<bool, sinex_primitives::error::SinexError>(info.state.consumer_count > 0)
            }
        },
        DEFAULT_WAIT_SECS,
    )
    .await?;

    if let Ok(info) = js.get_stream(&material_stream).await?.info().await {
        assert_eq!(info.state.consumer_count, 1);
    }

    // Fire off begin messages for each material.
    for (material_id, _, _, _) in &material_plans {
        js.publish(
            ctx.pipeline_namespace()
                .subject("source_material.frames.begin"),
            json!({
                "material_id": material_id.to_string(),
                "material_kind": "annex",
                "source_identifier": format!("test://concurrent/{}", material_id),
                "metadata": {"idx": material_id.to_string()},
                "started_at": temporal::now().format_rfc3339(),
            })
            .to_string()
            .into(),
        )
        .await?
        .await?;
    }

    // Interleave slices across materials (out-of-order) to exercise buffering.
    let mut publish_futs = Vec::new();
    for slice_idx in 0..3 {
        for (material_id, slices, _, _) in &material_plans {
            let (_, offset, payload) = &slices[slice_idx];
            let payload = payload.clone();
            let mut headers = async_nats::HeaderMap::new();
            headers.insert("Nats-Msg-Id", format!("{material_id}-{slice_idx}").as_str());
            headers.insert("Slice-Index", slice_idx.to_string().as_str());
            headers.insert("Offset", offset.to_string().as_str());
            headers.insert("Chunk-Hash", "deadbeefcafebabe");

            let subject = ctx
                .pipeline_namespace()
                .subject(&format!("source_material.frames.slices.{material_id}"));
            publish_futs.push(js.publish_with_headers(subject, headers, payload.into()));
        }
    }
    for fut in join_all(publish_futs).await {
        fut?.await?;
    }

    // Send end markers.
    for (material_id, slices, total_size, hash) in &material_plans {
        js.publish(
            ctx.pipeline_namespace()
                .subject("source_material.frames.end"),
            json!({
                "material_id": material_id.to_string(),
                "ended_at": temporal::now().format_rfc3339(),
                "content_hash": hash,
                "total_slices": slices.len(),
                "total_size_bytes": total_size,
            })
            .to_string()
            .into(),
        )
        .await?
        .await?;
    }

    // Wait for ledger entries to appear for all materials, while also surfacing assembler failures.
    let ledger_wait = async {
        for (material_id, _, total_size, _) in &material_plans {
            let material_id_uuid = *material_id;
            let expected_size = *total_size;
            WaitHelpers::wait_for_condition(
                || {
                    let pool = ctx.pool.clone();
                    async move {
                        let row = sqlx::query!(
                            r#"
                            SELECT EXISTS(
                                SELECT 1
                                FROM raw.temporal_ledger
                                WHERE source_material_id = $1::uuid
                                  AND offset_start = 0
                                  AND offset_end = $2
                                  AND offset_kind = 'byte'
                            ) AS "exists!"
                            "#,
                            material_id_uuid,
                            expected_size,
                        )
                        .fetch_one(&pool)
                        .await?;

                        Ok::<bool, sqlx::Error>(row.exists)
                    }
                },
                30,
            )
            .await?;
        }
        Ok::<_, color_eyre::Report>(())
    };

    // Also ensure the assembler task stays healthy while we wait.
    let mut handle = handle;
    tokio::select! {
        res = ledger_wait => {
            res?;
            handle.abort();
            let _ = (&mut handle).await;
        }
        res = &mut handle => {
            match res {
                Ok(Ok(())) => color_eyre::eyre::bail!("assembler exited unexpectedly"),
                Ok(Err(e)) => return Err(e.into()),
                Err(join_err) if join_err.is_cancelled() => {
                    color_eyre::eyre::bail!("assembler task was cancelled")
                }
                Err(join_err) => return Err(join_err.into()),
            }
        }
        () = tokio::time::sleep(Duration::from_secs(90)) => {
            handle.abort();
            let _ = (&mut handle).await;
            color_eyre::eyre::bail!("timed out waiting for ledger entries");
        }
    }

    // DLQ should remain empty for valid slices.
    let base_stream = ctx.pipeline_namespace().stream("SINEX_RAW_EVENTS");
    let dlq_stream = format!("{base_stream}_DLQ");
    if let Ok(mut stream) = js.get_stream(&dlq_stream).await
        && let Ok(info) = stream.info().await
    {
        assert_eq!(
            info.state.messages, 0,
            "DLQ should stay empty for valid materials"
        );
    }

    // Source material rows should be finalized with blobs recorded.
    for (material_id, _, total_size, _) in &material_plans {
        let row = sqlx::query(
            r"
            SELECT
                status,
                optional_blob_id::uuid AS optional_blob_id,
                metadata->>'file_size_bytes' AS file_size
            FROM raw.source_material_registry
            WHERE id = $1::uuid
            ",
        )
        .bind(*material_id)
        .fetch_one(&ctx.pool)
        .await?;

        let status: Option<String> = row.try_get("status")?;
        let blob: Option<Uuid> = row.try_get("optional_blob_id")?;
        let file_size: Option<String> = row.try_get("file_size")?;

        assert_eq!(status.as_deref(), Some("completed"));
        assert!(blob.is_some(), "blob should be registered");
        assert_eq!(
            file_size.and_then(|v| v.parse::<i64>().ok()),
            Some(*total_size),
            "expected metadata to capture file size"
        );
    }

    Ok(())
}
