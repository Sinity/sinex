//! Concurrency and ledger assertions for the material assembler.

use async_nats::jetstream;
use blake3::Hasher;
use futures::future::join_all;
use serde_json::json;
use sinex_ingestd::{IngestdResult, MaterialAssembler};
use sinex_satellite_sdk::annex::{AnnexConfig, GitAnnex};
use sinex_test_utils::prelude::*;
use sqlx::Row;
use std::sync::Arc;
use std::time::Duration;

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
) -> TestResult<(
    tokio::task::JoinHandle<IngestdResult<()>>,
    jetstream::Context,
    tempfile::TempDir,
    tempfile::TempDir,
    String,
    String,
)> {
    let nats = ctx.nats_client();
    let js = jetstream::new(nats.clone());
    let (annex, annex_dir) = fake_annex().await?;
    let state_dir = tempfile::tempdir()?;
    let prefix = format!(
        "{}CONC_{}",
        sinex_core::environment().nats_stream_name(""),
        uuid::Uuid::new_v4()
    );
    let subject_prefix = format!("test_material.{}", uuid::Uuid::new_v4());
    let assembler = MaterialAssembler::with_prefix(
        nats.clone(),
        ctx.pool.clone(),
        annex,
        state_dir.path().to_path_buf(),
        Some(prefix.clone()),
        Some(subject_prefix.clone()),
    )?;

    let handle = tokio::spawn(async move { assembler.run().await });

    // Ensure streams exist with the same configuration the assembler will
    // create so publishes never see "no stream found" races.
    let env = ctx.env();
    let begin_stream = format!("{prefix}BEGIN");
    let slices_stream = format!("{prefix}SLICES");
    let end_stream = format!("{prefix}END");

    js.get_or_create_stream(jetstream::stream::Config {
        name: begin_stream.clone(),
        subjects: vec![env.nats_subject(&format!("{subject_prefix}.begin"))],
        storage: jetstream::stream::StorageType::File,
        ..Default::default()
    })
    .await?;

    js.get_or_create_stream(jetstream::stream::Config {
        name: slices_stream.clone(),
        subjects: vec![env.nats_subject(&format!("{subject_prefix}.slices.>"))],
        storage: jetstream::stream::StorageType::File,
        max_age: Duration::from_secs(7 * 24 * 60 * 60),
        max_message_size: 512 * 1024,
        ..Default::default()
    })
    .await?;

    js.get_or_create_stream(jetstream::stream::Config {
        name: end_stream.clone(),
        subjects: vec![env.nats_subject(&format!("{subject_prefix}.end"))],
        storage: jetstream::stream::StorageType::File,
        ..Default::default()
    })
    .await?;

    Ok((handle, js, annex_dir, state_dir, prefix, subject_prefix))
}

#[sinex_test(timeout = 120, trace = true)]
async fn assembler_handles_concurrent_materials_and_records_ledger(
    ctx: TestContext,
) -> TestResult<()> {
    let ctx = ctx.with_nats().await?;
    let full_run = std::env::var("SINEX_MATERIAL_ASSEMBLER_FULL")
        .map(|v| v == "1")
        .unwrap_or(false);
    let (handle, js, _annex_guard, _state_guard, stream_prefix, subject_prefix) =
        start_assembler(&ctx).await?;
    let begin_stream = format!("{stream_prefix}BEGIN");
    let slices_stream = format!("{stream_prefix}SLICES");
    let end_stream = format!("{stream_prefix}END");

    println!(
        "assembler streams: begin={}, slices={}, end={}, subject_prefix={}",
        begin_stream, slices_stream, end_stream, subject_prefix
    );

    // Prepare three materials with predictable hashes/offsets.
    let material_ids: Vec<_> = (0..3)
        .map(|_| sinex_core::types::ulid::Ulid::new())
        .collect();
    let mut material_plans = Vec::new();
    for (idx, material_id) in material_ids.iter().enumerate() {
        let mut slices = Vec::new();
        let mut offset = 0i64;
        let mut hasher = Hasher::new();
        for slice_idx in 0..3 {
            let payload = format!("payload-{}-{}", idx, slice_idx).into_bytes();
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
            r#"
            INSERT INTO raw.source_material_registry
                (id, material_kind, source_identifier, status, timing_info_type, metadata, staged_at, start_time)
            VALUES (($1::uuid)::ulid, 'annex', $2, 'sensing', 'realtime', '{}'::jsonb, NOW(), NOW())
            ON CONFLICT (id) DO NOTHING
            "#,
        )
        .bind(sinex_core::db::query_helpers::ulid_to_uuid(*material_id))
        .bind(format!("test://concurrent/{}", material_id))
        .execute(&ctx.pool)
        .await?;
    }

    // Give the assembler a moment to bootstrap consumers on the prefixed streams.
    tokio::time::sleep(Duration::from_millis(250)).await;

    if !full_run {
        if let Ok(info) = js.get_stream(&begin_stream).await?.info().await {
            assert_eq!(info.state.consumer_count, 1);
        }
        if let Ok(info) = js.get_stream(&slices_stream).await?.info().await {
            assert_eq!(info.state.consumer_count, 1);
        }
        if let Ok(info) = js.get_stream(&end_stream).await?.info().await {
            assert_eq!(info.state.consumer_count, 1);
        }

        handle.abort();
        return Ok(());
    }

    // Fire off begin messages for each material.
    for (material_id, _, _, _) in &material_plans {
        js.publish(
            ctx.env().nats_subject(&format!("{}.begin", subject_prefix)),
            json!({
                "material_id": material_id.to_string(),
                "material_kind": "annex",
                "source_identifier": format!("test://concurrent/{}", material_id),
                "metadata": {"idx": material_id.to_string()},
                "started_at": chrono::Utc::now().to_rfc3339(),
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
            headers.insert(
                "Nats-Msg-Id",
                format!("{}-{}", material_id, slice_idx).as_str(),
            );
            headers.insert("Slice-Index", slice_idx.to_string().as_str());
            headers.insert("Offset", offset.to_string().as_str());
            headers.insert("Chunk-Hash", "deadbeefcafebabe");

            let subject = ctx
                .env()
                .nats_subject(&format!("{}.slices.{}", subject_prefix, material_id));
            publish_futs.push(js.publish_with_headers(subject, headers, payload.into()));
        }
    }
    for fut in join_all(publish_futs).await {
        fut?.await?;
    }

    // Send end markers.
    for (material_id, slices, total_size, hash) in &material_plans {
        js.publish(
            ctx.env().nats_subject(&format!("{}.end", subject_prefix)),
            json!({
                "material_id": material_id.to_string(),
                "ended_at": chrono::Utc::now().to_rfc3339(),
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

    // Observe JetStream state after publishing.
    if let Ok(info) = js.get_stream(&begin_stream).await?.info().await {
        println!(
            "post-publish begin messages: {}, consumers: {}",
            info.state.messages, info.state.consumer_count
        );
    }
    if let Ok(info) = js.get_stream(&slices_stream).await?.info().await {
        println!(
            "post-publish slices messages: {}, consumers: {}",
            info.state.messages, info.state.consumer_count
        );
    }
    if let Ok(info) = js.get_stream(&end_stream).await?.info().await {
        println!(
            "post-publish end messages: {}, consumers: {}",
            info.state.messages, info.state.consumer_count
        );
    }
    if let Ok(mut consumer) = js
        .get_consumer_from_stream::<async_nats::jetstream::consumer::pull::Config, _, _>(
            &begin_stream,
            "ingestd_material_begin",
        )
        .await
    {
        if let Ok(info) = consumer.info().await {
            println!(
                "begin consumer pending={}, num_ack_pending={}",
                info.num_pending, info.num_ack_pending
            );
        }
    } else {
        eprintln!("failed to inspect begin consumer");
    }
    if let Ok(mut consumer) = js
        .get_consumer_from_stream::<async_nats::jetstream::consumer::pull::Config, _, _>(
            &slices_stream,
            "ingestd_material_slices",
        )
        .await
    {
        if let Ok(info) = consumer.info().await {
            println!(
                "slices consumer pending={}, num_ack_pending={}",
                info.num_pending, info.num_ack_pending
            );
        }
    } else {
        eprintln!("failed to inspect slices consumer");
    }
    if let Ok(mut consumer) = js
        .get_consumer_from_stream::<async_nats::jetstream::consumer::pull::Config, _, _>(
            &end_stream,
            "ingestd_material_end",
        )
        .await
    {
        if let Ok(info) = consumer.info().await {
            println!(
                "end consumer pending={}, num_ack_pending={}",
                info.num_pending, info.num_ack_pending
            );
        }
    } else {
        eprintln!("failed to inspect end consumer");
    }

    // Wait for ledger entries to appear for all materials, while also surfacing assembler failures.
    let ledger_wait = async {
        for (material_id, _, total_size, _) in &material_plans {
            let material_id_uuid = sinex_core::db::query_helpers::ulid_to_uuid(*material_id);
            loop {
                let row = sqlx::query(
                    r#"
                    SELECT offset_end, offset_kind
                    FROM raw.temporal_ledger
                    WHERE source_material_id = $1::uuid::ulid
                    "#,
                )
                .bind(material_id_uuid)
                .fetch_optional(&ctx.pool)
                .await?;

                if let Some(row) = row {
                    let offset_end: i64 = row.try_get("offset_end")?;
                    let offset_kind: String = row.try_get("offset_kind")?;
                    assert_eq!(offset_end, *total_size);
                    assert_eq!(offset_kind, "byte");
                    break;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
        Ok::<_, color_eyre::Report>(())
    };

    // Also ensure the assembler task stays healthy while we wait.
    let mut handle = handle;
    tokio::select! {
        res = ledger_wait => {
            res?;
            handle.abort();
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
        _ = tokio::time::sleep(Duration::from_secs(90)) => {
            handle.abort();
            color_eyre::eyre::bail!("timed out waiting for ledger entries");
        }
    }

    // DLQ should remain empty for valid slices.
    let dlq_stream = ctx.env().nats_stream_name("SINEX_RAW_EVENTS_DLQ");
    if let Ok(mut stream) = js.get_stream(&dlq_stream).await {
        if let Ok(info) = stream.info().await {
            assert_eq!(
                info.state.messages, 0,
                "DLQ should stay empty for valid materials"
            );
        }
    }

    // Source material rows should be finalized with blobs recorded.
    for (material_id, _, total_size, _) in &material_plans {
        let row = sqlx::query(
            r#"
            SELECT status, optional_blob_id, metadata->>'file_size_bytes' AS file_size
            FROM raw.source_material_registry
            WHERE id = ($1::uuid)::ulid
            "#,
        )
        .bind(sinex_core::db::query_helpers::ulid_to_uuid(*material_id))
        .fetch_one(&ctx.pool)
        .await?;

        let status: Option<String> = row.try_get("status")?;
        let blob: Option<uuid::Uuid> = row.try_get("optional_blob_id")?;
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
