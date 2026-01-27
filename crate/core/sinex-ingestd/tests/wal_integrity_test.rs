use serde_json::json;
use sinex_ingestd::MaterialAssembler;
use sinex_node_sdk::annex::{AnnexConfig, GitAnnex};
use xtask::sandbox::prelude::*;
use std::sync::Arc;

#[sinex_test]
#[ignore]
async fn wal_recovers_state_after_crash(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let nats_client = ctx.nats_client();
    let namespace = ctx.pipeline_namespace().prefix().to_string();

    // Setup persistent state dir for the test
    let state_dir = tempfile::tempdir()?;
    let state_path = state_dir.path().to_path_buf();

    // Setup Annex (shared across restarts)
    let annex_dir = tempfile::tempdir()?;
    let repo_path = camino::Utf8PathBuf::from_path_buf(annex_dir.path().to_path_buf()).unwrap();
    GitAnnex::init(&repo_path, Some("wal-test")).await?;
    let annex = Arc::new(GitAnnex::new(AnnexConfig {
        repo_path,
        num_copies: None,
        large_files: None,
    })?);

    // Bootstrap streams
    sinex_node_sdk::AcquisitionManager::bootstrap_streams_with_namespace(
        &nats_client,
        Some(&namespace),
    )
    .await?;

    let material_id = sinex_core::types::ulid::Ulid::new();
    let js = ctx
        .nats_handle()?
        .jetstream_with_client(nats_client.clone());

    // --- RUN 1: Partial Ingestion ---
    {
        let assembler = MaterialAssembler::new(
            nats_client.clone(),
            ctx.pool.clone(),
            annex.clone(),
            state_path.clone(),
            Some(namespace.clone()),
            1_000,
        )?;
        let handle = tokio::spawn(async move { assembler.run().await });

        // Publish Begin
        js.publish(
            ctx.pipeline_namespace().subject("source_material.begin"),
            json!({
                "material_id": material_id.to_string(),
                "material_kind": "test-wal",
                "source_identifier": "test://wal-resume",
                "metadata": {"run": 1},
                "started_at": chrono::Utc::now().to_rfc3339(),
            })
            .to_string()
            .into(),
        )
        .await?
        .await?;

        // Publish Slice 1 (bytes 0-4 "PART")
        let mut headers = async_nats::HeaderMap::new();
        headers.insert("Offset", "0");
        js.publish_with_headers(
            ctx.pipeline_namespace()
                .subject(&format!("source_material.slices.{}", material_id)),
            headers,
            b"PART".to_vec().into(),
        )
        .await?
        .await?;

        // Wait for WAL to contain the Slice entry
        let wal_file = state_path.join(material_id.to_string()).join("state.wal");
        xtask::sandbox::timing::WaitHelpers::wait_for_condition(
            || {
                let p = wal_file.clone();
                async move {
                    if !p.exists() {
                        return Ok(false);
                    }
                    let content = tokio::fs::read_to_string(&p)
                        .await
                        .map_err(|e| sinex_core::types::error::SinexError::io(e.to_string()))?;
                    Ok(content.contains("\"Slice\""))
                }
            },
            10,
        )
        .await?;

        handle.abort();
    }

    // --- RUN 2: Resume & Complete ---
    {
        let assembler = MaterialAssembler::new(
            nats_client.clone(),
            ctx.pool.clone(),
            annex.clone(),
            state_path.clone(),
            Some(namespace.clone()),
            1_000,
        )?;
        let handle = tokio::spawn(async move { assembler.run().await });

        // Publish Slice 2 (bytes 4-8 "IAL!")
        let mut headers = async_nats::HeaderMap::new();
        headers.insert("Offset", "4");
        js.publish_with_headers(
            ctx.pipeline_namespace()
                .subject(&format!("source_material.slices.{}", material_id)),
            headers,
            b"IAL!".to_vec().into(),
        )
        .await?
        .await?;

        // Publish End
        let content = b"PARTIAL!";
        let hash = blake3::hash(content).to_hex().to_string();
        js.publish(
            ctx.pipeline_namespace().subject("source_material.end"),
            json!({
                "material_id": material_id.to_string(),
                "ended_at": chrono::Utc::now().to_rfc3339(),
                "content_hash": hash,
                "total_slices": 2,
                "total_size_bytes": 8,
            })
            .to_string()
            .into(),
        )
        .await?
        .await?;

        // Wait for completion (via DB check)
        let pool = ctx.pool.clone();

        xtask::sandbox::timing::WaitHelpers::wait_for_condition(
            move || {
                let pool = pool.clone();
                async move {
                    let repo = pool.source_materials();
                    let id = sinex_core::Id::from_ulid(material_id);
                    let rec = repo
                        .get_by_id(id)
                        .await
                        .map_err(sinex_core::types::error::SinexError::from)?;
                    if let Some(r) = rec {
                        if r.status == sinex_core::db::repositories::material_status::COMPLETED {
                            return Ok(true);
                        }
                        if r.status == sinex_core::db::repositories::material_status::FAILED {
                            return Err(sinex_core::types::error::SinexError::service(format!(
                                "Material failed: metadata={:?}",
                                r.metadata
                            )));
                        }
                    }
                    Ok(false)
                }
            },
            10,
        )
        .await?;

        handle.abort();
    }

    Ok(())
}
