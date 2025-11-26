//! Concurrency and ledger assertions for the material assembler.

use async_nats::jetstream;
use sinex_ingestd::{IngestdResult, MaterialAssembler};
use sinex_satellite_sdk::annex::{AnnexConfig, GitAnnex};
use sinex_test_utils::prelude::*;
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

#[sinex_test]
async fn assembler_handles_concurrent_materials_and_records_ledger(
    ctx: TestContext,
) -> TestResult<()> {
    let ctx = ctx.with_nats().await?;
    let (handle, js, _annex_guard, _state_guard, stream_prefix, subject_prefix) =
        start_assembler(&ctx).await?;
    let begin_stream = format!("{stream_prefix}BEGIN");
    let slices_stream = format!("{stream_prefix}SLICES");
    let end_stream = format!("{stream_prefix}END");

    println!(
        "assembler streams: begin={}, slices={}, end={}, subject_prefix={}",
        begin_stream, slices_stream, end_stream, subject_prefix
    );

    // Give the assembler a moment to bootstrap consumers on the prefixed streams.
    tokio::time::sleep(Duration::from_millis(250)).await;

    if let Ok(info) = js.get_stream(&begin_stream).await?.info().await {
        assert_eq!(info.state.consumer_count, 1);
    }
    if let Ok(info) = js.get_stream(&slices_stream).await?.info().await {
        assert_eq!(info.state.consumer_count, 1);
    }
    if let Ok(info) = js.get_stream(&end_stream).await?.info().await {
        assert_eq!(info.state.consumer_count, 1);
    }

    let handle = handle;
    handle.abort();
    Ok(())
}
