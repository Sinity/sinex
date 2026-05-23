use camino::Utf8PathBuf;
use sinex_node_sdk::content_store::{
    ContentStoreConfig, ContentStoreManager, manager::BLOB_EVENT_CHANNEL_CAPACITY,
};
use sinex_primitives::{Event, JsonValue};
use tempfile::TempDir;
use tokio::sync::mpsc;
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn storage_stats_emit_registered_blob_storage_event(ctx: TestContext) -> TestResult<()> {
    let temp_dir = TempDir::new()?;
    let repo_utf8 = Utf8PathBuf::from_path_buf(temp_dir.path().join("content-store"))
        .map_err(|_| color_eyre::eyre::eyre!("content-store path must be valid UTF-8"))?;
    let content_store_config = ContentStoreConfig {
        root_path: repo_utf8,
        num_copies: None,
        large_files: None,
        ..Default::default()
    };
    let (event_tx, mut event_rx) = mpsc::channel::<Event<JsonValue>>(BLOB_EVENT_CHANNEL_CAPACITY);
    let manager =
        ContentStoreManager::new(content_store_config, ctx.pool().clone(), Some(event_tx))?;

    manager.emit_storage_stats().await?;

    let event = tokio::time::timeout(
        std::time::Duration::from_secs(Timeouts::QUICK),
        event_rx.recv(),
    )
    .await?
    .ok_or_else(|| color_eyre::eyre::eyre!("storage stats event channel closed"))?;

    assert_eq!(event.source.as_str(), "blob_storage");
    assert_eq!(event.event_type.as_str(), "storage.statistics");
    Ok(())
}
