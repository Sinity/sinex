//! Filesystem helpers shared across crates.

use crate::Uuid;
use std::path::{Path, PathBuf};
use tokio::fs;
use tokio::io::AsyncWriteExt;

/// Write data to `path` by fsyncing a temp file and atomically renaming it in place.
pub async fn atomic_write(path: &Path, contents: &[u8]) -> std::io::Result<()> {
    let temp_path = temp_path_for(path)?;

    let mut file = fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&temp_path)
        .await?;
    file.write_all(contents).await?;
    file.sync_all().await?;

    fs::rename(&temp_path, path).await?;
    Ok(())
}

fn temp_path_for(path: &Path) -> std::io::Result<PathBuf> {
    let file_name = path
        .file_name()
        .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidInput, "missing file name"))?
        .to_string_lossy();

    let temp_name = format!("{}.{}.tmp", file_name, Uuid::now_v7());
    Ok(path.with_file_name(temp_name))
}
