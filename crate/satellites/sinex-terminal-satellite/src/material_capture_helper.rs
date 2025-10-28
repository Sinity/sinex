//! Material capture helper for terminal data

use camino::Utf8PathBuf;
use sinex_satellite_sdk::{
    acquisition_manager::{AcquisitionManager, AppendStreamAcquirer},
    SatelliteResult,
};
use std::sync::Arc;
use tracing::{error, info};

/// Capture a history file as source material
pub async fn capture_history_file(
    acquisition_manager: &Arc<AcquisitionManager>,
    history_file: &Utf8PathBuf,
) -> SatelliteResult<String> {
    info!("Capturing history file: {}", history_file);

    let acquirer = AppendStreamAcquirer::new(history_file.to_string());
    let material_handle = acquisition_manager
        .begin_capture(
            acquirer,
            sinex_core::types::MaterialKind::Text,
            history_file.to_string(),
        )
        .await?;

    let content = tokio::fs::read(history_file.as_std_path())
        .await
        .map_err(|e| {
            sinex_satellite_sdk::SatelliteError::Io(e)
        })?;

    material_handle.append(&content).await?;
    let material_id = material_handle.finalize().await?;

    info!("Captured history file {} as material {}", history_file, material_id);
    Ok(material_id.to_string())
}

/// Capture Atuin database as source material
pub async fn capture_atuin_db(
    acquisition_manager: &Arc<AcquisitionManager>,
    atuin_path: &Utf8PathBuf,
) -> SatelliteResult<String> {
    info!("Capturing Atuin database: {}", atuin_path);

    let acquirer = AppendStreamAcquirer::new(atuin_path.to_string());
    let material_handle = acquisition_manager
        .begin_capture(
            acquirer,
            sinex_core::types::MaterialKind::Database,
            atuin_path.to_string(),
        )
        .await?;

    let content = tokio::fs::read(atuin_path.as_std_path())
        .await
        .map_err(|e| {
            sinex_satellite_sdk::SatelliteError::Io(e)
        })?;

    material_handle.append(&content).await?;
    let material_id = material_handle.finalize().await?;

    info!("Captured Atuin database {} as material {}", atuin_path, material_id);
    Ok(material_id.to_string())
}
