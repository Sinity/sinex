#[cfg(feature = "messaging")]
use crate::{NodeResult, acquisition_manager::AcquisitionManager};
use camino::Utf8Path;
#[cfg(feature = "messaging")]
use serde_json::Value as JsonValue;
#[cfg(feature = "messaging")]
use sinex_primitives::Uuid;

#[cfg(feature = "messaging")]
const MAX_STAGE_FILE_CHUNK_BYTES: usize = 256 * 1024;

/// Stage source material bytes through the normal acquisition pipeline.
///
/// Each call creates a fresh source material with a `UUIDv7` ID — every observation
/// is a distinct material, even if the underlying source content is identical.
#[cfg(feature = "messaging")]
pub async fn stage_material(
    acquisition: &AcquisitionManager,
    source_identifier: &str,
    bytes: &[u8],
    reason: &str,
    metadata: Option<JsonValue>,
) -> NodeResult<Uuid> {
    let mut builder = acquisition.build_material(source_identifier);
    if let Some(metadata_value) = metadata.clone() {
        builder = builder.with_metadata(metadata_value);
    }

    let mut handle = builder.begin().await?;
    let material_id = handle.material_id;
    acquisition.append_slice(&mut handle, bytes).await?;

    if let Some(metadata_value) = metadata {
        acquisition
            .finalize_with_metadata(&mut handle, reason, metadata_value)
            .await?;
    } else {
        acquisition.finalize(handle, reason).await?;
    }

    Ok(material_id)
}

/// Stream a file into source-material storage through the normal acquisition pipeline.
#[cfg(feature = "messaging")]
pub async fn stage_material_from_file(
    acquisition: &AcquisitionManager,
    path: &Utf8Path,
    reason: &str,
    metadata: Option<JsonValue>,
) -> NodeResult<(Uuid, i64)> {
    use tokio::io::AsyncReadExt;

    let mut builder = acquisition.build_material(path.as_str());
    if let Some(metadata_value) = metadata.clone() {
        builder = builder.with_metadata(metadata_value);
    }

    let mut handle = builder.begin().await?;
    let material_id = handle.material_id;
    let mut file = tokio::fs::File::open(path).await?;
    let mut total_bytes = 0i64;
    let mut buffer = vec![0u8; MAX_STAGE_FILE_CHUNK_BYTES];

    loop {
        let read = file.read(&mut buffer).await?;
        if read == 0 {
            break;
        }
        acquisition
            .append_slice(&mut handle, &buffer[..read])
            .await?;
        total_bytes += read as i64;
    }

    if let Some(metadata_value) = metadata {
        acquisition
            .finalize_with_metadata(&mut handle, reason, metadata_value)
            .await?;
    } else {
        acquisition.finalize(handle, reason).await?;
    }

    Ok((material_id, total_bytes))
}
