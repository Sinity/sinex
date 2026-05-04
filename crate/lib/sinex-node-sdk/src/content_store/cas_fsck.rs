//! Local CAS filesystem check (fsck) for the content store.
//!
//! Walks the `sinex-cas/XX/YY/<hash>` directory tree, cross-references each
//! file against the `core.blobs` table, and classifies entries as:
//!
//! - **referenced**: on disk AND in `core.blobs` (healthy).
//! - **orphaned**: on disk, NOT in `core.blobs` (candidate for removal).
//! - **corrupt**: on disk, hash does not match file content.
//! - **malformed**: wrong directory structure (e.g. file where a prefix dir is expected).
//! - **missing**: in `core.blobs` with `SINEXBLAKE3` backend, but not on disk.
//!
//! By default runs in dry-run mode. `--apply` removes orphaned files.

use crate::{NodeResult, SinexError};
use serde::{Deserialize, Serialize};
use sinex_db::DbPoolExt;
use sqlx::PgPool;
use std::collections::HashSet;

use super::{LOCAL_BLAKE3_CAS_BACKEND, LOCAL_BLAKE3_CAS_DIR, MaterialContentStore};

/// Result of a single CAS file check.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CasFileStatus {
    /// The hex-encoded BLAKE3 hash (filename in the CAS tree).
    pub hash: String,
    /// Full path on disk.
    pub path: String,
    /// Size on disk in bytes.
    pub size_bytes: u64,
    /// Classification.
    pub status: CasStatus,
    /// When `status` is `Referenced`, the matching blob ID.
    pub blob_id: Option<String>,
}

/// Classification of a CAS file entry.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum CasStatus {
    /// File exists on disk and has a row in `core.blobs` with matching data.
    Referenced,
    /// File exists on disk but has NO matching row in `core.blobs`.
    Orphaned,
    /// File exists on disk but its BLAKE3 hash does not match the filename.
    Corrupt,
    /// An entry in the CAS tree has an unexpected structure (not a regular file
    /// in the expected hash position).
    Malformed,
    /// A `SINEXBLAKE3` blob row exists in `core.blobs` but the file is not on disk.
    Missing,
}

/// Aggregate report from a CAS fsck run.
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct CasFsckReport {
    /// Files on disk that also have a matching `core.blobs` row.
    pub referenced: usize,
    /// Files on disk with no `core.blobs` row.
    pub orphaned: usize,
    /// Files on disk whose BLAKE3 hash does not match the filename.
    pub corrupt: usize,
    /// Entries in the CAS tree with unexpected structure.
    pub malformed: usize,
    /// `SINEXBLAKE3` blob rows with no file on disk.
    pub missing: usize,
    /// Number of orphaned files actually removed (only when `apply == true`).
    pub removed: usize,
    /// Total bytes of orphaned content identified.
    pub orphaned_bytes: u64,
}

/// Run a CAS filesystem check.
///
/// Walks the `sinex-cas/` directory tree, cross-references against `core.blobs`,
/// and returns a detailed report. When `apply` is true, orphaned files are removed.
pub async fn check_cas(
    pool: &PgPool,
    content_store: &MaterialContentStore,
    apply: bool,
) -> NodeResult<(CasFsckReport, Vec<CasFileStatus>)> {
    let entries = content_store.walk_cas().await?;
    let mut file_statuses: Vec<CasFileStatus> = Vec::new();
    let mut report = CasFsckReport::default();

    // Build a set of known hashes from core.blobs for SINEXBLAKE3 entries
    let known_blake3_hashes = load_sinexblake3_hashes(pool).await?;
    let mut known_hash_set: HashSet<String> = HashSet::new();
    for (hash, _blob_id) in &known_blake3_hashes {
        known_hash_set.insert(hash.clone());
    }
    let mut matched_blob_ids: HashSet<String> = HashSet::new();

    for (hash, path, size) in entries {
        // Check if hash is in the DB
        if known_hash_set.contains(&hash) {
            let blob_id = known_blake3_hashes
                .iter()
                .find(|(h, _)| h == &hash)
                .map(|(_, id)| id.clone())
                .unwrap_or_default();
            matched_blob_ids.insert(blob_id.clone());

            // Verify the file content matches the hash
            match verify_cas_file_content(&path, &hash).await {
                Ok(true) => {
                    report.referenced += 1;
                    file_statuses.push(CasFileStatus {
                        hash,
                        path: path.to_string(),
                        size_bytes: size,
                        status: CasStatus::Referenced,
                        blob_id: Some(blob_id),
                    });
                }
                Ok(false) => {
                    report.corrupt += 1;
                    file_statuses.push(CasFileStatus {
                        hash,
                        path: path.to_string(),
                        size_bytes: size,
                        status: CasStatus::Corrupt,
                        blob_id: Some(blob_id),
                    });
                }
                Err(error) => {
                    report.malformed += 1;
                    tracing::warn!(
                        error = %error,
                        hash = %hash,
                        "Failed to verify CAS file content"
                    );
                    file_statuses.push(CasFileStatus {
                        hash: hash.clone(),
                        path: path.to_string(),
                        size_bytes: size,
                        status: CasStatus::Malformed,
                        blob_id: Some(blob_id),
                    });
                }
            }
        } else {
            // Orphaned: on disk, not in DB
            report.orphaned += 1;
            report.orphaned_bytes += size;
            if apply {
                match tokio::fs::remove_file(path.as_str()).await {
                    Ok(()) => {
                        report.removed += 1;
                        file_statuses.push(CasFileStatus {
                            hash: hash.clone(),
                            path: path.to_string(),
                            size_bytes: size,
                            status: CasStatus::Orphaned,
                            blob_id: None,
                        });
                    }
                    Err(error) => {
                        tracing::warn!(
                            error = %error,
                            path = %path,
                            "Failed to remove orphaned CAS file"
                        );
                        file_statuses.push(CasFileStatus {
                            hash,
                            path: path.to_string(),
                            size_bytes: size,
                            status: CasStatus::Orphaned,
                            blob_id: None,
                        });
                    }
                }
            } else {
                file_statuses.push(CasFileStatus {
                    hash,
                    path: path.to_string(),
                    size_bytes: size,
                    status: CasStatus::Orphaned,
                    blob_id: None,
                });
            }
        }
    }

    // Detect missing: SINEXBLAKE3 blobs in DB but not on disk
    for (hash, blob_id) in &known_blake3_hashes {
        if !matched_blob_ids.contains(blob_id) {
            report.missing += 1;
            file_statuses.push(CasFileStatus {
                hash: hash.clone(),
                path: format!("{}/XX/YY/{}", LOCAL_BLAKE3_CAS_DIR, hash),
                size_bytes: 0,
                status: CasStatus::Missing,
                blob_id: Some(blob_id.clone()),
            });
        }
    }

    // Remove empty prefix directories after cleanup
    if apply && report.removed > 0 {
        clean_empty_cas_dirs(content_store).await;
    }

    Ok((report, file_statuses))
}

/// Load all BLAKE3 hashes from `core.blobs` where `annex_backend = 'SINEXBLAKE3'`.
async fn load_sinexblake3_hashes(pool: &PgPool) -> NodeResult<Vec<(String, String)>> {
    let rows = sqlx::query_as::<_, (String, String)>(
        r#"
        SELECT content_hash, id::text
        FROM core.blobs
        WHERE annex_backend = $1
        "#,
    )
    .bind(LOCAL_BLAKE3_CAS_BACKEND)
    .fetch_all(pool)
    .await
    .map_err(|e| SinexError::database(format!("failed to load SINEXBLAKE3 hashes: {e}")))?;

    Ok(rows)
}

/// Verify that a CAS file's BLAKE3 hash matches its filename.
async fn verify_cas_file_content(path: &camino::Utf8Path, expected_hash: &str) -> NodeResult<bool> {
    let content = tokio::fs::read(path)
        .await
        .map_err(|e| SinexError::io(e))?;
    let computed = blake3::hash(&content).to_hex();
    Ok(computed.as_str() == expected_hash)
}

/// Remove empty prefix directories under `sinex-cas/` after orphan cleanup.
async fn clean_empty_cas_dirs(content_store: &MaterialContentStore) {
    let cas_root = content_store.config.root_path.join(LOCAL_BLAKE3_CAS_DIR);
    // Walk the XX and YY directories; remove any that are empty.
    let Ok(mut prefix_a) = tokio::fs::read_dir(&cas_root).await else {
        return;
    };
    while let Ok(Some(entry)) = prefix_a.next_entry().await {
        if !entry.file_type().await.map(|ft| ft.is_dir()).unwrap_or(false) {
            continue;
        }
        let prefix_a_path = entry.path();
        let Ok(mut prefix_b) = tokio::fs::read_dir(&prefix_a_path).await else {
            continue;
        };
        let mut b_empty = true;
        while let Ok(Some(sub_entry)) = prefix_b.next_entry().await {
            if !sub_entry.file_type().await.map(|ft| ft.is_dir()).unwrap_or(false) {
                continue;
            }
            let sub_path = sub_entry.path();
            let Ok(mut hash_entries) = tokio::fs::read_dir(&sub_path).await else {
                continue;
            };
            if hash_entries.next_entry().await.ok().flatten().is_none() {
                let _ = tokio::fs::remove_dir(&sub_path).await;
            } else {
                b_empty = false;
            }
        }
        if b_empty {
            let _ = tokio::fs::remove_dir(&prefix_a_path).await;
        }
    }
}

/// Sweep orphaned CAS files that are not referenced by `core.blobs`.
///
/// This is the CAS-equivalent of `gc::sweep_orphans` for the legacy annex backend.
/// `apply = false` is a dry-run; returns counts but removes nothing.
pub async fn sweep_orphans_cas(
    pool: &PgPool,
    content_store: &MaterialContentStore,
    apply: bool,
) -> NodeResult<CasFsckReport> {
    let (report, _) = check_cas(pool, content_store, apply).await?;
    Ok(report)
}
