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

use crate::runtime::{RuntimeResult, SinexError};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use sqlx::PgPool;
use std::collections::HashSet;
use std::time::{Duration as StdDuration, Instant, SystemTime};
use tokio::io::AsyncReadExt;

use super::{
    ContentStoreKey, LOCAL_BLAKE3_CAS_BACKEND, LOCAL_BLAKE3_CAS_DIR, MaterialContentStore,
};
use crate::runtime::work_control::{WorkBudget, WorkCancellation, WorkController, WorkIdentity};

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
    /// A content-store staging file that has not been atomically published.
    Staged,
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
    /// Orphaned files protected by the minimum age/grace period.
    pub protected_recent: usize,
    /// In-flight staging files retained regardless of age.
    pub staged: usize,
    /// Orphans that became DB-referenced during the scan/apply race.
    pub recheck_protected: usize,
    /// Number of filesystem entries inspected before completion or a budget stop.
    pub entries_scanned: usize,
    /// Bytes read for cryptographic verification.
    pub bytes_verified: u64,
    /// Whether the report is incomplete because a bounded-work limit fired.
    pub incomplete: bool,
    /// Why the bounded scan stopped, when it did.
    pub stop_reason: Option<CasFsckStopReason>,
}

const CAS_ORPHAN_GRACE: StdDuration = StdDuration::from_secs(10 * 60);

/// Why a CAS fsck stopped before reaching the end of its snapshot.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CasFsckStopReason {
    RuntimeBudget,
    EntryBudget,
}

/// Resource limits for one fsck pass.
///
/// The defaults deliberately make a pass cooperative and bounded. A large
/// store must be handled by a resumable/quarantine lifecycle, not by silently
/// allowing one maintenance invocation to monopolize the host indefinitely.
#[derive(Debug, Clone, Copy)]
pub struct CasFsckOptions {
    pub max_runtime: Option<StdDuration>,
    pub max_entries: Option<usize>,
    pub verify_bytes_per_sec: Option<f64>,
}

impl Default for CasFsckOptions {
    fn default() -> Self {
        Self {
            max_runtime: Some(StdDuration::from_secs(55 * 60)),
            max_entries: None,
            verify_bytes_per_sec: Some(64.0 * 1024.0 * 1024.0),
        }
    }
}

/// Run a CAS filesystem check.
///
/// Walks the `sinex-cas/` directory tree, cross-references against `core.blobs`,
/// and returns a detailed report. When `apply` is true, orphaned files are removed.
pub async fn check_cas(
    pool: &PgPool,
    content_store: &MaterialContentStore,
    apply: bool,
) -> RuntimeResult<(CasFsckReport, Vec<CasFileStatus>)> {
    check_cas_with_options(pool, content_store, apply, CasFsckOptions::default()).await
}

/// Run a bounded CAS fsck pass with explicit work limits.
pub async fn check_cas_with_options(
    pool: &PgPool,
    content_store: &MaterialContentStore,
    apply: bool,
    options: CasFsckOptions,
) -> RuntimeResult<(CasFsckReport, Vec<CasFileStatus>)> {
    let started = Instant::now();
    let entries = content_store.walk_cas().await?;
    let mut file_statuses: Vec<CasFileStatus> = Vec::new();
    let mut report = CasFsckReport::default();
    let mut orphan_candidates = Vec::new();
    let mut present_hashes: HashSet<String> = HashSet::new();
    let mut work = WorkController::new(
        WorkIdentity::ephemeral("cas-fsck", content_store.root_path().as_str()),
        WorkBudget {
            // The scan-level deadline below turns this into an incomplete,
            // reportable pass; the controller owns rate/cancellation waits.
            max_runtime: None,
            bytes_per_sec: options.verify_bytes_per_sec,
            ..WorkBudget::default()
        },
        WorkCancellation::new(),
    );

    // Build a set of known hashes from core.blobs for SINEXBLAKE3 entries
    let known_blake3_hashes = load_sinexblake3_hashes(pool).await?;
    if apply && !entries.is_empty() && known_blake3_hashes.is_empty() {
        return Err(SinexError::validation(
            "refusing CAS orphan deletion because the paired database has no SINEXBLAKE3 rows",
        ));
    }
    if apply
        && !known_blake3_hashes.is_empty()
        && entries.len() > known_blake3_hashes.len().saturating_mul(2)
    {
        return Err(SinexError::validation(format!(
            "refusing CAS orphan deletion because the scanned store has an implausibly high orphan ratio ({} files vs {} DB rows)",
            entries.len(),
            known_blake3_hashes.len()
        )));
    }
    let mut known_hash_set: HashSet<String> = HashSet::new();
    for (hash, _blob_id) in &known_blake3_hashes {
        known_hash_set.insert(hash.clone());
    }
    for (index, (hash, path, size)) in entries.into_iter().enumerate() {
        if options
            .max_runtime
            .is_some_and(|limit| started.elapsed() >= limit)
        {
            report.incomplete = true;
            report.stop_reason = Some(CasFsckStopReason::RuntimeBudget);
            break;
        }
        if options.max_entries.is_some_and(|limit| index >= limit) {
            report.incomplete = true;
            report.stop_reason = Some(CasFsckStopReason::EntryBudget);
            break;
        }
        report.entries_scanned += 1;
        if hash.contains(".tmp-") {
            report.staged += 1;
            file_statuses.push(CasFileStatus {
                hash,
                path: path.to_string(),
                size_bytes: size,
                status: CasStatus::Staged,
                blob_id: None,
            });
            continue;
        }

        // Check if hash is in the DB
        if known_hash_set.contains(&hash) {
            present_hashes.insert(hash.clone());
            let blob_id = known_blake3_hashes
                .iter()
                .find(|(h, _)| h == &hash)
                .map(|(_, id)| id.clone())
                .unwrap_or_default();
            // Verify the file content matches the hash
            match verify_cas_file_content(&path, &hash).await {
                Ok((matches, bytes_read)) => {
                    report.bytes_verified = report.bytes_verified.saturating_add(bytes_read);
                    work
                        .record_batch("verify", 1, bytes_read, Some(path.to_string()))
                        .await?;
                    if matches {
                        report.referenced += 1;
                        file_statuses.push(CasFileStatus {
                            hash,
                            path: path.to_string(),
                            size_bytes: size,
                            status: CasStatus::Referenced,
                            blob_id: Some(blob_id),
                        });
                    } else {
                        report.corrupt += 1;
                        file_statuses.push(CasFileStatus {
                            hash,
                            path: path.to_string(),
                            size_bytes: size,
                            status: CasStatus::Corrupt,
                            blob_id: Some(blob_id),
                        });
                    }
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
            let is_recent = tokio::fs::metadata(path.as_std_path())
                .await
                .ok()
                .and_then(|metadata| metadata.modified().ok())
                .is_some_and(|modified| {
                    SystemTime::now()
                        .duration_since(modified)
                        .is_ok_and(|age| age < CAS_ORPHAN_GRACE)
                });
            if is_recent {
                report.protected_recent += 1;
            }
            orphan_candidates.push((hash.clone(), path.clone(), size, is_recent));
            file_statuses.push(CasFileStatus {
                hash,
                path: path.to_string(),
                size_bytes: size,
                status: CasStatus::Orphaned,
                blob_id: None,
            });
        }
    }

    // Detect missing: SINEXBLAKE3 blobs in DB but not on disk
    if !report.incomplete {
        for (hash, blob_id) in &known_blake3_hashes {
            if !present_hashes.contains(hash) {
                report.missing += 1;
                let path = content_store
                    .local_blake3_cas_path_for_hash(hash)
                    .map_or_else(
                        |_| {
                            content_store
                                .root_path()
                                .join(LOCAL_BLAKE3_CAS_DIR)
                                .join("<invalid-hash>")
                                .join(hash)
                        },
                        |path| path,
                    );
                file_statuses.push(CasFileStatus {
                    hash: hash.clone(),
                    path: path.to_string(),
                    size_bytes: 0,
                    status: CasStatus::Missing,
                    blob_id: Some(blob_id.clone()),
                });
            }
        }
    }

    if apply && report.incomplete {
        return Err(SinexError::validation(format!(
            "refusing CAS orphan deletion because fsck stopped before scanning the complete store ({:?})",
            report.stop_reason
        )));
    }

    // Apply deletion only after the complete, read-only classification pass.
    // This prevents a late budget stop from leaving an apparently successful
    // partial destructive run.
    if apply {
        for (hash, path, size, is_recent) in orphan_candidates {
            if is_recent {
                continue;
            }
            if cas_hash_is_referenced(pool, &hash).await?.is_some() {
                report.recheck_protected += 1;
                continue;
            }
            match tokio::fs::remove_file(path.as_str()).await {
                Ok(()) => report.removed += 1,
                Err(error) => tracing::warn!(
                    error = %error,
                    path = %path,
                    size_bytes = size,
                    "Failed to remove orphaned CAS file"
                ),
            }
        }
    }

    // Remove empty prefix directories after cleanup
    if apply && report.removed > 0 {
        clean_empty_cas_dirs(content_store).await;
    }

    Ok((report, file_statuses))
}

/// Re-check DB authority immediately before destructive filesystem mutation.
/// The initial hash snapshot is intentionally not sufficient: a material/blob
/// commit can race a long fsck walk and publish a reference after the snapshot.
async fn cas_hash_is_referenced(pool: &PgPool, hash: &str) -> RuntimeResult<Option<String>> {
    sqlx::query_scalar::<_, String>(
        r"
        SELECT id::text
        FROM core.blobs
        WHERE annex_backend = $1 AND checksum_blake3 = $2
        UNION ALL
        SELECT 'material-manifest:' || id::text
        FROM raw.source_material_registry
        WHERE metadata->'material_manifest'->>'content_key' LIKE $3
        LIMIT 1
        ",
    )
    .bind(LOCAL_BLAKE3_CAS_BACKEND)
    .bind(hash)
    .bind(format!("%--{hash}"))
    .fetch_optional(pool)
    .await
    .map_err(|error| {
        SinexError::database("re-check CAS authority before orphan removal").with_source(error)
    })
}

/// Load all BLAKE3 hashes that have a durable database authority.
///
/// Manifests are intentionally not ordinary user blobs: they are stored in CAS
/// and referenced from source-material metadata. Include those references in
/// the fsck authority set so a normal orphan sweep cannot delete replay
/// metadata that is still needed by a live material row.
async fn load_sinexblake3_hashes(pool: &PgPool) -> RuntimeResult<Vec<(String, String)>> {
    let rows = sqlx::query_as::<_, (String, String)>(
        r"
        SELECT content_hash, id::text
        FROM core.blobs
        WHERE annex_backend = $1
        ",
    )
    .bind(LOCAL_BLAKE3_CAS_BACKEND)
    .fetch_all(pool)
    .await
    .map_err(|e| SinexError::database(format!("failed to load SINEXBLAKE3 hashes: {e}")))?;

    let mut references = rows;
    let material_rows = sqlx::query_as::<_, (String, JsonValue)>(
        r"
        SELECT id::text, metadata
        FROM raw.source_material_registry
        WHERE metadata->'material_manifest'->>'content_key' IS NOT NULL
        ",
    )
    .fetch_all(pool)
    .await
    .map_err(|e| {
        SinexError::database(format!("failed to load material manifest references: {e}"))
    })?;
    for (material_id, metadata) in material_rows {
        let Some(content_key) = metadata
            .get("material_manifest")
            .and_then(JsonValue::as_object)
            .and_then(|manifest| manifest.get("content_key"))
            .and_then(JsonValue::as_str)
        else {
            continue;
        };
        let Ok(parsed) = ContentStoreKey::parse(content_key) else {
            continue;
        };
        if parsed.is_local_blake3_cas() {
            references.push((parsed.digest, format!("material-manifest:{material_id}")));
        }
    }
    Ok(references)
}

/// Verify that a CAS file's BLAKE3 hash matches its filename.
async fn verify_cas_file_content(
    path: &camino::Utf8Path,
    expected_hash: &str,
) -> RuntimeResult<(bool, u64)> {
    const BUFFER_SIZE: usize = 1024 * 1024;
    let mut file = tokio::fs::File::open(path).await.map_err(SinexError::io)?;
    let mut hasher = blake3::Hasher::new();
    let mut buffer = vec![0_u8; BUFFER_SIZE];
    let mut bytes_read = 0_u64;
    loop {
        let read = file.read(&mut buffer).await.map_err(SinexError::io)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
        bytes_read = bytes_read.saturating_add(read as u64);
        tokio::task::yield_now().await;
    }
    let computed = hasher.finalize().to_hex();
    Ok((computed.as_str() == expected_hash, bytes_read))
}

/// Remove empty prefix directories under `sinex-cas/` after orphan cleanup.
async fn clean_empty_cas_dirs(content_store: &MaterialContentStore) {
    let cas_root = content_store.config.root_path.join(LOCAL_BLAKE3_CAS_DIR);
    // Walk the XX and YY directories; remove any that are empty.
    let Ok(mut prefix_a) = tokio::fs::read_dir(&cas_root).await else {
        return;
    };
    while let Ok(Some(entry)) = prefix_a.next_entry().await {
        if !entry.file_type().await.is_ok_and(|ft| ft.is_dir()) {
            continue;
        }
        let prefix_a_path = entry.path();
        let Ok(mut prefix_b) = tokio::fs::read_dir(&prefix_a_path).await else {
            continue;
        };
        let mut b_empty = true;
        while let Ok(Some(sub_entry)) = prefix_b.next_entry().await {
            if !sub_entry.file_type().await.is_ok_and(|ft| ft.is_dir()) {
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
) -> RuntimeResult<CasFsckReport> {
    let (report, _) = check_cas(pool, content_store, apply).await?;
    Ok(report)
}

#[cfg(test)]
#[path = "cas_fsck_test.rs"]
mod tests;
