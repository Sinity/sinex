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
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use sinex_primitives::{DecodedMaterialManifest, MaterialManifestV1};
use sqlx::PgPool;
use std::collections::{HashMap, HashSet};
use std::sync::OnceLock;
use std::time::{Duration as StdDuration, SystemTime, UNIX_EPOCH};
use tokio::io::AsyncReadExt;

use super::{
    CasWalkCheckpoint, ContentStoreKey, LOCAL_BLAKE3_CAS_BACKEND, LOCAL_BLAKE3_CAS_DIR,
    MaterialContentStore,
};
use crate::runtime::work_control::{
    WorkAdmission, WorkBudget, WorkCancellation, WorkController, WorkIdentity, WorkOutcome,
    WorkStopReason,
};

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
    /// A published object still held by a live durable ingest lease.
    Leased,
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
    /// Published objects protected by a live ingest lease.
    pub leased: usize,
    /// Ingest leases older than the recovery grace period.
    pub stale_leases: usize,
    /// Orphans that became DB-referenced during the scan/apply race.
    pub recheck_protected: usize,
    /// Orphans moved into durable quarantine during an apply pass.
    pub quarantined: usize,
    /// Pending quarantines retained for a later reconciliation pass.
    pub pending_deletes: usize,
    /// Pending quarantines restored after a database reference reappeared.
    pub restored: usize,
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
const CAS_INGEST_LEASE_GRACE: StdDuration = StdDuration::from_secs(24 * 60 * 60);
static CAS_FSCK_ADMISSION: OnceLock<WorkAdmission> = OnceLock::new();

fn cas_fsck_admission() -> &'static WorkAdmission {
    CAS_FSCK_ADMISSION.get_or_init(|| WorkAdmission::new(1))
}

/// Why a CAS fsck stopped before reaching the end of its snapshot.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CasFsckStopReason {
    Cancelled,
    RuntimeBudget,
    EntryBudget,
}

/// Optional resource limits for one fsck pass.
///
/// An ordinary fsck is completion-oriented: it has no guessed wall-clock or
/// verification-throughput ceiling. Operators may provide limits when the
/// host needs admission control; an explicitly bounded incomplete apply pass
/// remains fail-closed. `verify_bytes_per_sec` rate-limits only bytes read by
/// cryptographic verification. `max_runtime` is checked at every fsck
/// controller boundary, including authority loading, reconciliation, scanning,
/// reporting, and destructive cleanup.
#[derive(Debug, Clone, Copy)]
pub struct CasFsckOptions {
    pub max_runtime: Option<StdDuration>,
    pub max_entries: Option<usize>,
    pub verify_bytes_per_sec: Option<f64>,
}

impl Default for CasFsckOptions {
    fn default() -> Self {
        Self {
            max_runtime: None,
            max_entries: None,
            verify_bytes_per_sec: None,
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
    let (report, statuses, _) = check_cas_with_options_and_control(
        pool,
        content_store,
        apply,
        options,
        None,
        WorkCancellation::new(),
    )
    .await?;
    Ok((report, statuses))
}

/// Run fsck with an externally owned cancellation token and resumable CAS
/// cursor. The returned cursor is safe to persist and pass back after a
/// partial dry-run. It advances only at completed prefix directories.
pub async fn check_cas_with_options_and_control(
    pool: &PgPool,
    content_store: &MaterialContentStore,
    apply: bool,
    options: CasFsckOptions,
    checkpoint: Option<CasWalkCheckpoint>,
    cancellation: WorkCancellation,
) -> RuntimeResult<(CasFsckReport, Vec<CasFileStatus>, CasWalkCheckpoint)> {
    let initial_checkpoint = checkpoint.unwrap_or_default();
    let mut walker = content_store
        .cas_walker_with_control(Some(initial_checkpoint.clone()), Some(cancellation.clone()))
        .await?;
    let mut file_statuses: Vec<CasFileStatus> = Vec::new();
    let mut report = CasFsckReport::default();
    let mut present_hashes: HashSet<String> = HashSet::new();
    if cancellation.is_cancelled() {
        report.incomplete = true;
        report.stop_reason = Some(CasFsckStopReason::Cancelled);
        if apply {
            return Err(SinexError::validation(
                "refusing CAS orphan deletion because fsck was cancelled before scanning",
            ));
        }
        return Ok((report, file_statuses, initial_checkpoint));
    }
    let _admission = cas_fsck_admission().acquire(&cancellation).await?;
    let mut work = WorkController::new(
        WorkIdentity::ephemeral("cas-fsck", content_store.root_path().as_str()),
        WorkBudget {
            max_runtime: options.max_runtime,
            bytes_per_sec: options.verify_bytes_per_sec,
            ..WorkBudget::default()
        },
        cancellation,
    );
    let mut progress_checkpoint = initial_checkpoint;
    let mut scan_complete = false;

    // Build a set of known hashes from core.blobs for SINEXBLAKE3 entries
    let mut known_blake3_hashes = HashMap::new();
    for (hash, authority) in load_sinexblake3_hashes(pool, content_store).await? {
        known_blake3_hashes.entry(hash).or_insert(authority);
    }
    let mut continue_work = fsck_work_boundary(&mut work, &mut report)?;
    if continue_work {
        let (lease_references, stale_leases) = load_ingest_lease_references(content_store).await?;
        report.stale_leases = stale_leases;
        for (hash, authority) in lease_references {
            known_blake3_hashes.entry(hash).or_insert(authority);
        }
        continue_work = fsck_work_boundary(&mut work, &mut report)?;
    }
    let known_hash_set: HashSet<String> = known_blake3_hashes.keys().cloned().collect();

    if continue_work {
        continue_work =
            reconcile_pending_deletions(pool, content_store, apply, &mut report, &mut work).await?;
    }

    'scan: while continue_work {
        let batch = match walker.next_batch(256).await {
            Ok(batch) => batch,
            Err(_error) if work.cancellation().is_cancelled() => {
                report.incomplete = true;
                report.stop_reason = Some(CasFsckStopReason::Cancelled);
                break 'scan;
            }
            Err(error) => return Err(error),
        };
        for (hash, path, size) in batch.entries {
            if options
                .max_entries
                .is_some_and(|limit| report.entries_scanned >= limit)
            {
                report.incomplete = true;
                report.stop_reason = Some(CasFsckStopReason::EntryBudget);
                break 'scan;
            }
            if !fsck_work_boundary(&mut work, &mut report)? {
                break 'scan;
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
                let blob_id = known_blake3_hashes.get(&hash).cloned().unwrap_or_default();
                // Verify the file content matches the hash.
                let cancellation = work.cancellation();
                match verify_cas_file_content(&path, &hash, &cancellation).await {
                    Ok((matches, bytes_read)) => {
                        report.bytes_verified = report.bytes_verified.saturating_add(bytes_read);
                        if let Err(error) = work
                            .record_batch("verify", 1, bytes_read, Some(path.to_string()))
                            .await
                        {
                            if let Some(reason) = fsck_stop_reason(&work) {
                                report.incomplete = true;
                                report.stop_reason = Some(reason);
                                break 'scan;
                            }
                            return Err(error);
                        }
                        if matches {
                            let status = if blob_id.starts_with("lease:") {
                                report.leased += 1;
                                CasStatus::Leased
                            } else {
                                report.referenced += 1;
                                CasStatus::Referenced
                            };
                            file_statuses.push(CasFileStatus {
                                hash,
                                path: path.to_string(),
                                size_bytes: size,
                                status,
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
                        if let Some(reason) = fsck_stop_reason(&work) {
                            report.incomplete = true;
                            report.stop_reason = Some(reason);
                            break 'scan;
                        }
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
                // Orphaned: on disk, not in DB.
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
                file_statuses.push(CasFileStatus {
                    hash,
                    path: path.to_string(),
                    size_bytes: size,
                    status: CasStatus::Orphaned,
                    blob_id: None,
                });
            }
        }
        // Commit the cursor only after every entry in this batch has been
        // classified.  If cancellation or an entry budget stops inside the
        // batch, retaining the previous cursor makes the resumed pass replay
        // the batch instead of skipping its unfinished suffix.
        progress_checkpoint = batch.checkpoint.clone();
        if batch.complete {
            scan_complete = true;
            break;
        }
    }

    if !scan_complete && !report.incomplete {
        report.incomplete = true;
        report.stop_reason = Some(CasFsckStopReason::RuntimeBudget);
    }

    if report.incomplete {
        if apply {
            return Err(SinexError::validation(format!(
                "refusing CAS orphan deletion because fsck stopped before scanning the complete store ({:?})",
                report.stop_reason
            )));
        }
        return Ok((report, file_statuses, progress_checkpoint));
    }

    if apply && report.entries_scanned > 0 && known_blake3_hashes.is_empty() {
        return Err(SinexError::validation(
            "refusing CAS orphan deletion because the paired database has no SINEXBLAKE3 rows",
        ));
    }
    if apply
        && !known_blake3_hashes.is_empty()
        && report.entries_scanned > known_blake3_hashes.len().saturating_mul(2)
    {
        return Err(SinexError::validation(format!(
            "refusing CAS orphan deletion because the scanned store has an implausibly high orphan ratio ({} files vs {} DB rows)",
            report.entries_scanned,
            known_blake3_hashes.len()
        )));
    }

    // Detect missing: SINEXBLAKE3 blobs in DB but not on disk
    for (hash, blob_id) in &known_blake3_hashes {
        if !fsck_work_boundary(&mut work, &mut report)? {
            break;
        }
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

    // Apply deletion only after the complete, read-only classification pass.
    // The returned statuses are also the bounded walk's classification output,
    // so no separate orphan-candidate list is retained.
    if apply && !report.incomplete {
        apply_orphan_deletions(pool, content_store, &file_statuses, &mut report, &mut work).await?;
    }

    // Remove empty prefix directories after cleanup
    if apply && report.removed > 0 && !report.incomplete {
        if !fsck_destructive_boundary(&mut work, &mut report)? {
            return Err(SinexError::validation(
                "refusing CAS cleanup because fsck stopped before destructive cleanup",
            ));
        }
        clean_empty_cas_dirs(content_store).await;
    }

    Ok((report, file_statuses, progress_checkpoint))
}

/// Run a dry-run fsck with bounded CAS-sized memory.
///
/// Unlike `check_cas_with_options`, this API does not retain the returned
/// status list or the complete database authority set. It emits each status
/// to `on_status`, checks authority per file, and streams missing-authority
/// rows from PostgreSQL. The callback must provide any durable/reporting
/// sink the caller needs. Apply mode is intentionally unsupported because a
/// destructive pass needs a stable candidate set or an external snapshot.
pub async fn check_cas_bounded_with_control<F>(
    pool: &PgPool,
    content_store: &MaterialContentStore,
    apply: bool,
    options: CasFsckOptions,
    checkpoint: Option<CasWalkCheckpoint>,
    cancellation: WorkCancellation,
    mut on_status: F,
) -> RuntimeResult<(CasFsckReport, CasWalkCheckpoint)>
where
    F: FnMut(CasFileStatus),
{
    if apply {
        return Err(SinexError::validation(
            "bounded CAS fsck reporting does not support apply mode",
        ));
    }

    if cancellation.is_cancelled() {
        let checkpoint = checkpoint.unwrap_or_default();
        return Ok((
            CasFsckReport {
                incomplete: true,
                stop_reason: Some(CasFsckStopReason::Cancelled),
                ..CasFsckReport::default()
            },
            checkpoint,
        ));
    }
    let _admission = cas_fsck_admission().acquire(&cancellation).await?;

    let initial_checkpoint = checkpoint.unwrap_or_default();
    let mut walker = content_store
        .cas_walker_with_control(Some(initial_checkpoint.clone()), Some(cancellation.clone()))
        .await?;
    let mut report = CasFsckReport::default();
    let mut work = WorkController::new(
        WorkIdentity::ephemeral("cas-fsck-bounded", content_store.root_path().as_str()),
        WorkBudget {
            max_runtime: options.max_runtime,
            bytes_per_sec: options.verify_bytes_per_sec,
            ..WorkBudget::default()
        },
        cancellation,
    );
    let mut progress_checkpoint = initial_checkpoint;
    let mut scan_complete = false;

    if work.cancellation().is_cancelled() {
        report.incomplete = true;
        report.stop_reason = Some(CasFsckStopReason::Cancelled);
        return Ok((report, progress_checkpoint));
    }

    'scan: loop {
        let batch = match walker.next_batch(256).await {
            Ok(batch) => batch,
            Err(_error) if work.cancellation().is_cancelled() => {
                report.incomplete = true;
                report.stop_reason = Some(CasFsckStopReason::Cancelled);
                break 'scan;
            }
            Err(error) => return Err(error),
        };
        for (hash, path, size) in batch.entries {
            if options
                .max_entries
                .is_some_and(|limit| report.entries_scanned >= limit)
            {
                report.incomplete = true;
                report.stop_reason = Some(CasFsckStopReason::EntryBudget);
                break 'scan;
            }
            if let Err(error) = work.check(0, 0) {
                if let Some(reason) = fsck_stop_reason(&work) {
                    report.incomplete = true;
                    report.stop_reason = Some(reason);
                    break 'scan;
                }
                return Err(error);
            }
            report.entries_scanned += 1;
            if hash.contains(".tmp-") {
                report.staged += 1;
                on_status(CasFileStatus {
                    hash,
                    path: path.to_string(),
                    size_bytes: size,
                    status: CasStatus::Staged,
                    blob_id: None,
                });
                continue;
            }

            let blob_id = cas_hash_is_referenced(pool, content_store, &hash).await?;
            if let Some(blob_id) = blob_id {
                let cancellation = work.cancellation();
                match verify_cas_file_content(&path, &hash, &cancellation).await {
                    Ok((matches, bytes_read)) => {
                        report.bytes_verified = report.bytes_verified.saturating_add(bytes_read);
                        if let Err(error) = work
                            .record_batch("verify", 1, bytes_read, Some(path.to_string()))
                            .await
                        {
                            if let Some(reason) = fsck_stop_reason(&work) {
                                report.incomplete = true;
                                report.stop_reason = Some(reason);
                                break 'scan;
                            }
                            return Err(error);
                        }
                        if matches {
                            let status = if blob_id.starts_with("lease:") {
                                report.leased += 1;
                                CasStatus::Leased
                            } else {
                                report.referenced += 1;
                                CasStatus::Referenced
                            };
                            on_status(CasFileStatus {
                                hash,
                                path: path.to_string(),
                                size_bytes: size,
                                status,
                                blob_id: Some(blob_id),
                            });
                        } else {
                            report.corrupt += 1;
                            on_status(CasFileStatus {
                                hash,
                                path: path.to_string(),
                                size_bytes: size,
                                status: CasStatus::Corrupt,
                                blob_id: Some(blob_id),
                            });
                        }
                    }
                    Err(error) => {
                        if let Some(reason) = fsck_stop_reason(&work) {
                            report.incomplete = true;
                            report.stop_reason = Some(reason);
                            break 'scan;
                        }
                        report.malformed += 1;
                        tracing::warn!(
                            error = %error,
                            hash = %hash,
                            "Failed to verify CAS file content"
                        );
                        on_status(CasFileStatus {
                            hash,
                            path: path.to_string(),
                            size_bytes: size,
                            status: CasStatus::Malformed,
                            blob_id: Some(blob_id),
                        });
                    }
                }
            } else {
                report.orphaned += 1;
                report.orphaned_bytes = report.orphaned_bytes.saturating_add(size);
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
                on_status(CasFileStatus {
                    hash,
                    path: path.to_string(),
                    size_bytes: size,
                    status: CasStatus::Orphaned,
                    blob_id: None,
                });
            }
        }
        // A status sink can cancel after the final entry in a batch. Observe
        // that request before advancing the resumable cursor or claiming the
        // bounded walk completed.
        if !fsck_work_boundary(&mut work, &mut report)? {
            break 'scan;
        }
        progress_checkpoint = batch.checkpoint.clone();
        if batch.complete {
            scan_complete = true;
            break;
        }
    }

    if !scan_complete && !report.incomplete {
        report.incomplete = true;
        report.stop_reason = Some(CasFsckStopReason::RuntimeBudget);
    }
    if report.incomplete {
        return Ok((report, progress_checkpoint));
    }

    if !fsck_work_boundary(&mut work, &mut report)? {
        return Ok((report, progress_checkpoint));
    }

    let mut authority_count = 0_usize;
    let mut blob_rows = sqlx::query_as::<_, (String, String)>(
        r"
        SELECT content_hash, id::text
        FROM core.blobs
        WHERE annex_backend = $1
        ",
    )
    .bind(LOCAL_BLAKE3_CAS_BACKEND)
    .fetch(pool);
    while let Some(row) = blob_rows.next().await {
        if !fsck_work_boundary(&mut work, &mut report)? {
            return Ok((report, progress_checkpoint));
        }
        let (hash, blob_id) = row.map_err(|error| {
            SinexError::database("stream SINEXBLAKE3 hashes for bounded CAS fsck")
                .with_source(error)
        })?;
        authority_count += 1;
        if content_store
            .local_blake3_cas_path_for_hash(&hash)
            .is_ok_and(|path| !path.exists())
        {
            report.missing += 1;
            on_status(CasFileStatus {
                hash: hash.clone(),
                path: missing_cas_path(content_store, &hash).to_string(),
                size_bytes: 0,
                status: CasStatus::Missing,
                blob_id: Some(blob_id),
            });
        }
        if !fsck_work_boundary(&mut work, &mut report)? {
            return Ok((report, progress_checkpoint));
        }
    }

    let mut material_rows = sqlx::query_as::<_, (String, JsonValue)>(
        r"
        SELECT id::text, metadata
        FROM raw.source_material_registry
        WHERE metadata->'material_manifest'->>'content_key' IS NOT NULL
        ",
    )
    .fetch(pool);
    while let Some(row) = material_rows.next().await {
        if !fsck_work_boundary(&mut work, &mut report)? {
            return Ok((report, progress_checkpoint));
        }
        let (material_id, metadata) = row.map_err(|error| {
            SinexError::database("stream material manifest hashes for bounded CAS fsck")
                .with_source(error)
        })?;
        for (hash, authority) in
            material_manifest_authorities(content_store, &material_id, &metadata).await?
        {
            authority_count += 1;
            if content_store
                .local_blake3_cas_path_for_hash(&hash)
                .is_ok_and(|path| !path.exists())
            {
                report.missing += 1;
                on_status(CasFileStatus {
                    hash: hash.clone(),
                    path: missing_cas_path(content_store, &hash).to_string(),
                    size_bytes: 0,
                    status: CasStatus::Missing,
                    blob_id: Some(authority),
                });
            }
            if !fsck_work_boundary(&mut work, &mut report)? {
                return Ok((report, progress_checkpoint));
            }
        }
    }

    if !fsck_work_boundary(&mut work, &mut report)? {
        return Ok((report, progress_checkpoint));
    }

    if report.entries_scanned > authority_count.saturating_mul(2) {
        tracing::warn!(
            entries_scanned = report.entries_scanned,
            authority_count,
            "bounded CAS fsck observed a high orphan ratio"
        );
    }
    Ok((report, progress_checkpoint))
}

fn missing_cas_path(content_store: &MaterialContentStore, hash: &str) -> camino::Utf8PathBuf {
    content_store
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
        )
}

/// Reconcile objects left in the durable quarantine by a prior sweep. A
/// reference that reappears wins over deletion; otherwise the quarantine grace
/// period gives an in-flight database commit time to publish its authority.
async fn reconcile_pending_deletions(
    pool: &PgPool,
    content_store: &MaterialContentStore,
    apply: bool,
    report: &mut CasFsckReport,
    work: &mut WorkController,
) -> RuntimeResult<bool> {
    let pending = content_store.list_pending_deletions().await?;
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    for record in pending {
        if !fsck_work_boundary(work, report)? {
            return Ok(false);
        }
        if !apply {
            report.pending_deletes += 1;
            continue;
        }
        content_store
            .fault_injector()
            .inject(crate::runtime::FaultPoint::CasReconciliation)?;
        if cas_hash_is_referenced(pool, content_store, &record.key.digest)
            .await?
            .is_some()
        {
            if !fsck_destructive_boundary(work, report)? {
                return Ok(false);
            }
            content_store.restore_pending_deletion(&record).await?;
            report.restored += 1;
            continue;
        }
        if now.saturating_sub(record.created_at_unix_secs) < CAS_ORPHAN_GRACE.as_secs() {
            report.pending_deletes += 1;
            continue;
        }
        if !fsck_destructive_boundary(work, report)? {
            return Ok(false);
        }
        match content_store.finalize_pending_deletion(&record).await {
            Ok(()) => report.removed += 1,
            Err(error) => {
                report.pending_deletes += 1;
                tracing::warn!(
                    operation_id = %record.operation_id,
                    error = %error,
                    "Failed to finalize pending CAS deletion; retaining record for retry"
                );
            }
        }
    }
    Ok(true)
}

/// Apply the destructive half of a complete CAS classification pass. Every
/// filesystem mutation has an immediately preceding cancellation/runtime
/// boundary so cancellation cannot begin another deletion after it is observed.
async fn apply_orphan_deletions(
    pool: &PgPool,
    content_store: &MaterialContentStore,
    file_statuses: &[CasFileStatus],
    report: &mut CasFsckReport,
    work: &mut WorkController,
) -> RuntimeResult<()> {
    for status in file_statuses
        .iter()
        .filter(|status| status.status == CasStatus::Orphaned)
    {
        if !fsck_work_boundary(work, report)? {
            return Ok(());
        }
        let hash = &status.hash;
        let path = &status.path;
        let size = status.size_bytes;
        let is_recent = tokio::fs::metadata(path)
            .await
            .ok()
            .and_then(|metadata| metadata.modified().ok())
            .is_some_and(|modified| {
                SystemTime::now()
                    .duration_since(modified)
                    .is_ok_and(|age| age < CAS_ORPHAN_GRACE)
            });
        if is_recent {
            continue;
        }
        if cas_hash_is_referenced(pool, content_store, hash)
            .await?
            .is_some()
        {
            report.recheck_protected += 1;
            continue;
        }
        let key = ContentStoreKey::parse(&format!("{LOCAL_BLAKE3_CAS_BACKEND}-s{size}--{hash}"))?;
        if !fsck_destructive_boundary(work, report)? {
            return Ok(());
        }
        match content_store.quarantine_local_cas(&key).await {
            Ok(Some(_pending)) => {
                report.quarantined += 1;
                report.pending_deletes += 1;
            }
            Ok(None) => {}
            Err(error) => tracing::warn!(
                error = %error,
                path = %path,
                size_bytes = size,
                "Failed to quarantine orphaned CAS file; retaining it for retry"
            ),
        }
    }
    Ok(())
}

fn fsck_work_boundary(
    work: &mut WorkController,
    report: &mut CasFsckReport,
) -> RuntimeResult<bool> {
    match work.check(0, 0) {
        Ok(()) => Ok(true),
        Err(error) => fsck_boundary_error(work, report, error),
    }
}

fn fsck_destructive_boundary(
    work: &mut WorkController,
    report: &mut CasFsckReport,
) -> RuntimeResult<bool> {
    match work.destructive_boundary_check() {
        Ok(()) => Ok(true),
        Err(error) => fsck_boundary_error(work, report, error),
    }
}

fn fsck_boundary_error(
    work: &WorkController,
    report: &mut CasFsckReport,
    error: SinexError,
) -> RuntimeResult<bool> {
    if let Some(reason) = fsck_stop_reason(work) {
        report.incomplete = true;
        report.stop_reason = Some(reason);
        return Ok(false);
    }
    Err(error)
}

fn fsck_stop_reason(work: &WorkController) -> Option<CasFsckStopReason> {
    match work.outcome() {
        WorkOutcome::Cancelled => Some(CasFsckStopReason::Cancelled),
        WorkOutcome::Partial(WorkStopReason::Cancelled) => Some(CasFsckStopReason::Cancelled),
        WorkOutcome::Partial(WorkStopReason::RuntimeBudget) => {
            Some(CasFsckStopReason::RuntimeBudget)
        }
        WorkOutcome::Partial(WorkStopReason::ItemBudget) => Some(CasFsckStopReason::EntryBudget),
        WorkOutcome::Partial(WorkStopReason::ByteBudget)
        | WorkOutcome::Completed
        | WorkOutcome::Failed => None,
    }
}

/// Re-check DB authority immediately before destructive filesystem mutation.
/// The initial hash snapshot is intentionally not sufficient: a material/blob
/// commit can race a long fsck walk and publish a reference after the snapshot.
async fn cas_hash_is_referenced(
    pool: &PgPool,
    content_store: &MaterialContentStore,
    hash: &str,
) -> RuntimeResult<Option<String>> {
    let database_authority = sqlx::query_scalar::<_, String>(
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
    })?;
    if database_authority.is_some() {
        return Ok(database_authority);
    }

    // A V1 material manifest is a two-object authority: its registry row
    // names the manifest object, and the manifest names the exact encoded
    // source bytes.  The SQL check above can see only the first half.  Re-read
    // the manifest before finalizing a quarantine so a material reference that
    // appears after the initial fsck snapshot can restore its encoded bytes.
    if let Some(authority) = material_manifest_hash_authority(pool, content_store, hash).await? {
        return Ok(Some(authority));
    }

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    Ok(content_store
        .list_write_leases()
        .await?
        .into_iter()
        .find(|lease| {
            lease.key.digest == hash
                && now.saturating_sub(lease.created_at_unix_secs) < CAS_INGEST_LEASE_GRACE.as_secs()
        })
        .map(|lease| format!("lease:{}", lease.operation_id)))
}

async fn material_manifest_hash_authority(
    pool: &PgPool,
    content_store: &MaterialContentStore,
    hash: &str,
) -> RuntimeResult<Option<String>> {
    let material_rows = sqlx::query_as::<_, (String, JsonValue)>(
        r"
        SELECT id::text, metadata
        FROM raw.source_material_registry
        WHERE metadata->'material_manifest'->>'content_key' IS NOT NULL
        ",
    )
    .fetch(pool);

    futures::pin_mut!(material_rows);
    while let Some(row) = material_rows.next().await {
        let (material_id, metadata) = row.map_err(|error| {
            SinexError::database("stream source-material manifests for CAS authority recheck")
                .with_source(error)
        })?;
        if let Some((_, authority)) =
            material_manifest_authorities(content_store, &material_id, &metadata)
                .await?
                .into_iter()
                .find(|(candidate, _)| candidate == hash)
        {
            return Ok(Some(authority));
        }
    }
    Ok(None)
}

async fn load_ingest_lease_references(
    content_store: &MaterialContentStore,
) -> RuntimeResult<(Vec<(String, String)>, usize)> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let mut references = Vec::new();
    let mut stale = 0;
    for lease in content_store.list_write_leases().await? {
        if now.saturating_sub(lease.created_at_unix_secs) < CAS_INGEST_LEASE_GRACE.as_secs() {
            references.push((lease.key.digest, format!("lease:{}", lease.operation_id)));
        } else {
            stale += 1;
        }
    }
    Ok((references, stale))
}

/// Load all BLAKE3 hashes that have a durable database authority.
///
/// Manifests are intentionally not ordinary user blobs: they are stored in CAS
/// and referenced from source-material metadata. Include those references in
/// the fsck authority set so a normal orphan sweep cannot delete replay
/// metadata that is still needed by a live material row.
async fn load_sinexblake3_hashes(
    pool: &PgPool,
    content_store: &MaterialContentStore,
) -> RuntimeResult<Vec<(String, String)>> {
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
        references
            .extend(material_manifest_authorities(content_store, &material_id, &metadata).await?);
    }
    Ok(references)
}

/// Return every CAS object that a source-material manifest makes authoritative.
///
/// The registry metadata names the manifest object itself. A valid V1 manifest
/// additionally names the exact encoded material through its existing digest
/// and size fields. Chunk/pack labels remain observed metadata; this helper
/// never invents child objects from those labels.
async fn material_manifest_authorities(
    content_store: &MaterialContentStore,
    material_id: &str,
    metadata: &JsonValue,
) -> RuntimeResult<Vec<(String, String)>> {
    let Some(content_key) = metadata
        .get("material_manifest")
        .and_then(JsonValue::as_object)
        .and_then(|manifest| manifest.get("content_key"))
        .and_then(JsonValue::as_str)
    else {
        return Ok(Vec::new());
    };
    let Ok(parsed) = ContentStoreKey::parse(content_key) else {
        return Ok(Vec::new());
    };
    if !parsed.is_local_blake3_cas() {
        return Ok(Vec::new());
    }

    let mut references = vec![(
        parsed.digest.clone(),
        format!("material-manifest:{material_id}"),
    )];
    let Some(path) = content_store.path_if_local(content_key)? else {
        return Ok(references);
    };
    if !path.exists() {
        return Ok(references);
    }
    let manifest_bytes =
        match MaterialContentStore::read_file_with_limit(&path, content_store.config.max_blob_size)
            .await
        {
            Ok(bytes) => bytes,
            Err(error) => {
                tracing::warn!(
                    material_id,
                    content_key,
                    error = %error,
                    "unable to read source-material manifest while reconciling CAS authorities"
                );
                return Ok(references);
            }
        };
    let Ok(DecodedMaterialManifest::V1(manifest)) = MaterialManifestV1::decode(&manifest_bytes)
    else {
        return Ok(references);
    };
    if manifest.source_material_id.to_string() != material_id
        || manifest.validate().is_err()
        || manifest.canonical_bytes().ok().as_deref() != Some(manifest_bytes.as_slice())
    {
        return Ok(references);
    }
    let Ok(encoded_key) = ContentStoreKey::local_blake3(
        manifest.bytes.encoded_size,
        manifest.bytes.encoded.value_hex,
    ) else {
        return Ok(references);
    };
    references.push((encoded_key.digest, format!("material-bytes:{material_id}")));
    Ok(references)
}

/// Verify that a CAS file's BLAKE3 hash matches its filename.
async fn verify_cas_file_content(
    path: &camino::Utf8Path,
    expected_hash: &str,
    cancellation: &WorkCancellation,
) -> RuntimeResult<(bool, u64)> {
    const BUFFER_SIZE: usize = 1024 * 1024;
    if cancellation.is_cancelled() {
        return Err(SinexError::validation("CAS verification cancelled"));
    }
    let mut file = tokio::fs::File::open(path).await.map_err(SinexError::io)?;
    let mut hasher = blake3::Hasher::new();
    let mut buffer = vec![0_u8; BUFFER_SIZE];
    let mut bytes_read = 0_u64;
    loop {
        if cancellation.is_cancelled() {
            return Err(SinexError::validation("CAS verification cancelled"));
        }
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
