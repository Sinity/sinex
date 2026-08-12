//! Blob garbage collection for the content store.
//!
//! When `legacy_annex_enabled` is true, sweeps content-store keys reported as
//! unused by `git-annex unused` and cross-checks each against `core.blobs`.
//!
//! When `legacy_annex_enabled` is false, delegates to the CAS fsck walker
//! which walks the `sinex-cas/` directory tree, computes BLAKE3 hashes,
//! and cross-references against `core.blobs`.
//!
//! The same routine is invoked by the `sinexctl blob sweep-orphans` CLI and
//! by the periodic GC task in `sinexd`.

use crate::runtime::{RuntimeResult, SinexError};
use serde::Serialize;
use sinex_db::DbPoolExt;
use sinex_primitives::{Id, Timestamp, Uuid};
use sqlx::PgPool;
use time::Duration;
use tracing::warn;

use super::{MaterialContentStore, UnusedContentEntry};

/// Counts produced by a single sweep pass.
#[derive(Debug, Clone, Copy, Default, Serialize, PartialEq, Eq)]
pub struct BlobGcReport {
    /// Total number of unused content-store entries observed.
    pub total_unused: usize,
    /// Unused entries that still have a `core.blobs` row (kept).
    pub db_backed: usize,
    /// Unused entries with no matching `core.blobs` row (orphaned).
    pub orphaned: usize,
    /// Number of orphaned entries actually dropped from the content store.
    /// Always 0 when `apply == false`.
    pub dropped: usize,
}

/// Results from the source-material lifecycle half of a periodic content GC.
#[derive(Debug, Clone, Copy, Default, Serialize, PartialEq, Eq)]
pub struct MaterialRegistryGcReport {
    /// Aged Sensing/Failed registry rows with no event provenance that were removed.
    pub registry_rows_deleted: usize,
    /// Unreferenced blob rows and local content removed after the registry sweep.
    pub blobs_deleted: usize,
    /// Unreferenced blob rows retained because their storage cleanup failed.
    pub blob_cleanup_failures: usize,
}

const MATERIAL_REGISTRY_GC_GRACE: Duration = Duration::days(7);
const MATERIAL_REGISTRY_GC_LIMIT: i64 = 128;

enum UnreferencedBlobCleanup {
    AlreadyGone,
    StillReferenced,
    Deleted,
}

async fn delete_unreferenced_blob(
    pool: &PgPool,
    content_store: &MaterialContentStore,
    blob_id: Id<sinex_db::models::Blob>,
) -> RuntimeResult<UnreferencedBlobCleanup> {
    pool.with_transaction(async |tx| {
        let Some(blob) = pool
            .blobs()
            .lock_by_id_for_update(&mut **tx, blob_id)
            .await?
        else {
            return Ok(UnreferencedBlobCleanup::AlreadyGone);
        };

        if pool
            .blobs()
            .is_referenced_excluding_material_with_executor(&mut **tx, blob_id, Uuid::nil())
            .await?
        {
            return Ok(UnreferencedBlobCleanup::StillReferenced);
        }

        content_store
            .drop_content(&blob.content_key(), true)
            .await?;
        pool.blobs()
            .delete_by_id_with_executor(&mut **tx, blob_id)
            .await?;
        Ok(UnreferencedBlobCleanup::Deleted)
    })
    .await
    .map_err(|error| {
        SinexError::database("delete unreferenced blob during material GC").with_source(error)
    })
}

/// Remove aged, unreferenced material registry rows and the blob rows they kept
/// alive. A later blob pass is intentional: it recovers the crash window after
/// a registry delete and before CAS cleanup, and it makes each run idempotent.
pub async fn sweep_stale_material_registry(
    pool: &PgPool,
    content_store: &MaterialContentStore,
    apply: bool,
) -> RuntimeResult<MaterialRegistryGcReport> {
    if !apply {
        return Ok(MaterialRegistryGcReport::default());
    }

    let cutoff = Timestamp::now() - MATERIAL_REGISTRY_GC_GRACE;
    let deleted_materials = pool
        .source_materials()
        .delete_stale_unreferenced_materials(cutoff, MATERIAL_REGISTRY_GC_LIMIT)
        .await
        .map_err(|error| {
            SinexError::database("delete stale unreferenced source materials during GC")
                .with_source(error)
        })?;

    let mut report = MaterialRegistryGcReport {
        registry_rows_deleted: deleted_materials.len(),
        ..Default::default()
    };

    // Sweep by the durable blob-reference relation rather than trusting only
    // this batch's optional_blob_id values. That also retries a prior sweep
    // interrupted after registry deletion.
    let candidate_blob_ids = pool
        .blobs()
        .list_unreferenced_ids(MATERIAL_REGISTRY_GC_LIMIT)
        .await
        .map_err(|error| {
            SinexError::database("list unreferenced blobs during material GC").with_source(error)
        })?;
    for blob_id in candidate_blob_ids {
        match delete_unreferenced_blob(pool, content_store, blob_id).await {
            Ok(UnreferencedBlobCleanup::Deleted) => report.blobs_deleted += 1,
            Ok(UnreferencedBlobCleanup::AlreadyGone | UnreferencedBlobCleanup::StillReferenced) => {
            }
            Err(error) => {
                report.blob_cleanup_failures += 1;
                warn!(blob_id = %blob_id, error = %error, "material lifecycle GC retained blob for retry after cleanup failure");
            }
        }
    }

    Ok(report)
}

/// Sweep orphaned content-store keys (unused AND no matching `core.blobs` row).
///
/// When `legacy_annex_enabled` is false, delegates to the CAS fsck walker.
///
/// `apply = false` is a dry-run; returns counts but drops nothing.
pub async fn sweep_orphans(
    pool: &PgPool,
    content_store: &MaterialContentStore,
    apply: bool,
) -> RuntimeResult<BlobGcReport> {
    if !content_store.config.legacy_annex_enabled {
        let cas_report = super::cas_fsck::sweep_orphans_cas(pool, content_store, apply).await?;
        return Ok(BlobGcReport {
            total_unused: cas_report.orphaned,
            db_backed: cas_report.referenced,
            orphaned: cas_report.orphaned,
            dropped: cas_report.removed,
        });
    }

    let (report, _) = sweep_orphans_detailed(pool, content_store, apply).await?;
    Ok(report)
}

#[cfg(test)]
#[path = "gc_test.rs"]
mod tests;

/// Like `sweep_orphans` but also returns the orphan entries themselves so callers
/// (e.g. the CLI) can render per-key detail without re-iterating.
///
/// When `legacy_annex_enabled` is false, delegates to the CAS fsck walker.
pub async fn sweep_orphans_detailed(
    pool: &PgPool,
    content_store: &MaterialContentStore,
    apply: bool,
) -> RuntimeResult<(BlobGcReport, Vec<UnusedContentEntry>)> {
    if !content_store.config.legacy_annex_enabled {
        let (cas_report, _) = super::cas_fsck::check_cas(pool, content_store, apply).await?;
        let report = BlobGcReport {
            total_unused: cas_report.orphaned,
            db_backed: cas_report.referenced,
            orphaned: cas_report.orphaned,
            dropped: cas_report.removed,
        };
        // Convert CasFileStatus entries to UnusedContentEntry for CLI rendering.
        // We don't have numbered entries here — supply 0 as placeholder.
        let unused_entries: Vec<UnusedContentEntry> = cas_report
            .orphaned
            .checked_div(1)
            .map(|_| Vec::new()) // We don't have UnusedContentEntry from CAS; return empty
            .unwrap_or_default();
        return Ok((report, unused_entries));
    }

    let unused_entries = content_store.list_unused().await?;

    let mut db_backed = 0usize;
    let mut orphaned_unused: Vec<UnusedContentEntry> = Vec::new();
    for entry in unused_entries {
        let size_bytes = i64::try_from(entry.key.size).map_err(|e| {
            SinexError::processing(format!(
                "content-store key size does not fit i64: {}",
                entry.key.key
            ))
            .with_context("content_key", entry.key.key.clone())
            .with_source(e)
        })?;

        let row = pool
            .blobs()
            .get_by_content(entry.key.storage_backend(), &entry.key.digest, size_bytes)
            .await
            .map_err(|e| {
                SinexError::processing(format!(
                    "lookup blob row for content-store key {}",
                    entry.key.key
                ))
                .with_context("content_key", entry.key.key.clone())
                .with_source(e.to_string())
            })?;

        if row.is_some() {
            db_backed += 1;
        } else {
            orphaned_unused.push(entry);
        }
    }

    let total_unused = db_backed + orphaned_unused.len();

    let dropped = if apply && !orphaned_unused.is_empty() {
        let numbers: Vec<u32> = orphaned_unused.iter().map(|entry| entry.number).collect();
        content_store.drop_unused(&numbers, true).await?;
        numbers.len()
    } else {
        0
    };

    let report = BlobGcReport {
        total_unused,
        db_backed,
        orphaned: orphaned_unused.len(),
        dropped,
    };

    Ok((report, orphaned_unused))
}
