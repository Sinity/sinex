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
//! by the periodic GC task in `sinex-ingestd`.

use crate::{NodeResult, SinexError};
use serde::Serialize;
use sinex_db::DbPoolExt;
use sqlx::PgPool;

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

/// Sweep orphaned content-store keys (unused AND no matching `core.blobs` row).
///
/// When `legacy_annex_enabled` is false, delegates to the CAS fsck walker.
///
/// `apply = false` is a dry-run; returns counts but drops nothing.
pub async fn sweep_orphans(
    pool: &PgPool,
    content_store: &MaterialContentStore,
    apply: bool,
) -> NodeResult<BlobGcReport> {
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

/// Like `sweep_orphans` but also returns the orphan entries themselves so callers
/// (e.g. the CLI) can render per-key detail without re-iterating.
///
/// When `legacy_annex_enabled` is false, delegates to the CAS fsck walker.
pub async fn sweep_orphans_detailed(
    pool: &PgPool,
    content_store: &MaterialContentStore,
    apply: bool,
) -> NodeResult<(BlobGcReport, Vec<UnusedContentEntry>)> {
    if !content_store.config.legacy_annex_enabled {
        let (cas_report, _) =
            super::cas_fsck::check_cas(pool, content_store, apply).await?;
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
