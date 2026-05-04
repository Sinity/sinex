use camino::Utf8PathBuf;
use clap::{Parser, Subcommand};
use color_eyre::eyre::{WrapErr, eyre};
use serde::Serialize;
use sinex_db::create_pool;
use sinex_primitives::Uuid;
use sinex_node_sdk::content_store::{
    CasFsckReport, CasFileStatus, ContentStoreConfig, MaterialContentStore, UnusedContentEntry,
    cas_fsck::check_cas,
    gc::{BlobGcReport, sweep_orphans_detailed},
};

use crate::Result;
use crate::fmt::{CommandOutput, format_bytes};
use crate::model::OutputFormat;

#[derive(Debug, Subcommand)]
pub enum BlobCommands {
    /// Reclaim unused content-store keys that no longer have a matching `core.blobs` row.
    SweepOrphans(BlobSweepOrphansCommand),
    /// Walk the local CAS tree and cross-reference against `core.blobs`.
    Fsck(BlobFsckCommand),
    /// Migrate blobs from legacy git-annex to local BLAKE3 CAS.
    Migrate(BlobMigrateCommand),
}

impl BlobCommands {
    pub async fn execute(&self, format: OutputFormat) -> Result<()> {
        match self {
            Self::SweepOrphans(cmd) => cmd.execute(format).await,
            Self::Fsck(cmd) => cmd.execute(format).await,
            Self::Migrate(cmd) => cmd.execute(format).await,
        }
    }
}

// ── SweepOrphans ──────────────────────────────────────────────────

#[derive(Debug, Parser)]
#[command(after_help = "\
EXAMPLES:
    # Show content-store keys that are unused and have no DB blob row
    sinexctl blob sweep-orphans

    # Actually drop those orphaned keys from the large-object backend
    sinexctl blob sweep-orphans --apply
")]
pub struct BlobSweepOrphansCommand {
    /// Content-store root path.
    #[arg(long, env = "SINEX_CONTENT_STORE_PATH")]
    pub content_store_path: Utf8PathBuf,

    /// Drop orphaned keys instead of only reporting them.
    #[arg(long)]
    pub apply: bool,
}

#[derive(Debug, Serialize)]
struct BlobSweepSummary {
    content_store_path: String,
    mode: &'static str,
    total_unused_entries: usize,
    db_backed_entries: usize,
    orphaned_entries: usize,
    dropped_entries: usize,
    orphaned_keys: Vec<BlobOrphanEntry>,
}

#[derive(Debug, Serialize)]
struct BlobOrphanEntry {
    number: u32,
    key: String,
    size_bytes: u64,
}

impl BlobSweepOrphansCommand {
    pub async fn execute(&self, format: OutputFormat) -> Result<()> {
        let database_url = std::env::var("DATABASE_URL").map_err(|_| {
            eyre!(
                "DATABASE_URL not set. Set it in your environment before running direct blob maintenance commands."
            )
        })?;
        let pool = create_pool(&database_url)
            .await
            .wrap_err("connect database for blob orphan sweep")?;

        let content_store = MaterialContentStore::new(ContentStoreConfig {
            root_path: self.content_store_path.clone(),
            num_copies: None,
            large_files: None,
            ..Default::default()
        })
        .wrap_err_with(|| format!("open content-store root {}", self.content_store_path))?;

        let (report, orphan_entries) = sweep_orphans_detailed(&pool, &content_store, self.apply)
            .await
            .wrap_err("sweep content-store orphans")?;

        let BlobGcReport {
            total_unused,
            db_backed,
            orphaned,
            dropped,
        } = report;

        let summary = BlobSweepSummary {
            content_store_path: self.content_store_path.to_string(),
            mode: if self.apply { "apply" } else { "dry-run" },
            total_unused_entries: total_unused,
            db_backed_entries: db_backed,
            orphaned_entries: orphaned,
            dropped_entries: dropped,
            orphaned_keys: orphan_entries.into_iter().map(blob_orphan_entry).collect(),
        };

        CommandOutput::single(summary, format_blob_sweep_summary).display(&format)
    }
}

fn blob_orphan_entry(entry: UnusedContentEntry) -> BlobOrphanEntry {
    BlobOrphanEntry {
        number: entry.number,
        key: entry.key.key,
        size_bytes: entry.key.size,
    }
}

fn format_blob_sweep_summary(summary: &BlobSweepSummary) -> String {
    let mut output = String::new();
    output.push_str("Blob Orphan Sweep\n");
    output.push_str(&format!(
        "  Content Store: {}\n",
        summary.content_store_path
    ));
    output.push_str(&format!("  Mode: {}\n", summary.mode));
    output.push_str(&format!(
        "  Total Unused Entries: {}\n",
        summary.total_unused_entries
    ));
    output.push_str(&format!(
        "  DB-backed Entries: {}\n",
        summary.db_backed_entries
    ));
    output.push_str(&format!(
        "  Orphaned Entries: {}\n",
        summary.orphaned_entries
    ));
    output.push_str(&format!("  Dropped Entries: {}\n", summary.dropped_entries));
    if !summary.orphaned_keys.is_empty() {
        output.push_str("  Orphaned Keys:\n");
        for orphan in &summary.orphaned_keys {
            output.push_str(&format!(
                "    {}  {}  ({})\n",
                orphan.number,
                orphan.key,
                format_bytes(orphan.size_bytes)
            ));
        }
    }
    output
}

// ── Fsck ──────────────────────────────────────────────────────────

#[derive(Debug, Parser)]
#[command(after_help = "\
EXAMPLES:
    # Check local CAS integrity (dry-run)
    sinexctl blob fsck

    # Remove orphaned CAS files
    sinexctl blob fsck --apply
")]
pub struct BlobFsckCommand {
    /// Content-store root path.
    #[arg(long, env = "SINEX_CONTENT_STORE_PATH")]
    pub content_store_path: Utf8PathBuf,

    /// Remove orphaned CAS files instead of only reporting them.
    #[arg(long)]
    pub apply: bool,
}

#[derive(Debug, Serialize)]
struct BlobFsckSummary {
    content_store_path: String,
    mode: &'static str,
    referenced: usize,
    orphaned: usize,
    corrupt: usize,
    malformed: usize,
    missing: usize,
    removed: usize,
    orphaned_bytes: u64,
    details: Vec<CasFileDetail>,
}

#[derive(Debug, Serialize)]
struct CasFileDetail {
    hash: String,
    path: String,
    size_bytes: u64,
    status: String,
    blob_id: Option<String>,
}

impl BlobFsckCommand {
    pub async fn execute(&self, format: OutputFormat) -> Result<()> {
        let database_url = std::env::var("DATABASE_URL").map_err(|_| {
            eyre!("DATABASE_URL not set. Set it in your environment before running blob fsck.")
        })?;
        let pool = create_pool(&database_url)
            .await
            .wrap_err("connect database for blob fsck")?;

        let content_store = MaterialContentStore::new(ContentStoreConfig {
            root_path: self.content_store_path.clone(),
            ..Default::default()
        })
        .wrap_err_with(|| format!("open content-store root {}", self.content_store_path))?;

        let (report, file_statuses) = check_cas(&pool, &content_store, self.apply)
            .await
            .wrap_err("CAS filesystem check")?;

        let CasFsckReport {
            referenced,
            orphaned,
            corrupt,
            malformed,
            missing,
            removed,
            orphaned_bytes,
        } = report;

        let details: Vec<CasFileDetail> = file_statuses
            .iter()
            .map(|fs| CasFileDetail {
                hash: fs.hash.clone(),
                path: fs.path.clone(),
                size_bytes: fs.size_bytes,
                status: format!("{:?}", fs.status).to_lowercase(),
                blob_id: fs.blob_id.clone(),
            })
            .collect();

        let summary = BlobFsckSummary {
            content_store_path: self.content_store_path.to_string(),
            mode: if self.apply { "apply" } else { "dry-run" },
            referenced,
            orphaned,
            corrupt,
            malformed,
            missing,
            removed,
            orphaned_bytes,
            details,
        };

        CommandOutput::single(summary, format_blob_fsck_summary).display(&format)
    }
}

fn format_blob_fsck_summary(summary: &BlobFsckSummary) -> String {
    let mut output = String::new();
    output.push_str("Blob CAS Fsck\n");
    output.push_str(&format!(
        "  Content Store: {}\n",
        summary.content_store_path
    ));
    output.push_str(&format!("  Mode: {}\n", summary.mode));
    output.push_str(&format!("  Referenced (healthy): {}\n", summary.referenced));
    output.push_str(&format!("  Orphaned: {}\n", summary.orphaned));
    output.push_str(&format!(
        "  Orphaned bytes: {}\n",
        format_bytes(summary.orphaned_bytes)
    ));
    output.push_str(&format!("  Corrupt: {}\n", summary.corrupt));
    output.push_str(&format!("  Malformed: {}\n", summary.malformed));
    output.push_str(&format!("  Missing (DB, not disk): {}\n", summary.missing));
    output.push_str(&format!("  Removed: {}\n", summary.removed));
    for d in &summary.details {
        output.push_str(&format!(
            "    [{}] {}  {}  ({})\n",
            d.status,
            d.hash,
            d.path,
            format_bytes(d.size_bytes)
        ));
    }
    output
}

// ── Migrate ────────────────────────────────────────────────────────

#[derive(Debug, Parser)]
#[command(after_help = "\
EXAMPLES:
    # Dry-run migration from git-annex to local CAS
    sinexctl blob migrate --from git-annex --to local-cas

    # Execute migration
    sinexctl blob migrate --from git-annex --to local-cas --apply
")]
pub struct BlobMigrateCommand {
    /// Content-store root path.
    #[arg(long, env = "SINEX_CONTENT_STORE_PATH")]
    pub content_store_path: Utf8PathBuf,

    /// Source backend (only 'git-annex' supported).
    #[arg(long, default_value = "git-annex")]
    pub from: String,

    /// Target backend (only 'local-cas' supported).
    #[arg(long, default_value = "local-cas")]
    pub to: String,

    /// Execute the migration instead of dry-run.
    #[arg(long)]
    pub apply: bool,
}

#[derive(Debug, Serialize)]
struct BlobMigrateSummary {
    content_store_path: String,
    mode: &'static str,
    from: String,
    to: String,
    total_annex_blobs: usize,
    already_migrated: usize,
    migrated: usize,
    failed: usize,
    migrated_keys: Vec<MigratedKey>,
}

#[derive(Debug, Serialize)]
struct MigratedKey {
    annex_key: String,
    cas_key: String,
    size_bytes: i64,
}

impl BlobMigrateCommand {
    pub async fn execute(&self, format: OutputFormat) -> Result<()> {
        if self.from != "git-annex" || self.to != "local-cas" {
            return Err(eyre!(
                "Only migration from 'git-annex' to 'local-cas' is currently supported."
            ));
        }

        let database_url = std::env::var("DATABASE_URL").map_err(|_| {
            eyre!("DATABASE_URL not set. Set it in your environment before running blob migrate.")
        })?;
        let pool = create_pool(&database_url)
            .await
            .wrap_err("connect database for blob migration")?;

        let content_store = MaterialContentStore::new(ContentStoreConfig {
            root_path: self.content_store_path.clone(),
            legacy_annex_enabled: true, // We need annex access to read source blobs
            ..Default::default()
        })
        .wrap_err_with(|| format!("open content-store root {}", self.content_store_path))?;

        // Find all blobs with non-SINEXBLAKE3 backend (legacy annex blobs)
        let legacy_blobs: Vec<(Uuid, String, String, i64)> = sqlx::query_as(
            "SELECT id, annex_backend, content_hash, size_bytes FROM core.blobs WHERE annex_backend != 'SINEXBLAKE3'",
        )
        .fetch_all(&pool)
        .await
        .wrap_err("query legacy blobs for migration")?;

        let total = legacy_blobs.len();
        let mut already_migrated = 0usize;
        let mut migrated_count = 0usize;
        let mut failed = 0usize;
        let mut migrated_keys: Vec<MigratedKey> = Vec::new();

        for (blob_id, backend, content_hash, size_bytes) in &legacy_blobs {
            let annex_key = format!("{backend}-s{size_bytes}--{content_hash}");

            // Check if already has a SINEXBLAKE3 equivalent
            let already: Option<(Uuid,)> = sqlx::query_as(
                "SELECT id FROM core.blobs WHERE checksum_blake3 = (SELECT checksum_blake3 FROM core.blobs WHERE id = $1) AND annex_backend = 'SINEXBLAKE3'",
            )
            .bind(blob_id)
            .fetch_optional(&pool)
            .await
            .wrap_err("check existing SINEXBLAKE3 blob")?;

            if already.is_some() {
                already_migrated += 1;
                continue;
            }

            // Ensure content is available locally (git-annex get)
            if let Err(e) = content_store.ensure_content_local(&annex_key).await {
                tracing::warn!(
                    error = %e,
                    annex_key = %annex_key,
                    "Failed to ensure legacy content is local, skipping"
                );
                failed += 1;
                continue;
            }

            // Resolve the annex content path and compute BLAKE3 hash
            match content_store.resolve_annex_content_path(&annex_key).await {
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        annex_key = %annex_key,
                        "Failed to resolve annex content path, skipping"
                    );
                    failed += 1;
                    continue;
                }
                Ok(path) => {
                    let blake3_hash = match MaterialContentStore::compute_blake3_hash(&path).await
                    {
                        Ok(h) => h,
                        Err(e) => {
                            tracing::warn!(
                                error = %e,
                                path = %path,
                                "Failed to compute BLAKE3 hash, skipping"
                            );
                            failed += 1;
                            continue;
                        }
                    };
                    if self.apply {
                        let cas_target = content_store
                            .store_file(&path)
                            .await;
                        match cas_target {
                            Ok(cas_key) => {
                                let update_result = sqlx::query(
                                    "UPDATE core.blobs SET annex_backend = 'SINEXBLAKE3', content_hash = $1, checksum_blake3 = $2 WHERE id = $3",
                                )
                                .bind(&blake3_hash)
                                .bind(&blake3_hash)
                                .bind(blob_id)
                                .execute(&pool)
                                .await;
                                match update_result {
                                    Ok(_) => {
                                        migrated_count += 1;
                                        migrated_keys.push(MigratedKey {
                                            annex_key: annex_key.clone(),
                                            cas_key: cas_key.key,
                                            size_bytes: *size_bytes,
                                        });
                                    }
                                    Err(e) => {
                                        tracing::warn!(
                                            error = %e,
                                            blob_id = %blob_id,
                                            "Failed to update blob row during migration"
                                        );
                                        failed += 1;
                                    }
                                }
                            }
                            Err(e) => {
                                tracing::warn!(
                                    error = %e,
                                    annex_key = %annex_key,
                                    "Failed to store blob in CAS, skipping"
                                );
                                failed += 1;
                            }
                        }
                    } else {
                        // Dry-run: report what would happen without doing I/O
                        migrated_count += 1;
                        migrated_keys.push(MigratedKey {
                            annex_key: annex_key.clone(),
                            cas_key: format!("would-migrate:{}", blake3_hash),
                            size_bytes: *size_bytes,
                        });
                    }
                }
            }
        }

        let summary = BlobMigrateSummary {
            content_store_path: self.content_store_path.to_string(),
            mode: if self.apply { "apply" } else { "dry-run" },
            from: self.from.clone(),
            to: self.to.clone(),
            total_annex_blobs: total,
            already_migrated,
            migrated: migrated_count,
            failed,
            migrated_keys,
        };

        CommandOutput::single(summary, format_blob_migrate_summary).display(&format)
    }
}

fn format_blob_migrate_summary(summary: &BlobMigrateSummary) -> String {
    let mut output = String::new();
    output.push_str("Blob Migration\n");
    output.push_str(&format!("  Content Store: {}\n", summary.content_store_path));
    output.push_str(&format!("  Mode: {}\n", summary.mode));
    output.push_str(&format!(
        "  From: {}  To: {}\n",
        summary.from, summary.to
    ));
    output.push_str(&format!(
        "  Total annex blobs: {}\n",
        summary.total_annex_blobs
    ));
    output.push_str(&format!("  Already migrated: {}\n", summary.already_migrated));
    output.push_str(&format!("  Migrated (this run): {}\n", summary.migrated));
    output.push_str(&format!("  Failed: {}\n", summary.failed));
    for m in &summary.migrated_keys {
        output.push_str(&format!(
            "    {} -> {}\n",
            m.annex_key, m.cas_key
        ));
    }
    output
}
