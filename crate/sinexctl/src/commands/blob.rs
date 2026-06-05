use camino::Utf8PathBuf;
use clap::{Parser, Subcommand};
use color_eyre::eyre::{WrapErr, eyre};
use serde::Serialize;
use sinex_db::create_pool;
use sinex_primitives::Id;
use sinex_primitives::Uuid;
use sinex_primitives::events::{Event, SourceMaterial};
use sinexd::runtime::content_store::{
    CasFsckReport, ContentStoreConfig, MaterialContentStore, UnusedContentEntry,
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
    /// Re-hash material-provenance event payloads against `anchor_payload_hash` (#1447).
    VerifyIntegrity(BlobVerifyIntegrityCommand),
}

impl BlobCommands {
    pub async fn execute(&self, format: OutputFormat) -> Result<()> {
        match self {
            Self::SweepOrphans(cmd) => cmd.execute(format).await,
            Self::Fsck(cmd) => cmd.execute(format).await,
            Self::Migrate(cmd) => cmd.execute(format).await,
            Self::VerifyIntegrity(cmd) => cmd.execute(format).await,
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

/// Outcome of attempting to migrate a single legacy blob.
enum BlobMigrateOutcome {
    AlreadyMigrated,
    Migrated(MigratedKey),
    Failed,
}

/// Attempt to migrate one legacy annex blob to the local CAS.
/// Returns the outcome without mutating counters so the caller stays flat.
async fn migrate_single_blob(
    blob_id: sinex_primitives::Uuid,
    annex_key: &str,
    size_bytes: i64,
    content_store: &MaterialContentStore,
    pool: &sqlx::PgPool,
    apply: bool,
) -> Result<BlobMigrateOutcome> {
    // Check if already has a SINEXBLAKE3 equivalent
    let already = sqlx::query_scalar!(
        r"
        SELECT id
        FROM core.blobs
        WHERE checksum_blake3 = (
            SELECT checksum_blake3
            FROM core.blobs
            WHERE id = $1
        )
          AND annex_backend = 'SINEXBLAKE3'
        ",
        blob_id,
    )
    .fetch_optional(pool)
    .await
    .wrap_err("check existing SINEXBLAKE3 blob")?;

    if already.is_some() {
        return Ok(BlobMigrateOutcome::AlreadyMigrated);
    }

    // Ensure content is available locally (git-annex get)
    if let Err(e) = content_store.ensure_content_local(annex_key).await {
        tracing::warn!(
            error = %e,
            annex_key = %annex_key,
            "Failed to ensure legacy content is local, skipping"
        );
        return Ok(BlobMigrateOutcome::Failed);
    }

    // Resolve the annex content path and compute BLAKE3 hash
    let path = match content_store.resolve_annex_content_path(annex_key).await {
        Ok(p) => p,
        Err(e) => {
            tracing::warn!(
                error = %e,
                annex_key = %annex_key,
                "Failed to resolve annex content path, skipping"
            );
            return Ok(BlobMigrateOutcome::Failed);
        }
    };

    let blake3_hash = match MaterialContentStore::compute_blake3_hash(&path).await {
        Ok(h) => h,
        Err(e) => {
            tracing::warn!(
                error = %e,
                path = %path,
                "Failed to compute BLAKE3 hash, skipping"
            );
            return Ok(BlobMigrateOutcome::Failed);
        }
    };

    if !apply {
        return Ok(BlobMigrateOutcome::Migrated(MigratedKey {
            annex_key: annex_key.to_string(),
            cas_key: format!("would-migrate:{blake3_hash}"),
            size_bytes,
        }));
    }

    migrate_blob_to_cas(
        blob_id,
        annex_key,
        size_bytes,
        &blake3_hash,
        &path,
        content_store,
        pool,
    )
    .await
}

/// Store the blob in CAS and update the DB row. Called only in apply mode.
async fn migrate_blob_to_cas(
    blob_id: sinex_primitives::Uuid,
    annex_key: &str,
    size_bytes: i64,
    blake3_hash: &str,
    path: &camino::Utf8Path,
    content_store: &MaterialContentStore,
    pool: &sqlx::PgPool,
) -> Result<BlobMigrateOutcome> {
    let cas_key = match content_store.store_file(path).await {
        Ok(k) => k,
        Err(e) => {
            tracing::warn!(
                error = %e,
                annex_key = %annex_key,
                "Failed to store blob in CAS, skipping"
            );
            return Ok(BlobMigrateOutcome::Failed);
        }
    };

    let update_result = sqlx::query!(
        r"
        UPDATE core.blobs
        SET annex_backend = 'SINEXBLAKE3',
            content_hash = $1,
            checksum_blake3 = $2
        WHERE id = $3
        ",
        blake3_hash,
        blake3_hash,
        blob_id,
    )
    .execute(pool)
    .await;

    match update_result {
        Ok(_) => Ok(BlobMigrateOutcome::Migrated(MigratedKey {
            annex_key: annex_key.to_string(),
            cas_key: cas_key.key,
            size_bytes,
        })),
        Err(e) => {
            tracing::warn!(
                error = %e,
                blob_id = %blob_id,
                "Failed to update blob row during migration"
            );
            Ok(BlobMigrateOutcome::Failed)
        }
    }
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
        let legacy_blobs = sqlx::query!(
            r"
            SELECT id, annex_backend, content_hash, size_bytes
            FROM core.blobs
            WHERE annex_backend != 'SINEXBLAKE3'
            ",
        )
        .fetch_all(&pool)
        .await
        .wrap_err("query legacy blobs for migration")?;

        let total = legacy_blobs.len();
        let mut already_migrated = 0usize;
        let mut migrated_count = 0usize;
        let mut failed = 0usize;
        let mut migrated_keys: Vec<MigratedKey> = Vec::new();

        for legacy_blob in &legacy_blobs {
            let blob_id = legacy_blob.id;
            let backend = legacy_blob.annex_backend.as_str();
            let content_hash = legacy_blob.content_hash.as_str();
            let size_bytes = legacy_blob.size_bytes;
            let annex_key = format!("{backend}-s{size_bytes}--{content_hash}");

            match migrate_single_blob(
                blob_id,
                &annex_key,
                size_bytes,
                &content_store,
                &pool,
                self.apply,
            )
            .await?
            {
                BlobMigrateOutcome::AlreadyMigrated => already_migrated += 1,
                BlobMigrateOutcome::Migrated(key) => {
                    migrated_count += 1;
                    migrated_keys.push(key);
                }
                BlobMigrateOutcome::Failed => failed += 1,
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
    output.push_str(&format!(
        "  Content Store: {}\n",
        summary.content_store_path
    ));
    output.push_str(&format!("  Mode: {}\n", summary.mode));
    output.push_str(&format!("  From: {}  To: {}\n", summary.from, summary.to));
    output.push_str(&format!(
        "  Total annex blobs: {}\n",
        summary.total_annex_blobs
    ));
    output.push_str(&format!(
        "  Already migrated: {}\n",
        summary.already_migrated
    ));
    output.push_str(&format!("  Migrated (this run): {}\n", summary.migrated));
    output.push_str(&format!("  Failed: {}\n", summary.failed));
    for m in &summary.migrated_keys {
        output.push_str(&format!("    {} -> {}\n", m.annex_key, m.cas_key));
    }
    output
}

// ── VerifyIntegrity ─────────────────────────────────────────────────────────

#[derive(Debug, Parser)]
#[command(after_help = "\
EXAMPLES:
    # Verify every event with anchor_payload_hash set
    sinexctl blob verify-integrity

    # Verify only events from one source material
    sinexctl blob verify-integrity --material-id 019e5be2-...

    # Bound the audit to N events
    sinexctl blob verify-integrity --limit 1000
")]
pub struct BlobVerifyIntegrityCommand {
    /// Content-store root path (the directory that holds `sinex-cas/`).
    #[arg(long, env = "SINEX_CONTENT_STORE_PATH")]
    pub content_store_path: Utf8PathBuf,

    /// Verify only events tied to this `raw.source_material_registry.id`.
    #[arg(long)]
    pub material_id: Option<Id<SourceMaterial>>,

    /// Cap the number of events checked (0 = unbounded).
    #[arg(long, default_value_t = 0)]
    pub limit: u64,

    /// On mismatch, archive the offending events with reason
    /// "`anchor_payload_hash` mismatch" so a replay can re-emit them from the
    /// current source-material bytes. The archive cascade is the same
    /// machinery the replay flow uses; events move to
    /// `audit.archived_events` and `core.events` no longer carries them.
    /// Without this flag, mismatches are reported only.
    #[arg(long = "apply-mismatches")]
    pub apply_mismatches: bool,
}

#[derive(Debug, Default, Serialize)]
pub struct BlobVerifyIntegrityReport {
    pub examined: u64,
    pub matched: u64,
    pub mismatched: u64,
    pub missing_offsets: u64,
    pub missing_blob: u64,
    pub missing_cas_file: u64,
    pub read_errors: u64,
    /// Count of mismatched events archived when `--apply-mismatches` is set.
    /// Zero in dry-run.
    #[serde(default)]
    pub archived_mismatches: u64,
    pub mismatches: Vec<BlobVerifyIntegrityMismatch>,
}

#[derive(Debug, Serialize)]
pub struct BlobVerifyIntegrityMismatch {
    pub event_id: Id<Event>,
    pub material_id: Id<SourceMaterial>,
    pub anchor_byte: i64,
    pub offset_start: Option<i64>,
    pub offset_end: Option<i64>,
    pub stored_hash_hex: String,
    pub recomputed_hash_hex: String,
}

impl BlobVerifyIntegrityCommand {
    pub async fn execute(&self, format: OutputFormat) -> Result<()> {
        let database_url = std::env::var("DATABASE_URL").map_err(|_| {
            eyre!(
                "DATABASE_URL not set. Set it in your environment before running blob verify-integrity."
            )
        })?;
        let pool = create_pool(&database_url)
            .await
            .wrap_err("connect database for blob integrity verification")?;

        let content_store = MaterialContentStore::new(ContentStoreConfig {
            root_path: self.content_store_path.clone(),
            num_copies: None,
            large_files: None,
            ..Default::default()
        })
        .wrap_err_with(|| format!("open content-store root {}", self.content_store_path))?;

        let mut report =
            verify_event_anchor_hashes(&pool, &content_store, self.material_id, self.limit).await?;

        if self.apply_mismatches && !report.mismatches.is_empty() {
            report.archived_mismatches =
                archive_mismatches(&pool, &report.mismatches).await? as u64;
        }

        CommandOutput::single(report, format_verify_integrity_report).display(&format)
    }
}

async fn archive_mismatches(
    pool: &sqlx::PgPool,
    mismatches: &[BlobVerifyIntegrityMismatch],
) -> Result<usize> {
    use sinex_db::DbPoolExt;
    let ids: Vec<Uuid> = mismatches.iter().map(|m| *m.event_id.as_uuid()).collect();
    let operation_id = Uuid::now_v7();

    // Log the operation so the cascade has an audit trail. Match the
    // operation_type pattern used elsewhere in the codebase: snake_case,
    // namespaced by intent.
    sqlx::query(
        r"
        INSERT INTO core.operations_log (
            operation_type, operator, scope, result_status, result_message
        ) VALUES ($1, $2, $3, 'running', $4)
        ",
    )
    .bind("archive.integrity_mismatch")
    .bind("sinexctl:blob-verify-integrity")
    .bind(serde_json::json!({
        "event_count": ids.len(),
        "reason": "anchor_payload_hash mismatch",
    }))
    .bind(format!(
        "blob verify-integrity --apply-mismatches: archiving {} mismatched event(s)",
        ids.len()
    ))
    .execute(pool)
    .await
    .wrap_err("log archive.integrity_mismatch operation")?;

    let count = pool
        .events()
        .execute_cascade_archive(
            &ids,
            "anchor_payload_hash mismatch",
            &operation_id.to_string(),
            "sinexctl:blob-verify-integrity",
        )
        .await
        .wrap_err("execute archive cascade for integrity mismatches")?;

    sqlx::query(
        r"
        UPDATE core.operations_log
        SET result_status = 'success',
            result_message = $1
        WHERE operator = 'sinexctl:blob-verify-integrity'
          AND result_status = 'running'
          AND scope->>'reason' = 'anchor_payload_hash mismatch'
          AND id = (
            SELECT id FROM core.operations_log
            WHERE operator = 'sinexctl:blob-verify-integrity'
              AND result_status = 'running'
              AND scope->>'reason' = 'anchor_payload_hash mismatch'
            ORDER BY id DESC LIMIT 1
          )
        ",
    )
    .bind(format!(
        "archived {count} of {} mismatched events via cascade",
        ids.len()
    ))
    .execute(pool)
    .await
    .wrap_err("update operations_log success status")?;

    Ok(count as usize)
}

async fn verify_event_anchor_hashes(
    pool: &sqlx::PgPool,
    content_store: &MaterialContentStore,
    material_id: Option<Id<SourceMaterial>>,
    limit: u64,
) -> Result<BlobVerifyIntegrityReport> {
    let mut report = BlobVerifyIntegrityReport::default();

    let rows = sqlx::query!(
        r#"
        SELECT
            e.id AS "id!: Uuid",
            e.source_material_id AS "source_material_id!: Uuid",
            e.anchor_byte,
            e.offset_start,
            e.offset_end,
            e.anchor_payload_hash AS "anchor_payload_hash!: Vec<u8>",
            b.checksum_blake3
        FROM core.events e
        JOIN raw.source_material_registry r ON r.id = e.source_material_id
        LEFT JOIN core.blobs b ON b.id = r.optional_blob_id
        WHERE e.anchor_payload_hash IS NOT NULL
          AND ($1::uuid IS NULL OR e.source_material_id = $1)
        ORDER BY e.id
        LIMIT CASE WHEN $2::bigint = 0 THEN NULL ELSE $2 END
        "#,
        material_id.map(Uuid::from),
        i64::try_from(limit).unwrap_or(i64::MAX),
    )
    .fetch_all(pool)
    .await
    .wrap_err("query material-provenance events with anchor_payload_hash")?;

    for row in rows {
        report.examined += 1;

        let Some(stored) = (row.anchor_payload_hash.len() == 32)
            .then(|| <[u8; 32]>::try_from(row.anchor_payload_hash.as_slice()).ok())
            .flatten()
        else {
            report.read_errors += 1;
            continue;
        };

        let Some(blob_hash) = row.checksum_blake3 else {
            report.missing_blob += 1;
            continue;
        };

        let Some(cas_path) = content_store.path_if_local(&format!("SINEXBLAKE3-{blob_hash}"))?
        else {
            report.missing_blob += 1;
            continue;
        };

        let material_bytes = match tokio::fs::read(cas_path.as_std_path()).await {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                report.missing_cas_file += 1;
                continue;
            }
            Err(error) => {
                tracing::warn!(
                    error = %error,
                    cas_path = %cas_path,
                    "Failed to read CAS file for integrity verification"
                );
                report.read_errors += 1;
                continue;
            }
        };

        // Two anchor shapes:
        //   - Stream records carry offset_start/offset_end (a byte range inside
        //     a rotation buffer). The hash was computed over that exact range.
        //   - Pre-materialized records (file-drop content staging, SQLite
        //     snapshots) wrote the whole record payload as the entire material
        //     content, with offsets None or 0..len. The hash covers all bytes.
        let payload: &[u8] = match (row.offset_start, row.offset_end) {
            (Some(start), Some(end)) if start >= 0 && end >= start => {
                let lo = usize::try_from(start).unwrap_or(usize::MAX);
                let hi = usize::try_from(end).unwrap_or(usize::MAX);
                if hi > material_bytes.len() {
                    report.read_errors += 1;
                    continue;
                }
                &material_bytes[lo..hi]
            }
            (None, None) => material_bytes.as_slice(),
            _ => {
                report.missing_offsets += 1;
                continue;
            }
        };

        let recomputed = *blake3::hash(payload).as_bytes();
        if recomputed == stored {
            report.matched += 1;
        } else {
            report.mismatched += 1;
            report.mismatches.push(BlobVerifyIntegrityMismatch {
                event_id: row.id.into(),
                material_id: row.source_material_id.into(),
                anchor_byte: row.anchor_byte.unwrap_or(0),
                offset_start: row.offset_start,
                offset_end: row.offset_end,
                stored_hash_hex: hash_to_hex(&stored),
                recomputed_hash_hex: hash_to_hex(&recomputed),
            });
        }
    }

    Ok(report)
}

fn hash_to_hex(bytes: &[u8; 32]) -> String {
    blake3::Hash::from_bytes(*bytes).to_hex().to_string()
}

fn format_verify_integrity_report(report: &BlobVerifyIntegrityReport) -> String {
    let mut s = String::new();
    s.push_str("Anchor Payload Hash Verification\n");
    s.push_str(&format!("  Examined:        {}\n", report.examined));
    s.push_str(&format!("  Matched:         {}\n", report.matched));
    s.push_str(&format!("  Mismatched:      {}\n", report.mismatched));
    s.push_str(&format!("  Missing offsets: {}\n", report.missing_offsets));
    s.push_str(&format!("  Missing blob:    {}\n", report.missing_blob));
    s.push_str(&format!("  Missing CAS file:{}\n", report.missing_cas_file));
    s.push_str(&format!("  Read errors:     {}\n", report.read_errors));
    s.push_str(&format!(
        "  Archived (apply-mismatches): {}\n",
        report.archived_mismatches
    ));
    if !report.mismatches.is_empty() {
        s.push_str("\nMismatches:\n");
        for m in &report.mismatches {
            s.push_str(&format!(
                "  event={} material={} range=[{:?},{:?}] stored={} recomputed={}\n",
                m.event_id,
                m.material_id,
                m.offset_start,
                m.offset_end,
                &m.stored_hash_hex[..16],
                &m.recomputed_hash_hex[..16],
            ));
        }
    }
    s
}
