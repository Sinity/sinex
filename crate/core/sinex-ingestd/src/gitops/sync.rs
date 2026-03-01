//! GitOps sync service: periodically clones/fetches configured repositories,
//! discovers schema files, and upserts them into the database.

use crate::gitops::discovery::SchemaDiscovery;
use crate::gitops::git::GitOperations;
use crate::gitops::types::{GitOpsSource, GitOpsSyncStats};
use crate::{IngestdResult, SinexError};
use sinex_db::repositories::schema_management::SchemaManagementRepository;
use sinex_primitives::Ulid;
use sinex_primitives::temporal::Timestamp;
use sqlx::PgPool;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::time::{Duration, interval};
use tracing::{debug, error, info, warn};

/// Background service that synchronizes schemas from configured Git repositories.
pub struct GitOpsSyncService {
    pool: PgPool,
    git_ops: GitOperations,
    shutdown_flag: Arc<AtomicBool>,
}

impl GitOpsSyncService {
    /// Create a new sync service.
    ///
    /// - `pool`: Database pool for querying sources and upserting schemas
    /// - `work_dir`: Directory where repositories are cloned
    /// - `shutdown_flag`: Shared flag for graceful shutdown
    pub fn new(pool: PgPool, work_dir: PathBuf, shutdown_flag: Arc<AtomicBool>) -> Self {
        Self {
            pool,
            git_ops: GitOperations::new(work_dir),
            shutdown_flag,
        }
    }

    /// Run the sync loop. This polls sources at their configured frequency.
    ///
    /// The loop checks every 60 seconds for sources that need syncing.
    pub async fn run(&self) {
        let mut poll_interval = interval(Duration::from_secs(60));

        loop {
            tokio::select! {
                _ = poll_interval.tick() => {
                    match self.run_sync_cycle().await {
                        Ok(stats) => {
                            if stats.sources_synced > 0 || !stats.errors.is_empty() {
                                info!(
                                    sources_checked = stats.sources_checked,
                                    sources_synced = stats.sources_synced,
                                    sources_skipped = stats.sources_skipped,
                                    schemas_discovered = stats.schemas_discovered,
                                    schemas_created = stats.schemas_created,
                                    schemas_updated = stats.schemas_updated,
                                    errors = stats.errors.len(),
                                    "GitOps sync cycle completed"
                                );
                            } else {
                                debug!(
                                    sources_checked = stats.sources_checked,
                                    "GitOps sync cycle: no sources needed sync"
                                );
                            }
                        }
                        Err(e) => {
                            error!(error = %e, "GitOps sync cycle failed");
                        }
                    }
                }
                () = shutdown_signal(&self.shutdown_flag) => {
                    info!("GitOps sync service shutting down");
                    break;
                }
            }
        }
    }

    /// Execute a single sync cycle: check all enabled sources and sync those due.
    pub async fn run_sync_cycle(&self) -> IngestdResult<GitOpsSyncStats> {
        let sources = self.load_enabled_sources().await?;
        let mut stats = GitOpsSyncStats::default();
        stats.sources_checked = sources.len();

        for source in sources {
            if self.shutdown_flag.load(Ordering::Relaxed) {
                break;
            }

            if !source.needs_sync() {
                stats.sources_skipped += 1;
                continue;
            }

            match self.sync_source(&source).await {
                Ok(source_stats) => {
                    stats.sources_synced += 1;
                    stats.schemas_discovered += source_stats.schemas_discovered;
                    stats.schemas_created += source_stats.schemas_created;
                    stats.schemas_updated += source_stats.schemas_updated;
                    stats.schemas_unchanged += source_stats.schemas_unchanged;
                }
                Err(e) => {
                    let msg = format!(
                        "Failed to sync source {} ({}): {e}",
                        source.repository_url, source.branch
                    );
                    warn!("{}", msg);
                    stats.errors.push(msg);
                }
            }
        }

        Ok(stats)
    }

    /// Sync a single source: clone/fetch, discover schemas, upsert into DB.
    async fn sync_source(&self, source: &GitOpsSource) -> IngestdResult<SourceSyncResult> {
        // Clone or open the repository
        let repo_path = self
            .git_ops
            .ensure_repo(&source.repository_url, &source.branch)
            .await?;

        // Fetch latest changes
        self.git_ops
            .fetch_and_checkout(repo_path.clone(), &source.branch)
            .await?;

        // Check if HEAD has changed since last sync
        let head_sha = GitOperations::get_head_commit_sha(repo_path.clone()).await?;
        if source
            .last_sync_commit
            .as_ref()
            .is_some_and(|commit| commit == &head_sha)
        {
            debug!(
                url = %source.repository_url,
                commit = %head_sha,
                "Skipping sync: HEAD unchanged since last sync"
            );
            return Ok(SourceSyncResult::default());
        }

        // Discover schemas in the checkout (blocking I/O, run in blocking task)
        let pattern = source.path_pattern.clone();
        let rp = repo_path.clone();
        let discovered =
            tokio::task::spawn_blocking(move || SchemaDiscovery::discover_schemas(&rp, &pattern))
                .await
                .map_err(|e| {
                    SinexError::service(format!("Schema discovery task panicked: {e}"))
                })??;

        let schemas_discovered = discovered.len();

        // Convert to the format expected by sync_discovered_schemas
        let schema_iter = discovered
            .into_iter()
            .map(|s| ((s.source, s.event_type, s.version), s.schema_content));

        // Upsert schemas into the database
        let repo = SchemaManagementRepository::new(&self.pool);
        let sync_result = repo.sync_discovered_schemas(schema_iter).await?;

        // Update the source with the current sync state
        self.update_source_sync_state(&source.id, &head_sha).await?;

        info!(
            url = %source.repository_url,
            commit = %head_sha,
            discovered = schemas_discovered,
            created = sync_result.created,
            updated = sync_result.updated,
            unchanged = sync_result.unchanged,
            "Synced schemas from git repository"
        );

        Ok(SourceSyncResult {
            schemas_discovered,
            schemas_created: sync_result.created,
            schemas_updated: sync_result.updated,
            schemas_unchanged: sync_result.unchanged,
        })
    }

    /// Load all enabled gitops sources from the database.
    async fn load_enabled_sources(&self) -> IngestdResult<Vec<GitOpsSource>> {
        let rows = sqlx::query!(
            r#"
            SELECT
                id::uuid as "id!: Ulid",
                repository_url,
                branch,
                path_pattern,
                sync_enabled,
                last_sync_at as "last_sync_at: Timestamp",
                last_sync_commit,
                sync_frequency_minutes
            FROM sinex_schemas.gitops_schema_sources
            WHERE sync_enabled = true
            ORDER BY last_sync_at NULLS FIRST
            "#
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| {
            SinexError::database(format!("Failed to load gitops sources: {e}"))
                .with_operation("gitops.load_sources")
        })?;

        Ok(rows
            .into_iter()
            .map(|row| GitOpsSource {
                id: row.id,
                repository_url: row.repository_url,
                branch: row.branch,
                path_pattern: row.path_pattern,
                sync_enabled: row.sync_enabled,
                last_sync_at: row.last_sync_at,
                last_sync_commit: row.last_sync_commit,
                sync_frequency_minutes: row.sync_frequency_minutes,
            })
            .collect())
    }

    /// Update a source's last_sync_at and last_sync_commit after a successful sync.
    async fn update_source_sync_state(
        &self,
        source_id: &Ulid,
        commit_sha: &str,
    ) -> IngestdResult<()> {
        sqlx::query!(
            r#"
            UPDATE sinex_schemas.gitops_schema_sources
            SET last_sync_at = NOW(),
                last_sync_commit = $1
            WHERE id = $2::uuid::ulid
            "#,
            commit_sha,
            source_id.as_uuid()
        )
        .execute(&self.pool)
        .await
        .map_err(|e| {
            SinexError::database(format!("Failed to update gitops source sync state: {e}"))
                .with_operation("gitops.update_sync_state")
        })?;

        Ok(())
    }
}

/// Per-source sync statistics.
#[derive(Debug, Default)]
struct SourceSyncResult {
    schemas_discovered: usize,
    schemas_created: usize,
    schemas_updated: usize,
    schemas_unchanged: usize,
}

/// Helper function to create a shutdown signal future.
async fn shutdown_signal(shutdown_flag: &Arc<AtomicBool>) {
    loop {
        if shutdown_flag.load(Ordering::Relaxed) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}
