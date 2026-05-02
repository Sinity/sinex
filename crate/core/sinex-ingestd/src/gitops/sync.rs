//! `GitOps` sync service: periodically clones/fetches configured repositories,
//! discovers schema files, and upserts them into the database.

use crate::gitops::discovery::SchemaDiscovery;
use crate::gitops::git::GitOperations;
use crate::gitops::types::{GitOpsSource, GitOpsSyncStats};
use crate::{IngestdResult, SinexError};
use sinex_db::DbPoolExt;
use sinex_db::repositories::gitops::GitOpsSchemaSource;
use sinex_db::repositories::schema_management::SchemaManagementRepository;
use sinex_primitives::Id;
use sinex_primitives::events::schema_registry::SchemaBundleEntry;
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
    shutdown_notify: Arc<tokio::sync::Notify>,
}

impl GitOpsSyncService {
    /// Create a new sync service.
    ///
    /// - `pool`: Database pool for querying sources and upserting schemas
    /// - `work_dir`: Directory where repositories are cloned
    /// - `shutdown_flag`: Shared flag for graceful shutdown
    /// - `shutdown_notify`: Notify for reactive shutdown waking
    pub fn new(
        pool: PgPool,
        work_dir: PathBuf,
        shutdown_flag: Arc<AtomicBool>,
        shutdown_notify: Arc<tokio::sync::Notify>,
    ) -> Self {
        Self {
            pool,
            git_ops: GitOperations::new(work_dir),
            shutdown_flag,
            shutdown_notify,
        }
    }

    /// Run the sync loop. This polls sources at their configured frequency.
    ///
    /// The loop checks every 60 seconds for sources that need syncing.
    pub async fn run(&self) {
        let mut poll_interval = interval(Duration::from_mins(1));

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
                () = sinex_node_sdk::wait_for_shutdown_signal_bool(&self.shutdown_flag, &self.shutdown_notify) => {
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
            if self.shutdown_flag.load(Ordering::Acquire) {
                break;
            }

            if !source.needs_sync() {
                stats.sources_skipped += 1;
                continue;
            }

            match self.sync_source(&source).await {
                Ok(source_stats) => {
                    stats.sources_synced += 1;
                    stats.schemas_discovered += source_stats.discovered;
                    stats.schemas_created += source_stats.created;
                    stats.schemas_updated += source_stats.updated;
                    stats.schemas_unchanged += source_stats.unchanged;
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

        let mut schema_bundle = Vec::with_capacity(discovered.len());
        for schema in discovered {
            schema_bundle.push(SchemaBundleEntry::new(
                schema.source.into_string(),
                schema.event_type.into_string(),
                schema.version,
                schema.schema_content,
            )?);
        }

        // Upsert schemas into the database
        let repo = SchemaManagementRepository::new(&self.pool);
        let sync_result = repo.sync_schema_bundle(schema_bundle).await?;

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
            discovered: schemas_discovered,
            created: sync_result.created,
            updated: sync_result.updated,
            unchanged: sync_result.unchanged,
        })
    }

    /// Load all enabled gitops sources from the database via repository.
    async fn load_enabled_sources(&self) -> IngestdResult<Vec<GitOpsSource>> {
        let records = self.pool.gitops().list_sources(false).await?;

        Ok(records
            .into_iter()
            .map(|r| GitOpsSource {
                id: r.id,
                repository_url: r.repository_url,
                branch: r.branch,
                path_pattern: r.path_pattern,
                sync_enabled: r.sync_enabled,
                last_sync_at: r.last_sync_at,
                last_sync_commit: r.last_sync_commit,
                sync_frequency_minutes: r.sync_frequency_minutes,
            })
            .collect())
    }

    /// Update a source's sync state after a successful sync via repository.
    async fn update_source_sync_state(
        &self,
        source_id: &Id<GitOpsSchemaSource>,
        commit_sha: &str,
    ) -> IngestdResult<()> {
        self.pool
            .gitops()
            .update_sync_state(source_id, commit_sha)
            .await?;
        Ok(())
    }
}

/// Per-source sync statistics.
#[derive(Debug, Default)]
struct SourceSyncResult {
    discovered: usize,
    created: usize,
    updated: usize,
    unchanged: usize,
}
