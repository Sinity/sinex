//! Service container that holds all service instances

use anyhow::{Context, Result};
use sinex_annex::BlobManager;
use sinex_db::create_pool;
use sinex_services::{AnalyticsService, ContentService, PkmService, SearchService};
use std::path::PathBuf;
use std::sync::Arc;

/// Container holding all service instances
#[derive(Clone)]
pub struct ServiceContainer {
    pub analytics: Arc<AnalyticsService>,
    pub content: Arc<ContentService>,
    pub pkm: Arc<PkmService>,
    pub search: Arc<SearchService>,
}

impl ServiceContainer {
    /// Create a new service container with the given database URL
    pub async fn new(database_url: Option<String>) -> Result<Self> {
        // Get database URL from parameter or environment
        let db_url = database_url
            .or_else(|| std::env::var("DATABASE_URL").ok())
            .context("Database URL not provided and DATABASE_URL not set")?;

        // Create database pool
        let pool = create_pool(&db_url)
            .await
            .context("Failed to create database pool")?;

        // Create blob manager for content service
        let annex_path = PathBuf::from(
            std::env::var("SINEX_ANNEX_PATH").unwrap_or_else(|_| "/tmp/sinex-annex".to_string()),
        );

        // Ensure the annex directory exists
        std::fs::create_dir_all(&annex_path)
            .with_context(|| format!("Failed to create annex directory: {:?}", annex_path))?;

        let annex_config = sinex_annex::AnnexConfig {
            repo_path: annex_path,
            num_copies: None,
            large_files: None,
        };
        let blob_manager = Arc::new(
            BlobManager::new(annex_config, pool.clone())
                .context("Failed to create blob manager")?,
        );

        // Initialize all services
        Ok(Self {
            analytics: Arc::new(AnalyticsService::new(pool.clone())),
            content: Arc::new(ContentService::new(pool.clone(), blob_manager)),
            pkm: Arc::new(PkmService::new(pool.clone())),
            search: Arc::new(SearchService::new(pool)),
        })
    }
}
