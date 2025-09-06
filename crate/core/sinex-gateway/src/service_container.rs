//! Service container that holds all service instances

use camino::Utf8PathBuf;
use color_eyre::eyre::Result;
use sinex_core::{
    db::{create_pool, query_helpers::db_error},
    types::{domain::SanitizedPath, error::SinexError},
};
use sinex_satellite_sdk::{annex::BlobManager, IngestClient};
use sinex_services::{AnalyticsService, ContentService, PkmService, SearchService};
use std::sync::Arc;
use tokio::sync::mpsc;

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
            .ok_or_else(|| {
                SinexError::configuration("Database URL not provided and DATABASE_URL not set")
            })?;

        // Create database pool
        let pool = create_pool(&db_url).await?;

        // Create blob manager for content service
        let annex_path_str =
            std::env::var("SINEX_ANNEX_PATH").unwrap_or_else(|_| "/tmp/sinex-annex".to_string());
        let annex_path = SanitizedPath::from_str_validated(&annex_path_str)
            .map_err(|e| SinexError::validation(format!("Invalid SINEX_ANNEX_PATH: {}", e)))?;
        let annex_path = Utf8PathBuf::from(annex_path.as_str());

        // Ensure the annex directory exists
        tokio::fs::create_dir_all(&annex_path).await.map_err(|e| {
            SinexError::io("Failed to create annex directory")
                .with_path(&annex_path)
                .with_source(e.to_string())
        })?;

        // Create IngestClient for BlobManager (required for proper event routing)
        let ingest_client = if let Ok(ingest_socket) = std::env::var("SINEX_INGEST_SOCKET") {
            IngestClient::new(&ingest_socket).await.map_err(|e| {
                SinexError::service("Failed to create ingest client for blob manager")
                    .with_source(e.to_string())
            })?
        } else {
            return Err(SinexError::configuration(
                "SINEX_INGEST_SOCKET environment variable not set - required for blob manager",
            )
            .into());
        };

        let annex_config = sinex_satellite_sdk::annex::AnnexConfig {
            repo_path: annex_path,
            num_copies: None,
            large_files: None,
        };
        let blob_manager = Arc::new(
            BlobManager::new(annex_config, pool.clone(), ingest_client.clone()).map_err(|e| {
                SinexError::service("Failed to create blob manager").with_source(e.to_string())
            })?,
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
