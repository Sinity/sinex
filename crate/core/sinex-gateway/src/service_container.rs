//! Service container that holds all service instances

use crate::replay_control::{spawn_replay_control, ReplayControlClient};
use crate::replay_state_machine::ReplayStateMachine;
use camino::Utf8PathBuf;
use color_eyre::eyre::Result;
use sinex_core::{
    db::create_pool,
    environment as sinex_environment,
    types::{domain::SanitizedPath, error::SinexError},
};
use sinex_satellite_sdk::annex::BlobManager;
use sinex_services::{AnalyticsService, ContentService, PkmService, SearchService};
use std::sync::Arc;
use tracing::warn;

/// Container holding all service instances
#[derive(Clone)]
pub struct ServiceContainer {
    pub analytics: Arc<AnalyticsService>,
    pub content: Arc<ContentService>,
    pub pkm: Arc<PkmService>,
    pub search: Arc<SearchService>,
    pub replay_control: Option<ReplayControlClient>,
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
        let annex_path_str = match std::env::var("SINEX_ANNEX_PATH") {
            Ok(value) => value,
            Err(_) => {
                let default_path =
                    sinex_environment::environment().work_directory("/tmp/sinex/annex");
                default_path.to_string_lossy().into_owned()
            }
        };
        let annex_path = SanitizedPath::from_str_validated(&annex_path_str)
            .map_err(|e| SinexError::validation(format!("Invalid SINEX_ANNEX_PATH: {}", e)))?;
        let annex_path = Utf8PathBuf::from(annex_path.as_str());

        // Ensure the annex directory exists
        tokio::fs::create_dir_all(&annex_path).await.map_err(|e| {
            SinexError::io("Failed to create annex directory")
                .with_path(&annex_path)
                .with_source(e.to_string())
        })?;

        let annex_config = sinex_satellite_sdk::annex::AnnexConfig {
            repo_path: annex_path,
            num_copies: None,
            large_files: None,
        };
        let blob_manager = Arc::new(BlobManager::new(annex_config, pool.clone(), None).map_err(
            |e| SinexError::service("Failed to create blob manager").with_source(e.to_string()),
        )?);

        // Initialize all services
        let replay = Arc::new(ReplayStateMachine::new(pool.clone()));

        let nats_url =
            std::env::var("SINEX_NATS_URL").unwrap_or_else(|_| "nats://localhost:4222".into());

        let allow_replay_bypass =
            std::env::var("SINEX_ALLOW_REPLAY_CONTROL_BYPASS").map_or(false, |value| {
                matches!(
                    value.trim().to_ascii_lowercase().as_str(),
                    "1" | "true" | "yes"
                )
            });

        let control_client = match spawn_replay_control(replay.clone(), &nats_url).await {
            Ok(client) => Some(client),
            Err(err) if allow_replay_bypass => {
                warn!(
                    error = %err,
                    "Replay control bus disabled (SINEX_ALLOW_REPLAY_CONTROL_BYPASS=1)"
                );
                None
            }
            Err(err) => {
                return Err(
                    SinexError::service("Failed to initialize replay control")
                        .with_operation("gateway.spawn_replay_control")
                        .with_source(err.to_string())
                        .into(),
                )
            }
        };

        Ok(Self {
            analytics: Arc::new(AnalyticsService::new(pool.clone())),
            content: Arc::new(ContentService::new(pool.clone(), blob_manager)),
            pkm: Arc::new(PkmService::new(pool.clone())),
            search: Arc::new(SearchService::new(pool)),
            replay_control: control_client,
        })
    }
}
