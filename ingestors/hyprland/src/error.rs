use thiserror::Error;

/// Application-specific errors for the Hyprland ingestor
#[derive(Error, Debug)]
pub enum IngestorError {
    /// Database connection or query errors
    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),

    /// Hyprland event listener errors
    #[error("Hyprland event listener error: {0}")]
    HyprlandListener(#[from] hyprland::shared::HyprError),

    /// Configuration parsing errors
    #[error("Configuration error: {0}")]
    Config(#[from] config::ConfigError),

    /// JSON serialization/deserialization errors
    #[error("JSON serialization error: {0}")]
    Json(#[from] serde_json::Error),

    /// Event ingestion errors
    #[error("Failed to ingest event of type '{event_type}': {source}")]
    EventIngestion {
        event_type: String,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    /// Database connection errors
    #[error("Failed to connect to database: {0}")]
    DatabaseConnection(String),

    /// Shutdown errors
    #[error("Shutdown error: {0}")]
    Shutdown(String),

    /// Generic application errors
    #[error("Application error: {0}")]
    Application(String),
    
    /// Anyhow errors
    #[error("Error: {0}")]
    Other(#[from] anyhow::Error),
}

/// Result type alias for the ingestor
pub type Result<T> = std::result::Result<T, IngestorError>;

impl IngestorError {
    /// Create a new event ingestion error
    pub fn event_ingestion<E>(event_type: impl Into<String>, error: E) -> Self
    where
        E: std::error::Error + Send + Sync + 'static,
    {
        Self::EventIngestion {
            event_type: event_type.into(),
            source: Box::new(error),
        }
    }

    /// Create a new database connection error
    pub fn database_connection(msg: impl Into<String>) -> Self {
        Self::DatabaseConnection(msg.into())
    }

    /// Create a new application error
    pub fn application(msg: impl Into<String>) -> Self {
        Self::Application(msg.into())
    }
}