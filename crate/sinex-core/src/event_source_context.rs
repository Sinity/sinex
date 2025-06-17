use sqlx::PgPool;
use serde_json::Value;

/// Context provided to event sources containing shared resources
#[derive(Clone)]
pub struct EventSourceContext {
    /// Database connection pool (if available)
    pub db_pool: Option<PgPool>,
    
    /// Source-specific configuration
    pub config: Value,
    
    /// Path to git-annex repository for large content (if configured)
    pub annex_repo_path: Option<String>,
}

impl EventSourceContext {
    pub fn new(config: Value) -> Self {
        Self {
            db_pool: None,
            config,
            annex_repo_path: None,
        }
    }
    
    pub fn with_db_pool(mut self, pool: PgPool) -> Self {
        self.db_pool = Some(pool);
        self
    }
    
    pub fn with_annex_path(mut self, path: String) -> Self {
        self.annex_repo_path = Some(path);
        self
    }
    
    /// Create a test context with empty configuration
    pub fn for_test() -> Self {
        Self::new(serde_json::json!({}))
    }
}