//! Centralized permission grant management for test databases.
//!
//! This module provides a unified interface for granting database permissions,
//! eliminating duplication between CI scripts and test utilities.

use crate::Result;
use sinex_schema::schema_registry;
use sqlx::PgPool;

/// Manages database permission grants for test environments.
pub struct PermissionGranter {
    superuser_url: String,
    app_username: String,
}

impl PermissionGranter {
    /// Creates a new PermissionGranter.
    ///
    /// # Arguments
    ///
    /// * `superuser_url` - Connection string with superuser credentials
    /// * `app_username` - Username that needs permissions granted
    pub fn new(superuser_url: String, app_username: String) -> Self {
        Self {
            superuser_url,
            app_username,
        }
    }

    /// Creates a PermissionGranter from environment variables.
    ///
    /// Reads `DATABASE_URL_SUPERUSER` and extracts username from `DATABASE_URL_APP`.
    pub fn from_env() -> Result<Option<Self>> {
        let superuser_url = match std::env::var("DATABASE_URL_SUPERUSER") {
            Ok(url) => url,
            Err(_) => return Ok(None), // Not in CI environment
        };

        let app_url = std::env::var("DATABASE_URL_APP").ok();
        let username = app_url
            .as_ref()
            .and_then(|url| url.split("://").nth(1))
            .and_then(|s| s.split('@').next())
            .map(|s| s.to_string());

        match username {
            Some(username) => Ok(Some(Self::new(superuser_url, username))),
            None => Ok(None),
        }
    }

    /// Grants permissions on all schemas to the app user for a specific database.
    pub async fn grant_all_schemas(&self, db_name: &str) -> Result<()> {
        let pool = self.connect_to_database(db_name).await?;

        for schema in schema_registry::SINEX_SCHEMAS {
            self.grant_schema_access(&pool, schema.name).await?;
        }

        Ok(())
    }

    /// Grants access to a single schema.
    pub async fn grant_schema_access(&self, pool: &PgPool, schema: &str) -> Result<()> {
        let queries = self.build_schema_grant_queries(schema);

        for query in queries {
            if let Err(e) = sqlx::query(&query).execute(pool).await {
                tracing::warn!(
                    error = %e,
                    query = %query,
                    schema = %schema,
                    "Failed to grant permission"
                );
            }
        }

        Ok(())
    }

    /// Builds grant queries for a schema.
    fn build_schema_grant_queries(&self, schema: &str) -> Vec<String> {
        vec![
            format!("GRANT USAGE ON SCHEMA {schema} TO {}", self.app_username),
            format!(
                "GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA {schema} TO {}",
                self.app_username
            ),
            format!(
                "GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA {schema} TO {}",
                self.app_username
            ),
            format!(
                "GRANT ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA {schema} TO {}",
                self.app_username
            ),
            format!(
                "ALTER DEFAULT PRIVILEGES IN SCHEMA {schema} GRANT ALL PRIVILEGES ON TABLES TO {}",
                self.app_username
            ),
            format!(
                "ALTER DEFAULT PRIVILEGES IN SCHEMA {schema} GRANT ALL PRIVILEGES ON SEQUENCES TO {}",
                self.app_username
            ),
            format!(
                "ALTER DEFAULT PRIVILEGES IN SCHEMA {schema} GRANT ALL PRIVILEGES ON FUNCTIONS TO {}",
                self.app_username
            ),
        ]
    }

    /// Connects to a specific database using superuser credentials.
    async fn connect_to_database(&self, db_name: &str) -> Result<PgPool> {
        // Replace the database name in the URL
        let url = if self.superuser_url.contains("/sinex_dev") {
            self.superuser_url
                .replace("/sinex_dev", &format!("/{}", db_name))
        } else {
            // Handle URLs that might already have a different database
            let base = self.superuser_url.rsplit_once('/').map(|(base, _)| base);
            match base {
                Some(base) => format!("{}/{}", base, db_name),
                None => {
                    return Err(crate::SinexError::database(format!(
                        "Invalid DATABASE_URL_SUPERUSER format: {}",
                        self.superuser_url
                    )))
                }
            }
        };

        Ok(sqlx::postgres::PgPoolOptions::new()
            .max_connections(1)
            .connect(&url)
            .await?)
    }
}

/// Grant permissions using the centralized granter if available.
///
/// This is a convenience function for existing code that can be called
/// directly without managing the PermissionGranter lifecycle.
pub async fn grant_pool_database_permissions(db_name: &str) -> Result<()> {
    if let Some(granter) = PermissionGranter::from_env()? {
        granter.grant_all_schemas(db_name).await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builds_correct_grant_queries() {
        let granter = PermissionGranter::new(
            "postgresql://postgres@localhost/test".to_string(),
            "testuser".to_string(),
        );

        let queries = granter.build_schema_grant_queries("core");

        assert!(queries
            .iter()
            .any(|q| q.contains("GRANT USAGE ON SCHEMA core TO testuser")));
        assert!(queries.iter().any(|q| q.contains("ALL TABLES")));
        assert!(queries.iter().any(|q| q.contains("ALL SEQUENCES")));
        assert!(queries
            .iter()
            .any(|q| q.contains("ALTER DEFAULT PRIVILEGES")));
    }

    #[test]
    fn from_env_returns_none_when_no_superuser_url() {
        std::env::remove_var("DATABASE_URL_SUPERUSER");
        let result = PermissionGranter::from_env().unwrap();
        assert!(result.is_none());
    }
}
