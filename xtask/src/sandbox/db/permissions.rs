use crate::sandbox::db::pool::replace_db_name;
use crate::sandbox::prelude::*;
use sinex_db::schema::registry;
use sinex_primitives::validation::validate_pg_identifier;

pub struct PermissionGranter {
    superuser_url: String,
}

impl PermissionGranter {
    pub fn from_env() -> Result<Option<Self>> {
        if let Ok(url) = std::env::var("SINEX_TEST_DATABASE_URL_SUPERUSER")
            .or_else(|_| std::env::var("DATABASE_URL_SUPERUSER"))
        {
            Ok(Some(Self { superuser_url: url }))
        } else {
            Ok(None)
        }
    }

    pub async fn grant_schema_access(&self, pool: &DbPool, schema: &str) -> Result<()> {
        // Validate before interpolating into DDL GRANT statements.
        validate_pg_identifier(schema, "schema")
            .map_err(|e| eyre!("cannot GRANT on schema: {e}"))?;

        // We use the provided pool (which should have enough permissions, or we'd use a superuser pool)
        // In some environments, the template pool is already superuser-connected.

        let queries = [
            format!("GRANT USAGE ON SCHEMA \"{schema}\" TO public"),
            format!("GRANT ALL PRIVILEGES ON SCHEMA \"{schema}\" TO public"),
            format!("GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA \"{schema}\" TO public"),
            format!("GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA \"{schema}\" TO public"),
            format!("GRANT ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA \"{schema}\" TO public"),
            format!(
                "ALTER DEFAULT PRIVILEGES IN SCHEMA \"{schema}\" GRANT ALL PRIVILEGES ON TABLES TO public"
            ),
            format!(
                "ALTER DEFAULT PRIVILEGES IN SCHEMA \"{schema}\" GRANT ALL PRIVILEGES ON SEQUENCES TO public"
            ),
            format!(
                "ALTER DEFAULT PRIVILEGES IN SCHEMA \"{schema}\" GRANT ALL PRIVILEGES ON FUNCTIONS TO public"
            ),
        ];

        for query in queries {
            sqlx::query(&query)
                .execute(pool)
                .await
                .wrap_err_with(|| format!("failed to grant schema access for {schema}: {query}"))?;
        }

        Ok(())
    }
}

#[must_use]
pub fn granted_schema_names() -> Vec<&'static str> {
    let mut schemas = vec!["public"];
    schemas.extend(registry::SINEX_SCHEMAS.iter().map(|schema| schema.name));
    schemas
}

pub async fn grant_pool_database_permissions(db_name: &str) -> TestResult<()> {
    let Some(granter) = PermissionGranter::from_env()? else {
        return Ok(());
    };

    let db_url = replace_db_name(&granter.superuser_url, db_name);

    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(&db_url)
        .await?;

    for schema in granted_schema_names() {
        granter.grant_schema_access(&pool, schema).await?;
    }

    Ok(())
}

#[cfg(test)]
#[path = "permissions_test.rs"]
mod tests;
