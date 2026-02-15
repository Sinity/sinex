use crate::sandbox::prelude::*;

pub struct PermissionGranter {
    superuser_url: String,
}

impl PermissionGranter {
    pub fn from_env() -> Result<Option<Self>> {
        if let Ok(url) = std::env::var("DATABASE_URL_SUPERUSER") {
            Ok(Some(Self { superuser_url: url }))
        } else {
            Ok(None)
        }
    }

    pub async fn grant_schema_access(&self, pool: &DbPool, schema: &str) -> Result<()> {
        // We use the provided pool (which should have enough permissions, or we'd use a superuser pool)
        // In some environments, the template pool is already superuser-connected.

        let queries = [
            format!("GRANT USAGE ON SCHEMA \"{schema}\" TO public"),
            format!("GRANT ALL PRIVILEGES ON SCHEMA \"{schema}\" TO public"),
            format!("GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA \"{schema}\" TO public"),
            format!("GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA \"{schema}\" TO public"),
            format!("GRANT ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA \"{schema}\" TO public"),
            format!("ALTER DEFAULT PRIVILEGES IN SCHEMA \"{schema}\" GRANT ALL PRIVILEGES ON TABLES TO public"),
            format!("ALTER DEFAULT PRIVILEGES IN SCHEMA \"{schema}\" GRANT ALL PRIVILEGES ON SEQUENCES TO public"),
            format!("ALTER DEFAULT PRIVILEGES IN SCHEMA \"{schema}\" GRANT ALL PRIVILEGES ON FUNCTIONS TO public"),
        ];

        for query in queries {
            if let Err(e) = sqlx::query(&query).execute(pool).await {
                // Some queries might fail if they are already set or if the schema is special
                tracing::debug!("Grant query failed (ignoring): {} - {}", query, e);
            }
        }

        Ok(())
    }
}

pub async fn grant_pool_database_permissions(db_name: &str) -> TestResult<()> {
    let Some(granter) = PermissionGranter::from_env()? else {
        return Ok(());
    };

    // Use the superuser URL already stored in the granter
    let admin_url = &granter.superuser_url;

    // We need to parse and replace the DB name in the URL
    // For simplicity in xtask, we assume it's a standard postgres URL
    let db_url = if admin_url.contains('?') {
        let (base, params) = admin_url.split_once('?').unwrap();
        format!("{}/{}/?{}", base.trim_end_matches('/'), db_name, params)
    } else {
        format!("{}/{}", admin_url.trim_end_matches('/'), db_name)
    };

    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(&db_url)
        .await?;

    granter.grant_schema_access(&pool, "public").await?;
    granter.grant_schema_access(&pool, "core").await?;
    granter.grant_schema_access(&pool, "raw").await?;
    granter.grant_schema_access(&pool, "sinex_schemas").await?;

    Ok(())
}
