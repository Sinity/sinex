use crate::DbPool;
use sinex_primitives::error::{Result, SinexError};
use sqlx::error::DatabaseError;
use tracing::info;

const SQLSTATE_UNDEFINED_FILE: &str = "58P01";
const ERROR_CLASS_TIMESCALEDB_MISSING_LIBRARY: &str = "timescaledb_missing_library";
const ERROR_CLASS_MISSING_REQUIRED_EXTENSIONS: &str = "missing_required_extensions";

fn map_apply_error(err: sinex_schema::apply::ApplyError) -> SinexError {
    match err {
        sinex_schema::apply::ApplyError::MissingExtensions(missing) => {
            SinexError::database("Schema apply failed: required PostgreSQL extensions missing")
                .with_context("error_class", ERROR_CLASS_MISSING_REQUIRED_EXTENSIONS)
                .with_context("missing_extensions", missing.join(","))
        }
        sinex_schema::apply::ApplyError::Sqlx(sqlx_err) => {
            let mut mapped = SinexError::database("Schema apply failed").with_std_error(&sqlx_err);
            if let sqlx::Error::Database(db_err) = &sqlx_err {
                if let Some(code) = db_err.code() {
                    mapped = mapped.with_context("sqlstate", code.as_ref());
                }
                if db_err
                    .code()
                    .as_deref()
                    .is_some_and(|code| code == SQLSTATE_UNDEFINED_FILE)
                {
                    mapped =
                        mapped.with_context("error_class", ERROR_CLASS_TIMESCALEDB_MISSING_LIBRARY);
                }
            }
            mapped
        }
    }
}

/// Apply declarative schema using the given pool.
pub async fn apply_schema(pool: &DbPool) -> Result<()> {
    info!("Applying declarative database schema...");
    sinex_schema::apply::apply(pool)
        .await
        .map_err(map_apply_error)?;
    info!("Database schema apply completed");
    Ok(())
}

/// Apply declarative schema for a given database URL by creating a temporary connection.
pub async fn apply_schema_for_url(database_url: &str) -> Result<()> {
    use crate::pool::create_pool;

    let pool = create_pool(database_url).await.map_err(|e| {
        SinexError::database("Failed to create pool for schema apply").with_std_error(&e)
    })?;

    apply_schema(&pool).await?;
    pool.close().await;
    Ok(())
}
