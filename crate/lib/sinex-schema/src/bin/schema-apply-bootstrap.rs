// PgPoolOptions is used directly here instead of sinex_db::pool::create_pool because
// sinex-schema cannot depend on sinex-db without reversing the dependency direction
// (sinex-db depends on sinex-schema for schema definitions). These binaries are
// standalone tools that only need a minimal pool to apply or diff schema; they have
// no need for the full sinex-db pool configuration or repository layer.
use sqlx::postgres::PgPoolOptions;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let database_url = std::env::var("DATABASE_URL").map_err(|error| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("DATABASE_URL environment variable is required: {error}"),
        )
    })?;

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await?;

    sinex_schema::apply::ensure_shared_access_roles(&pool).await?;
    sinex_schema::apply::apply(&pool).await?;
    Ok(())
}
