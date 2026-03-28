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

    sinex_schema::apply::apply(&pool).await?;
    Ok(())
}
