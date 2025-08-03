use sea_orm_migration::prelude::*;
use color_eyre::eyre::Result;

#[tokio::main]
async fn main() -> Result<()> {
    color_eyre::install()?;
    cli::run_cli(sinex_db_migration::Migrator).await;
    Ok(())
}
