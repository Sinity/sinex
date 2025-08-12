use color_eyre::eyre::Result;
use sea_orm_migration::prelude::*;

#[tokio::main]
async fn main() -> Result<()> {
    color_eyre::install()?;
    cli::run_cli(sinex_schema::Migrator).await;
    Ok(())
}
