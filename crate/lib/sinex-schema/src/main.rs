//! CLI entrypoint for schema migrations.

use sea_orm_migration::prelude::*;
use sinex_schema::Migrator;

#[tokio::main]
async fn main() {
    cli::run_cli(Migrator).await;
}
