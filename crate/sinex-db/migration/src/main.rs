use sea_orm_migration::prelude::*;

#[tokio::main]
async fn main() {
    cli::run_cli(sinex_db_migration::Migrator).await;
}
