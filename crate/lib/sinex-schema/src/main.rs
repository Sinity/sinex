//! `sinex-schema` — schema apply/diff CLI
//!
//! Usage:
//!   sinex-schema up    # Apply schema (idempotent, safe to re-run)
//!   sinex-schema diff  # Show schema drift vs. current DB state (exit 1 if drift)

use sinex_schema::apply;
use sqlx::postgres::PgPoolOptions;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let subcommand = std::env::args().nth(1).unwrap_or_else(|| "up".to_string());
    let database_url =
        std::env::var("DATABASE_URL").expect("DATABASE_URL environment variable is required");

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await?;

    match subcommand.as_str() {
        "up" | "apply" => {
            apply::apply(&pool).await?;
            println!("sinex-schema: schema applied.");
        }
        "diff" => {
            let drifts = apply::diff(&pool).await?;
            if drifts.is_empty() {
                println!("sinex-schema: schema is up to date.");
            } else {
                for d in &drifts {
                    eprintln!("drift: {d}");
                }
                std::process::exit(1);
            }
        }
        other => {
            eprintln!("sinex-schema: unknown subcommand '{other}'");
            eprintln!("Usage: sinex-schema [up|diff]");
            std::process::exit(1);
        }
    }

    Ok(())
}
