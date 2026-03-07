use color_eyre::eyre::Result;
use tracing_subscriber::prelude::*;

#[tokio::main]
async fn main() -> Result<()> {
    color_eyre::install()?;

    // Better panic messages for users
    human_panic::setup_panic!();

    // Initialize tracing subscriber. Default is silent — set SINEX_LOG=debug for verbose output.
    // The EnvFilter is initialized from the SINEX_LOG environment variable.
    // Example: SINEX_LOG=debug xtask check  →  verbose preflight and pool diagnostics.
    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer().with_writer(std::io::stderr))
        .with(
            tracing_subscriber::EnvFilter::builder()
                .with_default_directive(tracing_subscriber::filter::LevelFilter::OFF.into())
                .with_env_var("SINEX_LOG")
                .from_env_lossy(),
        )
        .init();

    xtask::run_cli().await
}
