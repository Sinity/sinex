use color_eyre::eyre::Result;

#[tokio::main]
async fn main() -> Result<()> {
    color_eyre::install()?;

    // Better panic messages for users
    human_panic::setup_panic!();

    xtask::run_cli().await
}
