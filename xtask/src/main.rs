use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    // Better panic messages for users
    human_panic::setup_panic!();

    xtask::run_cli().await
}
