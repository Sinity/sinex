use color_eyre::eyre::Result;

#[tokio::main]
async fn main() -> Result<()> {
    color_eyre::install()?;

    // Tracing subscriber is initialized inside run_cli() after arg parse,
    // so -v/-vv/-vvv flags can influence the log level.
    Box::pin(xtask::run_cli()).await
}
