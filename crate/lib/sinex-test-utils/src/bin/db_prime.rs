use color_eyre::Result;

#[tokio::main]
async fn main() -> Result<()> {
    color_eyre::install()?;
    println!("Priming test database pool...");
    sinex_test_utils::prime_pool().await?;
    println!("Test database pool ready.");
    Ok(())
}
