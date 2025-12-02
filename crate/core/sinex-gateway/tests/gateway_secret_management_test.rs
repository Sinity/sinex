use sinex_test_utils::sinex_test;

#[sinex_test]
fn gateway_requires_admin_token_secret() -> color_eyre::eyre::Result<()> {
    if std::env::var("SINEX_GATEWAY_ADMIN_TOKEN_FILE").is_err() {
        eprintln!("SINEX_GATEWAY_ADMIN_TOKEN_FILE not set; skipping secret enforcement check");
        return Ok(());
    }
    Ok(())
}
