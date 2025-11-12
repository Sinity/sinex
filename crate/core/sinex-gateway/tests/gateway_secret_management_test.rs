use sinex_test_utils::sinex_test;

#[sinex_test]
fn gateway_requires_admin_token_secret() -> color_eyre::eyre::Result<()> {
    let secret = std::env::var("SINEX_GATEWAY_ADMIN_TOKEN_FILE");
    assert!(
        secret.is_ok(),
        "Gateway should source admin tokens via agenix-managed files (set SINEX_GATEWAY_ADMIN_TOKEN_FILE)"
    );
    Ok(())
}
