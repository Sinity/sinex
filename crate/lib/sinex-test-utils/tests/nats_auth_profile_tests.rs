use sinex_test_utils::nats::{
    reset_shared_ephemeral_nats, shared_ephemeral_nats_with_key, EphemeralNats,
};
use sinex_test_utils::prelude::*;
use sinex_test_utils::EnvGuard;

fn normalize_nats_url(url: &str) -> String {
    if url.starts_with("nats://") || url.starts_with("tls://") {
        url.to_string()
    } else {
        format!("nats://{url}")
    }
}

#[sinex_serial_test]
async fn shared_nats_auth_token_requires_token(_ctx: TestContext) -> TestResult<()> {
    reset_shared_ephemeral_nats().await?;
    let token = "test-token";
    let key = format!("auth-token-{}", Ulid::new());
    let nats =
        shared_ephemeral_nats_with_key(&key, EphemeralNats::builder().with_auth_token(token))
            .await?;

    let plain_res = async_nats::connect(normalize_nats_url(nats.client_url())).await;
    ensure!(
        plain_res.is_err(),
        "plaintext client should be rejected when auth token is required"
    );

    nats.connect().await?;
    Ok(())
}

#[sinex_serial_test]
async fn shared_nats_env_token_enforces_auth(ctx: TestContext) -> TestResult<()> {
    reset_shared_ephemeral_nats().await?;
    let mut guard = EnvGuard::new();
    guard.set("SINEX_TEST_NATS_TOKEN", "env-secret");

    let ctx = ctx.with_nats().shared().await?;
    let nats = ctx.nats_handle()?;
    let plain_res = async_nats::connect(normalize_nats_url(nats.client_url())).await;
    ensure!(
        plain_res.is_err(),
        "shared NATS should reject plaintext clients when token env is set"
    );

    Ok(())
}
