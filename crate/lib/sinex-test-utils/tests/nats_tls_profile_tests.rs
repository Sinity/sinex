use std::sync::Arc;

use sinex_test_utils::nats::{
    reset_shared_ephemeral_nats, shared_ephemeral_nats, shared_ephemeral_nats_with_key,
    EphemeralNats, SharedNatsProfile,
};
use sinex_test_utils::prelude::*;
use sinex_test_utils::EnvGuard;

#[sinex_test]
async fn shared_nats_profiles_are_distinct(_ctx: TestContext) -> TestResult<()> {
    let default = shared_ephemeral_nats(SharedNatsProfile::Default).await?;
    let secure = shared_ephemeral_nats(SharedNatsProfile::SecureTls).await?;

    ensure!(
        default.client_url() != secure.client_url(),
        "shared profiles should use separate NATS instances"
    );

    default.connect().await?;
    secure.connect().await?;
    Ok(())
}

#[sinex_test]
async fn shared_nats_profile_reuses_instance(_ctx: TestContext) -> TestResult<()> {
    let first = shared_ephemeral_nats(SharedNatsProfile::Default).await?;
    let second = shared_ephemeral_nats(SharedNatsProfile::Default).await?;

    ensure!(
        first.client_url() == second.client_url(),
        "shared profile should reuse the same NATS instance"
    );
    Ok(())
}

#[sinex_serial_test]
async fn shared_nats_reset_starts_new_instance(_ctx: TestContext) -> TestResult<()> {
    let first = shared_ephemeral_nats(SharedNatsProfile::Default).await?;
    reset_shared_ephemeral_nats().await?;
    let second = shared_ephemeral_nats(SharedNatsProfile::Default).await?;

    ensure!(
        !Arc::ptr_eq(&first, &second),
        "shared NATS reset should return a new instance"
    );
    Ok(())
}

#[sinex_serial_test]
async fn shared_nats_custom_key_uses_distinct_instance(_ctx: TestContext) -> TestResult<()> {
    reset_shared_ephemeral_nats().await?;
    let custom = shared_ephemeral_nats_with_key("custom-config", EphemeralNats::builder()).await?;
    let default = shared_ephemeral_nats(SharedNatsProfile::Default).await?;

    ensure!(
        custom.client_url() != default.client_url(),
        "custom shared key should start a distinct NATS instance"
    );
    Ok(())
}

#[sinex_test]
async fn shared_nats_tls_env_selects_secure_profile(ctx: TestContext) -> TestResult<()> {
    let mut guard = EnvGuard::new();
    guard.set("SINEX_TEST_USE_TLS", "1");

    let ctx = ctx.with_shared_nats().await?;
    let nats = ctx.nats_handle()?;
    ensure!(
        nats.client_url().starts_with("tls://"),
        "secure profile should use tls:// URL"
    );
    Ok(())
}

#[sinex_test]
async fn secure_nats_rejects_plaintext_clients(_ctx: TestContext) -> TestResult<()> {
    let secure = shared_ephemeral_nats(SharedNatsProfile::SecureTls).await?;
    let tls_url = secure.client_url();
    let plain_url = format!("nats://{}", tls_url.trim_start_matches("tls://"));

    let plain_res = async_nats::connect(plain_url).await;
    ensure!(
        plain_res.is_err(),
        "plaintext connect should fail against TLS-only NATS"
    );
    Ok(())
}
