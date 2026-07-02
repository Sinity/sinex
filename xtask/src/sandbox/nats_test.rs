use super::*;
use crate::sandbox::sinex_test;

#[sinex_test]
async fn test_create_or_open_kv_store_reuses_existing_bucket() -> Result<()> {
    let nats = EphemeralNats::start().await?;
    let js = nats.jetstream().await?;
    let bucket = format!("KV_TEST_REUSE_{}", uuid::Uuid::now_v7().simple());

    let first = create_or_open_kv_store(
        &js,
        kv::Config {
            bucket: bucket.clone(),
            history: 1,
            ..Default::default()
        },
    )
    .await?;
    let second = create_or_open_kv_store(
        &js,
        kv::Config {
            bucket: bucket.clone(),
            history: 1,
            ..Default::default()
        },
    )
    .await?;

    first
        .put("probe".to_string(), b"ok".to_vec().into())
        .await?;
    assert!(
        second.entry("probe").await?.is_some(),
        "second handle should see entries written through the first"
    );
    nats.shutdown().await?;
    Ok(())
}
