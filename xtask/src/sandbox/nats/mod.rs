pub mod ephemeral;
pub mod jetstream;
pub mod pipeline;
pub mod setup;

pub use ephemeral::*;
pub use jetstream::*;
pub use pipeline::*;
pub use setup::*;

use async_nats::jetstream::{Context as JetStreamContext, kv};
use color_eyre::eyre::Result;
use std::sync::Arc;

/// Get a handle to the shared ephemeral NATS instance (default profile).
pub async fn shared_nats_handle() -> Result<Arc<EphemeralNats>> {
    shared_ephemeral_nats(SharedNatsProfile::Default).await
}

/// Get a handle to the shared ephemeral NATS instance (secure profile).
pub async fn shared_secure_nats_handle() -> Result<Arc<EphemeralNats>> {
    shared_ephemeral_nats(SharedNatsProfile::SecureTls).await
}

pub async fn create_or_open_kv_store(
    js: &JetStreamContext,
    config: kv::Config,
) -> Result<kv::Store> {
    sinex_primitives::nats::create_or_open_kv_store(js, config)
        .await
        .map_err(Into::into)
}

#[cfg(test)]
mod tests {
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
}
