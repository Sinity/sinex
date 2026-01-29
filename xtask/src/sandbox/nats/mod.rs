pub mod ephemeral;
pub mod jetstream;
pub mod pipeline;
pub mod setup;

pub use ephemeral::*;
pub use jetstream::*;
pub use pipeline::*;
pub use setup::*;

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
