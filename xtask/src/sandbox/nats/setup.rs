//! `NatsSetup` builder for Sandbox NATS initialization.
//!
//! This module provides a unified builder pattern for configuring NATS in tests,
//! replacing the previous proliferation of `with_nats*` methods.
//!
//! # Usage
//!
//! ```rust,ignore
//! // Shared NATS (default, recommended)
//! let ctx = ctx.with_nats().shared().await?;
//!
//! // Dedicated NATS instance (isolated)
//! let ctx = ctx.with_nats().dedicated().await?;
//!
//! // Shared with TLS
//! let ctx = ctx.with_nats().shared().secure().await?;
//!
//! // Custom configuration
//! let ctx = ctx.with_nats().config(builder).await?;
//! ```

use crate::sandbox::context::{NatsMode, Sandbox};
use crate::sandbox::nats::{
    shared_ephemeral_nats_with_key, EphemeralNats, EphemeralNatsBuilder, SharedNatsProfile,
};
use crate::sandbox::nats::{shared_nats_handle, shared_secure_nats_handle};
use crate::sandbox::prelude::TestResult;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Builder for NATS configuration in Sandbox.
///
/// Created via `Sandbox::with_nats()`. Use method chaining to configure
/// the desired NATS setup, then call `.await` to apply.
pub struct NatsSetup {
    ctx: Sandbox,
    mode: NatsSetupMode,
    secure: bool,
    custom_key: Option<String>,
    custom_builder: Option<EphemeralNatsBuilder>,
}

#[derive(Clone, Copy, Debug, Default)]
enum NatsSetupMode {
    #[default]
    Shared,
    Dedicated,
}

impl NatsSetup {
    pub(crate) fn new(ctx: Sandbox) -> Self {
        Self {
            ctx,
            mode: NatsSetupMode::Shared,
            secure: Self::env_secure_requested(),
            custom_key: Self::env_shared_key_override(),
            custom_builder: None,
        }
    }

    /// Use a shared process-wide NATS instance (default).
    ///
    /// Shared NATS is faster as it reuses a single server across tests.
    /// Tests are isolated via namespace prefixing.
    #[must_use]
    pub fn shared(mut self) -> Self {
        self.mode = NatsSetupMode::Shared;
        self
    }

    /// Use a dedicated NATS instance for this test.
    ///
    /// This starts a fresh ephemeral NATS server for complete isolation.
    /// Use when tests require exclusive NATS access or custom server config.
    #[must_use]
    pub fn dedicated(mut self) -> Self {
        self.mode = NatsSetupMode::Dedicated;
        self
    }

    /// Enable TLS for the NATS connection.
    ///
    /// For shared mode, uses the secure TLS profile.
    /// For dedicated mode, configures TLS on the ephemeral server.
    #[must_use]
    pub fn secure(mut self) -> Self {
        self.secure = true;
        self
    }

    /// Use a custom shared instance key.
    ///
    /// This allows multiple test suites to share a NATS instance with specific
    /// configuration by using the same key.
    pub fn key(mut self, key: impl Into<String>) -> Self {
        self.custom_key = Some(key.into());
        self
    }

    /// Apply custom NATS builder configuration.
    ///
    /// For dedicated mode: uses the builder directly.
    /// For shared mode: uses the builder with the computed shared key.
    #[must_use]
    pub fn config(mut self, builder: EphemeralNatsBuilder) -> Self {
        self.custom_builder = Some(builder);
        self
    }

    /// Finalize the NATS setup and return the configured Sandbox.
    pub async fn build(self) -> TestResult<Sandbox> {
        match self.mode {
            NatsSetupMode::Dedicated => self.build_dedicated().await,
            NatsSetupMode::Shared => self.build_shared().await,
        }
    }

    async fn build_dedicated(mut self) -> TestResult<Sandbox> {
        let builder = self.custom_builder.take().unwrap_or_else(|| {
            let mut b = EphemeralNats::builder();
            if self.secure {
                let fixture_dir =
                    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../tests/fixtures/tls");
                let fixture_dir = fixture_dir.canonicalize().unwrap_or(fixture_dir);
                b = b.with_tls_fixtures_path(fixture_dir);
            }
            if let Some(token) = Self::env_auth_token() {
                b = b.with_auth_token(token);
            }
            if let Some(config_file) = Self::env_config_file() {
                b = b.with_config_file(config_file);
            }
            b
        });

        let nats = builder.start().await?;
        let client = nats.connect().await?;
        let shutdown_proc = nats.process_handle();

        self.ctx
            .register_background_handle("nats-server", shutdown_proc.clone())
            .await;
        self.ctx
            .register_shutdown_hook("nats-shutdown", async move {
                if let Some(mut child) = shutdown_proc.lock().await.take() {
                    let _ = child.start_kill();
                    let _ =
                        tokio::time::timeout(std::time::Duration::from_secs(2), child.wait()).await;
                }
            })
            .await;

        self.ctx.set_nats(
            Some(Arc::new(nats)),
            Some(client.clone()),
            NatsMode::Dedicated,
        );
        self.ctx.register_reaper_client(client);
        self.ctx.install_current();

        Ok(self.ctx)
    }

    async fn build_shared(mut self) -> TestResult<Sandbox> {
        let token = Self::env_auth_token();
        let config_file = Self::env_config_file();

        // Build the base builder with env overrides
        let mut builder = if self.secure {
            SharedNatsProfile::SecureTls.builder()
        } else {
            EphemeralNats::builder()
        };

        // Apply custom builder if provided, otherwise apply env config
        if let Some(custom) = self.custom_builder.take() {
            builder = custom;
        } else {
            if let Some(config_path) = &config_file {
                builder = builder.with_config_file(config_path.clone());
            }
            if let Some(t) = &token {
                builder = builder.with_auth_token(t.clone());
            }
        }

        // Determine the shared key
        let key = if let Some(k) = self.custom_key {
            // Custom key with optional token hash
            if let Some(t) = &token {
                let hash = blake3::hash(t.as_bytes());
                format!("{k}-token-{}", hash.to_hex())
            } else {
                k
            }
        } else if let Some(t) = &token {
            Self::shared_nats_token_key(t, self.secure)
        } else if let Some(config_path) = &config_file {
            Self::shared_nats_config_key(config_path, self.secure)
        } else {
            String::new() // Use default profile key
        };

        // Get or create the shared NATS instance
        let nats = if key.is_empty() {
            if self.secure {
                shared_secure_nats_handle().await?
            } else {
                shared_nats_handle().await?
            }
        } else {
            shared_ephemeral_nats_with_key(&key, builder).await?
        };

        let client = nats.connect().await?;
        self.ctx
            .set_nats(Some(nats), Some(client.clone()), NatsMode::Shared);
        self.ctx.register_reaper_client(client);
        self.ctx.install_current();

        Ok(self.ctx)
    }

    // Environment variable helpers

    fn env_secure_requested() -> bool {
        matches!(
            std::env::var("SINEX_TEST_USE_TLS")
                .unwrap_or_default()
                .to_lowercase()
                .as_str(),
            "1" | "true" | "yes" | "on"
        )
    }

    fn env_auth_token() -> Option<String> {
        std::env::var("SINEX_TEST_NATS_TOKEN")
            .ok()
            .map(|t| t.trim().to_string())
            .filter(|t| !t.is_empty())
    }

    fn env_shared_key_override() -> Option<String> {
        std::env::var("SINEX_TEST_NATS_SHARED_KEY")
            .ok()
            .map(|k| k.trim().to_string())
            .filter(|k| !k.is_empty())
    }

    fn env_config_file() -> Option<PathBuf> {
        std::env::var("SINEX_TEST_NATS_CONFIG_FILE")
            .ok()
            .map(|p| PathBuf::from(p.trim()))
            .filter(|p| !p.as_os_str().is_empty())
    }

    fn shared_nats_token_key(token: &str, secure_tls: bool) -> String {
        let hash = blake3::hash(token.as_bytes());
        if secure_tls {
            format!("auth-token-tls-{}", hash.to_hex())
        } else {
            format!("auth-token-{}", hash.to_hex())
        }
    }

    fn shared_nats_config_key(config_file: &Path, secure_tls: bool) -> String {
        let mut hasher = blake3::Hasher::new();
        hasher.update(config_file.to_string_lossy().as_bytes());
        hasher.update(if secure_tls { b":tls" } else { b":plain" });
        format!("config-{}", hasher.finalize().to_hex())
    }
}

// Implement IntoFuture so `.await` works directly on the builder
impl std::future::IntoFuture for NatsSetup {
    type Output = TestResult<Sandbox>;
    type IntoFuture = std::pin::Pin<Box<dyn std::future::Future<Output = Self::Output> + Send>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(self.build())
    }
}
