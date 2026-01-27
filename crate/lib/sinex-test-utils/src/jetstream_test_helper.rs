//! JetStream Test Helper
//!
//! Provides ergonomic helpers for setting up and testing JetStream-based
//! event processing pipelines. Eliminates boilerplate around topology creation,
//! stream waiting, and DLQ assertions.
//!
//! # Example
//!
//! ```rust,ignore
//! use xtask::sandbox::JetStreamTestHelper;
//!
//! #[sinex_test]
//! async fn test_consumer(ctx: TestContext) -> TestResult<()> {
//!     let ctx = ctx.with_nats().shared().await?;
//!     let helper = JetStreamTestHelper::new(&ctx, "my-test").await?;
//!
//!     // Use helper.topology() and helper.jetstream() for setup
//!     // ...
//!
//!     // Assert DLQ is empty
//!     helper.assert_dlq_empty().await?;
//!     Ok(())
//! }
//! ```

use crate::nats::EphemeralNats;
use crate::timing_utils::Timeouts;
use crate::{TestContext, TestResult};
use async_nats::jetstream::{self, stream::State as StreamState};
use color_eyre::eyre::{eyre, WrapErr};
use sinex_ingestd::JetStreamTopology;
use std::sync::Arc;
use std::time::Duration;

/// Helper for JetStream-based test setup and assertions.
///
/// Encapsulates the common pattern of:
/// - Creating a JetStreamTopology
/// - Waiting for all streams (events, confirmations, DLQ) to be ready
/// - Providing DLQ state assertions
pub struct JetStreamTestHelper {
    nats: Arc<EphemeralNats>,
    js: jetstream::Context,
    topology: JetStreamTopology,
    stream_timeout: Duration,
}

impl JetStreamTestHelper {
    /// Create a new JetStreamTestHelper with the given suffix.
    ///
    /// This will:
    /// 1. Create a JetStreamTopology with namespaced stream/consumer names
    /// 2. Wait for all three streams (events, confirmations, DLQ) to be ready
    ///
    /// # Arguments
    /// * `ctx` - Test context (must have NATS enabled via `with_nats()`)
    /// * `suffix` - Unique suffix for stream names (e.g., test name or UUID)
    pub async fn new(ctx: &TestContext, suffix: &str) -> TestResult<Self> {
        Self::with_timeout(ctx, suffix, Duration::from_secs(Timeouts::SHORT)).await
    }

    /// Create a new JetStreamTestHelper with a custom stream wait timeout.
    pub async fn with_timeout(
        ctx: &TestContext,
        suffix: &str,
        stream_timeout: Duration,
    ) -> TestResult<Self> {
        let nats = ctx.nats_handle()?;
        let nats_client = ctx.nats_client();
        let js = nats.jetstream_with_client(nats_client);

        let env = ctx.env();
        let namespace = ctx.pipeline_namespace().prefix().to_string();
        let stream = ctx
            .pipeline_namespace()
            .stream(&format!("SINEX_RAW_EVENTS_{suffix}"));
        let topology = JetStreamTopology::new(
            &env,
            stream,
            ctx.pipeline_namespace()
                .consumer_name(&format!("ingestd-{suffix}")),
            Some(&namespace),
        );

        let helper = Self {
            nats,
            js: js.clone(),
            topology: topology.clone(),
            stream_timeout,
        };

        // Create streams before waiting
        let env = ctx.env();
        let _ = js
            .get_or_create_stream(async_nats::jetstream::stream::Config {
                name: topology.events_stream.clone(),
                subjects: vec![env.nats_subject_with_namespace(Some(&namespace), "events.>")],
                ..Default::default()
            })
            .await
            .wrap_err("Failed to create events stream")?;

        let _ = js
            .get_or_create_stream(async_nats::jetstream::stream::Config {
                name: topology.confirmations_stream.clone(),
                subjects: vec![format!("{}_CONFIRMATIONS", topology.events_stream)],
                ..Default::default()
            })
            .await
            .wrap_err("Failed to create confirmations stream")?;

        let _ = js
            .get_or_create_stream(async_nats::jetstream::stream::Config {
                name: topology.dlq_stream.clone(),
                subjects: vec![format!("{}_DLQ", topology.events_stream)],
                ..Default::default()
            })
            .await
            .wrap_err("Failed to create DLQ stream")?;

        // Wait for all streams to be ready
        helper.wait_for_all_streams().await?;

        Ok(helper)
    }

    /// Get the JetStream topology for consumer setup.
    pub fn topology(&self) -> &JetStreamTopology {
        &self.topology
    }

    /// Get the JetStream context for stream operations.
    pub fn jetstream(&self) -> &jetstream::Context {
        &self.js
    }

    /// Get the NATS handle for additional operations.
    pub fn nats(&self) -> &Arc<EphemeralNats> {
        &self.nats
    }

    /// Wait for all three streams (events, confirmations, DLQ) to be ready.
    ///
    /// This is called automatically in `new()`, but can be called again
    /// if streams need to be recreated during a test.
    pub async fn wait_for_all_streams(&self) -> TestResult<()> {
        self.nats
            .wait_for_stream(&self.js, &self.topology.events_stream, self.stream_timeout)
            .await
            .wrap_err("Failed to wait for events stream")?;
        self.nats
            .wait_for_stream(
                &self.js,
                &self.topology.confirmations_stream,
                self.stream_timeout,
            )
            .await
            .wrap_err("Failed to wait for confirmations stream")?;
        self.nats
            .wait_for_stream(&self.js, &self.topology.dlq_stream, self.stream_timeout)
            .await
            .wrap_err("Failed to wait for DLQ stream")?;
        Ok(())
    }

    /// Get the current state of the DLQ stream.
    pub async fn dlq_state(&self) -> TestResult<StreamState> {
        let state = self
            .js
            .get_stream(&self.topology.dlq_stream)
            .await
            .wrap_err("Failed to get DLQ stream")?
            .info()
            .await
            .wrap_err("Failed to get DLQ stream info")?
            .state;
        Ok(state)
    }

    /// Get the current state of the events stream.
    pub async fn events_state(&self) -> TestResult<StreamState> {
        let state = self
            .js
            .get_stream(&self.topology.events_stream)
            .await
            .wrap_err("Failed to get events stream")?
            .info()
            .await
            .wrap_err("Failed to get events stream info")?
            .state;
        Ok(state)
    }

    /// Get the current state of the confirmations stream.
    pub async fn confirmations_state(&self) -> TestResult<StreamState> {
        let state = self
            .js
            .get_stream(&self.topology.confirmations_stream)
            .await
            .wrap_err("Failed to get confirmations stream")?
            .info()
            .await
            .wrap_err("Failed to get confirmations stream info")?
            .state;
        Ok(state)
    }

    /// Assert that the DLQ is empty.
    ///
    /// Use this at the end of happy-path tests to verify no events
    /// were routed to the dead letter queue.
    pub async fn assert_dlq_empty(&self) -> TestResult<()> {
        let state = self.dlq_state().await?;
        if state.messages != 0 {
            return Err(eyre!(
                "DLQ should be empty but contains {} message(s)",
                state.messages
            ));
        }
        Ok(())
    }

    /// Assert that the DLQ has exactly the expected number of messages.
    pub async fn assert_dlq_count(&self, expected: u64) -> TestResult<()> {
        let state = self.dlq_state().await?;
        if state.messages != expected {
            return Err(eyre!(
                "DLQ should have {} message(s) but has {}",
                expected,
                state.messages
            ));
        }
        Ok(())
    }

    /// Assert that the DLQ has at least the expected number of messages.
    pub async fn assert_dlq_at_least(&self, min: u64) -> TestResult<()> {
        let state = self.dlq_state().await?;
        if state.messages < min {
            return Err(eyre!(
                "DLQ should have at least {} message(s) but has {}",
                min,
                state.messages
            ));
        }
        Ok(())
    }

    /// Assert that the events stream has exactly the expected number of messages.
    pub async fn assert_events_count(&self, expected: u64) -> TestResult<()> {
        let state = self.events_state().await?;
        if state.messages != expected {
            return Err(eyre!(
                "Events stream should have {} message(s) but has {}",
                expected,
                state.messages
            ));
        }
        Ok(())
    }

    /// Get the number of messages in the DLQ.
    pub async fn dlq_message_count(&self) -> TestResult<u64> {
        Ok(self.dlq_state().await?.messages)
    }

    /// Get the number of messages in the events stream.
    pub async fn events_message_count(&self) -> TestResult<u64> {
        Ok(self.events_state().await?.messages)
    }

    /// Get the number of messages in the confirmations stream.
    pub async fn confirmations_message_count(&self) -> TestResult<u64> {
        Ok(self.confirmations_state().await?.messages)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sinex_test;

    #[sinex_test]
    async fn jetstream_test_helper_creates_topology(ctx: TestContext) -> TestResult<()> {
        let ctx = ctx.with_nats().shared().await?;
        let helper = JetStreamTestHelper::new(&ctx, "helper-test").await?;

        // Verify topology was created
        assert!(!helper.topology().events_stream.is_empty());
        assert!(!helper.topology().confirmations_stream.is_empty());
        assert!(!helper.topology().dlq_stream.is_empty());

        // Verify DLQ is empty initially
        helper.assert_dlq_empty().await?;

        Ok(())
    }
}
