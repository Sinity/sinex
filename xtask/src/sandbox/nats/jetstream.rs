//! `JetStream` Test Helper
//!
//! Provides ergonomic helpers for setting up and testing JetStream-based
//! event processing pipelines. Eliminates boilerplate around topology creation,
//! stream waiting, and DLQ assertions.
//!
//! # Example
//!
//! ```rust,ignore
//! use sinex_test_utils::JetStreamTestHelper;
//!
//! #[sinex_test]
//! async fn test_consumer(ctx: Sandbox) -> TestResult<()> {
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

use crate::sandbox::prelude::*;
use async_nats::jetstream::{self, stream::State as StreamState};
use serde::Serialize;
use sinex_primitives::nats::JetStreamTopology;
use std::sync::Arc;
use std::time::Duration;

/// Helper for JetStream-based test setup and assertions.
///
/// Encapsulates the common pattern of:
/// - Creating a `JetStreamTopology`
/// - Waiting for all streams (events, confirmations, raw DLQ, processing failures) to be ready
/// - Providing DLQ state assertions
pub struct JetStreamTestHelper {
    nats: Arc<EphemeralNats>,
    js: jetstream::Context,
    topology: JetStreamTopology,
    stream_timeout: Duration,
}

impl JetStreamTestHelper {
    /// Create a new `JetStreamTestHelper` with the given suffix.
    ///
    /// This will:
    /// 1. Create a `JetStreamTopology` with namespaced stream/consumer names
    /// 2. Wait for all required streams to be ready
    ///
    /// # Arguments
    /// * `ctx` - Test context (must have NATS enabled via `with_nats()`)
    /// * `suffix` - Unique suffix for stream names (e.g., test name or UUID)
    pub async fn new(ctx: &Sandbox, suffix: &str) -> TestResult<Self> {
        Self::with_timeout(ctx, suffix, Duration::from_secs(Timeouts::SHORT)).await
    }

    /// Create a new `JetStreamTestHelper` with a custom stream wait timeout.
    pub async fn with_timeout(
        ctx: &Sandbox,
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
            env,
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
        let _ = js
            .get_or_create_stream(async_nats::jetstream::stream::Config {
                name: topology.events_stream.clone(),
                subjects: vec![topology.events_subject.clone()],
                ..Default::default()
            })
            .await
            .wrap_err("Failed to create events stream")?;

        let _ = js
            .get_or_create_stream(async_nats::jetstream::stream::Config {
                name: topology.confirmations_stream.clone(),
                subjects: vec![topology.confirmations_subject.clone()],
                ..Default::default()
            })
            .await
            .wrap_err("Failed to create confirmations stream")?;

        let _ = js
            .get_or_create_stream(async_nats::jetstream::stream::Config {
                name: topology.dlq_stream.clone(),
                subjects: vec![topology.dlq_subject.clone()],
                ..Default::default()
            })
            .await
            .wrap_err("Failed to create DLQ stream")?;

        let _ = js
            .get_or_create_stream(async_nats::jetstream::stream::Config {
                name: topology.processing_failures_stream.clone(),
                subjects: vec![topology.processing_failures_subject.clone()],
                ..Default::default()
            })
            .await
            .wrap_err("Failed to create processing-failures stream")?;

        // Wait for all streams to be ready
        helper.wait_for_all_streams().await?;

        Ok(helper)
    }

    /// Get the `JetStream` topology for consumer setup.
    #[must_use]
    pub fn topology(&self) -> &JetStreamTopology {
        &self.topology
    }

    /// Get the `JetStream` context for stream operations.
    #[must_use]
    pub fn jetstream(&self) -> &jetstream::Context {
        &self.js
    }

    /// Get the NATS handle for additional operations.
    #[must_use]
    pub fn nats(&self) -> &Arc<EphemeralNats> {
        &self.nats
    }

    /// Wait for all required streams (events, confirmations, raw DLQ, processing failures) to be ready.
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
        self.nats
            .wait_for_stream(
                &self.js,
                &self.topology.processing_failures_stream,
                self.stream_timeout,
            )
            .await
            .wrap_err("Failed to wait for processing-failures stream")?;
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
            .state
            .clone();
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
            .state
            .clone();
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
            .state
            .clone();
        Ok(state)
    }

    /// Assert that the raw-ingest DLQ is empty.
    ///
    /// Use this at the end of happy-path tests to verify no events
    /// were routed to the raw-ingest dead-letter queue.
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

    /// Get a snapshot of a consumer's delivery/ack state.
    ///
    /// Consumer must exist on the events stream.
    pub async fn consumer_snapshot(&self, consumer_name: &str) -> TestResult<ConsumerSnapshot> {
        let stream = self
            .js
            .get_stream(&self.topology.events_stream)
            .await
            .wrap_err("Failed to get events stream")?;

        let mut consumer = stream
            .get_consumer::<jetstream::consumer::pull::Config>(consumer_name)
            .await
            .map_err(|e| eyre!("Failed to get consumer '{consumer_name}': {e}"))?;

        let info = consumer
            .info()
            .await
            .map_err(|e| eyre!("Failed to get info for consumer '{consumer_name}': {e}"))?;

        Ok(ConsumerSnapshot {
            num_pending: info.num_pending as u64,
            num_ack_pending: info.num_ack_pending as u64,
            num_waiting: info.num_waiting as u64,
            delivered_stream_sequence: info.delivered.stream_sequence as u64,
            ack_floor_stream_sequence: info.ack_floor.stream_sequence as u64,
        })
    }

    /// Get the number of pending (undelivered) messages for a consumer.
    pub async fn consumer_pending(&self, consumer_name: &str) -> TestResult<u64> {
        Ok(self.consumer_snapshot(consumer_name).await?.num_pending)
    }

    /// Wait until a consumer has no pending or ack-pending messages.
    pub async fn wait_for_consumer_drain(
        &self,
        consumer_name: &str,
        timeout: Duration,
    ) -> TestResult<()> {
        let consumer_name = consumer_name.to_string();
        let start = std::time::Instant::now();
        loop {
            let snapshot = self.consumer_snapshot(&consumer_name).await?;
            if snapshot.num_pending == 0 && snapshot.num_ack_pending == 0 {
                return Ok(());
            }
            if start.elapsed() > timeout {
                return Err(eyre!(
                    "Consumer '{}' not drained after {:?}: pending={}, ack_pending={}",
                    consumer_name,
                    timeout,
                    snapshot.num_pending,
                    snapshot.num_ack_pending,
                ));
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
}

/// Snapshot of a JetStream consumer's delivery and acknowledgment state.
///
/// Useful for mid-test inspection of consumer progress.
#[derive(Debug, Serialize)]
pub struct ConsumerSnapshot {
    pub num_pending: u64,
    pub num_ack_pending: u64,
    pub num_waiting: u64,
    pub delivered_stream_sequence: u64,
    pub ack_floor_stream_sequence: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[sinex_test]
    async fn jetstream_test_helper_creates_topology(ctx: Sandbox) -> TestResult<()> {
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
