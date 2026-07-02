//! JetStream stream bootstrap for `JetStreamConsumer`.

use super::dlq::DLQ_DUPLICATE_WINDOW;
use super::*;

impl JetStreamConsumer {
    /// Bootstrap all required `JetStream` streams
    pub(super) async fn bootstrap_streams(&self) -> EventEngineResult<()> {
        // When SINEX_NATS_STREAMS_MANAGED_EXTERNALLY=true, the NixOS module owns
        // stream configuration. Skip bootstrap so the two sources of truth don't
        // conflict on stream shape or subject overlap.
        if std::env::var(env_vars::NATS_STREAMS_MANAGED_EXTERNALLY).as_deref() == Ok("true") {
            info!("NATS streams managed externally -- skipping bootstrap");
            return Ok(());
        }

        info!("Bootstrapping JetStream streams");

        // Events stream - bounded delivery buffer for the event engine.
        // Source material/archive are the replay authority, not JetStream. A
        // full raw stream must not reject fresh source or self-observation
        // publishes: with discard: New, a saturated dev stream wedges ingestion
        // with "maximum bytes exceeded" while the database already holds older
        // admitted interpretations. Discard oldest when the bounded buffer is
        // full so current work continues flowing.
        crate::runtime::jetstream_streams::ensure_raw_events_stream_for_topology(
            &self.js,
            &self.topology,
        )
        .await?;

        // Confirmed-events stream — the FINAL persisted+redacted events that
        // automata and the SSE bus consume directly. Carries full event payloads
        // (NOT a watermark), so it is sized like the raw events stream and is
        // NON-compacted (every confirmed event is delivered exactly once to each
        // durable consumer). This replaces the raw-provisional-buffer + watermark
        // + Postgres-refetch path: automata receive authoritative redacted events
        // with no DB round-trip and no commit/confirmation visibility race.
        //
        // discard: Old (NOT New). Every message here is an event already durably
        // persisted in Postgres, so this stream is a bounded *delivery bus*, never
        // an archive — Postgres is the archive. It must NEVER reject a publish:
        // the durability gate acks a raw event only after its confirmed-event
        // publish succeeds (`gate raw-ack on confirmed-event publish`), so a full
        // stream with discard: New rejects the publish, the raw event is never
        // acked, JetStream redelivers it, it re-persists (ON CONFLICT no-op) and
        // re-publishes — an unbounded redelivery storm that wedges the whole
        // pipeline (observed: tens of thousands of "maximum messages exceeded"
        // and a stalled engine). discard: Old makes the publish always succeed by
        // dropping the oldest already-persisted confirmed event; a consumer that
        // falls >max_messages behind loses that tail (recoverable from Postgres),
        // which is a far better failure mode than jamming production. The ideal
        // shape is RetentionPolicy::Interest (drain once every durable automaton
        // has acked, retain only for a lagging consumer) — tracked as follow-up.
        self.js
            .create_or_update_stream(jetstream::stream::Config {
                name: self.topology.confirmed_events_stream.to_string(),
                subjects: vec![self.topology.confirmed_events_subject.to_string()],
                retention: jetstream::stream::RetentionPolicy::Limits,
                max_messages: 2_000_000,
                max_bytes: JETSTREAM_BOOTSTRAP_MAX_BYTES,
                max_age: Duration::from_hours(72), // 3 days
                storage: jetstream::stream::StorageType::File,
                discard: DiscardPolicy::Old,
                ..Default::default()
            })
            .await
            .map_err(|e| {
                SinexError::network("Failed to create confirmed-events stream").with_source(e)
            })?;

        // Confirmations stream — ephemeral operational notifications, not durable
        // history. Per-event-id subject pattern means `max_messages_per_subject = 1`
        // is structurally a no-op (each subject only ever holds one message); see
        // #1306 for the intended per-kind redesign. Until that lands, cap with
        // max_messages + max_bytes and discard oldest when full so newly-confirmed
        // events still get published.
        const CONFIRMATIONS_MAX_MESSAGES: i64 = 5_000_000;
        const CONFIRMATIONS_MAX_BYTES: i64 = 512 * 1024 * 1024; // 512 MiB
        self.js
            .create_or_update_stream(jetstream::stream::Config {
                name: self.topology.confirmations_stream.to_string(),
                subjects: vec![self.topology.confirmations_subject.to_string()],
                retention: jetstream::stream::RetentionPolicy::Limits,
                max_messages_per_subject: 1,
                max_messages: CONFIRMATIONS_MAX_MESSAGES,
                max_bytes: CONFIRMATIONS_MAX_BYTES,
                max_age: Duration::from_hours(72), // 3 days
                storage: jetstream::stream::StorageType::File,
                discard: DiscardPolicy::Old,
                ..Default::default()
            })
            .await
            .map_err(|e| {
                SinexError::network("Failed to create confirmations stream").with_source(e)
            })?;

        // Cap the total backlog to prevent unbounded growth when confirmation publish failures
        // persist. DiscardPolicy::New combined with max_messages ensures the stream does not
        // grow beyond the cap even if many events are continuously failing confirmation.
        const CONFIRMATION_RETRY_MAX_MESSAGES: i64 = 50_000;
        self.js
            .create_or_update_stream(jetstream::stream::Config {
                name: self.topology.confirmation_retry_stream.to_string(),
                subjects: vec![self.topology.confirmation_retry_subject.to_string()],
                retention: jetstream::stream::RetentionPolicy::Limits,
                max_messages_per_subject: 1,
                max_messages: CONFIRMATION_RETRY_MAX_MESSAGES,
                max_age: Duration::from_hours(72),
                storage: jetstream::stream::StorageType::File,
                discard: DiscardPolicy::New,
                ..Default::default()
            })
            .await
            .map_err(|e| {
                SinexError::network("Failed to create confirmation retry stream").with_source(e)
            })?;

        // DLQ stream
        self.js
            .create_or_update_stream(jetstream::stream::Config {
                name: self.topology.dlq_stream.to_string(),
                subjects: vec![self.topology.dlq_subject.to_string()],
                retention: jetstream::stream::RetentionPolicy::Limits,
                max_bytes: JETSTREAM_BOOTSTRAP_MAX_BYTES,
                max_age: Duration::from_hours(72), // 3 days
                storage: jetstream::stream::StorageType::File,
                duplicate_window: DLQ_DUPLICATE_WINDOW,
                allow_direct: true,
                discard: DiscardPolicy::New,
                ..Default::default()
            })
            .await
            .map_err(|e| SinexError::network("Failed to create DLQ stream").with_source(e))?;

        self.js
            .create_or_update_stream(jetstream::stream::Config {
                name: self.topology.processing_failures_stream.to_string(),
                subjects: vec![self.topology.processing_failures_subject.to_string()],
                retention: jetstream::stream::RetentionPolicy::Limits,
                max_bytes: JETSTREAM_BOOTSTRAP_MAX_BYTES,
                max_age: Duration::from_hours(72), // 3 days
                storage: jetstream::stream::StorageType::File,
                duplicate_window: DLQ_DUPLICATE_WINDOW,
                allow_direct: true,
                discard: DiscardPolicy::New,
                ..Default::default()
            })
            .await
            .map_err(|e| {
                SinexError::network("Failed to create processing-failures stream").with_source(e)
            })?;

        // Derived invalidation stream — scope invalidation signals for automatons.
        // Short retention since invalidations are only relevant for running automata.
        self.js
            .create_or_update_stream(jetstream::stream::Config {
                name: self.topology.invalidation_stream.to_string(),
                subjects: vec![self.topology.invalidation_subject.to_string()],
                retention: jetstream::stream::RetentionPolicy::Limits,
                max_age: Duration::from_hours(24), // 24h — running automata only
                storage: jetstream::stream::StorageType::File,
                ..Default::default()
            })
            .await
            .map_err(|e| {
                SinexError::network("Failed to create derived invalidation stream").with_source(e)
            })?;

        info!("JetStream streams bootstrapped successfully");
        Ok(())
    }
}
