//! JetStream stream bootstrap for `JetStreamConsumer`.

use super::dlq::DLQ_DUPLICATE_WINDOW;
use super::*;
use sinex_primitives::nats::JetStreamEventLane;

impl JetStreamConsumer {
    /// Bootstrap all required `JetStream` streams
    pub(super) async fn bootstrap_streams(&self) -> EventEngineResult<()> {
        // When SINEX_NATS_STREAMS_MANAGED_EXTERNALLY=true, the NixOS module owns
        // stream configuration. Skip bootstrap so the two sources of truth don't
        // conflict on stream shape or subject overlap — but sinex-bor: verify
        // every stream this consumer needs actually exists before serving.
        // Previously this branch trusted external management blindly; a Nix
        // topology gap (e.g. the reflection lane, which had zero coverage in
        // nats.nix until this bead) meant sinexd could start and silently run
        // with no durable backing for streams it publishes/consumes on. Loud
        // failure here is strictly better than a consumer that starts, then
        // fails every publish at runtime with no startup-time signal.
        if std::env::var(env_vars::NATS_STREAMS_MANAGED_EXTERNALLY).as_deref() == Ok("true") {
            info!("NATS streams managed externally -- verifying required streams instead of bootstrapping");
            return self.verify_externally_managed_streams_present().await;
        }

        info!("Bootstrapping JetStream streams");

        // Events stream - bounded delivery buffer for the event engine.
        // Source material/archive are the replay authority, not JetStream. A
        // full raw/reflection stream must not reject fresh source or
        // self-observation publishes: with discard: New, a saturated dev stream wedges ingestion
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
        // dropping the oldest already-persisted confirmed event. A consumer that
        // falls >max_messages behind recovers from Postgres through its mandatory
        // startup catch-up, which is a far better failure mode than jamming
        // production. RetentionPolicy::Interest is deliberately NOT the current
        // target: it becomes safe only after stale durable-consumer GC and an
        // ephemeral-consumer non-pinning proof, because one orphaned durable would
        // otherwise re-create the retention jam this stream shape prevents.
        self.js
            .create_or_update_stream(jetstream::stream::Config {
                name: self.topology.confirmed_events_stream.to_string(),
                subjects: vec![self.topology.confirmed_events_subject.to_string()],
                retention: jetstream::stream::RetentionPolicy::Limits,
                max_messages: 2_000_000,
                max_bytes: confirmed_events_max_bytes(self.topology.lane),
                max_age: confirmed_events_max_age(self.topology.lane),
                storage: jetstream::stream::StorageType::File,
                discard: DiscardPolicy::Old,
                ..Default::default()
            })
            .await
            .map_err(|e| {
                SinexError::network("Failed to create confirmed-events stream").with_source(e)
            })?;

        // DLQ stream
        self.js
            .create_or_update_stream(jetstream::stream::Config {
                name: self.topology.dlq_stream.to_string(),
                subjects: vec![self.topology.dlq_subject.to_string()],
                retention: jetstream::stream::RetentionPolicy::Limits,
                max_bytes: diagnostic_stream_max_bytes(self.topology.lane),
                max_age: diagnostic_stream_max_age(self.topology.lane),
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
                max_bytes: diagnostic_stream_max_bytes(self.topology.lane),
                max_age: diagnostic_stream_max_age(self.topology.lane),
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

    /// sinex-bor: when Nix owns stream provisioning, verify every stream this
    /// consumer needs is actually present before the caller lets this
    /// consumer start serving. Fails loud (one combined error naming every
    /// missing stream) rather than skipping bootstrap and hoping — a prod
    /// deploy with a stale or incomplete Nix topology must not silently run
    /// degraded.
    pub(super) async fn verify_externally_managed_streams_present(&self) -> EventEngineResult<()> {
        let required = [
            self.topology.events_stream.to_string(),
            self.topology.confirmed_events_stream.to_string(),
            self.topology.dlq_stream.to_string(),
            self.topology.processing_failures_stream.to_string(),
            self.topology.invalidation_stream.to_string(),
        ];

        let mut missing = Vec::new();
        for name in &required {
            if self.js.get_stream(name).await.is_err() {
                missing.push(name.clone());
            }
        }

        if !missing.is_empty() {
            return Err(SinexError::configuration(format!(
                "NATS streams are externally managed (SINEX_NATS_STREAMS_MANAGED_EXTERNALLY=true) \
                 but {} required stream(s) are missing: {}. sinexd refuses to start serving this \
                 lane against an incomplete topology — provision these streams (nixos/modules/nats.nix \
                 services.sinex.nats.bootstrapStreams.streams) before restarting.",
                missing.len(),
                missing.join(", ")
            )));
        }

        info!(
            lane = ?self.topology.lane,
            streams = ?required,
            "Verified all required externally-managed JetStream streams are present"
        );
        Ok(())
    }
}

fn confirmed_events_max_bytes(lane: JetStreamEventLane) -> i64 {
    match lane {
        JetStreamEventLane::Activity => JETSTREAM_BOOTSTRAP_MAX_BYTES,
        JetStreamEventLane::Reflection => REFLECTION_CONFIRMED_MAX_BYTES,
    }
}

fn confirmed_events_max_age(lane: JetStreamEventLane) -> Duration {
    match lane {
        JetStreamEventLane::Activity => Duration::from_hours(72),
        JetStreamEventLane::Reflection => Duration::from_hours(24),
    }
}

fn diagnostic_stream_max_bytes(lane: JetStreamEventLane) -> i64 {
    match lane {
        JetStreamEventLane::Activity => JETSTREAM_BOOTSTRAP_MAX_BYTES,
        JetStreamEventLane::Reflection => REFLECTION_DIAGNOSTIC_MAX_BYTES,
    }
}

fn diagnostic_stream_max_age(lane: JetStreamEventLane) -> Duration {
    match lane {
        JetStreamEventLane::Activity => Duration::from_hours(72),
        JetStreamEventLane::Reflection => Duration::from_hours(24),
    }
}

#[cfg(test)]
#[path = "bootstrap_test.rs"]
mod tests;
