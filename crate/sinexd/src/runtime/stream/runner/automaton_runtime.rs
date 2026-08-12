//! Automaton runtime loop for `RuntimeRunner`.
//!
//! Runs the automaton continuous mode entry point, drives leader-standby
//! coordination over the NATS coordination KV, and operates the
//! confirmation-event bridge that resolves provisional events to fully
//! materialized inputs and feeds them into the module implementation.

use super::{
    Arc, CONFIRMED_EVENT_CHANNEL_CAPACITY, Checkpoint, Event, JetStreamEventConsumer,
    JetStreamEventConsumerConfig, JsonValue,
    RunnerConfirmedEventHandler, RuntimeResult, RuntimeRunner, ScanArgs, SinexError, TimeHorizon,
    Uuid, debug, info, mpsc, systemd_notify, warn,
};

impl RuntimeRunner {
    /// Run automaton in continuous mode
    #[cfg(feature = "messaging")]
    pub(super) async fn run_automaton_continuous_mode(&mut self) -> RuntimeResult<()> {
        info!("Starting automaton continuous mode");
        let drain_controller = self
            .runtime_state()
            .ok_or_else(|| SinexError::lifecycle("Runtime state missing".to_string()))?
            .handles()
            .runtime_drain();

        // Get current checkpoint to resume from previous state if available
        let current_checkpoint = self.module.current_checkpoint().await?;
        let capabilities = self.module.capabilities();

        if capabilities.supports_continuous {
            info!("Starting continuous event processing for automaton");

            if capabilities.manages_own_continuous_loop {
                // A standby automaton is still a healthy, ready service. Satisfy
                // the systemd notify contract before waiting on lease handoff or
                // expiry so host activation does not fail on a legitimate standby.
                systemd_notify::notify_ready("sinex-runtime");
                let _continuous_report = self
                    .module
                    .scan(
                        current_checkpoint,
                        TimeHorizon::Continuous,
                        ScanArgs::default(),
                    )
                    .await?;
            } else {
                self.run_automaton_event_bridge(current_checkpoint).await?;
            }

            if drain_controller.is_requested() {
                info!("Automaton continuous processing completed after runtime drain");
            } else {
                info!("Automaton continuous processing completed");
            }
        } else {
            // Automata can also run in batch mode for historical processing
            if capabilities.supports_historical {
                info!("Running automaton in historical batch mode");

                // Process all historical events up to now
                let _historical_report = self
                    .module
                    .scan(
                        current_checkpoint,
                        TimeHorizon::Historical {
                            end_time: sinex_primitives::temporal::Timestamp::now(),
                        },
                        ScanArgs::default(),
                    )
                    .await?;

                info!("Automaton historical processing completed");
            } else {
                warn!("Automaton does not support continuous or historical mode");
            }
        }

        Ok(())
    }

    #[cfg(feature = "messaging")]
    pub(super) async fn run_automaton_event_bridge(
        &mut self,
        from: Checkpoint,
    ) -> RuntimeResult<()> {
        let handles = self
            .handles
            .as_ref()
            .ok_or_else(|| SinexError::lifecycle("Runner handles not initialized".to_string()))?;
        let drain_controller = handles.runtime_drain();
        let capabilities = self.module.capabilities();

        if !capabilities.supports_historical {
            return Err(SinexError::validation(format!(
                "Automaton bridge for module '{}' requires historical scan support before consuming confirmed events",
                self.module.module_name()
            )));
        }

        let transport = handles.transport().clone();
        let bridge_manages_checkpoints = !capabilities.manages_own_checkpoints;
        if !bridge_manages_checkpoints {
            debug!(
                module = %self.module.module_name(),
                "Skipping generic automaton-bridge checkpoint tracking because the module persists its own state"
            );
        }
        let checkpoint_manager = bridge_manages_checkpoints.then(|| handles.checkpoint_manager());
        let mut checkpoint_state = if let Some(manager) = checkpoint_manager.as_deref() {
            Some(Self::load_bridge_checkpoint_state(manager).await?)
        } else {
            None
        };
        let catchup_from = checkpoint_state
            .as_ref()
            .map_or_else(|| from.clone(), |state| state.checkpoint.clone());

        let service_name = self.service_info.as_ref().map_or_else(
            || self.module.module_name().to_string(),
            |info| info.service_name().to_string(),
        );

        let (sender, mut receiver) =
            mpsc::channel::<Event<JsonValue>>(CONFIRMED_EVENT_CHANNEL_CAPACITY);
        let handler = Arc::new(RunnerConfirmedEventHandler::new(sender));

        let env = sinex_primitives::environment::environment().clone();

        let nats_client = transport.nats_publisher()?.nats_client().clone();

        let consumer_config = Self::automaton_consumer_config(
            service_name.as_str(),
            self.module.confirmed_event_provenance_filter(),
            self.module.event_type_filters(),
        );
        let liveness_observer = Arc::new(crate::runtime::SelfObserver::new(
            nats_client.clone(),
            crate::runtime::SelfObserverConfig::from_env(&format!(
                "{}.confirmed-stream",
                service_name
            )),
        ));
        let mut consumer_config = consumer_config;
        consumer_config.liveness_observer = Some(liveness_observer);

        let consumer = Arc::new(JetStreamEventConsumer::new(
            nats_client.clone(),
            env.clone(),
            consumer_config,
            handler,
        ));

        let mut invalidation_sub = {
            let stream_name = env.nats_stream_name("SINEX_RAW_EVENTS_DERIVED_INVALIDATIONS");
            let queue_group = format!("derived.invalidation.{}", self.module.module_name());
            let deliver_subject = nats_client.new_inbox();
            let js = async_nats::jetstream::new(nats_client.clone());
            match js.get_stream(&stream_name).await {
                Ok(stream) => {
                    let config = async_nats::jetstream::consumer::push::Config {
                        deliver_subject,
                        deliver_group: Some(queue_group.clone()),
                        ..Default::default()
                    };
                    match stream.create_consumer(config).await {
                        Ok(consumer) => match consumer.messages().await {
                            Ok(messages) => Some(messages),
                            Err(error) => {
                                warn!(automaton = %self.module.module_name(), error = %error,
                                    "Failed to start bridge invalidation consumer");
                                None
                            }
                        },
                        Err(error) => {
                            warn!(automaton = %self.module.module_name(), error = %error,
                                "Failed to create bridge invalidation consumer");
                            None
                        }
                    }
                }
                Err(error) => {
                    warn!(automaton = %self.module.module_name(), error = %error,
                        "Failed to get bridge invalidation stream");
                    None
                }
            }
        };

        // sinex-li78: hand the test/harness-only consumer-ready sender (see
        // `RuntimeRunner::confirmed_consumer_ready_tx` field doc) to the
        // consumer task below via `run_with_ready_signal`, so a test can
        // deterministically wait for the durable JetStream consumer to exist
        // before publishing a confirmed event, instead of racing
        // `DeliverPolicy::New`. `None` outside test/testing builds — zero
        // behavior change (`run_with_ready_signal(None)` is exactly `run()`).
        #[cfg(any(test, feature = "testing"))]
        let ready_tx = self.confirmed_consumer_ready_tx.take();
        #[cfg(not(any(test, feature = "testing")))]
        let ready_tx = None;

        // Process historical backlog BEFORE starting the JetStream consumer.
        // The confirmed-event consumer ACKs after enqueueing into this bridge,
        // before the automaton finishes processing the batch. A restart is safe
        // only because every bridge-backed automaton can replay from its
        // durable bridge checkpoint through the DB before it subscribes with
        // DeliverPolicy::New. Load that checkpoint before catch-up: generic
        // bridge-managed automata may report Checkpoint::None from
        // module.current_checkpoint(), while the bridge KV holds the last
        // successfully processed event.
        info!("Processing historical backlog before entering continuous mode");
        let _ = self
            .module
            .scan(
                catchup_from,
                TimeHorizon::Historical {
                    end_time: sinex_primitives::temporal::Timestamp::now(),
                },
                ScanArgs::default(),
            )
            .await?;

        let consumer_failure = Arc::new(tokio::sync::Mutex::new(None));
        let consumer_runner = consumer.clone();
        let consumer_failure_reporter = Arc::clone(&consumer_failure);
        let consumer_handle = tokio::spawn(async move {
            if let Err(err) = consumer_runner.run_with_ready_signal(ready_tx).await {
                warn!(error = %err, "Automaton JetStream consumer terminated unexpectedly");
                let mut guard = consumer_failure_reporter.lock().await;
                *guard = Some(err);
            }
        });
        drain_controller.register_runtime_abort(consumer_handle.abort_handle());
        self.consumer_handle = Some(consumer_handle);

        // A bridge-backed automaton is not warmed until its historical scan
        // completed and the live durable consumer is bound. Readiness before
        // this point lets re-import health checks mistake a catch-up storm for
        // a serving automaton.
        systemd_notify::notify_ready("sinex-runtime");

        if drain_controller.is_requested() {
            let _ = drain_controller.abort_runtime_work();
            info!("Drain requested before automaton bridge entered live processing");
        }

        // Periodic checkpoint saves: prevent data loss on crash by persisting
        // progress every CHECKPOINT_EVENT_INTERVAL events or CHECKPOINT_TIME_INTERVAL.
        const CHECKPOINT_EVENT_INTERVAL: u64 = 100;
        const CHECKPOINT_TIME_INTERVAL: std::time::Duration = std::time::Duration::from_secs(30);

        let mut processed_events = 0u64;
        let mut events_since_checkpoint = 0u64;
        let mut last_checkpoint_time = std::time::Instant::now();
        let mut last_event_id: Option<Uuid> = None;
        let mut consecutive_checkpoint_failures = 0u32;

        // Batch processing: accumulate up to BATCH_SIZE events before processing.
        // Block on the first event, then non-blocking drain whatever else is queued.
        const BATCH_SIZE: usize = 100;

        // Periodic flush for Windowed automata (trailing-bucket emission).
        // Configurable via SINEX_WINDOWED_FLUSH_INTERVAL_SECS; default 60 s.
        // Non-windowed automata return 0 from `periodic_flush` immediately.
        let flush_interval_secs = sinex_primitives::env::parse_or(
            "SINEX_WINDOWED_FLUSH_INTERVAL_SECS",
            60_u64,
            "windowed automaton flush interval",
        );
        let mut flush_ticker =
            tokio::time::interval(std::time::Duration::from_secs(flush_interval_secs));
        // Skip the immediately-firing first tick so we don't flush on startup.
        flush_ticker.tick().await;

        loop {
            // Normal mode: select! between an incoming event and the flush timer.
            // Once drain is requested the consumer is aborted; switch to draining
            // whatever is still buffered before exiting cleanly.
            enum LoopAction {
                Event(Option<Event<JsonValue>>),
                FlushTick,
                Invalidation(Option<Vec<u8>>),
            }

            let action = if drain_controller.is_requested() {
                LoopAction::Event(receiver.try_recv().ok())
            } else {
                tokio::select! {
                    event = receiver.recv() => LoopAction::Event(event),
                    _ = flush_ticker.tick() => LoopAction::FlushTick,
                    payload = crate::runtime::automaton::recv_invalidation(
                        &mut invalidation_sub, None,
                    ) => LoopAction::Invalidation(payload),
                }
            };

            match action {
                LoopAction::FlushTick => {
                    let now = sinex_primitives::temporal::Timestamp::now();
                    if let Err(e) = self.module.periodic_flush(now).await {
                        warn!(
                            error = %e,
                            module = %self.module.module_name(),
                            "Windowed periodic flush failed; continuing"
                        );
                    }
                }
                LoopAction::Invalidation(Some(payload)) => {
                    self.module.process_invalidation_message(&payload).await?;
                }
                LoopAction::Invalidation(None) => {
                    invalidation_sub = None;
                }
                LoopAction::Event(next_event) => {
                    let Some(first) = next_event else {
                        if let Some(error) = consumer_failure.lock().await.take() {
                            return Err(error);
                        }
                        break;
                    };

                    // Non-blocking drain: grab whatever else is already queued.
                    // Confirmed events arrive as fully materialized
                    // `Event<JsonValue>` from the confirmed-events stream — no DB
                    // refetch, no provisional resolution (#2187 / #2202).
                    let mut events = vec![first];
                    while events.len() < BATCH_SIZE {
                        match receiver.try_recv() {
                            Ok(e) => events.push(e),
                            Err(_) => break,
                        }
                    }

                    let batch_last_event_id = events
                        .last()
                        .and_then(|event| event.id)
                        .map(|id| *id.as_uuid());

                    let batch_count = Self::process_batch_with_dlq_fallback(
                        self.module.as_mut(),
                        &transport,
                        events,
                    )
                    .await?;

                    processed_events += batch_count;
                    events_since_checkpoint += batch_count;
                    if let Some(eid) = batch_last_event_id {
                        last_event_id = Some(eid);
                    }

                    // Periodic checkpoint save: every N events or M seconds
                    if bridge_manages_checkpoints
                        && (events_since_checkpoint >= CHECKPOINT_EVENT_INTERVAL
                            || last_checkpoint_time.elapsed() >= CHECKPOINT_TIME_INTERVAL)
                        && let (Some(manager), Some(state)) =
                            (checkpoint_manager.as_deref(), checkpoint_state.as_mut())
                        && let Some(revision) = Self::try_save_checkpoint(
                            manager,
                            state,
                            last_event_id,
                            processed_events,
                            &mut consecutive_checkpoint_failures,
                        )
                        .await?
                    {
                        state.revision = revision;
                        events_since_checkpoint = 0;
                        last_checkpoint_time = std::time::Instant::now();
                    }
                }
            }
        }

        // Save final checkpoint on clean exit
        if bridge_manages_checkpoints
            && let (Some(manager), Some(state)) =
                (checkpoint_manager.as_deref(), checkpoint_state.as_mut())
            && Self::try_save_checkpoint(
                manager,
                state,
                last_event_id,
                processed_events,
                &mut consecutive_checkpoint_failures,
            )
            .await?
            .is_some()
        {
            info!(processed_events, "Final checkpoint saved on clean shutdown");
        }

        if drain_controller.is_requested() {
            info!(
                processed_events,
                "JetStream bridge drained after runtime drain request"
            );
        } else {
            info!(
                processed_events,
                "JetStream confirmed event channel closed; stopping automaton bridge"
            );
        }

        consumer.stop().await;
        drain_controller.clear_runtime_abort();

        if let Some(handle) = self.consumer_handle.take() {
            match handle.await {
                Ok(()) => {}
                Err(err) if err.is_cancelled() => {
                    debug!(error = ?err, "Automaton consumer task aborted during shutdown");
                }
                Err(err) => {
                    return Err(SinexError::service(format!(
                        "Failed to join automaton consumer task: {err}"
                    )));
                }
            }
        }

        Ok(())
    }

    /// NATS `max_ack_pending` for an automaton's confirmed-events consumer.
    ///
    /// Every automaton has its own durable consumer on the confirmed-events
    /// stream. Bounding per-consumer in-flight messages keeps aggregate boot
    /// memory flat when the daemon drains a large backlog.
    ///
    /// Overridable via `SINEX_AUTOMATON_CONSUMER_MAX_ACK_PENDING`.
    #[cfg(feature = "messaging")]
    const DEFAULT_AUTOMATON_CONSUMER_MAX_ACK_PENDING: i64 = 128;

    #[cfg(feature = "messaging")]
    fn automaton_consumer_max_ack_pending() -> i64 {
        match sinex_primitives::env::strict_parsed::<i64>(
            "SINEX_AUTOMATON_CONSUMER_MAX_ACK_PENDING",
        ) {
            Ok(Some(value)) if value > 0 => value,
            Ok(_) => Self::DEFAULT_AUTOMATON_CONSUMER_MAX_ACK_PENDING,
            Err(error) => {
                warn!(
                    %error,
                    "invalid SINEX_AUTOMATON_CONSUMER_MAX_ACK_PENDING; using default"
                );
                Self::DEFAULT_AUTOMATON_CONSUMER_MAX_ACK_PENDING
            }
        }
    }

    #[cfg(feature = "messaging")]
    pub(super) fn automaton_consumer_config(
        service_name: &str,
        provenance_filter: crate::runtime::automaton::traits::InputProvenanceFilter,
        event_type_filters: Vec<&str>,
    ) -> JetStreamEventConsumerConfig {
        let sanitized_service_name = service_name.replace('.', "_");
        let provenance_suffix = match provenance_filter {
            crate::runtime::automaton::traits::InputProvenanceFilter::Any => "",
            crate::runtime::automaton::traits::InputProvenanceFilter::MaterialOnly => "-material",
            crate::runtime::automaton::traits::InputProvenanceFilter::SynthesizedOnly => {
                "-synthesized"
            }
        };
        let filter_suffix = if event_type_filters.is_empty() {
            None
        } else {
            Some(format!(
                "-filter-{}",
                event_type_filters
                    .iter()
                    .map(|event_type| {
                        sinex_primitives::environment::SinexEnvironment::nats_subject_token(
                            event_type,
                        )
                    })
                    .collect::<Vec<_>>()
                    .join("_or_")
            ))
        };
        JetStreamEventConsumerConfig {
            provenance_filter,
            event_type_filters: event_type_filters.into_iter().map(str::to_string).collect(),
            batch_size: 128,
            max_ack_pending: Self::automaton_consumer_max_ack_pending(),
            consumer_name: format!(
                "{}-confirmed-events{}{}",
                sanitized_service_name,
                provenance_suffix,
                filter_suffix.as_deref().unwrap_or("")
            ),
            // Anything before the consumer's creation point is covered by the
            // mandatory per-automaton historical scan that runs before the
            // consumer starts, even on first start from Checkpoint::None. Live
            // delivery therefore only needs new confirmed events and does not
            // re-deliver the whole retained confirmed stream on startup.
            deliver_policy: async_nats::jetstream::consumer::DeliverPolicy::New,
            ..Default::default()
        }
    }
}
