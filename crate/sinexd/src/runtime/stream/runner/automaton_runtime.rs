//! Automaton runtime loop for `RuntimeRunner<T>`.
//!
//! Runs the automaton continuous mode entry point, drives leader-standby
//! coordination over the NATS coordination KV, and operates the
//! confirmation-event bridge that resolves provisional events to fully
//! materialized inputs and feeds them into the module implementation.

use super::{
    Arc, CONFIRMED_EVENT_CHANNEL_CAPACITY, Checkpoint, JetStreamEventConsumer,
    JetStreamEventConsumerConfig, LeaderState, ProcessingModel, ProvisionalEvent,
    RunnerConfirmedEventHandler, RuntimeModule, RuntimeResult, RuntimeRunner, ScanArgs, SinexError,
    TimeHorizon, Uuid, debug, info, mpsc, systemd_notify, warn,
};

impl<T: RuntimeModule + 'static> RuntimeRunner<T> {
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

            // A standby automaton is still a healthy, ready service. Satisfy
            // the systemd notify contract before waiting on lease handoff or
            // expiry so host activation does not fail on a legitimate standby.
            systemd_notify::notify_ready("sinex-runtime");

            if self.processing_model == ProcessingModel::LeaderStandby {
                let leader_acquired = self.acquire_leader_standby().await?;
                if !leader_acquired {
                    info!("Drain requested while waiting in leader standby; exiting cleanly");
                    return Ok(());
                }
            }

            if capabilities.manages_own_continuous_loop {
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

    /// Acquire leadership for `LeaderStandby` processing model.
    ///
    /// If another instance currently holds the lease, remain in standby and
    /// retry until the lease is handed off or expires.
    pub(super) async fn acquire_leader_standby(&mut self) -> RuntimeResult<bool> {
        #[cfg(feature = "messaging")]
        {
            let rs = self
                .runtime_state()
                .ok_or_else(|| SinexError::lifecycle("Runtime state missing".to_string()))?;
            let drain_controller = rs.handles().runtime_drain();
            let nc = rs
                .nats_client()
                .ok_or_else(|| SinexError::lifecycle("NATS client missing".to_string()))?;
            let service = rs.service_info().service_name().to_string();
            let host = rs.service_info().host().as_str().to_string();
            let pid = std::process::id();
            let instance_id = format!("{host}-{pid}");

            let js = async_nats::jetstream::new(nc);
            let kv_client =
                sinex_primitives::coordination::CoordinationKvClient::new(js, service.clone());
            let heartbeat_interval = kv_client.heartbeat_interval();
            let mut announced_standby = false;

            loop {
                if drain_controller.is_requested() {
                    return Ok(false);
                }

                let is_leader = kv_client
                    .acquire_leadership(&instance_id)
                    .await
                    .map_err(|e| {
                        SinexError::processing(format!("Failed to acquire leadership: {e}"))
                    })?;

                if is_leader {
                    break;
                }

                if !announced_standby {
                    info!(
                        service = %service,
                        heartbeat_interval_ms = heartbeat_interval.as_millis(),
                        "Not leader; entering standby and waiting for lease handoff or expiry"
                    );
                    announced_standby = true;
                }

                tokio::time::sleep(heartbeat_interval).await;
            }

            info!("Confirmed as leader, proceeding with processing");

            // Reuse the configured coordination heartbeat interval so stream-mode
            // leader/standby timing matches the main coordination runtime.
            let kv_clone = kv_client.clone();
            let instance_id_clone = instance_id.clone();
            let (heartbeat_shutdown, heartbeat_shutdown_rx) = tokio::sync::oneshot::channel();
            let heartbeat_handle = tokio::spawn(async move {
                let mut interval = tokio::time::interval(heartbeat_interval);
                let mut heartbeat_shutdown_rx = heartbeat_shutdown_rx;
                loop {
                    tokio::select! {
                        _ = interval.tick() => {
                            if let Err(e) = kv_clone.acquire_leadership(&instance_id_clone).await {
                                warn!("Heartbeat failed: {e}");
                            }
                        }
                        _ = &mut heartbeat_shutdown_rx => {
                            break;
                        }
                    }
                }
            });

            self.leader_state = Some(LeaderState {
                kv_client,
                instance_id,
                heartbeat_shutdown,
                heartbeat_handle,
            });
        }

        #[cfg(not(feature = "messaging"))]
        {
            self.runtime_state()
                .ok_or_else(|| SinexError::lifecycle("Runtime state missing".to_string()))?;
            warn!("LeaderStandby mode requires messaging feature. Skipping leadership check.");
        }

        Ok(true)
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

        #[cfg(feature = "db")]
        let db_pool = handles.db_pool().cloned();
        // No db_pool variable if db feature is off
        #[cfg(feature = "db")]
        let db_backed_confirmations = db_pool.is_some();
        #[cfg(not(feature = "db"))]
        let db_backed_confirmations = false;
        let transport = handles.transport().clone();

        let service_name = self.service_info.as_ref().map_or_else(
            || self.module.module_name().to_string(),
            |info| info.service_name().to_string(),
        );

        let (sender, mut receiver) =
            mpsc::channel::<ProvisionalEvent>(CONFIRMED_EVENT_CHANNEL_CAPACITY);
        let handler = Arc::new(RunnerConfirmedEventHandler::new(sender));

        let env = sinex_primitives::environment::environment().clone();

        let nats_client = transport.nats_publisher()?.nats_client().clone();

        let consumer_config = Self::automaton_consumer_config(
            service_name.as_str(),
            db_backed_confirmations,
            self.processing_model,
            self.module.raw_event_type_filter(),
        );

        let consumer = Arc::new(JetStreamEventConsumer::new(
            nats_client,
            env,
            consumer_config,
            handler,
            None,
        ));

        // Process historical backlog BEFORE starting the JetStream consumer.
        // This ensures events published after consumer creation but present in
        // the DB at scan time are not processed twice.
        if !matches!(from, Checkpoint::None) && self.module.capabilities().supports_historical {
            info!("Processing historical backlog before entering continuous mode");
            let _ = self
                .module
                .scan(
                    from,
                    TimeHorizon::Historical {
                        end_time: sinex_primitives::temporal::Timestamp::now(),
                    },
                    ScanArgs::default(),
                )
                .await?;
        }

        let consumer_failure = Arc::new(tokio::sync::Mutex::new(None));
        let consumer_runner = consumer.clone();
        let consumer_failure_reporter = Arc::clone(&consumer_failure);
        let consumer_handle = tokio::spawn(async move {
            if let Err(err) = consumer_runner.run().await {
                warn!(error = %err, "Automaton JetStream consumer terminated unexpectedly");
                let mut guard = consumer_failure_reporter.lock().await;
                *guard = Some(err);
            }
        });
        drain_controller.register_runtime_abort(consumer_handle.abort_handle());
        self.consumer_handle = Some(consumer_handle);

        if drain_controller.is_requested() {
            let _ = drain_controller.abort_runtime_work();
            info!("Drain requested before automaton bridge entered live processing");
        }

        let bridge_manages_checkpoints = !self.module.capabilities().manages_own_checkpoints;
        if !bridge_manages_checkpoints {
            debug!(
                module = %self.module.module_name(),
                "Skipping generic automaton-bridge checkpoint tracking because the module persists its own state"
            );
        }

        // Periodic checkpoint saves: prevent data loss on crash by persisting
        // progress every CHECKPOINT_EVENT_INTERVAL events or CHECKPOINT_TIME_INTERVAL.
        const CHECKPOINT_EVENT_INTERVAL: u64 = 100;
        const CHECKPOINT_TIME_INTERVAL: std::time::Duration = std::time::Duration::from_secs(30);

        let checkpoint_manager = bridge_manages_checkpoints.then(|| handles.checkpoint_manager());
        let mut checkpoint_state = if let Some(manager) = checkpoint_manager.as_deref() {
            match Self::load_bridge_checkpoint_state(manager).await {
                Ok(state) => Some(state),
                Err(err) => {
                    // Stop the consumer before returning so its background tasks
                    // do not run as orphans ACKing confirmation messages that
                    // should be redelivered to the next automaton run.
                    consumer.stop().await;
                    drain_controller.clear_runtime_abort();
                    if let Some(handle) = self.consumer_handle.take() {
                        let _ = handle.await;
                    }
                    return Err(err);
                }
            }
        } else {
            None
        };

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
                Event(Option<ProvisionalEvent>),
                FlushTick,
            }

            let action = if drain_controller.is_requested() {
                LoopAction::Event(receiver.try_recv().ok())
            } else {
                tokio::select! {
                    event = receiver.recv() => LoopAction::Event(event),
                    _ = flush_ticker.tick() => LoopAction::FlushTick,
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
                LoopAction::Event(next_event) => {
                    let Some(first) = next_event else {
                        if let Some(error) = consumer_failure.lock().await.take() {
                            return Err(error);
                        }
                        break;
                    };

                    // Non-blocking drain: grab whatever else is already queued
                    let mut provisionals = vec![first];
                    while provisionals.len() < BATCH_SIZE {
                        match receiver.try_recv() {
                            Ok(p) => provisionals.push(p),
                            Err(_) => break,
                        }
                    }

                    // Resolve each provisional to a full Event
                    let resolve_result = Self::resolve_provisionals_to_events(
                        &provisionals,
                        #[cfg(feature = "db")]
                        &db_pool,
                    )
                    .await?;

                    if resolve_result.events.is_empty() {
                        continue;
                    }

                    let batch_count = Self::process_batch_with_dlq_fallback(
                        &mut self.module,
                        &transport,
                        resolve_result.events,
                    )
                    .await?;

                    processed_events += batch_count;
                    events_since_checkpoint += batch_count;
                    if let Some(eid) = resolve_result.last_event_id {
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

    /// NATS `max_ack_pending` for an automaton's confirmation-buffered raw
    /// consumer.
    ///
    /// Every automaton subscribes to the *raw* events stream so its confirmation
    /// buffer can resolve provisional inputs, so on a backlog drain the in-flight
    /// unacked messages are multiplied by the automaton count. At the old
    /// `Default` of 1000 this fan-out (14 automata × 1000 × ~120 KB messages,
    /// held client-side once the confirmation buffer saturates and starts
    /// NAK-redelivering) drove the boot OOM that crash-looped prod. Bounding it
    /// keeps aggregate boot memory flat; the confirmation buffer's own
    /// capacity/byte caps remain the steady-state holding bound.
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
        db_backed_confirmations: bool,
        processing_model: ProcessingModel,
        raw_event_type_filter: Option<&str>,
    ) -> JetStreamEventConsumerConfig {
        JetStreamEventConsumerConfig {
            processing_model,
            raw_event_type_filter: raw_event_type_filter.map(str::to_string),
            batch_size: 128,
            max_ack_pending: Self::automaton_consumer_max_ack_pending(),
            confirmation_timeout: std::time::Duration::from_mins(1),
            consumer_name: if db_backed_confirmations {
                format!("{}-automaton-confirmed-v2", service_name.replace('.', "_"))
            } else {
                format!("{}-automaton", service_name.replace('.', "_"))
            },
            enable_provisional_processing: false,
            // Even with DB-backed confirmation hydration, payload-driven
            // automata need the raw event stream so confirmation watermarks can
            // resolve buffered inputs instead of synthetic kind stand-ins.
            buffer_raw_events: true,
            accept_unbuffered_confirmations: db_backed_confirmations,
            deliver_policy: if db_backed_confirmations {
                async_nats::jetstream::consumer::DeliverPolicy::New
            } else {
                async_nats::jetstream::consumer::DeliverPolicy::All
            },
            ..Default::default()
        }
    }
}
