//! Automaton runtime loop for `NodeRunner<T>`.
//!
//! Runs the automaton continuous mode entry point, drives leader-standby
//! coordination over the NATS coordination KV, and operates the
//! confirmation-event bridge that resolves provisional events to fully
//! materialized inputs and feeds them into the node implementation.

use super::*;

impl<T: Node + 'static> NodeRunner<T> {
    /// Run automaton in continuous mode
    #[cfg(feature = "messaging")]
    pub(super) async fn run_automaton_continuous_mode(&mut self) -> NodeResult<()> {
        info!("Starting automaton continuous mode");
        let drain_controller = self
            .runtime_state()
            .ok_or_else(|| SinexError::lifecycle("Runtime state missing".to_string()))?
            .handles()
            .runtime_drain();

        // Get current checkpoint to resume from previous state if available
        let current_checkpoint = self.node.current_checkpoint().await?;
        let capabilities = self.node.capabilities();

        if capabilities.supports_continuous {
            info!("Starting continuous event processing for automaton");

            // A standby automaton is still a healthy, ready service. Satisfy
            // the systemd notify contract before waiting on lease handoff or
            // expiry so host activation does not fail on a legitimate standby.
            systemd_notify::notify_ready("sinex-node");

            if self.processing_model == ProcessingModel::LeaderStandby {
                let leader_acquired = self.acquire_leader_standby().await?;
                if !leader_acquired {
                    info!("Drain requested while waiting in leader standby; exiting cleanly");
                    return Ok(());
                }
            }

            if capabilities.manages_own_continuous_loop {
                let _continuous_report = self
                    .node
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
                    .node
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
    pub(super) async fn acquire_leader_standby(&mut self) -> NodeResult<bool> {
        let rs = self
            .runtime_state()
            .ok_or_else(|| SinexError::lifecycle("Runtime state missing".to_string()))?;
        let drain_controller = rs.handles().runtime_drain();

        #[cfg(feature = "messaging")]
        {
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
            let _ = rs; // suppress unused variable
            warn!("LeaderStandby mode requires messaging feature. Skipping leadership check.");
        }

        Ok(true)
    }

    #[cfg(feature = "messaging")]
    pub(super) async fn run_automaton_event_bridge(&mut self, from: Checkpoint) -> NodeResult<()> {
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
            || self.node.node_name().to_string(),
            |info| info.service_name().to_string(),
        );

        let (sender, mut receiver) =
            mpsc::channel::<ProvisionalEvent>(CONFIRMED_EVENT_CHANNEL_CAPACITY);
        let handler = Arc::new(RunnerConfirmedEventHandler::new(sender));

        let env = sinex_primitives::environment::environment().clone();

        let nats_client = match &transport {
            EventTransport::Nats(publisher) => publisher.nats_client().clone(),
        };

        let consumer_config = JetStreamEventConsumerConfig {
            processing_model: self.processing_model,
            batch_size: 128,
            confirmation_timeout: std::time::Duration::from_mins(1),
            consumer_name: if db_backed_confirmations {
                format!("{}-automaton-confirmed-v2", service_name.replace('.', "_"))
            } else {
                format!("{}-automaton", service_name.replace('.', "_"))
            },
            enable_provisional_processing: false,
            buffer_raw_events: !db_backed_confirmations,
            accept_unbuffered_confirmations: db_backed_confirmations,
            deliver_policy: if db_backed_confirmations {
                async_nats::jetstream::consumer::DeliverPolicy::New
            } else {
                async_nats::jetstream::consumer::DeliverPolicy::All
            },
            ..Default::default()
        };

        let consumer = Arc::new(JetStreamEventConsumer::new(
            nats_client,
            env,
            consumer_config,
            handler,
            None,
        ));

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

        if !matches!(from, Checkpoint::None) && self.node.capabilities().supports_historical {
            info!("Processing historical backlog before entering continuous mode");
            let _ = self
                .node
                .scan(
                    from,
                    TimeHorizon::Historical {
                        end_time: sinex_primitives::temporal::Timestamp::now(),
                    },
                    ScanArgs::default(),
                )
                .await?;
        }

        if drain_controller.is_requested() {
            let _ = drain_controller.abort_runtime_work();
            info!("Drain requested before automaton bridge entered live processing");
        }

        let bridge_manages_checkpoints = !self.node.capabilities().manages_own_checkpoints;
        if !bridge_manages_checkpoints {
            debug!(
                node = %self.node.node_name(),
                "Skipping generic automaton-bridge checkpoint tracking because the node persists its own state"
            );
        }

        // Periodic checkpoint saves: prevent data loss on crash by persisting
        // progress every CHECKPOINT_EVENT_INTERVAL events or CHECKPOINT_TIME_INTERVAL.
        const CHECKPOINT_EVENT_INTERVAL: u64 = 100;
        const CHECKPOINT_TIME_INTERVAL: std::time::Duration = std::time::Duration::from_secs(30);

        let checkpoint_manager = bridge_manages_checkpoints.then(|| handles.checkpoint_manager());
        let mut checkpoint_state = if let Some(manager) = checkpoint_manager.as_deref() {
            Some(Self::load_bridge_checkpoint_state(manager).await?)
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

        loop {
            // Normal mode blocks for more work. Once drain is requested, the
            // runner-owned consumer is aborted and the bridge switches to
            // draining whatever is already buffered before exiting cleanly.
            let next_event = if drain_controller.is_requested() {
                match receiver.try_recv() {
                    Ok(event) => Some(event),
                    Err(tokio::sync::mpsc::error::TryRecvError::Empty)
                    | Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => None,
                }
            } else {
                receiver.recv().await
            };

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
                &mut self.node,
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

}
