//! Pull-loop orchestration and batch scheduling for `JetStreamConsumer`.

use std::sync::atomic::Ordering;

use super::confirmation::CONFIRM_RETRY_POLL_INTERVAL;
use super::*;

impl JetStreamConsumer {
    pub async fn run(self) -> EventEngineResult<()> {
        self.run_with_ready_signal(None).await
    }

    /// Run the consumer, optionally signalling readiness after streams are bound.
    ///
    /// `ready_tx` is sent on after the durable consumer has been created and
    /// the pull loop is about to start. Callers can await the corresponding
    /// receiver before emitting `sd_notify(READY)` to systemd.
    #[instrument(skip(self, ready_tx), fields(consumer = %self.topology.consumer_durable))]
    pub async fn run_with_ready_signal(
        self,
        ready_tx: Option<tokio::sync::oneshot::Sender<()>>,
    ) -> EventEngineResult<()> {
        info!("Starting JetStream consumer");

        // Bootstrap streams
        self.bootstrap_streams().await?;

        // Get events stream and create durable consumer through shared kernel.
        let stream_name = self.topology.events_stream.to_string();
        let mut consumer_spec =
            PullConsumerSpec::new(stream_name.clone(), self.topology.consumer_durable.clone());
        consumer_spec.filter_subject = Some(self.topology.events_subject.to_string());
        consumer_spec.deliver_policy = jetstream::consumer::DeliverPolicy::All;
        consumer_spec.ack_wait = self.ack_wait;
        consumer_spec.max_ack_pending = self.max_ack_pending;
        consumer_spec.max_deliver = MAIN_CONSUMER_JETSTREAM_MAX_DELIVER;
        consumer_spec.reject_initial_replay = self.reject_initial_replay;
        let mut consumer = ensure_pull_consumer(&self.js, &consumer_spec)
            .await
            .map_err(|e| SinexError::network("Failed to create consumer").with_source(e))?;
        crate::runtime::stream::reconcile_raw_stream_consumers(
            &self.js,
            &stream_name,
            &self.topology.consumer_durable,
        )
        .await
        .map_err(|e| {
            SinexError::network("Failed to reconcile raw stream consumers").with_source(e)
        })?;
        let mut lag_consumer = consumer.clone();
        let mut confirmation_retry_spec = PullConsumerSpec::new(
            self.topology.confirmation_retry_stream.to_string(),
            self.topology.confirmation_retry_consumer.clone(),
        );
        confirmation_retry_spec.filter_subject =
            Some(self.topology.confirmation_retry_subject.to_string());
        confirmation_retry_spec.deliver_policy = jetstream::consumer::DeliverPolicy::All;
        confirmation_retry_spec.ack_wait = self.ack_wait;
        confirmation_retry_spec.max_ack_pending = self.max_ack_pending;
        confirmation_retry_spec.max_deliver = 10;
        let confirmation_retry_consumer = ensure_pull_consumer(&self.js, &confirmation_retry_spec)
            .await
            .map_err(|e| {
                SinexError::network("Failed to create confirmation retry consumer").with_source(e)
            })?;

        // Emit startup snapshot before READY so operators can distinguish
        // normal resume from cold-start full replay from catch-up runs.
        if let Some(ref observer) = self.observer {
            // Best-effort: if we can't query stream/consumer state, emit
            // the snapshot with zeroed fields rather than block startup.
            let (
                stream_messages,
                stream_bytes,
                stream_first_seq,
                stream_last_seq,
                stream_max_msgs,
                stream_max_bytes,
                stream_max_age_secs,
            ) = match self.js.get_stream(&stream_name).await {
                Ok(mut stream) => match stream.info().await {
                    Ok(info) => {
                        let s = &info.state;
                        let c = &info.config;
                        (
                            s.messages,
                            s.bytes,
                            s.first_sequence,
                            s.last_sequence,
                            c.max_messages as u64,
                            c.max_bytes as u64,
                            c.max_age.as_secs(),
                        )
                    }
                    Err(e) => {
                        warn!("Failed to get stream info for startup snapshot: {e}");
                        (0, 0, 0, 0, 0, 0, 0)
                    }
                },
                Err(e) => {
                    warn!("Failed to get stream for startup snapshot: {e}");
                    (0, 0, 0, 0, 0, 0, 0)
                }
            };
            let consumer_info = consumer.info().await.ok();
            let consumer_existed = consumer_info.as_ref().is_some_and(|ci| ci.num_pending > 0);
            let deliver_policy = format!("{:?}", consumer_spec.deliver_policy);
            let initial_replay_risk = !consumer_existed
                && matches!(
                    consumer_spec.deliver_policy,
                    jetstream::consumer::DeliverPolicy::All
                )
                && stream_messages > 0;

            let _ = observer
                .emit_consumer_startup_snapshot(
                    stream_name.clone(),
                    self.topology.consumer_durable.clone(),
                    consumer_existed,
                    deliver_policy,
                    stream_messages,
                    stream_bytes,
                    stream_first_seq,
                    stream_last_seq,
                    stream_max_msgs,
                    stream_max_bytes,
                    stream_max_age_secs,
                    consumer_info.as_ref().map_or(0, |ci| ci.num_pending),
                    consumer_info.as_ref().map_or(0, |ci| ci.num_ack_pending),
                    0,
                    consumer_spec.max_ack_pending,
                    consumer_spec.max_deliver,
                    initial_replay_risk,
                )
                .await;

            if initial_replay_risk {
                warn!(
                    stream = %stream_name,
                    consumer = %self.topology.consumer_durable,
                    "Dangerous cold-start replay detected: new consumer with non-empty stream"
                );
            }
        }

        // Signal readiness: consumer is bound and the pull loop is about to start.
        // This allows callers to delay sd_notify(READY) until the subscription is live.
        signal_ready(ready_tx, "jetstream-consumer");

        // Stats logging interval
        let mut stats_interval = tokio::time::interval(self.stats_log_interval);
        // Stream capacity monitoring interval
        let mut capacity_check_interval = tokio::time::interval(STREAM_CAPACITY_CHECK_INTERVAL);
        // Consumer lag check interval (30s)
        let mut lag_check_interval = tokio::time::interval(std::time::Duration::from_secs(30));
        let mut confirmation_retry_interval = tokio::time::interval(CONFIRM_RETRY_POLL_INTERVAL);

        // Startup catch-up semaphore: limits I/O pressure while the consumer
        // works through the initial backlog. Once the consumer is caught up
        // (num_pending == 0), the semaphore is no longer used.
        let catch_up_semaphore = (self.startup_catch_up_max_concurrent > 0).then(|| {
            Arc::new(tokio::sync::Semaphore::new(
                self.startup_catch_up_max_concurrent,
            ))
        });
        let mut catching_up = catch_up_semaphore.is_some();
        let mut batch_future: BoxFuture<'_, EventEngineResult<()>> = Box::pin(
            Self::process_batch_with_semaphore(&self, &consumer, &catch_up_semaphore, catching_up),
        );

        loop {
            tokio::select! {
                _ = stats_interval.tick() => {
                    self.stats.log();
                    // Emit processing stats via self-observer
                    if let Some(ref observer) = self.observer {
                        let processed = self.stats.events_processed.load(Ordering::Relaxed);
                        let failed = self.stats.events_failed.load(Ordering::Relaxed);
                        let deferred = self.stats.events_deferred.load(Ordering::Relaxed);
                        let dlq_routed = self.stats.dlq_routed.load(Ordering::Relaxed);
                        if let Err(e) = observer.emit_source_processing_stats(
                            "jetstream-consumer",
                            processed,
                            deferred + dlq_routed, // events_dropped = deferred + routed to DLQ
                            None, // avg_latency_ms - not tracked yet
                            0,    // queue_depth - would need consumer info
                            failed,
                        ).await {
                            warn!("Failed to emit processing stats: {}", e);
                        }

                        // Emit operational health counters not covered by source processing stats.
                        // These are monotonic cumulative totals emitted as gauges (snapshot-at-tick).
                        let operational_gauges: &[(&'static str, u64)] = &[
                            ("event_engine.tombstoned_events_rejected_total", self.stats.tombstoned_events_rejected.load(Ordering::Relaxed)),
                            ("event_engine.confirmation_failures_total", self.stats.confirmation_failures.load(Ordering::Relaxed)),
                            ("event_engine.confirmation_retries_enqueued_total", self.stats.confirmation_retries_enqueued.load(Ordering::Relaxed)),
                            ("event_engine.confirmation_retry_failures_total", self.stats.confirmation_retry_failures.load(Ordering::Relaxed)),
                            ("event_engine.confirmation_durability_gaps_total", self.stats.confirmation_durability_gaps.load(Ordering::Relaxed)),
                            ("event_engine.dlq_publish_failures_total", self.stats.dlq_publish_failures.load(Ordering::Relaxed)),
                            ("event_engine.nack_failures_total", self.stats.nack_failures.load(Ordering::Relaxed)),
                            ("event_engine.nats_errors_total", self.stats.nats_errors.load(Ordering::Relaxed)),
                            ("event_engine.telemetry_publish_failures_total", self.stats.telemetry_publish_failures.load(Ordering::Relaxed)),
                        ];
                        for (metric, value) in operational_gauges {
                            self.emit_observer_gauge(metric, *value as f64, None).await;
                        }
                    }
                }
                _ = capacity_check_interval.tick() => {
                    self.check_stream_capacity(&stream_name).await;
                    // DLQ growth is a durable signal of persistent failures; monitor it too.
                    self.check_stream_capacity(self.topology.dlq_stream.as_ref()).await;
                }
                _ = lag_check_interval.tick() => {
                    if self.observer.is_some() {
                        match lag_consumer.info().await {
                            Ok(info) => {
                                let mut labels = HashMap::new();
                                labels.insert("consumer".to_string(), self.topology.consumer_durable.clone());
                                self.emit_observer_gauge(
                                    "event_engine.consumer.lag.pending",
                                    info.num_pending as f64,
                                    Some(labels.clone()),
                                ).await;
                                self.emit_observer_gauge(
                                    "event_engine.consumer.lag.ack_pending",
                                    info.num_ack_pending as f64,
                                    Some(labels),
                                ).await;
                                // Detect catch-up completion when pending drops to zero
                                if catching_up && info.num_pending == 0 {
                                    catching_up = false;
                                    info!("Startup catch-up complete; releasing semaphore throttle");
                                }
                            }
                            Err(e) => {
                                debug!("Consumer lag check failed: {e}");
                            }
                        }
                    }
                }
                _ = confirmation_retry_interval.tick() => {
                    if let Err(e) = self.process_confirmation_retry_batch(&confirmation_retry_consumer).await {
                        error!(
                            target: "sinex_metrics",
                            metric = "event_engine.confirmation_retry_failures_total",
                            error = %e,
                            "Confirmation retry processing error"
                        );
                    }
                }
                batch_result = &mut batch_future => {
                    if let Err(e) = batch_result {
                        if Self::is_fatal_batch_processing_error(&e) {
                            error!(
                                target: "sinex_metrics",
                                metric = "event_engine.fatal_batch_errors_total",
                                error = %e,
                                "Fatal batch processing error"
                            );
                            return Err(e);
                        }
                        error!(
                            target: "sinex_metrics",
                            metric = "event_engine.batch_errors_total",
                            error = %e,
                            "Batch processing error"
                        );
                    }
                    batch_future = Box::pin(Self::process_batch_with_semaphore(
                        &self,
                        &consumer,
                        &catch_up_semaphore,
                        catching_up,
                    ));
                }
            }
        }
    }

    /// Process a batch, acquiring a catch-up semaphore permit during the
    /// startup catch-up phase to limit I/O pressure.
    ///
    /// Catch-up detection (setting `catching_up = false`) is handled in the
    /// lag-check interval of the main loop, where we already have mutable
    /// access to `lag_consumer`.
    pub(super) async fn process_batch_with_semaphore(
        this: &Self,
        consumer: &jetstream::consumer::Consumer<jetstream::consumer::pull::Config>,
        catch_up_semaphore: &Option<Arc<tokio::sync::Semaphore>>,
        catching_up: bool,
    ) -> EventEngineResult<()> {
        if let (true, Some(sem)) = (catching_up, catch_up_semaphore.as_ref()) {
            let _permit = sem.acquire().await;
            this.process_batch(consumer).await?;
        } else {
            this.process_batch(consumer).await?;
        }
        Ok(())
    }

    #[tracing::instrument(skip(self, consumer), fields(consumer_name = %self.topology.consumer_durable))]
    pub(super) async fn process_batch(
        &self,
        consumer: &jetstream::consumer::Consumer<jetstream::consumer::pull::Config>,
    ) -> EventEngineResult<()> {
        let batch_start = std::time::Instant::now();
        let mut batch = Vec::new();
        let messages = pull_batch_bounded(
            consumer,
            self.batch_fetch_max_messages,
            self.batch_fetch_max_bytes,
            self.batch_fetch_timeout,
        )
        .await
        .map_err(|e| SinexError::network("Failed to fetch messages").with_source(e))?;
        for msg in messages {
            #[cfg(any(test, feature = "testing"))]
            if let Some(counter) = &self.delivery_observer {
                counter.fetch_add(1, Ordering::Relaxed);
            }

            let prepared_events = self.prepare_events(msg).await?;
            batch.extend(prepared_events);
        }

        if batch.is_empty() {
            return Ok(());
        }

        let batch_size = batch.len() as u32;
        let had_derived = batch.iter().any(|p| {
            matches!(
                p.event.provenance,
                sinex_primitives::events::Provenance::Derived { .. }
            )
        });

        // Snapshot cumulative counters before persist so we can compute per-batch deltas
        let deferred_before = self.stats.events_deferred.load(Ordering::Relaxed);
        let failed_before = self.stats.events_failed.load(Ordering::Relaxed);

        let result = self.persist_and_confirm_batch(&mut batch).await;

        // Emit batch stats on success
        if result.is_ok()
            && let Some(ref observer) = self.observer
        {
            let fetch_to_ack_ms = batch_start.elapsed().as_millis() as u64;
            let events_deferred =
                (self.stats.events_deferred.load(Ordering::Relaxed) - deferred_before) as u32;
            let events_failed =
                (self.stats.events_failed.load(Ordering::Relaxed) - failed_before) as u32;
            let insert_path = if had_derived {
                "query_builder"
            } else if batch_size as usize >= COPY_BATCH_THRESHOLD {
                "copy"
            } else {
                "query_builder"
            };
            let val_stats = self.validator.read().await.stats();
            let suspicious_future_ts_orig =
                self.stats.suspicious_future_ts_orig.load(Ordering::Relaxed);
            if let Err(error) = observer
                .emit_event_engine_batch_stats(
                    batch_size,
                    fetch_to_ack_ms,
                    events_deferred,
                    events_failed,
                    had_derived,
                    insert_path,
                    val_stats.valid,
                    val_stats.skipped,
                    val_stats.no_schema,
                    val_stats.schema_not_found,
                    val_stats.invalid,
                    val_stats.coverage_pct(),
                    suspicious_future_ts_orig,
                    self.stats
                        .telemetry_publish_failures
                        .load(Ordering::Relaxed),
                    self.stats
                        .confirmation_durability_gaps
                        .load(Ordering::Relaxed),
                )
                .await
            {
                Self::log_observer_error(&self.stats, "event_engine.batch", &error);
            }
        }

        result
    }
}
