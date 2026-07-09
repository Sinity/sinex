//! Persistence, source-material settlement, and batch failure handling for `JetStreamConsumer`.

use std::sync::atomic::Ordering;

use super::confirmation::BATCH_ATOMICITY_SCOPE;
use super::*;
use sinex_primitives::events::Event;

impl JetStreamConsumer {
    #[tracing::instrument(skip(self, batch), fields(batch_size = batch.len()))]
    pub(super) async fn persist_and_confirm_batch(
        &self,
        batch: &mut Vec<PreparedEvent>,
    ) -> EventEngineResult<()> {
        // Pre-filter: defer events whose source material isn't registered yet.
        // This prevents FK violations without relying on database error handling.
        // We partition by index (not reference) so that ready events can be
        // mutated in place by the post-readiness `ts_orig` resolution below.
        let ready_indices: Vec<usize> = if let Some(ref ready_set) = self.ready_set {
            let mut ready = Vec::with_capacity(batch.len());
            let mut not_ready = Vec::new();

            for (idx, prepared) in batch.iter().enumerate() {
                let is_ready = match &prepared.event.provenance {
                    // Material provenance: first consult the in-memory set, then fall back
                    // to the registry so externally-registered materials are not deferred forever.
                    Provenance::Material { id, .. } => {
                        ready_set.ensure_ready(&self.pool, *id.as_uuid()).await?
                    }
                    // Derived provenance has no material FK — always ready.
                    Provenance::Derived { .. } => true,
                };

                if is_ready {
                    ready.push(idx);
                } else {
                    not_ready.push(idx);
                }
            }

            if !not_ready.is_empty() {
                debug!(
                    deferred = not_ready.len(),
                    ready = ready.len(),
                    "Deferring events whose source material is not yet registered"
                );
                let mut settlement_errors = Vec::new();
                let mut deferred_count = 0_u64;
                for &idx in &not_ready {
                    let prepared = &batch[idx];
                    let material_id = match &prepared.event.provenance {
                        Provenance::Material { id, .. } => Some(*id.as_uuid()),
                        Provenance::Derived { .. } => None,
                    };
                    match self
                        .settle_unready_source_material_event(prepared, material_id, None)
                        .await
                    {
                        Ok(SourceMaterialSettlement::Deferred) => deferred_count += 1,
                        Ok(SourceMaterialSettlement::RoutedToDlq) => {}
                        Err(err) => settlement_errors.push((prepared.parsed_id, err)),
                    }
                }
                Self::collapse_settlement_errors(
                    "source-material readiness settlement",
                    settlement_errors,
                )?;
                self.stats
                    .events_deferred
                    .fetch_add(deferred_count, Ordering::Relaxed);
            }

            if ready.is_empty() {
                return Ok(());
            }
            ready
        } else {
            (0..batch.len()).collect()
        };

        // #1570 Prong B: resolve deferred `ts_orig` for ready material events.
        // This runs *after* the readiness gate above, so every material here has
        // a registry row visible in the DB — the source-material timing tier can
        // always resolve a stable `(ts_orig, ts_quality)`.
        self.resolve_ready_ts_orig(batch, &ready_indices).await?;

        let ready: Vec<&PreparedEvent> = ready_indices.iter().map(|&idx| &batch[idx]).collect();
        self.persist_and_confirm_prepared_batch(&ready).await
    }

    /// Persist and settle a prepared batch.
    ///
    /// Atomicity is intentionally scoped to each successful persistence attempt,
    /// not to the original `JetStream` pull batch. If a non-retryable row poisons a
    /// mixed batch, event_engine bisects the batch to isolate the poison row. Any sibling
    /// sub-batch that already persisted keeps its commit and raw-message ACKs, while
    /// the isolated row is retried or routed to the DLQ on its own. Replay and
    /// lineage therefore reason at event granularity, not at raw pull-batch
    /// granularity.
    #[tracing::instrument(skip(self, batch), fields(batch_size = batch.len()))]
    pub(super) async fn persist_and_confirm_prepared_batch(
        &self,
        batch: &[&PreparedEvent],
    ) -> EventEngineResult<()> {
        let mut pending_batches = vec![batch.to_vec()];

        while let Some(batch) = pending_batches.pop() {
            let persist_result = self.persist_batch_optimized(&batch).await;
            match persist_result {
                Ok(persisted) => {
                    let inserted_set = persisted
                        .inserted_ids
                        .as_ref()
                        .map(|ids| ids.iter().copied().collect::<HashSet<_>>());
                    let mut confirmation_ids: HashSet<Uuid> =
                        persisted.duplicate_event_ids.iter().copied().collect();
                    if let Some(ids) = &persisted.inserted_ids {
                        confirmation_ids.extend(ids.iter().copied());
                    }
                    let tombstoned_ids: HashSet<Uuid> =
                        persisted.tombstoned_event_ids.iter().copied().collect();
                    let confirmation_batch: Vec<_> = batch
                        .iter()
                        .copied()
                        .filter(|prepared| confirmation_ids.contains(&prepared.parsed_id))
                        .collect();
                    let tombstoned_batch: Vec<_> = batch
                        .iter()
                        .copied()
                        .filter(|prepared| tombstoned_ids.contains(&prepared.parsed_id))
                        .collect();
                    #[cfg(any(test, feature = "testing"))]
                    if let Some(delay) = self.processing_delay {
                        tokio::time::sleep(delay).await;
                    }
                    // Publish each FINAL persisted+redacted event onto the
                    // confirmed-events stream. This is the authoritative delivery
                    // channel automata and the SSE bus consume directly (no
                    // provisional buffer, no DB refetch, no visibility race).
                    // Tombstoned events are intentionally excluded
                    // (confirmation_batch only), preserving tombstone-suppression.
                    // The raw message is acked below only if this publish also
                    // succeeds; otherwise JetStream must redeliver and re-publish
                    // rather than silently dropping the event downstream.
                    //
                    // sinex-z8p: publish `persisted.redacted_events`, NOT
                    // `prepared.event`. The latter is the pre-redaction image
                    // this consumer parsed off the raw message; the former is
                    // exactly what `persist_batch_optimized` redacted and
                    // attempted to persist. `redacted_events` is built 1:1 from
                    // the same `batch` this loop iterates, keyed by
                    // `prepared.parsed_id` — a miss here means that invariant
                    // broke, not a benign gap, so it panics loudly rather than
                    // silently falling back to the unredacted image.
                    let redacted_events = &persisted.redacted_events;
                    let confirmed_event_futs: Vec<_> = confirmation_batch
                        .iter()
                        .map(|prepared| {
                            let sem = Arc::clone(&self.confirmation_semaphore);
                            let confirmed_event = redacted_events
                                .get(&prepared.parsed_id)
                                .expect("redacted_events is built 1:1 from this same batch");
                            async move {
                                let _permit = match sem.acquire().await {
                                    Ok(permit) => permit,
                                    Err(error) => {
                                        return (
                                            prepared.parsed_id,
                                            Err(SinexError::processing(
                                                "confirmation semaphore closed",
                                            )
                                            .with_std_error(&error)),
                                        );
                                    }
                                };
                                let result = self
                                    .publish_confirmed_event_with_retry(confirmed_event)
                                    .await;
                                (prepared.parsed_id, result)
                            }
                        })
                        .collect();
                    let confirmed_publish_failures: HashMap<Uuid, SinexError> =
                        join_all(confirmed_event_futs)
                            .await
                            .into_iter()
                            .filter_map(|(id, result)| result.err().map(|err| (id, err)))
                            .collect();
                    // sinex-r6d.12: settle through each prepared event's
                    // shared envelope coordinator, never ack `prepared.message`
                    // directly — siblings from the same raw message may be
                    // elsewhere in this batch (or already settled, or still
                    // pending) and must not be silently orphaned.
                    let mut settled_count = 0u64;
                    for prepared in &tombstoned_batch {
                        self.settlement_registry.resolve(
                            prepared.parsed_id,
                            EmissionReceiptState::Suppressed {
                                reason: SuppressionReason::Tombstoned,
                                existing_event_id: None,
                            },
                        );
                        prepared.settlement.settle_child(ChildOutcome::Safe).await?;
                        settled_count += 1;
                    }
                    let mut confirmation_durability_gaps = Vec::new();
                    for prepared in &confirmation_batch {
                        if let Some(err) = confirmed_publish_failures.get(&prepared.parsed_id) {
                            // Durability gap: deliberately not settled — see
                            // the matching note in settle_admission_skips.
                            confirmation_durability_gaps.push((
                                prepared.parsed_id,
                                Self::confirmed_event_durability_gap_error(
                                    prepared.parsed_id,
                                    err,
                                ),
                            ));
                            continue;
                        }
                        let inserted = inserted_set
                            .as_ref()
                            .is_some_and(|set| set.contains(&prepared.parsed_id));
                        if let Some(set) = &inserted_set
                            && !set.contains(&prepared.parsed_id)
                        {
                            debug!(
                                event_id = %prepared.parsed_id,
                                "Re-published confirmed event for already persisted event"
                            );
                        }
                        // sinex-r6d.11: the row is persisted (freshly inserted or
                        // already present — `inserted` distinguishes which) and its
                        // confirmed-stream publish already succeeded (the
                        // durability-gap branch above continues before reaching
                        // here), so this is PersistedConfirmed regardless of which
                        // case it is. `confirmed_sequence` is unavailable:
                        // `publish_confirmed_event` discards the JetStream
                        // `PublishAck` it awaits (confirmation.rs) rather than
                        // surfacing its sequence number.
                        self.settlement_registry.resolve(
                            prepared.parsed_id,
                            EmissionReceiptState::PersistedConfirmed {
                                lane: self.admission.storage_lane(),
                                inserted,
                                confirmed_sequence: None,
                            },
                        );
                        prepared.settlement.settle_child(ChildOutcome::Safe).await?;
                        settled_count += 1;
                    }

                    let acked_count = settled_count;
                    if acked_count > 0 {
                        self.stats
                            .events_processed
                            .fetch_add(acked_count, Ordering::Relaxed);
                        if let Some(ref handle) = self.heartbeat_handle {
                            handle.increment_events_processed(acked_count);
                        }
                    }

                    if !confirmation_durability_gaps.is_empty() {
                        let gap_count = confirmation_durability_gaps.len() as u64;
                        self.stats
                            .confirmation_durability_gaps
                            .fetch_add(gap_count, Ordering::Relaxed);
                        if let Some(ref handle) = self.heartbeat_handle {
                            handle.record_error("confirmation durability gap");
                        }
                        return Err(Self::confirmation_durability_gap_error(
                            confirmation_durability_gaps,
                            acked_count as usize,
                        ));
                    }
                    // Steady-state confirmed-only batches carry no operator
                    // signal and fire ~6/sec; demote them to debug! so they do
                    // not dominate journal volume (#1726 measurement). Keep
                    // info! only when tombstones are present — that is the
                    // operationally interesting case worth a default-level line.
                    if tombstoned_batch.is_empty() {
                        debug!(
                            confirmed = confirmation_batch.len(),
                            "Processed admission batch"
                        );
                    } else {
                        info!(
                            confirmed = confirmation_batch.len(),
                            tombstoned = tombstoned_batch.len(),
                            "Processed admission batch (tombstones present)"
                        );
                    }
                }
                Err(failure) => {
                    self.settle_admission_skips(
                        &batch,
                        &failure.duplicate_event_ids,
                        &failure.tombstoned_event_ids,
                    )
                    .await?;
                    let e = failure.error;
                    let attempted_ids: HashSet<Uuid> =
                        failure.attempted_event_ids.iter().copied().collect();
                    let attempted_batch: Vec<_> = batch
                        .iter()
                        .copied()
                        .filter(|prepared| attempted_ids.contains(&prepared.parsed_id))
                        .collect();
                    // Check if this is a transient FK violation (source material not yet registered).
                    // Safety net: the ready set should prevent most FK violations, but races are
                    // possible (e.g. material registered between ready-set check and DB insert).
                    let is_fk_error =
                        is_source_material_fk_violation_for_prepared_batch(&e, &attempted_batch);
                    if is_fk_error {
                        let mut settlement_errors = Vec::new();
                        let mut deferred_count = 0_u64;
                        debug!(
                            batch_size = attempted_batch.len(),
                            "FK violation on batch - source material likely still registering"
                        );
                        for prepared in &attempted_batch {
                            let material_id = match &prepared.event.provenance {
                                Provenance::Material { id, .. } => Some(*id.as_uuid()),
                                Provenance::Derived { .. } => None,
                            };
                            match self
                                .settle_unready_source_material_event(
                                    prepared,
                                    material_id,
                                    Some(&e),
                                )
                                .await
                            {
                                Ok(SourceMaterialSettlement::Deferred) => deferred_count += 1,
                                Ok(SourceMaterialSettlement::RoutedToDlq) => {}
                                Err(err) => settlement_errors.push((prepared.parsed_id, err)),
                            }
                        }
                        Self::collapse_settlement_errors(
                            "FK violation retry settlement",
                            settlement_errors,
                        )?;
                        self.stats
                            .events_deferred
                            .fetch_add(deferred_count, Ordering::Relaxed);
                        // Don't count as failed - this is a transient condition
                        continue;
                    }

                    if is_isolatable_batch_persistence_failure(&e) {
                        if attempted_batch.len() > 1 {
                            let split_at = attempted_batch.len() / 2;
                            warn!(
                                batch_size = attempted_batch.len(),
                                split_at,
                                batch_atomicity = BATCH_ATOMICITY_SCOPE,
                                sqlstate = ?e.context_map().get("sqlstate"),
                                constraint = ?e.context_map().get("constraint"),
                                "Splitting batch to isolate non-retryable persistence failure; already-persisted sibling sub-batches remain committed"
                            );
                            pending_batches.push(attempted_batch[split_at..].to_vec());
                            pending_batches.push(attempted_batch[..split_at].to_vec());
                            continue;
                        }

                        let prepared = attempted_batch[0];
                        warn!(
                            event_id = %prepared.parsed_id,
                            sqlstate = ?e.context_map().get("sqlstate"),
                            constraint = ?e.context_map().get("constraint"),
                            "Routing isolated non-retryable persistence failure to DLQ"
                        );
                        // sinex-r6d.12: this is the bisection-isolated poison
                        // case. This prepared event may still have siblings
                        // from the same raw message elsewhere (already
                        // settled, or in a sibling sub-batch) — settle
                        // through the shared coordinator, never ack the
                        // message directly.
                        match self.route_to_dlq(&prepared.message, format!("Persistence error: {e}")).await {
                            Ok(()) => {
                                self.stats.dlq_routed.fetch_add(1, Ordering::Relaxed);
                                // sinex-r6d.11: the DLQ write itself (route_to_dlq's
                                // Ok(())) is the durable debt record — debt_id has no
                                // dedicated DB row id available here (DLQ routing is a
                                // NATS publish, not a DB insert returning a primary
                                // key), so the event's own id is the identifier an
                                // operator would actually look this DLQ entry up by
                                // (it's also what `route_to_dlq` stamps into the
                                // `Event-Id` header).
                                self.settlement_registry.resolve(
                                    prepared.parsed_id,
                                    EmissionReceiptState::DurableDebt {
                                        debt_id: prepared.parsed_id,
                                        reason: format!(
                                            "isolated (bisection) persistence failure DLQ'd: {e}"
                                        ),
                                    },
                                );
                                prepared.settlement.settle_child(ChildOutcome::Safe).await?;
                            }
                            Err(err) => {
                                self.stats
                                    .dlq_publish_failures
                                    .fetch_add(1, Ordering::Relaxed);
                                // sinex-r6d.11: deliberately NOT resolved here --
                                // ChildOutcome::Retry means this message is NAK'd
                                // for redelivery, so `prepared.parsed_id` will pass
                                // through settlement again. Resolving now with a
                                // non-progress state would consume the one-shot
                                // registry entry before the eventual terminal
                                // outcome is known, permanently stranding any
                                // waiter (same bug class the durable-debt DLQ test
                                // caught: a premature `Deferred` resolve here
                                // starves the later `DurableDebt` resolution).
                                prepared
                                    .settlement
                                    .settle_child(ChildOutcome::Retry(None))
                                    .await?;
                                return Err(err.with_context(
                                    "settlement_operation",
                                    "route_isolated_persistence_failure_to_dlq",
                                ));
                            }
                        }
                        self.stats.events_failed.fetch_add(1, Ordering::Relaxed);
                        if let Some(ref handle) = self.heartbeat_handle {
                            handle.record_error("isolated persistence failure");
                        }
                        continue;
                    }

                    error!(
                        target: "sinex_metrics",
                        metric = "event_engine.batch_persistence_failures_total",
                        error = %e,
                        "Failed to persist batch"
                    );
                    let mut settlement_errors = Vec::new();
                    for prepared in &attempted_batch {
                        // sinex-r6d.12: every settlement here goes through the
                        // shared per-message coordinator (settle_child), not
                        // prepared.message directly — this loop can contain
                        // several siblings of the same raw message with
                        // different individual verdicts (e.g. one terminal,
                        // one retryable); the coordinator is what keeps that
                        // safe instead of double/partial-acking the message.
                        match self.should_route_terminal_persistence_failure(&prepared.message, &e)
                        {
                            Ok(true) => {
                                match self
                                    .route_to_dlq(&prepared.message, format!("Persistence error: {e}"))
                                    .await
                                {
                                    Ok(()) => {
                                        self.stats.dlq_routed.fetch_add(1, Ordering::Relaxed);
                                        // sinex-r6d.11: same debt_id reasoning as the
                                        // bisection-isolated-poison DLQ site above — no
                                        // dedicated DB debt-record id exists, so the
                                        // event's own id is what identifies this DLQ
                                        // entry.
                                        self.settlement_registry.resolve(
                                            prepared.parsed_id,
                                            EmissionReceiptState::DurableDebt {
                                                debt_id: prepared.parsed_id,
                                                reason: format!("Persistence error: {e}"),
                                            },
                                        );
                                        if let Err(err) =
                                            prepared.settlement.settle_child(ChildOutcome::Safe).await
                                        {
                                            settlement_errors.push((prepared.parsed_id, err));
                                        }
                                    }
                                    Err(err) => {
                                        warn!(
                                            event_id = %prepared.parsed_id,
                                            error = %err,
                                            "Failed to route persistence error to DLQ"
                                        );
                                        self.stats
                                            .dlq_publish_failures
                                            .fetch_add(1, Ordering::Relaxed);
                                        // sinex-r6d.11: deliberately not resolved --
                                        // see the matching note at the isolated
                                        // (bisection) DLQ-publish-failure site above.
                                        // Retry means redelivery, so this event_id
                                        // will reach a settle_child call again.
                                        if let Err(settle_err) = prepared
                                            .settlement
                                            .settle_child(ChildOutcome::Retry(None))
                                            .await
                                        {
                                            settlement_errors.push((prepared.parsed_id, settle_err));
                                        } else {
                                            settlement_errors.push((
                                                prepared.parsed_id,
                                                Self::message_settlement_failure(
                                                    "failed to route persistence error to DLQ",
                                                    prepared.parsed_id,
                                                    &err,
                                                ),
                                            ));
                                        }
                                    }
                                }
                            }
                            Ok(false) => {
                                // sinex-r6d.11: retryable/transient persistence
                                // failure (or delivery-attempt count below the DLQ
                                // threshold) — non-progress, recoverable via the
                                // NAK-triggered redelivery below. Deliberately not
                                // resolved: redelivery means this event_id reaches
                                // settle_child again, and only that eventual
                                // terminal outcome may consume the registry entry.
                                if let Err(err) = prepared
                                    .settlement
                                    .settle_child(ChildOutcome::Retry(None))
                                    .await
                                {
                                    warn!(
                                        event_id = %prepared.parsed_id,
                                        error = %err,
                                        "Failed to NAK after persistence failure"
                                    );
                                    self.stats.nack_failures.fetch_add(1, Ordering::Relaxed);
                                    settlement_errors.push((
                                        prepared.parsed_id,
                                        Self::message_settlement_failure(
                                            "failed to NAK after persistence failure",
                                            prepared.parsed_id,
                                            &err,
                                        ),
                                    ));
                                }
                            }
                            Err(err) => {
                                warn!(
                                    event_id = %prepared.parsed_id,
                                    error = %err,
                                    "Failed to inspect persistence retry state; NAKing for retry"
                                );
                                // sinex-r6d.11: inspecting the retry state itself
                                // failed (e.g. JetStream delivery metadata
                                // unreadable) — a genuine settlement-path error,
                                // not a routine deferred-retry. Still NAK'd for
                                // redelivery below, so deliberately not resolved
                                // here for the same reason as the other
                                // Retry-paired sites in this function.
                                settlement_errors.push((
                                    prepared.parsed_id,
                                    err.with_context(
                                        "settlement_operation",
                                        "inspect_persistence_retry_state",
                                    ),
                                ));
                                if let Err(nak_err) = prepared
                                    .settlement
                                    .settle_child(ChildOutcome::Retry(None))
                                    .await
                                {
                                    warn!(
                                        event_id = %prepared.parsed_id,
                                        error = %nak_err,
                                        "Failed to NAK after persistence retry-state inspection failure"
                                    );
                                    self.stats.nack_failures.fetch_add(1, Ordering::Relaxed);
                                    settlement_errors.push((
                                        prepared.parsed_id,
                                        Self::message_settlement_failure(
                                            "failed to NAK after persistence retry-state inspection failure",
                                            prepared.parsed_id,
                                            &nak_err,
                                        ),
                                    ));
                                }
                            }
                        }
                    }
                    let failed_count = attempted_batch.len() as u64;
                    self.stats
                        .events_failed
                        .fetch_add(failed_count, Ordering::Relaxed);
                    if let Some(ref handle) = self.heartbeat_handle {
                        handle.record_error("batch persistence failure");
                    }
                    Self::collapse_settlement_errors(
                        "persistence failure settlement",
                        settlement_errors,
                    )?;
                }
            }
        }

        Ok(())
    }

    /// Persist batch through `EventRepository::insert_stream_batch()`.
    ///
    /// The repository owns all routing decisions (`QueryBuilder` for small batches,
    /// COPY for large material-only batches, REPEATABLE READ for derived batches).
    /// The recent-ID cache acts as a prefilter only.
    #[tracing::instrument(skip(self, batch), fields(batch_size = batch.len()))]
    pub(super) async fn persist_batch_optimized(
        &self,
        batch: &[&PreparedEvent],
    ) -> Result<PersistBatchResult, PersistBatchFailure> {
        if batch.is_empty() {
            return Ok(PersistBatchResult {
                inserted_ids: None,
                duplicate_event_ids: Vec::new(),
                tombstoned_event_ids: Vec::new(),
                redacted_events: HashMap::new(),
            });
        }

        let admitted_batch: Vec<AdmittedEvent> = batch
            .iter()
            .map(|prepared| AdmittedEvent {
                event: prepared.event.clone(),
                event_id: prepared.parsed_id,
                metadata: None,
            })
            .collect();

        // ── Privacy policy chokepoint (#1042 Slice 4) ──────────────────────
        // Apply DB-backed user-defined privacy rules to every event payload
        // before persistence. This covers BOTH source (material-provenance) and
        // derived (parent-provenance) events — they share this code path.
        // Refresh is best-effort; stale cache is used on DB error (fail-open).
        self.policy_engine.ensure_fresh().await;
        let admitted_batch = self.policy_engine.redact_batch(admitted_batch).await;
        // ── End chokepoint ──────────────────────────────────────────────────

        // sinex-z8p: capture the redacted image per event_id BEFORE the plan
        // filters duplicates/tombstones out of `plan.events` — confirmation
        // needs this for every attempted event_id, not just the ones that end
        // up newly inserted.
        let redacted_events: HashMap<Uuid, Event<JsonValue>> = admitted_batch
            .iter()
            .map(|admitted| (admitted.event_id, admitted.event.clone()))
            .collect();

        let admitted_refs: Vec<&AdmittedEvent> = admitted_batch.iter().collect();

        let plan = self
            .admission
            .plan_persistence_batch_refs(&admitted_refs)
            .await
            .map_err(|error| PersistBatchFailure {
                error,
                attempted_event_ids: admitted_batch.iter().map(|event| event.event_id).collect(),
                duplicate_event_ids: Vec::new(),
                tombstoned_event_ids: Vec::new(),
            })?;
        let attempted_event_ids = plan.attempted_event_ids();
        let duplicate_event_ids = plan.cached_duplicate_event_ids.clone();
        let tombstoned_event_ids = plan.tombstoned_event_ids.clone();
        let result =
            self.admission
                .persist_plan(&plan)
                .await
                .map_err(|error| PersistBatchFailure {
                    error,
                    attempted_event_ids: attempted_event_ids.clone(),
                    duplicate_event_ids: duplicate_event_ids.clone(),
                    tombstoned_event_ids: tombstoned_event_ids.clone(),
                })?;
        if result.tombstoned_events_rejected > 0 {
            self.stats
                .tombstoned_events_rejected
                .fetch_add(result.tombstoned_events_rejected as u64, Ordering::Relaxed);
        }
        Ok(PersistBatchResult {
            inserted_ids: result.inserted_ids,
            duplicate_event_ids: result.duplicate_event_ids,
            tombstoned_event_ids: result.tombstoned_event_ids,
            redacted_events,
        })
    }

    pub(super) fn should_route_terminal_persistence_failure(
        &self,
        msg: &jetstream::Message,
        err: &SinexError,
    ) -> EventEngineResult<bool> {
        let delivery_attempt = msg
            .info()
            .map(|info| info.delivered)
            .map_err(|error| error.to_string());
        Self::should_route_persistence_failure(self.route_db_errors_to_dlq, delivery_attempt, err)
    }

    pub(super) fn source_material_delivery_attempt(
        &self,
        msg: &jetstream::Message,
    ) -> EventEngineResult<i64> {
        msg.info().map(|info| info.delivered).map_err(|error| {
            SinexError::processing(
                "Failed to inspect JetStream delivery metadata for source-material readiness",
            )
            .with_context("delivery_metadata_error", error.to_string())
        })
    }

    pub(super) fn source_material_ready_dlq_threshold(&self) -> i64 {
        #[cfg(any(test, feature = "testing"))]
        {
            self.source_material_ready_dlq_threshold
                .unwrap_or(SOURCE_MATERIAL_READY_DLQ_THRESHOLD)
        }
        #[cfg(not(any(test, feature = "testing")))]
        {
            SOURCE_MATERIAL_READY_DLQ_THRESHOLD
        }
    }

    pub(super) fn source_material_ready_retry_delay(&self) -> Duration {
        #[cfg(any(test, feature = "testing"))]
        {
            self.source_material_ready_retry_delay
                .unwrap_or(FK_VIOLATION_RETRY_DELAY)
        }
        #[cfg(not(any(test, feature = "testing")))]
        {
            FK_VIOLATION_RETRY_DELAY
        }
    }

    pub(super) async fn settle_unready_source_material_event(
        &self,
        prepared: &PreparedEvent,
        material_id: Option<Uuid>,
        persistence_error: Option<&SinexError>,
    ) -> EventEngineResult<SourceMaterialSettlement> {
        let delivery_attempt = if self.route_db_errors_to_dlq {
            None
        } else {
            Some(self.source_material_delivery_attempt(&prepared.message)?)
        };
        let retry_threshold = self.source_material_ready_dlq_threshold();
        let retry_delay = self.source_material_ready_retry_delay();
        let should_dlq = self.route_db_errors_to_dlq
            || delivery_attempt.is_some_and(|attempt| attempt >= retry_threshold);

        // sinex-r6d.12: settle through the shared envelope coordinator, not
        // prepared.message directly. This is exactly the "not-ready + ready
        // siblings" scenario: a not-ready child here must not NAK/ack a raw
        // message a ready sibling elsewhere in the batch has already (or
        // will) mark Safe — the coordinator only NAKs the shared message
        // once every child (ready and not-ready alike) has reported, and
        // NAKing wins over Safe, so the ready sibling's work simply
        // re-admits idempotently on redelivery rather than being lost.
        if should_dlq {
            warn!(
                event_id = %prepared.parsed_id,
                material_id = ?material_id,
                delivery_attempt = ?delivery_attempt,
                threshold = retry_threshold,
                "Source material remained unavailable after retry budget; routing event to DLQ"
            );
            let dlq_reason = source_material_unavailable_error(
                prepared,
                material_id,
                persistence_error,
                retry_threshold,
            );
            match self.route_to_dlq(&prepared.message, dlq_reason.clone()).await {
                Ok(()) => {
                    self.stats.dlq_routed.fetch_add(1, Ordering::Relaxed);
                    self.settlement_registry.resolve(
                        prepared.parsed_id,
                        EmissionReceiptState::DurableDebt {
                            debt_id: prepared.parsed_id,
                            reason: dlq_reason,
                        },
                    );
                    prepared.settlement.settle_child(ChildOutcome::Safe).await?;
                }
                Err(err) => {
                    self.stats
                        .dlq_publish_failures
                        .fetch_add(1, Ordering::Relaxed);
                    // sinex-r6d.11: deliberately not resolved -- Retry means
                    // redelivery, so this event_id reaches settle_child again;
                    // see the matching note on the below-threshold Deferred
                    // branch a few lines down.
                    prepared
                        .settlement
                        .settle_child(ChildOutcome::Retry(None))
                        .await?;
                    return Err(err);
                }
            }
            self.stats.events_failed.fetch_add(1, Ordering::Relaxed);
            if let Some(ref handle) = self.heartbeat_handle {
                handle.record_error("source material unresolved");
            }
            return Ok(SourceMaterialSettlement::RoutedToDlq);
        }

        // sinex-r6d.11: deliberately NOT resolved here. This is the
        // below-DLQ-threshold retry path -- ChildOutcome::Retry means the
        // message is NAK'd and this event_id will reach
        // settle_unready_source_material_event again on redelivery, either
        // succeeding once the material registers or eventually crossing the
        // DLQ threshold above. Resolving on this transient `Deferred` state
        // would consume the one-shot registry entry immediately (on the
        // FIRST retry attempt) and permanently strand any waiter before the
        // real terminal outcome is known -- this exact bug made
        // `settlement_registry_resolves_durable_debt_for_a_dlqd_event`
        // observe `Deferred` instead of the eventual `DurableDebt`.
        if let Err(err) = prepared
            .settlement
            .settle_child(ChildOutcome::Retry(Some(retry_delay)))
            .await
        {
            warn!(
                event_id = %prepared.parsed_id,
                material_id = ?material_id,
                error = %err,
                "Failed to NAK deferred source-material event"
            );
            self.stats.nack_failures.fetch_add(1, Ordering::Relaxed);
            return Err(Self::message_settlement_failure(
                "failed to NAK deferred source-material event",
                prepared.parsed_id,
                &err,
            ));
        }

        Ok(SourceMaterialSettlement::Deferred)
    }

    pub(super) fn should_route_persistence_failure(
        route_db_errors_to_dlq: bool,
        delivery_attempt: std::result::Result<i64, String>,
        err: &SinexError,
    ) -> EventEngineResult<bool> {
        if route_db_errors_to_dlq {
            return Ok(true);
        }

        if sinex_db::query_helpers::is_retryable_db_error(err) {
            return Ok(false);
        }

        match delivery_attempt {
            Ok(delivered) => Ok(delivered >= MAIN_CONSUMER_TERMINAL_DLQ_THRESHOLD),
            Err(error) => Err(SinexError::processing(
                "Failed to inspect JetStream delivery metadata for persistence failure",
            )
            .with_context("delivery_metadata_error", error)),
        }
    }

    pub(super) fn message_settlement_failure(
        operation: &'static str,
        event_id: Uuid,
        error: impl std::fmt::Display,
    ) -> SinexError {
        crate::runtime::error_helpers::nats_settlement_error(
            operation,
            "",
            Some(event_id.to_string().as_str()),
            error,
        )
    }

    pub(super) fn collapse_settlement_errors(
        stage: &'static str,
        mut errors: Vec<(Uuid, SinexError)>,
    ) -> EventEngineResult<()> {
        if errors.is_empty() {
            return Ok(());
        }

        let (event_id, error) = errors.remove(0);
        let mut combined = error
            .with_context("settlement_stage", stage)
            .with_context("event_id", event_id.to_string());
        for (index, (event_id, extra)) in errors.into_iter().enumerate() {
            combined = combined
                .with_context(
                    format!("additional_settlement_event_id_{}", index + 1),
                    event_id.to_string(),
                )
                .with_context(
                    format!("additional_settlement_error_{}", index + 1),
                    extra.to_string(),
                );
        }
        Err(combined)
    }
}
