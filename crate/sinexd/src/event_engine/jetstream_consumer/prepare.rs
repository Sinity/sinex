//! Admission, validation, skip settlement, and material-timing preparation for `JetStreamConsumer`.

use std::sync::atomic::Ordering;

use super::dlq::DLQ_RETRY_DELAY;
use super::*;
use sinex_primitives::events::Event;

impl JetStreamConsumer {
    #[cfg(test)]
    pub(super) fn require_inserted_ids(
        inserted_ids: Option<Vec<Uuid>>,
        attempted_rows: usize,
    ) -> EventEngineResult<Vec<Uuid>> {
        inserted_ids.ok_or_else(|| {
            SinexError::invalid_state(format!(
                "Event repository omitted inserted_ids for a non-empty stream batch of {attempted_rows} row(s)"
            ))
        })
    }

    #[instrument(skip(self, msg))]
    pub(super) async fn prepare_events(
        &self,
        msg: jetstream::Message,
    ) -> EventEngineResult<Vec<PreparedEvent>> {
        let decisions = self.admission.admit_intent_bytes(&msg.payload).await?;

        // sinex-r6d.12: an intent with zero decisions (empty EventIntent.events)
        // has no children to settle through — ack it directly. Every other
        // path below goes through the shared RawEnvelopeSettlement so no
        // child can unilaterally ACK/NAK/DLQ a message a sibling still needs.
        if decisions.is_empty() {
            msg.ack().await.map_err(|error| {
                SinexError::network("Failed to ack empty-intent admission message")
                    .with_source(error.to_string())
            })?;
            return Ok(Vec::new());
        }

        let settlement = RawEnvelopeSettlement::new(msg.clone(), decisions.len());
        let mut prepared = Vec::with_capacity(decisions.len());

        for decision in decisions {
            match decision {
                AdmissionDecision::Admitted(admitted)
                | AdmissionDecision::Transformed(admitted) => {
                    prepared.push(PreparedEvent {
                        event: admitted.event,
                        parsed_id: admitted.event_id,
                        message: msg.clone(),
                        settlement: Arc::clone(&settlement),
                    });
                }
                AdmissionDecision::Superseded {
                    admitted,
                    superseded_event_id,
                } => {
                    // sinex-n9a: the candidate carries an occurrence key whose
                    // live row changed. Archive the predecessor BEFORE the new
                    // interpretation is persisted (single-live-interpretation
                    // upheld across this prepare→persist boundary). If the
                    // archive fails we fall back to suppression rather than
                    // admit a second live row for the same occurrence — the
                    // revision re-arrives on the next re-emit (occurrence keys
                    // are stable) or replay.
                    //
                    // Downstream propagation is deliberately NOT an
                    // invalidation publish: the admitted revision flows through
                    // the normal confirmed-events -> derived-consumer path, so
                    // downstream automata see it as ordinary new input. A
                    // DerivedScopeInvalidation naming the archived predecessor
                    // would be a verified no-op here — the adapter's
                    // prepare_invalidation derives scope keys by get_by_id on
                    // core.events (invalidate.rs), which returns None for the
                    // row we just archived, and none of the SupersedeOnChange
                    // event types stamp scope_key on their outputs anyway, so
                    // it always resolved to "No scope keys to recompute".
                    if self
                        .apply_supersession(superseded_event_id, &admitted.event)
                        .await
                    {
                        self.record_admission_supersession().await;
                        prepared.push(PreparedEvent {
                            event: admitted.event,
                            parsed_id: admitted.event_id,
                            message: msg.clone(),
                            settlement: Arc::clone(&settlement),
                        });
                    } else {
                        let rejection = AdmissionRejection {
                            kind: AdmissionRejectionKind::OccurrenceDuplicate,
                            reason: format!(
                                "supersession archive of {superseded_event_id} failed; \
                                 kept existing live interpretation and suppressed this revision"
                            ),
                        };
                        self.record_admission_suppression(&rejection).await;
                        settlement.settle_child(ChildOutcome::Safe).await?;
                    }
                }
                AdmissionDecision::Suppressed(rejection) => {
                    self.record_admission_suppression(&rejection).await;
                    // TODO(sinex-r6d.11): no settlement_registry.resolve() call here.
                    // AdmissionRejection carries only `kind`/`reason` (admission.rs:218-221),
                    // never an event_id — most rejection sites in admit_event_with_metadata
                    // construct it before or without a parsed candidate id, so there is no
                    // reliable key to resolve against here. Threading an `Option<Uuid>`
                    // through AdmissionRejection's ~20 construction sites to recover it for
                    // the few paths (e.g. OccurrenceDuplicate) where an id happens to be in
                    // scope is a wider change to admission.rs's own shape than this bead's
                    // settle_child-site wiring scope; left as a documented gap rather than
                    // guessed at.
                    settlement.settle_child(ChildOutcome::Safe).await?;
                }
                AdmissionDecision::Rejected(rejection)
                | AdmissionDecision::QuarantineNeeded(rejection) => {
                    self.record_admission_rejection(&rejection).await;
                    self.route_validation_failure(&msg, rejection.reason, &settlement)
                        .await?;
                }
            }
        }

        Ok(prepared)
    }

    /// Resolve a deferred `ts_orig` (and its `ts_quality` rung) for ready
    /// material-provenance events from the source-material timing tier
    /// (#1570 Prong B).
    ///
    /// The parser owns the `IntrinsicContent` case and stamps `ts_orig`
    /// directly; otherwise material events arrive with `ts_orig = None` as the
    /// "derive me" signal. Here — guaranteed to run after the `MaterialReadySet`
    /// FK gate has confirmed the registry row is visible — we read the registry
    /// timing summary plus any sub-material `temporal_ledger` entries (wrapped
    /// streams) and resolve a stable value. The `staged_at` floor guarantees a
    /// result, so the NOT-NULL `ts_orig` column is always satisfied.
    ///
    /// Timing rows + ledger entries are fetched once per distinct material so a
    /// large same-material batch (the COPY fast path) does not incur a DB round
    /// trip per event.
    pub(super) async fn resolve_ready_ts_orig(
        &self,
        batch: &mut [PreparedEvent],
        ready_indices: &[usize],
    ) -> EventEngineResult<()> {
        // Collect distinct materials that actually need resolution.
        let mut needed: Vec<Uuid> = Vec::new();
        for &idx in ready_indices {
            let event = &batch[idx].event;
            if event.ts_orig.is_some() {
                continue;
            }
            if let Provenance::Material { id, .. } = &event.provenance {
                let material_id = *id.as_uuid();
                if !needed.contains(&material_id) {
                    needed.push(material_id);
                }
            }
        }
        if needed.is_empty() {
            return Ok(());
        }

        // Fetch timing + ledger once per material into a batch-local cache.
        let materials = self.pool.source_materials();
        let mut cache: HashMap<Uuid, (MaterialTiming, LedgerReader)> =
            HashMap::with_capacity(needed.len());
        for material_id in needed {
            let record = materials
                .get_by_id(Id::<SourceMaterialRecord>::from_uuid(material_id))
                .await
                .map_err(|e| {
                    SinexError::database("failed to read source material for ts_orig resolution")
                        .with_context("material_id", material_id.to_string())
                        .with_source(e)
                })?;
            let Some(record) = record else {
                // Should not happen post-readiness, but skip rather than mis-stamp.
                warn!(
                    %material_id,
                    "ts_orig resolution: registry row missing after readiness gate"
                );
                continue;
            };

            let timing_info_type = record
                .timing_info_type
                .parse::<SourceMaterialTimingInfoType>()
                .unwrap_or(SourceMaterialTimingInfoType::Unknown);
            let timing = MaterialTiming {
                timing_info_type,
                start_time: record.start_time,
                staged_at: record.staged_at,
            };

            let ledger_rows = materials
                .read_temporal_ledger(material_id)
                .await
                .map_err(|e| {
                    SinexError::database("failed to read temporal ledger for ts_orig resolution")
                        .with_context("material_id", material_id.to_string())
                        .with_source(e)
                })?;
            let entries: Vec<LedgerEntry> = ledger_rows
                .into_iter()
                .map(|entry| LedgerEntry {
                    offset_start: entry.offset_start,
                    offset_end: entry.offset_end,
                    ts_capture: entry.ts_capture,
                    precision: entry.precision,
                    source_type: entry.source_type,
                })
                .collect();
            cache.insert(
                material_id,
                (timing, LedgerReader::new(material_id, entries)),
            );
        }

        // Assign the resolved value per event.
        for &idx in ready_indices {
            let event = &mut batch[idx].event;
            if event.ts_orig.is_some() {
                continue;
            }
            let (material_id, anchor_byte) = match &event.provenance {
                Provenance::Material {
                    id, anchor_byte, ..
                } => (*id.as_uuid(), *anchor_byte),
                Provenance::Derived { .. } => continue,
            };
            if let Some((timing, reader)) = cache.get(&material_id) {
                let (ts_orig, rung) = reader.derive_ts_orig(anchor_byte, timing);
                event.ts_orig = Some(ts_orig);
                event.ts_quality = Some(rung);
            }
        }
        Ok(())
    }

    /// sinex-r6d.12: writes the DLQ record, then reports this child's
    /// outcome to the shared envelope settlement instead of ACKing/NAKing
    /// the raw message directly — a rejected child must never unilaterally
    /// settle a message that admitted siblings still need.
    pub(super) async fn route_validation_failure(
        &self,
        msg: &jetstream::Message,
        error: String,
        settlement: &Arc<RawEnvelopeSettlement>,
    ) -> EventEngineResult<()> {
        self.stats
            .validation_failures
            .fetch_add(1, Ordering::Relaxed);
        // TODO(sinex-r6d.11): no settlement_registry.resolve() calls in this function.
        // This runs for AdmissionDecision::Rejected/QuarantineNeeded, both carrying only
        // an AdmissionRejection (kind + reason string, admission.rs:218-221) with no
        // event_id — several rejection kinds (MissingEventId, InvalidEventId,
        // EnvelopeDeserialization, StructuralJson...) are rejected precisely because no
        // valid id could be parsed, so there is no trustworthy key to resolve against
        // even in principle. Same documented gap as the Suppressed arm above.
        match self.route_to_dlq(msg, error).await {
            Ok(()) => {
                self.stats.dlq_routed.fetch_add(1, Ordering::Relaxed);
                settlement.settle_child(ChildOutcome::Safe).await
            }
            Err(e) => {
                warn!(
                    error = %e,
                    "Failed to route validation failure to DLQ after retries; requesting redelivery"
                );
                self.stats
                    .dlq_publish_failures
                    .fetch_add(1, Ordering::Relaxed);
                settlement
                    .settle_child(ChildOutcome::Retry(Some(DLQ_RETRY_DELAY)))
                    .await
            }
        }
    }

    pub(super) async fn record_admission_rejection(&self, rejection: &AdmissionRejection) {
        // Keep operator-facing rejection counters in sync with admission decisions.
        match rejection.kind {
            AdmissionRejectionKind::PastTimestamp => {
                self.stats
                    .suspicious_past_ts_orig
                    .fetch_add(1, Ordering::Relaxed);
            }
            AdmissionRejectionKind::FutureTimestamp => {
                self.stats
                    .suspicious_future_ts_orig
                    .fetch_add(1, Ordering::Relaxed);
            }
            AdmissionRejectionKind::NegativeAnchor => {
                self.stats
                    .negative_anchor_byte
                    .fetch_add(1, Ordering::Relaxed);
            }
            AdmissionRejectionKind::SchemaValidation
            | AdmissionRejectionKind::MissingTimestamp
            | AdmissionRejectionKind::PayloadTooLarge
            | AdmissionRejectionKind::InvalidUtf8
            | AdmissionRejectionKind::StructuralJson
            | AdmissionRejectionKind::EventDeserialization
            | AdmissionRejectionKind::CandidateMetadata
            | AdmissionRejectionKind::PrivacyPolicy
            | AdmissionRejectionKind::QuarantinePolicy
            | AdmissionRejectionKind::MissingEventId
            | AdmissionRejectionKind::InvalidEventId
            | AdmissionRejectionKind::EnvelopeDeserialization
            | AdmissionRejectionKind::EnvelopeValidation
            | AdmissionRejectionKind::OccurrenceDuplicate => {
                self.stats
                    .validation_failures
                    .fetch_add(1, Ordering::Relaxed);
            }
        }

        // Emit a unified rejection counter with kind label so every rejection
        // variant is visible in NATS metrics, not just PastTimestamp/FutureTimestamp.
        let kind_label = match rejection.kind {
            AdmissionRejectionKind::PayloadTooLarge => "payload_too_large",
            AdmissionRejectionKind::InvalidUtf8 => "invalid_utf8",
            AdmissionRejectionKind::StructuralJson => "structural_json",
            AdmissionRejectionKind::EventDeserialization => "event_deserialization",
            AdmissionRejectionKind::EnvelopeDeserialization => "envelope_deserialization",
            AdmissionRejectionKind::EnvelopeValidation => "envelope_validation",
            AdmissionRejectionKind::MissingTimestamp => "missing_timestamp",
            AdmissionRejectionKind::PastTimestamp => "past_timestamp",
            AdmissionRejectionKind::FutureTimestamp => "future_timestamp",
            AdmissionRejectionKind::NegativeAnchor => "negative_anchor",
            AdmissionRejectionKind::SchemaValidation => "schema_validation",
            AdmissionRejectionKind::CandidateMetadata => "candidate_metadata",
            AdmissionRejectionKind::PrivacyPolicy => "privacy_policy",
            AdmissionRejectionKind::QuarantinePolicy => "quarantine_policy",
            AdmissionRejectionKind::MissingEventId => "missing_event_id",
            AdmissionRejectionKind::InvalidEventId => "invalid_event_id",
            AdmissionRejectionKind::OccurrenceDuplicate => "occurrence_duplicate",
        };

        tracing::debug!(
            target: "sinex_metrics",
            metric = "event_engine.admission_rejections_total",
            kind = kind_label,
            "Event rejected by admission service"
        );

        if let Some(ref observer) = self.observer {
            let labels = Some(std::collections::HashMap::from([(
                "kind".to_string(),
                kind_label.to_string(),
            )]));
            if let Err(error) = observer
                .emit_counter("event_engine.admission_rejections_total", 1, labels)
                .await
            {
                Self::log_observer_error(
                    &self.stats,
                    "event_engine.admission_rejections_total",
                    &error,
                );
            }
        }
    }

    pub(super) async fn record_admission_suppression(&self, rejection: &AdmissionRejection) {
        let kind_label = match rejection.kind {
            AdmissionRejectionKind::OccurrenceDuplicate => "occurrence_duplicate",
            AdmissionRejectionKind::PayloadTooLarge => "payload_too_large",
            AdmissionRejectionKind::InvalidUtf8 => "invalid_utf8",
            AdmissionRejectionKind::StructuralJson => "structural_json",
            AdmissionRejectionKind::EventDeserialization => "event_deserialization",
            AdmissionRejectionKind::EnvelopeDeserialization => "envelope_deserialization",
            AdmissionRejectionKind::EnvelopeValidation => "envelope_validation",
            AdmissionRejectionKind::MissingTimestamp => "missing_timestamp",
            AdmissionRejectionKind::PastTimestamp => "past_timestamp",
            AdmissionRejectionKind::FutureTimestamp => "future_timestamp",
            AdmissionRejectionKind::NegativeAnchor => "negative_anchor",
            AdmissionRejectionKind::SchemaValidation => "schema_validation",
            AdmissionRejectionKind::CandidateMetadata => "candidate_metadata",
            AdmissionRejectionKind::PrivacyPolicy => "privacy_policy",
            AdmissionRejectionKind::QuarantinePolicy => "quarantine_policy",
            AdmissionRejectionKind::MissingEventId => "missing_event_id",
            AdmissionRejectionKind::InvalidEventId => "invalid_event_id",
        };

        tracing::debug!(
            target: "sinex_metrics",
            metric = "event_engine.admission_suppressions_total",
            kind = kind_label,
            "Event suppressed by admission service"
        );

        if let Some(ref observer) = self.observer {
            let labels = Some(std::collections::HashMap::from([(
                "kind".to_string(),
                kind_label.to_string(),
            )]));
            if let Err(error) = observer
                .emit_counter("event_engine.admission_suppressions_total", 1, labels)
                .await
            {
                Self::log_observer_error(
                    &self.stats,
                    "event_engine.admission_suppressions_total",
                    &error,
                );
            }
        }
    }

    /// Apply an occurrence supersession (sinex-n9a): archive the prior live
    /// interpretation so the candidate revision can replace it.
    ///
    /// Returns `true` when the predecessor was archived (the caller then
    /// prepares the candidate for persistence), `false` when the archive
    /// failed (the caller suppresses the candidate to preserve
    /// single-live-interpretation). The archive commits in its own
    /// transaction; the candidate is inserted later in the persist phase, so
    /// there is a brief window where the occurrence has no live row — a crash
    /// there self-heals on redelivery (the archived predecessor is gone, so
    /// re-admission inserts the revision fresh; no duplicate is ever created).
    ///
    /// No `DerivedScopeInvalidation` is published here: downstream automata
    /// receive the admitted revision through the normal confirmed-events →
    /// derived-consumer path as ordinary new input, which is what actually
    /// drives descendant recomputation. An invalidation naming the archived
    /// predecessor would be a no-op — the adapter derives scope keys by
    /// `get_by_id` on `core.events` (`runtime/automaton/adapter/invalidate.rs`),
    /// which returns `None` for the row this function just archived, and the
    /// `SupersedeOnChange` event types don't stamp `scope_key` on their
    /// outputs, so the adapter always resolved "no scope keys to recompute".
    pub(super) async fn apply_supersession(
        &self,
        superseded_event_id: Uuid,
        candidate: &Event<JsonValue>,
    ) -> bool {
        // Fresh audit-correlation id for this single-row supersession archive.
        let operation_id = Uuid::now_v7();
        match self
            .pool
            .events()
            .execute_cascade_archive(
                &[superseded_event_id],
                "occurrence supersession (sinex-n9a): live interpretation replaced by a changed re-emit",
                &operation_id.to_string(),
                "admission:supersede",
            )
            .await
        {
            Ok(_) => true,
            Err(error) => {
                warn!(
                    superseded_event_id = %superseded_event_id,
                    event_type = %candidate.event_type,
                    error = %error,
                    "supersession archive failed; keeping existing live interpretation and suppressing this revision"
                );
                false
            }
        }
    }

    /// Record telemetry for an admitted occurrence supersession (sinex-n9a),
    /// kept distinct from suppression counters so a revision that replaced a
    /// predecessor is separately visible from an ordinary duplicate drop.
    pub(super) async fn record_admission_supersession(&self) {
        self.stats.supersessions.fetch_add(1, Ordering::Relaxed);

        tracing::debug!(
            target: "sinex_metrics",
            metric = "event_engine.admission_supersessions_total",
            "Occurrence revision admitted via supersession"
        );

        if let Some(ref observer) = self.observer
            && let Err(error) = observer
                .emit_counter("event_engine.admission_supersessions_total", 1, None)
                .await
        {
            Self::log_observer_error(
                &self.stats,
                "event_engine.admission_supersessions_total",
                &error,
            );
        }
    }

    pub(super) async fn settle_admission_skips(
        &self,
        batch: &[&PreparedEvent],
        duplicate_event_ids: &[Uuid],
        tombstoned_event_ids: &[Uuid],
    ) -> EventEngineResult<()> {
        if duplicate_event_ids.is_empty() && tombstoned_event_ids.is_empty() {
            return Ok(());
        }

        let duplicate_ids: HashSet<Uuid> = duplicate_event_ids.iter().copied().collect();
        let tombstoned_ids: HashSet<Uuid> = tombstoned_event_ids.iter().copied().collect();
        let duplicate_batch: Vec<_> = batch
            .iter()
            .copied()
            .filter(|prepared| duplicate_ids.contains(&prepared.parsed_id))
            .collect();
        let tombstoned_batch: Vec<_> = batch
            .iter()
            .copied()
            .filter(|prepared| tombstoned_ids.contains(&prepared.parsed_id))
            .collect();

        // sinex-r6d.12: settle through the shared per-message coordinator,
        // never ack `prepared.message` directly — a sibling from the same
        // raw message may still be pending elsewhere in this batch.
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
        let confirmation_futs: Vec<_> = duplicate_batch
            .iter()
            .map(|prepared| {
                let sem = Arc::clone(&self.confirmation_semaphore);
                async move {
                    let _permit = match sem.acquire().await {
                        Ok(permit) => permit,
                        Err(error) => {
                            return (
                                prepared.parsed_id,
                                Err(SinexError::processing("confirmation semaphore closed")
                                    .with_std_error(&error)),
                            );
                        }
                    };
                    let result = self
                        .publish_confirmed_event_with_retry(&prepared.event)
                        .await;
                    (prepared.parsed_id, result)
                }
            })
            .collect();
        let confirmed_publish_failures: HashMap<Uuid, SinexError> = join_all(confirmation_futs)
            .await
            .into_iter()
            .filter_map(|(id, result)| result.err().map(|err| (id, err)))
            .collect();

        for prepared in &duplicate_batch {
            if let Some(err) = confirmed_publish_failures.get(&prepared.parsed_id) {
                // Durability gap: deliberately NOT settled here. Leaving this
                // child's contribution to its envelope's countdown pending
                // means the shared message stays unacked for redelivery
                // (below), the same "left_unacked_for_redelivery" contract
                // confirmed_event_durability_gap_error already documents.
                confirmation_durability_gaps.push((
                    prepared.parsed_id,
                    Self::confirmed_event_durability_gap_error(prepared.parsed_id, err),
                ));
            } else {
                debug!(
                    event_id = %prepared.parsed_id,
                    "Re-published confirmed event for duplicate already admitted event"
                );
                self.settlement_registry.resolve(
                    prepared.parsed_id,
                    EmissionReceiptState::Suppressed {
                        reason: SuppressionReason::CachedDuplicate,
                        existing_event_id: None,
                    },
                );
                prepared.settlement.settle_child(ChildOutcome::Safe).await?;
                settled_count += 1;
            }
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

        Ok(())
    }

    #[cfg(test)]
    pub(super) fn resolve_validation_result(
        validation: ValidationResult,
        strict_mode: bool,
        source: &sinex_primitives::domain::EventSource,
        event_type: &sinex_primitives::domain::EventType,
    ) -> EventEngineResult<Option<Uuid>> {
        match validation {
            ValidationResult::Valid { schema_id } => Ok(Some(schema_id)),
            ValidationResult::Skipped => Ok(None),
            ValidationResult::NoSchema => {
                if strict_mode {
                    Err(SinexError::validation(format!(
                        "Strict validation enabled: event has no registered schema (source={source}, event_type={event_type})"
                    ))
                    .with_operation("jetstream_consumer.validate_event")
                    .with_context("strict_mode", "enabled"))
                } else {
                    Ok(None)
                }
            }
            ValidationResult::SchemaNotFound { schema_id } => {
                warn!(
                    schema_id = %schema_id,
                    source = %source,
                    event_type = %event_type,
                    "Schema referenced by validator lookup is missing from cache; accepting event without payload schema id"
                );
                Ok(None)
            }
            ValidationResult::Invalid { errors } => Err(SinexError::validation(format!(
                "Schema validation failed: {}",
                errors.join(", ")
            ))
            .with_operation("jetstream_consumer.validate_event")),
        }
    }
}
