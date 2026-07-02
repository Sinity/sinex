//! Admission, validation, skip settlement, and material-timing preparation for `JetStreamConsumer`.

use std::sync::atomic::Ordering;

use super::*;

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
        let mut prepared = Vec::with_capacity(decisions.len());
        let mut suppressed_count = 0usize;
        let mut routed_terminal_failure = false;

        for decision in decisions {
            match decision {
                AdmissionDecision::Admitted(admitted)
                | AdmissionDecision::Transformed(admitted) => {
                    prepared.push(PreparedEvent {
                        event: admitted.event,
                        parsed_id: admitted.event_id,
                        message: msg.clone(),
                    });
                }
                AdmissionDecision::Suppressed(rejection) => {
                    suppressed_count += 1;
                    self.record_admission_suppression(&rejection).await;
                }
                AdmissionDecision::Rejected(rejection)
                | AdmissionDecision::QuarantineNeeded(rejection) => {
                    routed_terminal_failure = true;
                    self.record_admission_rejection(&rejection).await;
                    self.route_validation_failure(&msg, rejection.reason)
                        .await?;
                }
            }
        }

        if prepared.is_empty() && suppressed_count > 0 && !routed_terminal_failure {
            msg.ack().await.map_err(|error| {
                SinexError::network("Failed to ack all-suppressed admission message")
                    .with_context("suppressed_count", suppressed_count.to_string())
                    .with_source(error.to_string())
            })?;
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

    pub(super) async fn route_validation_failure(
        &self,
        msg: &jetstream::Message,
        error: String,
    ) -> EventEngineResult<()> {
        self.route_to_dlq_and_ack(msg, error).await?;
        self.stats
            .validation_failures
            .fetch_add(1, Ordering::Relaxed);
        Ok(())
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

        // Per #1306: per-kind watermark, not per-event.
        let mut by_kind: HashMap<(String, String), Vec<&PreparedEvent>> = HashMap::new();
        for prepared in &duplicate_batch {
            let key = (
                prepared.event.source.as_str().to_string(),
                prepared.event.event_type.as_str().to_string(),
            );
            by_kind.entry(key).or_default().push(*prepared);
        }

        let confirmation_futs: Vec<_> = by_kind
            .into_iter()
            .filter_map(|(kind, preps)| {
                let sem = Arc::clone(&self.confirmation_semaphore);
                let watermark = Arc::clone(&self.confirmation_watermark);
                let max_event_id = preps.iter().map(|p| p.parsed_id).max()?;
                let (source, event_type) = (kind.0.clone(), kind.1.clone());
                let key = kind;
                Some(async move {
                    // Check-only; do not advance the watermark until publish succeeds.
                    // See the matching comment on the primary batch path above.
                    let should_publish = {
                        let wm = watermark.lock().await;
                        wm.get(&key).copied().is_none_or(|prev| max_event_id > prev)
                    };
                    if !should_publish {
                        return (preps, max_event_id, source, event_type, Ok(()));
                    }
                    let _permit = match sem.acquire().await {
                        Ok(permit) => permit,
                        Err(error) => {
                            return (
                                preps,
                                max_event_id,
                                source,
                                event_type,
                                Err(SinexError::processing("confirmation semaphore closed")
                                    .with_std_error(&error)),
                            );
                        }
                    };
                    let result = self
                        .publish_confirmation_with_retry(&max_event_id, &source, &event_type)
                        .await;
                    // Advance the watermark only on success, with monotonic guard.
                    if result.is_ok() {
                        let mut wm = watermark.lock().await;
                        let cur = wm.get(&key).copied();
                        if cur.is_none_or(|prev| max_event_id > prev) {
                            wm.insert(key, max_event_id);
                        }
                    }
                    (preps, max_event_id, source, event_type, result)
                })
            })
            .collect();
        let kind_results = join_all(confirmation_futs).await;

        let mut ack_messages = Vec::with_capacity(duplicate_batch.len() + tombstoned_batch.len());
        ack_messages.extend(tombstoned_batch.iter().map(|prepared| &prepared.message));
        let mut confirmation_durability_gaps = Vec::new();
        for (preps, max_event_id, source, event_type, result) in &kind_results {
            match result {
                Ok(()) => {
                    for prepared in preps {
                        debug!(
                            event_id = %prepared.parsed_id,
                            "Re-published confirmation for duplicate already admitted event"
                        );
                        ack_messages.push(&prepared.message);
                    }
                }
                Err(err) => {
                    warn!(
                        source = %source,
                        event_type = %event_type,
                        watermark = %max_event_id,
                        error = %err,
                        "Failed to publish duplicate-confirmation watermark after retries"
                    );
                    self.stats
                        .confirmation_failures
                        .fetch_add(1, Ordering::Relaxed);
                    match self
                        .enqueue_confirmation_retry(max_event_id, source, event_type)
                        .await
                    {
                        Ok(()) => {
                            self.stats
                                .confirmation_retries_enqueued
                                .fetch_add(1, Ordering::Relaxed);
                            for prepared in preps {
                                ack_messages.push(&prepared.message);
                            }
                        }
                        Err(retry_err) => {
                            self.stats
                                .confirmation_retry_failures
                                .fetch_add(1, Ordering::Relaxed);
                            for prepared in preps {
                                confirmation_durability_gaps.push((
                                    prepared.parsed_id,
                                    SinexError::network(
                                        "Duplicate event could not publish a confirmation or durably enqueue its retry",
                                    )
                                    .with_context("confirmation_publish_error", err.to_string())
                                    .with_context(
                                        "confirmation_retry_enqueue_error",
                                        retry_err.to_string(),
                                    )
                                    .with_context("kind_source", source.clone())
                                    .with_context("kind_event_type", event_type.clone())
                                    .with_context("kind_watermark", max_event_id.to_string()),
                                ));
                            }
                        }
                    }
                }
            }
        }

        let ack_futs: Vec<_> = ack_messages.iter().map(|message| message.ack()).collect();
        let ack_results = join_all(ack_futs).await;
        for result in &ack_results {
            if let Err(error) = result {
                return Err(
                    SinexError::network("Failed to ack admission-skipped messages")
                        .with_context("batch_size", ack_messages.len().to_string())
                        .with_source(error.to_string()),
                );
            }
        }

        let acked_count = ack_messages.len() as u64;
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
