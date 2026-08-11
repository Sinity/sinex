use sinex_db::DbPoolExt;
use sinex_primitives::{
    AdmissionOutcome, AdmissionOutcomeRef, DynamicPayload, Id, JsonValue,
    STANDARD_EVENT_ADMISSION_POLICY_ID, SourceMaterial, Timestamp, Uuid,
    activity::ActivitySourceKind,
    domain::HostName,
    event_contracts::SHELL_HISTORY_COMMAND_IMPORTED_CONTRACT_ID,
    events::Event,
    events::admission::EventIntent,
    events::payloads::{ActivityDailySummaryPayload, ActivityHourlySummaryPayload, StateIntervalPayload},
};
use std::collections::BTreeMap;
use sinexd::event_engine::{
    AdmissionDecision, AdmissionRejection, AdmissionRejectionKind, AdmissionService, AdmittedEvent,
    CandidateEvent, CandidateEventMetadata, IngestEventValidator,
};
use sqlx::Row;
use std::sync::Arc;
use tokio::sync::RwLock;
use xtask::sandbox::prelude::*;

fn admission_service(ctx: &TestContext) -> AdmissionService {
    AdmissionService::new(
        ctx.pool.clone(),
        Arc::new(RwLock::new(IngestEventValidator::new(false))),
    )
}

fn material_event(
    material_id: Id<SourceMaterial>,
    event_id: Uuid,
    source: &str,
    event_type: &str,
    payload: JsonValue,
) -> TestResult<Event<JsonValue>> {
    let mut event = DynamicPayload::new(source, event_type, payload)
        .from_material_at(material_id, 0)
        .build()?
        .to_json_event()?;
    event.id = Some(Id::from_uuid(event_id));
    // Direct AdmissionService tests bypass the consumer's #1570 Prong B ts_orig
    // resolution (which reads raw.temporal_ledger), so set an explicit ts_orig
    // to represent the post-resolution event the persistence stage validates.
    event.ts_orig = Some(Timestamp::now());
    Ok(event)
}

async fn admit(service: &AdmissionService, event: Event<JsonValue>) -> TestResult<AdmittedEvent> {
    match service.admit_event(event).await? {
        AdmissionDecision::Admitted(admitted) => Ok(admitted),
        AdmissionDecision::Rejected(rejection) => {
            panic!("event should be admitted before persistence: {rejection:?}");
        }
        other => panic!("unexpected admission decision: {other:?}"),
    }
}

async fn insert_tombstone(ctx: &TestContext, event_id: Uuid, event_type: &str) -> TestResult<()> {
    sqlx::query(
        r"
        INSERT INTO core.event_tombstones (
            id, source, event_type, ts_orig, ts_purged,
            purge_reason, purge_operation_id, archived_at
        )
        VALUES (
            $1::uuid, 'admission-test', $2, NOW(), NOW(),
            'admission test tombstone', $3::uuid, NOW()
        )
        ",
    )
    .bind(event_id)
    .bind(event_type)
    .bind(Uuid::now_v7())
    .execute(&ctx.pool)
    .await?;
    Ok(())
}

#[sinex_test]
async fn admission_decision_outcome_refs_event_contract_for_admitted_shell_history(
    ctx: TestContext,
) -> TestResult<()> {
    let material_id = ctx
        .create_source_material(Some("admission-contract-shell-history"))
        .await?;
    let event_id = Uuid::now_v7();
    let event = material_event(
        material_id,
        event_id,
        "shell.history",
        "command.imported",
        serde_json::json!({ "command": "git status", "shell": "bash" }),
    )?;

    let service = admission_service(&ctx);
    let decision = service.admit_event(event).await?;
    let outcome = decision.to_admission_outcome();

    match outcome {
        AdmissionOutcome::Admitted {
            policy_id,
            event_contract_id,
            event_ids,
        } => {
            assert_eq!(policy_id, STANDARD_EVENT_ADMISSION_POLICY_ID);
            assert_eq!(
                event_contract_id.as_deref(),
                Some(SHELL_HISTORY_COMMAND_IMPORTED_CONTRACT_ID)
            );
            assert_eq!(event_ids, vec![Id::from_uuid(event_id)]);
        }
        other => panic!("shell-history event should map to admitted outcome: {other:?}"),
    }

    Ok(())
}

#[sinex_test]
async fn admission_decision_outcome_maps_negative_anchor_rejection(
    ctx: TestContext,
) -> TestResult<()> {
    let material_id = ctx
        .create_source_material(Some("admission-contract-negative-anchor"))
        .await?;
    let event_id = Uuid::now_v7();
    let mut event = DynamicPayload::new(
        "shell.history",
        "command.imported",
        serde_json::json!({ "command": "git status", "shell": "bash" }),
    )
    .from_material_at(material_id, -1)
    .build()?
    .to_json_event()?;
    event.id = Some(Id::from_uuid(event_id));
    event.ts_orig = Some(Timestamp::now());

    let service = admission_service(&ctx);
    let decision = service.admit_event(event).await?;
    let outcome = decision.to_admission_outcome();

    match outcome {
        AdmissionOutcome::Rejected {
            policy_id,
            reason,
            refs,
        } => {
            assert_eq!(policy_id, STANDARD_EVENT_ADMISSION_POLICY_ID);
            assert_eq!(reason.code, "negative_anchor");
            assert!(refs.contains(&AdmissionOutcomeRef::Policy(
                STANDARD_EVENT_ADMISSION_POLICY_ID.to_string(),
            )));
        }
        other => panic!("negative-anchor event should map to rejected outcome: {other:?}"),
    }

    Ok(())
}

#[sinex_test]
async fn admission_decision_outcome_maps_occurrence_duplicate_to_deduplicated() -> TestResult<()> {
    let decision = AdmissionDecision::Suppressed(AdmissionRejection {
        kind: AdmissionRejectionKind::OccurrenceDuplicate,
        reason: "live event with equivalence_key test-key already exists".to_string(),
        event_id: None,
    });

    match decision.to_admission_outcome() {
        AdmissionOutcome::Deduplicated {
            policy_id,
            reason,
            existing_event_id,
            refs,
        } => {
            assert_eq!(policy_id, STANDARD_EVENT_ADMISSION_POLICY_ID);
            assert_eq!(reason.code, "occurrence_duplicate");
            assert!(existing_event_id.is_none());
            assert!(refs.contains(&AdmissionOutcomeRef::Policy(
                STANDARD_EVENT_ADMISSION_POLICY_ID.to_string(),
            )));
        }
        other => panic!("occurrence duplicate should map to deduplicated outcome: {other:?}"),
    }

    Ok(())
}

#[sinex_test]
async fn admission_service_persists_direct_candidate_without_nats(
    ctx: TestContext,
) -> TestResult<()> {
    let material_id = ctx
        .create_source_material(Some("admission-direct-candidate"))
        .await?;
    let event_id = Uuid::now_v7();
    let mut event = DynamicPayload::new(
        "admission-test",
        "direct.candidate",
        serde_json::json!({ "ok": true }),
    )
    .from_material_at(material_id, 0)
    .build()?
    .to_json_event()?;
    event.id = Some(Id::from_uuid(event_id));
    // Direct AdmissionService tests bypass the consumer's #1570 Prong B ts_orig
    // resolution (which reads raw.temporal_ledger), so set an explicit ts_orig
    // to represent the post-resolution event the persistence stage validates.
    event.ts_orig = Some(Timestamp::now());

    let service = AdmissionService::new(
        ctx.pool.clone(),
        Arc::new(RwLock::new(IngestEventValidator::new(false))),
    );

    let admitted = match service.admit_event(event).await? {
        AdmissionDecision::Admitted(admitted) => admitted,
        AdmissionDecision::Rejected(rejection) => {
            panic!("direct candidate should be admitted: {rejection:?}");
        }
        other => panic!("unexpected direct candidate admission decision: {other:?}"),
    };
    let result = service.persist_batch(&[admitted]).await?;

    assert_eq!(result.inserted_ids.as_deref(), Some(&[event_id][..]));
    let persisted = ctx
        .pool
        .events()
        .get_by_id(Id::<Event>::from_uuid(event_id))
        .await?
        .expect("directly admitted event should be persisted");
    assert_eq!(persisted.source.as_str(), "admission-test");
    assert_eq!(persisted.event_type.as_str(), "direct.candidate");

    Ok(())
}

#[sinex_test]
async fn admission_service_rejects_direct_negative_anchor(ctx: TestContext) -> TestResult<()> {
    let material_id = ctx
        .create_source_material(Some("admission-negative-anchor"))
        .await?;
    let event_id = Uuid::now_v7();
    let mut event = DynamicPayload::new(
        "admission-test",
        "negative.anchor",
        serde_json::json!({ "ok": false }),
    )
    .from_material_at(material_id, -1)
    .build()?
    .to_json_event()?;
    event.id = Some(Id::from_uuid(event_id));
    // Direct AdmissionService tests bypass the consumer's #1570 Prong B ts_orig
    // resolution (which reads raw.temporal_ledger), so set an explicit ts_orig
    // to represent the post-resolution event the persistence stage validates.
    event.ts_orig = Some(Timestamp::now());

    let service = AdmissionService::new(
        ctx.pool.clone(),
        Arc::new(RwLock::new(IngestEventValidator::new(false))),
    );

    match service.admit_event(event).await? {
        AdmissionDecision::Rejected(rejection) => {
            assert_eq!(rejection.kind, AdmissionRejectionKind::NegativeAnchor);
            // sinex-tpv9: the id is genuinely known at this point (the caller
            // already stamped it above); the rejection must carry it so
            // operators can trace which event was dropped, instead of
            // rejecting silently with event_id: None.
            assert_eq!(
                rejection.event_id,
                Some(event_id),
                "negative-anchor rejection must carry the known event_id (sinex-tpv9)"
            );
        }
        AdmissionDecision::Admitted(_) => panic!("negative anchor should be rejected"),
        other => panic!("unexpected negative-anchor admission decision: {other:?}"),
    }

    let persisted = ctx
        .pool
        .events()
        .get_by_id(Id::<Event>::from_uuid(event_id))
        .await?;
    assert!(persisted.is_none());

    Ok(())
}

#[sinex_test]
async fn admission_service_rejects_future_timestamp_with_event_id(
    ctx: TestContext,
) -> TestResult<()> {
    let material_id = ctx
        .create_source_material(Some("admission-future-timestamp"))
        .await?;
    let event_id = Uuid::now_v7();
    let mut event = DynamicPayload::new(
        "admission-test",
        "future.timestamp",
        serde_json::json!({ "ok": false }),
    )
    .from_material_at(material_id, 0)
    .build()?
    .to_json_event()?;
    event.id = Some(Id::from_uuid(event_id));
    event.ts_orig = Some(Timestamp::now() + time::Duration::days(3650));

    let service = admission_service(&ctx);

    match service.admit_event(event).await? {
        AdmissionDecision::Rejected(rejection) => {
            assert_eq!(rejection.kind, AdmissionRejectionKind::FutureTimestamp);
            // sinex-tpv9: the id is genuinely known at this point; the
            // rejection must carry it so operators can trace which event was
            // dropped, instead of rejecting silently with event_id: None.
            assert_eq!(
                rejection.event_id,
                Some(event_id),
                "future-timestamp rejection must carry the known event_id (sinex-tpv9)"
            );
        }
        AdmissionDecision::Admitted(_) => panic!("implausibly future ts_orig should be rejected"),
        other => panic!("unexpected future-timestamp admission decision: {other:?}"),
    }

    let persisted = ctx
        .pool
        .events()
        .get_by_id(Id::<Event>::from_uuid(event_id))
        .await?;
    assert!(persisted.is_none());

    Ok(())
}

#[sinex_test]
async fn admission_candidate_metadata_stamps_existing_event_columns(
    ctx: TestContext,
) -> TestResult<()> {
    let material_id = ctx
        .create_source_material(Some("admission-candidate-metadata"))
        .await?;
    let event_id = Uuid::now_v7();
    let operation_id = Uuid::now_v7();
    let mut event = DynamicPayload::new(
        "admission-test",
        "candidate.metadata",
        serde_json::json!({ "ok": true }),
    )
    .from_material_at(material_id, 0)
    .build()?
    .to_json_event()?;
    event.id = Some(Id::from_uuid(event_id));
    // Direct AdmissionService tests bypass the consumer's #1570 Prong B ts_orig
    // resolution (which reads raw.temporal_ledger), so set an explicit ts_orig
    // to represent the post-resolution event the persistence stage validates.
    event.ts_orig = Some(Timestamp::now());

    let service = AdmissionService::new(
        ctx.pool.clone(),
        Arc::new(RwLock::new(IngestEventValidator::new(false))),
    );
    let metadata = CandidateEventMetadata {
        source_material_id: Some(*material_id.as_uuid()),
        stable_natural_key: Some("source:row:42".to_string()),
        parser_source_id: Some("test.parser".to_string()),
        parser_semantics_version: Some("parser-v2".to_string()),
        timestamp_derivation_evidence: Some("payload.started_at".to_string()),
        privacy_context: Some("metadata".to_string()),
        privacy_profile: Some("default".to_string()),
        operation_id: Some(operation_id),
    };

    let admitted = match service
        .admit_candidate(CandidateEvent::new(event, metadata.clone()))
        .await?
    {
        AdmissionDecision::Admitted(admitted) => admitted,
        AdmissionDecision::Rejected(rejection) => {
            panic!("candidate metadata should be admitted: {rejection:?}");
        }
        other => panic!("unexpected candidate admission decision: {other:?}"),
    };
    assert_eq!(admitted.metadata.as_ref(), Some(&metadata));

    let result = service.persist_batch(&[admitted]).await?;
    assert_eq!(result.inserted_ids.as_deref(), Some(&[event_id][..]));

    let row = sqlx::query(
        r"
        SELECT semantics_version, created_by_operation_id
        FROM core.events
        WHERE id = $1::uuid
        ",
    )
    .bind(event_id)
    .fetch_one(&ctx.pool)
    .await?;
    let semantics_version: Option<String> = row.try_get("semantics_version")?;
    let created_by_operation_id: Option<Uuid> = row.try_get("created_by_operation_id")?;
    assert_eq!(semantics_version.as_deref(), Some("parser-v2"));
    assert_eq!(created_by_operation_id, Some(operation_id));

    Ok(())
}

#[sinex_test]
async fn admission_plan_reports_tombstoned_disposition(ctx: TestContext) -> TestResult<()> {
    let material_id = ctx
        .create_source_material(Some("admission-tombstone-disposition"))
        .await?;
    let event_id = Uuid::now_v7();
    sqlx::query(
        r"
        INSERT INTO core.event_tombstones (
            id, source, event_type, ts_orig, ts_purged,
            purge_reason, purge_operation_id, archived_at
        )
        VALUES (
            $1::uuid, 'admission-test', 'tombstoned.event', NOW(), NOW(),
            'admission test tombstone', $2::uuid, NOW()
        )
        ",
    )
    .bind(event_id)
    .bind(Uuid::now_v7())
    .execute(&ctx.pool)
    .await?;

    let mut event = DynamicPayload::new(
        "admission-test",
        "tombstoned.event",
        serde_json::json!({ "ok": false }),
    )
    .from_material_at(material_id, 0)
    .build()?
    .to_json_event()?;
    event.id = Some(Id::from_uuid(event_id));
    // Direct AdmissionService tests bypass the consumer's #1570 Prong B ts_orig
    // resolution (which reads raw.temporal_ledger), so set an explicit ts_orig
    // to represent the post-resolution event the persistence stage validates.
    event.ts_orig = Some(Timestamp::now());

    let service = AdmissionService::new(
        ctx.pool.clone(),
        Arc::new(RwLock::new(IngestEventValidator::new(false))),
    );
    let admitted = match service.admit_event(event).await? {
        AdmissionDecision::Admitted(admitted) => admitted,
        AdmissionDecision::Rejected(rejection) => {
            panic!("tombstoned event should pass pre-persistence admission: {rejection:?}");
        }
        other => panic!("unexpected tombstone admission decision: {other:?}"),
    };

    let plan = service
        .plan_persistence_batch(std::slice::from_ref(&admitted))
        .await?;
    assert!(plan.events.is_empty());
    assert_eq!(plan.tombstoned_event_ids, vec![event_id]);

    let result = service.persist_batch(&[admitted]).await?;
    assert!(result.inserted_ids.is_none());
    assert_eq!(result.tombstoned_event_ids, vec![event_id]);
    assert_eq!(result.tombstoned_events_rejected, 1);

    Ok(())
}

#[sinex_test]
async fn admission_plan_keeps_batch_duplicates_with_representative_until_success(
    ctx: TestContext,
) -> TestResult<()> {
    let material_id = ctx
        .create_source_material(Some("admission-batch-duplicate"))
        .await?;
    let event_id = Uuid::now_v7();
    let service = admission_service(&ctx);
    let first = admit(
        &service,
        material_event(
            material_id,
            event_id,
            "admission-test",
            "batch.duplicate",
            serde_json::json!({ "sequence": 1 }),
        )?,
    )
    .await?;
    let second = admit(
        &service,
        material_event(
            material_id,
            event_id,
            "admission-test",
            "batch.duplicate",
            serde_json::json!({ "sequence": 1 }),
        )?,
    )
    .await?;

    let plan = service
        .plan_persistence_batch(&[first.clone(), second.clone()])
        .await?;
    assert_eq!(plan.events.len(), 1);
    assert!(plan.cached_duplicate_event_ids.is_empty());
    assert_eq!(plan.batch_duplicate_event_ids, vec![event_id]);
    assert!(plan.tombstoned_event_ids.is_empty());

    let result = service.persist_batch(&[first, second]).await?;
    assert_eq!(result.inserted_ids.as_deref(), Some(&[event_id][..]));
    assert_eq!(result.duplicate_event_ids, vec![event_id]);

    Ok(())
}

#[sinex_test]
async fn admission_persist_reports_cache_cold_db_duplicates(ctx: TestContext) -> TestResult<()> {
    let material_id = ctx
        .create_source_material(Some("admission-cache-cold-duplicate"))
        .await?;
    let event_id = Uuid::now_v7();
    let first_service = admission_service(&ctx);
    let first = admit(
        &first_service,
        material_event(
            material_id,
            event_id,
            "admission-test",
            "cache-cold.duplicate",
            serde_json::json!({ "sequence": 1 }),
        )?,
    )
    .await?;
    let first_result = first_service.persist_batch(&[first]).await?;
    assert_eq!(first_result.inserted_ids.as_deref(), Some(&[event_id][..]));

    let cache_cold_service = admission_service(&ctx);
    let duplicate = admit(
        &cache_cold_service,
        material_event(
            material_id,
            event_id,
            "admission-test",
            "cache-cold.duplicate",
            serde_json::json!({ "sequence": 1 }),
        )?,
    )
    .await?;
    let result = cache_cold_service.persist_batch(&[duplicate]).await?;
    let empty: &[Uuid] = &[];
    assert_eq!(result.inserted_ids.as_deref(), Some(empty));
    assert_eq!(result.duplicate_event_ids, vec![event_id]);
    assert!(result.tombstoned_event_ids.is_empty());

    Ok(())
}

#[sinex_test]
async fn admission_tombstone_disposition_wins_over_recent_id_cache(
    ctx: TestContext,
) -> TestResult<()> {
    let material_id = ctx
        .create_source_material(Some("admission-tombstone-cache-precedence"))
        .await?;
    let live_id = Uuid::now_v7();
    let tombstoned_id = Uuid::now_v7();
    insert_tombstone(&ctx, tombstoned_id, "tombstone.cache").await?;

    let service = admission_service(&ctx);
    let live = admit(
        &service,
        material_event(
            material_id,
            live_id,
            "admission-test",
            "tombstone.cache.live",
            serde_json::json!({ "ok": true }),
        )?,
    )
    .await?;
    let tombstoned = admit(
        &service,
        material_event(
            material_id,
            tombstoned_id,
            "admission-test",
            "tombstone.cache",
            serde_json::json!({ "ok": false }),
        )?,
    )
    .await?;

    let result = service.persist_batch(&[live, tombstoned.clone()]).await?;
    assert_eq!(result.inserted_ids.as_deref(), Some(&[live_id][..]));
    assert!(result.duplicate_event_ids.is_empty());
    assert_eq!(result.tombstoned_event_ids, vec![tombstoned_id]);

    let repeated_tombstone = service.persist_batch(&[tombstoned]).await?;
    assert!(repeated_tombstone.inserted_ids.is_none());
    assert!(repeated_tombstone.duplicate_event_ids.is_empty());
    assert_eq!(repeated_tombstone.tombstoned_event_ids, vec![tombstoned_id]);

    Ok(())
}

// ─── sinex-n9a: RevisionPolicy occurrence reconciliation ──────────────────

/// A schema-valid `state.interval` payload (a `SupersedeOnChange` event type).
/// `duration_secs` is the content knob the tests vary to force a hash change;
/// all timestamps come from `ts` so two calls with the same `ts`/`duration`
/// are byte-for-byte identical content.
fn interval_payload(ts: Timestamp, duration_secs: u64) -> JsonValue {
    serde_json::to_value(StateIntervalPayload {
        interval_id: "iv-n9a".to_string(),
        state_kind: "reading".to_string(),
        subject_id: None,
        label: None,
        start_time: ts,
        end_time: ts,
        duration_secs,
        start_event_type: "start".to_string(),
        end_event_type: "end".to_string(),
        attributes: BTreeMap::new(),
    })
    .expect("state.interval payload serializes")
}

/// Admit and persist a single event, returning its inserted id. Used to seed a
/// live occurrence row before a supersession/suppression re-emit.
async fn admit_and_persist(
    service: &AdmissionService,
    event: Event<JsonValue>,
) -> TestResult<Uuid> {
    let admitted = admit(service, event).await?;
    let result = service.persist_batch(&[admitted]).await?;
    let inserted = result
        .inserted_ids
        .and_then(|ids| ids.first().copied())
        .expect("event persisted");
    Ok(inserted)
}

#[sinex_test]
async fn supersede_on_change_changed_content_returns_superseded(
    ctx: TestContext,
) -> TestResult<()> {
    let material_id = ctx.create_source_material(Some("n9a-supersede-changed")).await?;
    let ts = Timestamp::now();
    let key = "n9a-supersede-changed-key".to_string();
    let service = admission_service(&ctx);

    // Seed the live interpretation.
    let live_id = Uuid::now_v7();
    let mut live = material_event(
        material_id,
        live_id,
        "derived.interval-lift",
        "state.interval",
        interval_payload(ts, 300),
    )?;
    live.equivalence_key = Some(key.clone());
    let persisted_id = admit_and_persist(&service, live).await?;
    assert_eq!(persisted_id, live_id);

    // A changed re-emit with the SAME occurrence key must supersede.
    let revision_id = Uuid::now_v7();
    let mut revision = material_event(
        material_id,
        revision_id,
        "derived.interval-lift",
        "state.interval",
        interval_payload(ts, 999),
    )?;
    revision.equivalence_key = Some(key.clone());

    match service.admit_event(revision).await? {
        AdmissionDecision::Superseded {
            admitted,
            superseded_event_id,
        } => {
            assert_eq!(
                superseded_event_id, live_id,
                "the live interpretation is the supersession target"
            );
            assert_eq!(admitted.event_id, revision_id, "the revision is admitted");
        }
        other => panic!("changed re-emit of a SupersedeOnChange type must supersede: {other:?}"),
    }

    Ok(())
}

#[sinex_test]
async fn supersede_on_change_identical_content_suppresses(ctx: TestContext) -> TestResult<()> {
    let material_id = ctx.create_source_material(Some("n9a-supersede-identical")).await?;
    let ts = Timestamp::now();
    let key = "n9a-supersede-identical-key".to_string();
    let service = admission_service(&ctx);

    let live_id = Uuid::now_v7();
    let mut live = material_event(
        material_id,
        live_id,
        "derived.interval-lift",
        "state.interval",
        interval_payload(ts, 300),
    )?;
    live.equivalence_key = Some(key.clone());
    admit_and_persist(&service, live).await?;

    // Identical content (same ts, same duration) → idempotent re-emit → suppress.
    let repeat_id = Uuid::now_v7();
    let mut repeat = material_event(
        material_id,
        repeat_id,
        "derived.interval-lift",
        "state.interval",
        interval_payload(ts, 300),
    )?;
    repeat.equivalence_key = Some(key.clone());

    match service.admit_event(repeat).await? {
        AdmissionDecision::Suppressed(rejection) => {
            assert_eq!(rejection.kind, AdmissionRejectionKind::OccurrenceDuplicate);
        }
        other => panic!("identical re-emit must suppress, not supersede: {other:?}"),
    }

    Ok(())
}

#[sinex_test]
async fn suppress_duplicate_type_changed_content_still_suppresses(
    ctx: TestContext,
) -> TestResult<()> {
    // A type that did NOT opt into SupersedeOnChange keeps the pre-n9a
    // behavior: any live row on the same key suppresses, even changed content.
    let material_id = ctx.create_source_material(Some("n9a-suppress-default")).await?;
    let key = "n9a-suppress-default-key".to_string();
    let service = admission_service(&ctx);

    let live_id = Uuid::now_v7();
    let mut live = material_event(
        material_id,
        live_id,
        "admission-test",
        "pipeline.event",
        serde_json::json!({ "sequence": 1 }),
    )?;
    live.equivalence_key = Some(key.clone());
    admit_and_persist(&service, live).await?;

    let changed_id = Uuid::now_v7();
    let mut changed = material_event(
        material_id,
        changed_id,
        "admission-test",
        "pipeline.event",
        serde_json::json!({ "sequence": 2 }),
    )?;
    changed.equivalence_key = Some(key.clone());

    match service.admit_event(changed).await? {
        AdmissionDecision::Suppressed(rejection) => {
            assert_eq!(rejection.kind, AdmissionRejectionKind::OccurrenceDuplicate);
        }
        other => {
            panic!("default SuppressDuplicate type must suppress a changed re-emit: {other:?}")
        }
    }

    Ok(())
}

// ─── sinex-74yj: rollup types opted into SupersedeOnChange ────────────────
//
// Rollup equivalence keys (`hour_id`/`day_id` in hourly.rs/daily.rs) are
// derived purely from the floored civil-hour/day bucket start timestamp, so
// the same bucket always yields the same key across re-emits (occurrence
// stable) and a changed aggregate for the same bucket is a genuine content
// revision, not a different occurrence -- the same shape as the four
// interval-class types n9a opted in. `event_count` is the content knob
// varied between calls: two calls with the same value are byte-for-byte
// identical content, differing values are a genuine change.

fn daily_summary_payload(ts: Timestamp, day_id: &str, event_count: u64) -> JsonValue {
    serde_json::to_value(ActivityDailySummaryPayload {
        day_id: day_id.to_string(),
        day_start: ts,
        day_end: ts,
        duration_secs: 3600,
        hour_count: 1,
        window_count: 1,
        event_count,
        source_count: 1,
        sources: vec!["test-source".to_string()],
        top_sources: vec!["test-source".to_string()],
        source_window_counts: BTreeMap::new(),
        activity_sources: vec![ActivitySourceKind::Window],
        activity_source_counts: BTreeMap::new(),
        focus_time_secs_by_source: BTreeMap::new(),
        primary_source: ActivitySourceKind::Window,
    })
    .expect("activity.summary.daily payload serializes")
}

fn hourly_summary_payload(ts: Timestamp, hour_id: &str, event_count: u64) -> JsonValue {
    serde_json::to_value(ActivityHourlySummaryPayload {
        hour_id: hour_id.to_string(),
        hour_start: ts,
        hour_end: ts,
        duration_secs: 3600,
        window_count: 1,
        event_count,
        source_count: 1,
        sources: vec!["test-source".to_string()],
        top_sources: vec!["test-source".to_string()],
        source_window_counts: BTreeMap::new(),
        activity_sources: vec![ActivitySourceKind::Window],
        activity_source_counts: BTreeMap::new(),
        focus_time_secs_by_source: BTreeMap::new(),
        primary_source: ActivitySourceKind::Window,
    })
    .expect("activity.summary.hourly payload serializes")
}

/// Repro from the bead: persist a daily rollup with an occurrence-stable
/// bucket key, then re-emit the SAME bucket with changed aggregate content
/// (as a post-supersession recompute of the same day would produce). Before
/// this bead, `ActivityDailySummaryPayload` defaulted to `SuppressDuplicate`
/// so the changed re-emit was silently discarded, leaving the stored rollup
/// stale forever. It must now supersede.
#[sinex_test]
async fn daily_summary_supersede_on_change_changed_content_returns_superseded(
    ctx: TestContext,
) -> TestResult<()> {
    let material_id = ctx.create_source_material(Some("74yj-daily-supersede-changed")).await?;
    let ts = Timestamp::now();
    let day_id = "activity-day-74yj-changed".to_string();
    let key = day_id.clone();
    let service = admission_service(&ctx);

    let live_id = Uuid::now_v7();
    let mut live = material_event(
        material_id,
        live_id,
        "derived.daily-summarizer",
        "activity.summary.daily",
        daily_summary_payload(ts, &day_id, 10),
    )?;
    live.equivalence_key = Some(key.clone());
    let persisted_id = admit_and_persist(&service, live).await?;
    assert_eq!(persisted_id, live_id);

    // Recomputed totals for the SAME day bucket: same key, changed content.
    let revision_id = Uuid::now_v7();
    let mut revision = material_event(
        material_id,
        revision_id,
        "derived.daily-summarizer",
        "activity.summary.daily",
        daily_summary_payload(ts, &day_id, 42),
    )?;
    revision.equivalence_key = Some(key.clone());

    match service.admit_event(revision).await? {
        AdmissionDecision::Superseded {
            admitted,
            superseded_event_id,
        } => {
            assert_eq!(
                superseded_event_id, live_id,
                "the live rollup interpretation is the supersession target"
            );
            assert_eq!(admitted.event_id, revision_id, "the revision is admitted");
        }
        other => panic!(
            "changed-content re-emit of a rollup bucket must supersede, not {other:?} \
             (activity.summary.daily is expected to opt into SupersedeOnChange)"
        ),
    }

    Ok(())
}

/// Identical-content re-emit for the same day bucket (e.g. a harmless
/// re-run that recomputes the exact same totals) must still suppress, not
/// supersede -- SupersedeOnChange only fires on an actual content change.
#[sinex_test]
async fn daily_summary_supersede_on_change_identical_content_suppresses(
    ctx: TestContext,
) -> TestResult<()> {
    let material_id = ctx.create_source_material(Some("74yj-daily-supersede-identical")).await?;
    let ts = Timestamp::now();
    let day_id = "activity-day-74yj-identical".to_string();
    let key = day_id.clone();
    let service = admission_service(&ctx);

    let live_id = Uuid::now_v7();
    let mut live = material_event(
        material_id,
        live_id,
        "derived.daily-summarizer",
        "activity.summary.daily",
        daily_summary_payload(ts, &day_id, 10),
    )?;
    live.equivalence_key = Some(key.clone());
    admit_and_persist(&service, live).await?;

    let repeat_id = Uuid::now_v7();
    let mut repeat = material_event(
        material_id,
        repeat_id,
        "derived.daily-summarizer",
        "activity.summary.daily",
        daily_summary_payload(ts, &day_id, 10),
    )?;
    repeat.equivalence_key = Some(key.clone());

    match service.admit_event(repeat).await? {
        AdmissionDecision::Suppressed(rejection) => {
            assert_eq!(rejection.kind, AdmissionRejectionKind::OccurrenceDuplicate);
        }
        other => panic!("identical-content rollup re-emit must suppress, not {other:?}"),
    }

    Ok(())
}

/// Same contract, hourly rollup: a changed-content re-emit for the same hour
/// bucket must supersede. Cross-checked against the daily test above so the
/// fix is proven for both opted-in rollup types, not just one call site.
#[sinex_test]
async fn hourly_summary_supersede_on_change_changed_content_returns_superseded(
    ctx: TestContext,
) -> TestResult<()> {
    let material_id = ctx.create_source_material(Some("74yj-hourly-supersede-changed")).await?;
    let ts = Timestamp::now();
    let hour_id = "activity-hour-74yj-changed".to_string();
    let key = hour_id.clone();
    let service = admission_service(&ctx);

    let live_id = Uuid::now_v7();
    let mut live = material_event(
        material_id,
        live_id,
        "derived.hourly-summarizer",
        "activity.summary.hourly",
        hourly_summary_payload(ts, &hour_id, 10),
    )?;
    live.equivalence_key = Some(key.clone());
    let persisted_id = admit_and_persist(&service, live).await?;
    assert_eq!(persisted_id, live_id);

    let revision_id = Uuid::now_v7();
    let mut revision = material_event(
        material_id,
        revision_id,
        "derived.hourly-summarizer",
        "activity.summary.hourly",
        hourly_summary_payload(ts, &hour_id, 42),
    )?;
    revision.equivalence_key = Some(key.clone());

    match service.admit_event(revision).await? {
        AdmissionDecision::Superseded {
            admitted,
            superseded_event_id,
        } => {
            assert_eq!(superseded_event_id, live_id);
            assert_eq!(admitted.event_id, revision_id);
        }
        other => panic!(
            "changed-content re-emit of an hourly rollup bucket must supersede, not {other:?}"
        ),
    }

    Ok(())
}

// ─── sinex-h3g: ActivityWatch grown-row supersession ───────────────────────
//
// AW's `aw-server-rust` extends the newest event row per bucket in place via
// heartbeat merging (`endtime`, and therefore `duration_ms`, grows without a
// new SQLite row). `ActivityWatchParser::parse_record` keys its occurrence
// identity on `bucket_id` + the START timestamp only (never `endtime`), so a
// re-read of the same row after it has grown carries the SAME
// `equivalence_key` here. `duration_ms` is the content knob: two calls with
// the same value are byte-for-byte identical content (an unmodified re-read),
// a larger value is the genuine growth a heartbeat produces.

fn window_active_payload(duration_ms: u64) -> JsonValue {
    serde_json::to_value(sinex_primitives::events::payloads::ActivityWatchWindowActivePayload {
        app: "kitty".to_string(),
        title: "sinex-h3g".to_string(),
        duration_ms,
        bucket_id: "aw-watcher-window_test-host".to_string(),
    })
    .expect("window.active payload serializes")
}

/// Reproduces the bead: an initial short-duration read followed by a
/// re-read of the SAME (start-anchored) occurrence after AW's heartbeat grew
/// the row's `endtime`. Before this fix, `window.active` defaulted to
/// `SuppressDuplicate` (silently dropping the grown re-read) AND the parser
/// never set `occurrence_key` at all (so no dedup path fired in either
/// direction). It must now supersede, leaving exactly the grown duration
/// live.
#[sinex_test]
async fn activitywatch_window_active_supersede_on_change_grown_duration_returns_superseded(
    ctx: TestContext,
) -> TestResult<()> {
    let material_id = ctx.create_source_material(Some("h3g-aw-supersede-grown")).await?;
    let key = "desktop.activitywatch|bucket_id=aw-watcher-window_test-host|event_timestamp=2026-07-01T00:00:00Z".to_string();
    let service = admission_service(&ctx);

    // Initial scan: the row read at heartbeat-merge time N, duration 30s.
    let live_id = Uuid::now_v7();
    let mut live = material_event(
        material_id,
        live_id,
        "activitywatch",
        "window.active",
        window_active_payload(30_000),
    )?;
    live.equivalence_key = Some(key.clone());
    let persisted_id = admit_and_persist(&service, live).await?;
    assert_eq!(persisted_id, live_id);

    // Re-scan after a heartbeat grew `endtime`: SAME occurrence key (start
    // anchor unchanged), duration grown from 30s to 300s.
    let revision_id = Uuid::now_v7();
    let mut revision = material_event(
        material_id,
        revision_id,
        "activitywatch",
        "window.active",
        window_active_payload(300_000),
    )?;
    revision.equivalence_key = Some(key.clone());

    match service.admit_event(revision).await? {
        AdmissionDecision::Superseded {
            admitted,
            superseded_event_id,
        } => {
            assert_eq!(
                superseded_event_id, live_id,
                "the short-duration read is the supersession target"
            );
            assert_eq!(
                admitted.event_id, revision_id,
                "the grown-duration revision is admitted"
            );
        }
        other => panic!(
            "a grown re-read of the same AW occurrence must supersede, not {other:?} \
             (window.active is expected to opt into SupersedeOnChange, sinex-h3g)"
        ),
    }

    Ok(())
}

/// An unmodified re-read of the same row (e.g. a poll cycle that lands
/// between heartbeats, so nothing actually grew) must suppress, not
/// supersede — a bare re-observation is not a revision.
#[sinex_test]
async fn activitywatch_window_active_supersede_on_change_unmodified_reread_suppresses(
    ctx: TestContext,
) -> TestResult<()> {
    let material_id = ctx.create_source_material(Some("h3g-aw-supersede-identical")).await?;
    let key = "desktop.activitywatch|bucket_id=aw-watcher-window_test-host|event_timestamp=2026-07-01T00:05:00Z".to_string();
    let service = admission_service(&ctx);

    let live_id = Uuid::now_v7();
    let mut live = material_event(
        material_id,
        live_id,
        "activitywatch",
        "window.active",
        window_active_payload(30_000),
    )?;
    live.equivalence_key = Some(key.clone());
    admit_and_persist(&service, live).await?;

    let repeat_id = Uuid::now_v7();
    let mut repeat = material_event(
        material_id,
        repeat_id,
        "activitywatch",
        "window.active",
        window_active_payload(30_000),
    )?;
    repeat.equivalence_key = Some(key.clone());

    match service.admit_event(repeat).await? {
        AdmissionDecision::Suppressed(rejection) => {
            assert_eq!(rejection.kind, AdmissionRejectionKind::OccurrenceDuplicate);
        }
        other => panic!("unmodified AW re-read must suppress, not {other:?}"),
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// sinex-audit-h3g-atuin-browser: the same settling-window bug in
// terminal.atuin-history and browser.history (Chromium leg). Atuin/Chromium
// mutate the SAME row (`rowid`) in place after this parser's first read
// (Atuin: exit/duration filled in on command completion; Chromium:
// visit_duration filled in on the next same-tab navigation) instead of
// inserting a new row, so the occurrence key never changes across the
// mutation. `AtuinCommandExecutedPayload` / `PageVisitedPayload` opt into
// `RevisionPolicy::SupersedeOnChange` for exactly the same reason
// `ActivityWatchWindowActivePayload` does above.
// ---------------------------------------------------------------------------

fn atuin_command_payload(exit_code: i64, duration_ns: i64) -> JsonValue {
    serde_json::to_value(
        sinex_primitives::events::payloads::AtuinCommandExecutedPayload::from_raw_history(
            "echo sinex-audit-h3g-atuin-browser",
            sinex_primitives::domain::RecordedPath::from_observed("/home/test").unwrap(),
            exit_code,
            duration_ns,
            "atuin-history-1",
            "atuin-session-1",
            1_772_000_000_000_000_000,
            "test-host",
        )
        .expect("atuin payload constructs"),
    )
    .expect("command.executed payload serializes")
}

/// Reproduces the bead for Atuin: an initial in-flight read (Atuin's
/// `#[default = "0"]` exit/duration, since the row was inserted at command
/// START before Atuin knows either) followed by a re-read of the SAME
/// `rowid`-anchored occurrence after Atuin's completion UPDATE filled in the
/// real exit code and duration. Must supersede, leaving exactly the
/// completed values live.
#[sinex_test]
async fn atuin_command_executed_supersede_on_change_completion_returns_superseded(
    ctx: TestContext,
) -> TestResult<()> {
    let material_id = ctx
        .create_source_material(Some("h3g-atuin-supersede-completion"))
        .await?;
    let key = "terminal.atuin-history|rowid=42".to_string();
    let service = admission_service(&ctx);

    // Initial scan: row read the instant it was inserted, before Atuin's
    // completion UPDATE — exit/duration at their pre-completion defaults.
    let live_id = Uuid::now_v7();
    let mut live = material_event(
        material_id,
        live_id,
        "shell.atuin",
        "command.executed",
        atuin_command_payload(0, 0),
    )?;
    live.equivalence_key = Some(key.clone());
    let persisted_id = admit_and_persist(&service, live).await?;
    assert_eq!(persisted_id, live_id);

    // Re-scan after Atuin's completion UPDATE: SAME rowid-anchored
    // occurrence, real exit code and duration now present.
    let revision_id = Uuid::now_v7();
    let mut revision = material_event(
        material_id,
        revision_id,
        "shell.atuin",
        "command.executed",
        atuin_command_payload(1, 250_000_000),
    )?;
    revision.equivalence_key = Some(key.clone());

    match service.admit_event(revision).await? {
        AdmissionDecision::Superseded {
            admitted,
            superseded_event_id,
        } => {
            assert_eq!(
                superseded_event_id, live_id,
                "the pre-completion read is the supersession target"
            );
            assert_eq!(
                admitted.event_id, revision_id,
                "the completed exit_code/duration_ns revision is admitted"
            );
        }
        other => panic!(
            "a post-completion re-read of the same Atuin occurrence must supersede, not \
             {other:?} (command.executed is expected to opt into SupersedeOnChange, \
             sinex-audit-h3g-atuin-browser)"
        ),
    }

    Ok(())
}

/// An unmodified re-read of the same Atuin row (e.g. a poll cycle landing
/// before the command finished) must suppress, not supersede.
#[sinex_test]
async fn atuin_command_executed_supersede_on_change_unmodified_reread_suppresses(
    ctx: TestContext,
) -> TestResult<()> {
    let material_id = ctx
        .create_source_material(Some("h3g-atuin-supersede-identical"))
        .await?;
    let key = "terminal.atuin-history|rowid=43".to_string();
    let service = admission_service(&ctx);

    let live_id = Uuid::now_v7();
    let mut live = material_event(
        material_id,
        live_id,
        "shell.atuin",
        "command.executed",
        atuin_command_payload(0, 0),
    )?;
    live.equivalence_key = Some(key.clone());
    admit_and_persist(&service, live).await?;

    let repeat_id = Uuid::now_v7();
    let mut repeat = material_event(
        material_id,
        repeat_id,
        "shell.atuin",
        "command.executed",
        atuin_command_payload(0, 0),
    )?;
    repeat.equivalence_key = Some(key.clone());

    match service.admit_event(repeat).await? {
        AdmissionDecision::Suppressed(rejection) => {
            assert_eq!(rejection.kind, AdmissionRejectionKind::OccurrenceDuplicate);
        }
        other => panic!("unmodified Atuin re-read must suppress, not {other:?}"),
    }

    Ok(())
}

/// Fixed content-hash-relevant `visit_time` — the two reads of the same
/// visit row share the same Chromium `visits.visit_time`, so this must stay
/// constant across a test's initial + re-read payloads. Using
/// `Timestamp::now()` per call would make every call site differ in
/// `visit_time` alone, spuriously changing content hash regardless of
/// `visit_duration_ms`.
fn fixed_visit_time() -> Timestamp {
    Timestamp::from_unix_timestamp_nanos(1_772_000_000_000_000_000).expect("valid timestamp")
}

fn page_visited_payload(visit_duration_ms: Option<u64>) -> JsonValue {
    serde_json::to_value(sinex_primitives::events::payloads::PageVisitedPayload {
        browser: "chromium".to_string(),
        title: "sinex-audit-h3g-atuin-browser".to_string(),
        url: "https://example.invalid/".to_string(),
        normalized_url: None,
        visit_time: fixed_visit_time(),
        referrer: None,
        transition: Some("0".to_string()),
        visit_id: Some("7".to_string()),
        visit_duration_ms,
        source_file: "browser.history.sqlite-1".to_string(),
        line_number: None,
        db_row_id: Some(7),
    })
    .expect("page.visited payload serializes")
}

/// Reproduces the bead for Chromium browser history: an initial read with no
/// `visit_duration` (Chromium hasn't navigated away from the tab yet)
/// followed by a re-read of the SAME `visit_id`-anchored occurrence after
/// Chromium's next-navigation UPDATE filled it in. Must supersede, leaving
/// exactly the finalized duration live.
#[sinex_test]
async fn browser_history_page_visited_supersede_on_change_finalized_duration_returns_superseded(
    ctx: TestContext,
) -> TestResult<()> {
    let material_id = ctx
        .create_source_material(Some("h3g-browser-supersede-finalized"))
        .await?;
    let key = "browser.history|browser=chromium|source_file=browser.history.sqlite-1|visit_id=7"
        .to_string();
    let service = admission_service(&ctx);

    // Initial scan: visit row read before the tab navigated away, so
    // Chromium has not yet finalized `visit_duration`.
    let live_id = Uuid::now_v7();
    let mut live = material_event(
        material_id,
        live_id,
        "webhistory",
        "page.visited",
        page_visited_payload(None),
    )?;
    live.equivalence_key = Some(key.clone());
    let persisted_id = admit_and_persist(&service, live).await?;
    assert_eq!(persisted_id, live_id);

    // Re-scan after the next same-tab navigation finalized the row's
    // `visit_duration`: SAME visit_id-anchored occurrence.
    let revision_id = Uuid::now_v7();
    let mut revision = material_event(
        material_id,
        revision_id,
        "webhistory",
        "page.visited",
        page_visited_payload(Some(45_000)),
    )?;
    revision.equivalence_key = Some(key.clone());

    match service.admit_event(revision).await? {
        AdmissionDecision::Superseded {
            admitted,
            superseded_event_id,
        } => {
            assert_eq!(
                superseded_event_id, live_id,
                "the pre-finalization read is the supersession target"
            );
            assert_eq!(
                admitted.event_id, revision_id,
                "the finalized visit_duration_ms revision is admitted"
            );
        }
        other => panic!(
            "a post-finalization re-read of the same Chromium visit must supersede, not \
             {other:?} (page.visited is expected to opt into SupersedeOnChange, \
             sinex-audit-h3g-atuin-browser)"
        ),
    }

    Ok(())
}

/// An unmodified re-read of the same visit row (e.g. a poll cycle landing
/// before the next navigation) must suppress, not supersede.
#[sinex_test]
async fn browser_history_page_visited_supersede_on_change_unmodified_reread_suppresses(
    ctx: TestContext,
) -> TestResult<()> {
    let material_id = ctx
        .create_source_material(Some("h3g-browser-supersede-identical"))
        .await?;
    let key = "browser.history|browser=chromium|source_file=browser.history.sqlite-1|visit_id=8"
        .to_string();
    let service = admission_service(&ctx);

    let live_id = Uuid::now_v7();
    let mut live = material_event(
        material_id,
        live_id,
        "webhistory",
        "page.visited",
        page_visited_payload(None),
    )?;
    live.equivalence_key = Some(key.clone());
    admit_and_persist(&service, live).await?;

    let repeat_id = Uuid::now_v7();
    let mut repeat = material_event(
        material_id,
        repeat_id,
        "webhistory",
        "page.visited",
        page_visited_payload(None),
    )?;
    repeat.equivalence_key = Some(key.clone());

    match service.admit_event(repeat).await? {
        AdmissionDecision::Suppressed(rejection) => {
            assert_eq!(rejection.kind, AdmissionRejectionKind::OccurrenceDuplicate);
        }
        other => panic!("unmodified browser-history re-read must suppress, not {other:?}"),
    }

    Ok(())
}

/// sinex-audit-intrabatchsuperseed: three revisions of the SAME occurrence
/// key arriving in ONE `EventIntent` (the shape `EventBatcher` produces for
/// occurrence-stable interval sources) must all classify correctly against
/// each other, not just against the pre-batch DB snapshot. Before the fix,
/// `live_by_key` was built once before the loop, so revisions 2 and 3 both
/// classified against the ORIGINAL live row and both tried to archive it a
/// second time -- which `execute_cascade_archive`'s existence pre-check
/// rejects, so both were silently suppressed as an ordinary
/// `OccurrenceDuplicate`, indistinguishable from a legitimate duplicate.
///
/// The fix must also avoid the opposite trap: naively pointing revision 2's
/// archive target at revision 1's (not-yet-persisted) id would fail
/// identically, since revision 1 never reaches `core.events` until after
/// `prepare_events` returns. Only the FINAL revision in a same-key run may
/// carry a real archive target -- the run's original pre-batch live row, if
/// any -- and every earlier same-key sibling in the batch must be dropped
/// under a distinct rejection kind rather than treated as a genuine
/// duplicate.
#[sinex_test]
async fn supersede_on_change_intrabatch_multiple_revisions_only_final_applies(
    ctx: TestContext,
) -> TestResult<()> {
    let material_id = ctx
        .create_source_material(Some("audit-intrabatch-supersede"))
        .await?;
    let ts = Timestamp::now();
    let key = "audit-intrabatch-supersede-key".to_string();
    let service = admission_service(&ctx);

    // Seed the live interpretation, exactly like the single-event supersede
    // test above.
    let live_id = Uuid::now_v7();
    let mut live = material_event(
        material_id,
        live_id,
        "derived.interval-lift",
        "state.interval",
        interval_payload(ts, 100),
    )?;
    live.equivalence_key = Some(key.clone());
    let persisted_id = admit_and_persist(&service, live).await?;
    assert_eq!(persisted_id, live_id);

    // Three same-key revisions land in ONE intent, each a genuine content
    // change from the last.
    let revision_1_id = Uuid::now_v7();
    let mut revision_1 = material_event(
        material_id,
        revision_1_id,
        "derived.interval-lift",
        "state.interval",
        interval_payload(ts, 200),
    )?;
    revision_1.equivalence_key = Some(key.clone());

    let revision_2_id = Uuid::now_v7();
    let mut revision_2 = material_event(
        material_id,
        revision_2_id,
        "derived.interval-lift",
        "state.interval",
        interval_payload(ts, 300),
    )?;
    revision_2.equivalence_key = Some(key.clone());

    let revision_3_id = Uuid::now_v7();
    let mut revision_3 = material_event(
        material_id,
        revision_3_id,
        "derived.interval-lift",
        "state.interval",
        interval_payload(ts, 400),
    )?;
    revision_3.equivalence_key = Some(key.clone());

    let intent = EventIntent::new(
        "test-source",
        "test-parser",
        "1.0.0",
        vec![revision_1, revision_2, revision_3],
        HostName::from_static("test-host"),
    );
    let payload = serde_json::to_vec(&intent)?;

    let decisions = service.admit_intent_bytes(&payload).await?;
    assert_eq!(
        decisions.len(),
        3,
        "all three intent events produce a decision"
    );

    for (idx, decision) in decisions.iter().take(2).enumerate() {
        match decision {
            AdmissionDecision::Suppressed(rejection) => {
                assert_eq!(
                    rejection.kind,
                    AdmissionRejectionKind::SupersededWithinBatch,
                    "revision {idx} loses to a later same-batch revision -- distinct from an \
                     ordinary OccurrenceDuplicate re-emit"
                );
            }
            other => panic!(
                "expected same-key revision {idx} to be demoted within the batch, not persisted \
                 and later fail an archive against an already-archived or not-yet-durable row: \
                 {other:?}"
            ),
        }
    }

    match &decisions[2] {
        AdmissionDecision::Superseded {
            admitted,
            superseded_event_id,
        } => {
            assert_eq!(
                *superseded_event_id, live_id,
                "the final revision must supersede the ORIGINAL pre-batch live row, not an \
                 in-batch sibling that was never persisted"
            );
            assert_eq!(
                admitted.event_id, revision_3_id,
                "the final revision -- not the first -- is the one that goes live"
            );
        }
        other => panic!(
            "expected the final same-key revision to supersede the original live row: {other:?}"
        ),
    }

    Ok(())
}
