use sinex_db::DbPoolExt;
use sinex_primitives::{
    AdmissionOutcome, AdmissionOutcomeRef, DynamicPayload, Id, JsonValue,
    STANDARD_EVENT_ADMISSION_POLICY_ID, SourceMaterial, Timestamp, Uuid,
    activity::ActivitySourceKind,
    event_contracts::SHELL_HISTORY_COMMAND_IMPORTED_CONTRACT_ID,
    events::Event,
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
