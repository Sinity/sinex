use sinex_db::DbPoolExt;
use sinexd::event_engine::{
    AdmissionDecision, AdmissionRejectionKind, AdmissionService, AdmittedEvent, CandidateEvent,
    CandidateEventMetadata, IngestEventValidator,
};
use sinex_primitives::{DynamicPayload, Id, JsonValue, SourceMaterial, Uuid, events::Event};
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
