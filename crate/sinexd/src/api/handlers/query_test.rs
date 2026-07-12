#![allow(clippy::expect_used)]

use super::*;
use sinex_primitives::domain::{EventSource, EventType, HostName, TemporalSourceType};
use sinex_primitives::{Provenance, SourceMaterial, Timestamp};
use xtask::sandbox::sinex_test;

fn event(id: u128, source: &str, event_type: &str, ts: Timestamp) -> Event<JsonValue> {
    Event {
        id: Some(Id::from_uuid(sinex_primitives::Uuid::from_u128(id))),
        source: EventSource::new(source).expect("fixture source must be valid"),
        event_type: EventType::new(event_type).expect("fixture event type must be valid"),
        payload: json!({ "scope": source }),
        ts_orig: Some(ts),
        ts_quality: Some(TemporalSourceType::RealtimeCapture),
        host: HostName::new("test-host").expect("fixture host must be valid"),
        module_run_id: None,
        payload_schema_id: None,
        provenance: Provenance::from_material(
            Id::<SourceMaterial>::from_uuid(sinex_primitives::Uuid::from_u128(id + 1000)),
            0,
            None,
            None,
        ),
        anchor_payload_hash: None,
        associated_blob_ids: None,
        temporal_policy: None,
        semantics_version: None,
        scope_key: None,
        equivalence_key: None,
        created_by_operation_id: None,
        automaton_model: None,
        product_class: None,
        claim_support: None,
        derivation_declaration_id: None,
        derivation_epoch_id: None,
        derivation_lane_id: None,
        adjudication_event_id: None,
    }
}

#[sinex_test]
async fn relation_evidence_defaults_candidate_query_to_seed_neighborhood()
-> xtask::sandbox::TestResult<()> {
    let seed_ts = Timestamp::now();
    let seed = event(1, "test.seed", "seed.hit", seed_ts);
    let candidate = event(
        2,
        "test.candidate",
        "candidate.hit",
        seed_ts + Duration::seconds(30),
    );
    let req = EventsRelationEvidenceRequest {
        seed_query: EventQuery {
            event_types: vec![EventType::new("seed.hit")?],
            limit: 5,
            ..EventQuery::default()
        },
        candidate_query: None,
        relation: EventRelationExpr::Within { within_secs: 60 },
    };

    let mut calls = 0;
    let envelope = evaluate_relation_evidence_request(req, |query| {
        calls += 1;
        let seed = seed.clone();
        let candidate = candidate.clone();
        async move {
            if calls == 1 {
                assert_eq!(query.limit, 5);
                Ok(vec![seed])
            } else {
                assert_eq!(query.limit, sinex_primitives::Pagination::MAX_LIMIT);
                assert!(
                    query.time_range.is_some(),
                    "default candidate query must bound temporal relations"
                );
                Ok(vec![candidate])
            }
        }
    })
    .await?;

    assert_eq!(calls, 2);
    assert_eq!(envelope.source_surface, "sinexd.events.relation_evidence");
    assert_eq!(envelope.payload.seed_refs.len(), 1);
    assert_eq!(envelope.payload.support_refs.len(), 1);
    assert_eq!(
        envelope
            .query_echo
            .as_ref()
            .and_then(|echo| echo["candidate_query_defaulted"].as_bool()),
        Some(true)
    );
    Ok(())
}

#[sinex_test]
async fn relation_evidence_rejects_aggregation_seed_query() -> xtask::sandbox::TestResult<()> {
    let req = EventsRelationEvidenceRequest {
        seed_query: EventQuery {
            aggregation: Some(sinex_primitives::query::AggregationMode::Count),
            ..EventQuery::default()
        },
        candidate_query: None,
        relation: EventRelationExpr::Overlaps,
    };

    let err = evaluate_relation_evidence_request(req, |_query| async {
        Ok(Vec::<Event<JsonValue>>::new())
    })
    .await
    .expect_err("aggregation query must be rejected");

    assert!(
        err.to_string().contains("seed_query"),
        "error must identify the invalid query field: {err}"
    );
    Ok(())
}

#[sinex_test]
async fn relation_evidence_query_result_requires_events() -> TestResult<()> {
    let err = events_from_query_result(EventQueryResult::Count { count: 7 })
        .expect_err("non-event query result must be rejected");
    assert!(
        err.to_string().contains("event-list"),
        "error must describe the required query result kind: {err}"
    );

    Ok(())
}
