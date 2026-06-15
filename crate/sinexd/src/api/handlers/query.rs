//! Composable event query, provenance lineage, and event-annotation handlers.

use serde_json::json;
use sinex_db::repositories::DbPoolExt;
use sinex_primitives::events::Event;
use sinex_primitives::query::{
    EventQuery, EventQueryResult, LineageQuery, LineageResult, TimeRange,
};
use sinex_primitives::relations::{EventRelationExpr, EvidenceWindow, ObservedRange, TimeBasis};
use sinex_primitives::rpc::events::{
    EventsAnnotateRequest, EventsAnnotateResponse, EventsRelationEvidenceRequest,
};
use sinex_primitives::views::{EventCardListView, ViewEnvelope};
use sinex_primitives::{Id, JsonValue, Result, SinexError};
use sqlx::PgPool;
use std::future::Future;
use std::str::FromStr;
use time::Duration;

pub async fn handle_events_query(pool: &PgPool, query: EventQuery) -> Result<EventQueryResult> {
    pool.events().query(query).await
}

pub async fn handle_events_lineage(pool: &PgPool, query: LineageQuery) -> Result<LineageResult> {
    pool.events().lineage(query).await
}

pub async fn handle_events_relation_evidence(
    pool: &PgPool,
    req: EventsRelationEvidenceRequest,
) -> Result<ViewEnvelope<EvidenceWindow>> {
    evaluate_relation_evidence_request(req, |query| async move {
        let result = pool.events().query(query).await?;
        events_from_query_result(result)
    })
    .await
}

/// `events.annotate` (#1172 AC-9): write a typed annotation to
/// `core.event_annotations` against an existing event id.
///
/// Distinct from `sources.annotate` (material-level annotation).
pub async fn handle_events_annotate(
    pool: &PgPool,
    req: EventsAnnotateRequest,
    auth: &crate::api::rpc_server::RpcAuthContext,
) -> Result<EventsAnnotateResponse> {
    let event_id_str = req.event_id.as_str();
    let annotation_type = req.annotation_type.as_str();
    let content = req.content.as_str();
    let metadata = req.metadata.unwrap_or_else(|| json!({}));

    if annotation_type.trim().is_empty() {
        return Err(SinexError::validation(
            "events.annotate: annotation_type must not be empty",
        ));
    }
    if content.trim().is_empty() {
        return Err(SinexError::validation(
            "events.annotate: content must not be empty",
        ));
    }

    let event_uuid = sinex_primitives::Uuid::from_str(event_id_str).map_err(|error| {
        SinexError::validation("events.annotate: invalid event_id UUID")
            .with_context("event_id", event_id_str)
            .with_std_error(&error)
    })?;
    let event_id =
        Id::<sinex_primitives::events::Event<sinex_primitives::JsonValue>>::from_uuid(event_uuid);

    let record = pool
        .events()
        .add_annotation(
            event_id,
            annotation_type,
            content,
            metadata,
            auth.actor_id(),
        )
        .await
        .map_err(|error| {
            SinexError::database("events.annotate: failed to record annotation")
                .with_source(error.to_string())
        })?;

    Ok(EventsAnnotateResponse {
        id: record.id.as_uuid().to_string(),
        event_id: record.event_id.as_uuid().to_string(),
        annotation_type: record.annotation_type,
        content: record.content,
        metadata: record.metadata,
        created_by: record.created_by,
        created_at: record.created_at.format_rfc3339(),
        updated_at: record.updated_at.format_rfc3339(),
    })
}

/// `events.cards` — query events and return them as `EventCardView`s
/// with refs, caveats, privacy state, and action availability preserved.
pub async fn handle_events_cards(pool: &PgPool, query: EventQuery) -> Result<EventCardListView> {
    let result = pool.events().query(query).await?;
    match result {
        EventQueryResult::Events { events, .. } => {
            Ok(EventCardListView::from_query_events(&events))
        }
        other => Err(SinexError::validation(format!(
            "events.cards requires an Events query result, got {:?}",
            std::mem::discriminant(&other)
        ))),
    }
}

async fn evaluate_relation_evidence_request<F, Fut>(
    req: EventsRelationEvidenceRequest,
    mut resolve: F,
) -> Result<ViewEnvelope<EvidenceWindow>>
where
    F: FnMut(EventQuery) -> Fut,
    Fut: Future<Output = Result<Vec<Event<JsonValue>>>>,
{
    validate_relation_event_query(&req.seed_query, "seed_query")?;
    if let Some(candidate_query) = &req.candidate_query {
        validate_relation_event_query(candidate_query, "candidate_query")?;
    }

    let seed_events = resolve(req.seed_query.clone()).await?;
    let candidate_query = req
        .candidate_query
        .clone()
        .unwrap_or_else(|| default_candidate_query(&req.relation, &seed_events));
    let candidate_events = resolve(candidate_query.clone()).await?;

    let window = req.relation.evaluate(&seed_events, &candidate_events);
    Ok(window
        .into_view("sinexd.events.relation_evidence")
        .with_query_echo(json!({
            "seed_query": req.seed_query,
            "candidate_query": candidate_query,
            "candidate_query_defaulted": req.candidate_query.is_none(),
            "relation": req.relation,
        })))
}

fn validate_relation_event_query(query: &EventQuery, field: &'static str) -> Result<()> {
    if query.aggregation.is_some() {
        return Err(SinexError::validation(format!(
            "events.relation_evidence: {field} must select events, not an aggregation"
        )));
    }
    Ok(())
}

fn events_from_query_result(result: EventQueryResult) -> Result<Vec<Event<JsonValue>>> {
    match result {
        EventQueryResult::Events { events, .. } => {
            Ok(events.into_iter().map(|row| row.event).collect())
        }
        other => Err(SinexError::validation(format!(
            "events.relation_evidence requires event-list query results, got {:?}",
            std::mem::discriminant(&other)
        ))),
    }
}

fn default_candidate_query(relation: &EventRelationExpr, seeds: &[Event<JsonValue>]) -> EventQuery {
    let mut query = EventQuery {
        limit: sinex_primitives::Pagination::MAX_LIMIT,
        ..EventQuery::default()
    };

    let seed_range = seeds
        .iter()
        .map(ObservedRange::from_event)
        .reduce(|acc, range| acc.union(&range))
        .unwrap_or_else(|| ObservedRange::unknown(TimeBasis::AtemporalAnchor));

    query.time_range = default_candidate_time_range(relation, seed_range);
    query
}

fn default_candidate_time_range(
    relation: &EventRelationExpr,
    seed_range: ObservedRange,
) -> Option<TimeRange> {
    let start = seed_range.start.or(seed_range.end)?;
    let end = seed_range.end.or(seed_range.start)?;
    let (start, end) = match relation {
        EventRelationExpr::Within { within_secs } => {
            let bound = Duration::seconds(*within_secs);
            (start - bound, end + bound)
        }
        EventRelationExpr::Before { max_gap_secs } => {
            (start - Duration::seconds(*max_gap_secs), end)
        }
        EventRelationExpr::After { max_gap_secs } => {
            (start, end + Duration::seconds(*max_gap_secs))
        }
        EventRelationExpr::Overlaps | EventRelationExpr::Sequence { .. } => (start, end),
        EventRelationExpr::Same { .. } => return None,
    };

    TimeRange::new(Some(start), Some(end)).ok()
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used)]

    use super::*;
    use sinex_primitives::domain::{EventSource, EventType, HostName, TemporalSourceType};
    use sinex_primitives::{OffsetKind, Provenance, SourceMaterial, Timestamp};
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
            provenance: Provenance::Material {
                id: Id::<SourceMaterial>::from_uuid(sinex_primitives::Uuid::from_u128(id + 1000)),
                anchor_byte: 0,
                offset_start: None,
                offset_end: None,
                offset_kind: OffsetKind::Byte,
            },
            anchor_payload_hash: None,
            associated_blob_ids: None,
            temporal_policy: None,
            semantics_version: None,
            scope_key: None,
            equivalence_key: None,
            created_by_operation_id: None,
            automaton_model: None,
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

    #[test]
    fn relation_evidence_query_result_requires_events() {
        let err = events_from_query_result(EventQueryResult::Count { count: 7 })
            .expect_err("non-event query result must be rejected");
        assert!(
            err.to_string().contains("event-list"),
            "error must describe the required query result kind: {err}"
        );
    }
}
