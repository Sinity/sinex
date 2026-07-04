use super::*;
use crate::fmt::render_finite_envelope;
use sinex_primitives::events::{Event, SourceMaterial};
use sinex_primitives::ids::Id;
use sinex_primitives::query::Cursor;
use sinex_primitives::rpc::curation::{
    CurationDuplicateCandidateCluster, CurationDuplicateCandidateEvent,
};
use sinex_primitives::temporal::Timestamp;
use sinex_primitives::views::VIEW_ENVELOPE_SCHEMA_VERSION;
use sinex_primitives::JsonValue;
use xtask::sandbox::sinex_test;

fn empty_proposals() -> EventQueryResult {
    EventQueryResult::Events {
        events: Vec::new(),
        next_cursor: None,
        total_estimate: Some(0),
    }
}

fn duplicate_event() -> CurationDuplicateCandidateEvent {
    CurationDuplicateCandidateEvent {
        event_id: Id::<Event>::new(),
        source_material_id: Id::<SourceMaterial>::new(),
        ts_orig: Timestamp::now(),
    }
}

fn duplicate_query(limit: i64, events_per_cluster: i64) -> CurationDuplicateQueryEcho<'static> {
    CurationDuplicateQueryEcho {
        source: Some("browser.history"),
        event_type: Some("page.visited"),
        limit,
        events_per_cluster,
    }
}

#[sinex_test]
async fn curation_proposals_empty_envelope_names_absent_candidates() -> xtask::TestResult<()> {
    let envelope = curation_proposals_envelope(empty_proposals(), "pending", 100);

    assert_eq!(envelope.source_surface, "sinexctl.semantic.curation.proposals");
    assert_eq!(envelope.query_echo.as_ref().unwrap()["status"], "pending");
    assert_eq!(envelope.caveats.len(), 1);
    assert_eq!(envelope.caveats[0].id, "source.absent");
    assert!(
        envelope.caveats[0]
            .message
            .contains("does not prove there are no curatable observations")
    );
    assert_eq!(
        envelope.caveats[0]
            .ref_
            .as_ref()
            .and_then(|ref_| ref_.command_hint.as_deref()),
        Some("sinexctl semantic curation proposals")
    );
    Ok(())
}

#[sinex_test]
async fn curation_proposals_cursor_marks_partial_window() -> xtask::TestResult<()> {
    let envelope = curation_proposals_envelope(
        EventQueryResult::Events {
            events: Vec::new(),
            next_cursor: Some(Cursor::after_id(Id::<Event<JsonValue>>::new())),
            total_estimate: Some(101),
        },
        "pending",
        100,
    );
    let caveat_ids: Vec<&str> = envelope
        .caveats
        .iter()
        .map(|caveat| caveat.id.as_str())
        .collect();

    assert!(caveat_ids.contains(&"source.absent"));
    assert!(caveat_ids.contains(&"window.partial"));
    Ok(())
}

#[sinex_test]
async fn curation_duplicates_empty_envelope_names_absent_projection() -> xtask::TestResult<()> {
    let envelope = curation_duplicates_envelope(
        CurationListDuplicateCandidatesResponse {
            clusters: Vec::new(),
        },
        duplicate_query(100, 10),
    );
    let rendered = render_finite_envelope(&envelope, OutputFormat::Json)?
        .expect("json renders finite curation duplicate envelope");
    let parsed: serde_json::Value = serde_json::from_str(&rendered)?;

    assert_eq!(parsed["schema_version"], VIEW_ENVELOPE_SCHEMA_VERSION);
    assert_eq!(
        parsed["source_surface"],
        "sinexctl.semantic.curation.duplicates"
    );
    assert_eq!(parsed["query_echo"]["source"], "browser.history");
    assert_eq!(parsed["caveats"][0]["id"], "source.absent");
    Ok(())
}

#[sinex_test]
async fn curation_duplicates_bounded_cluster_marks_partial_window() -> xtask::TestResult<()> {
    let envelope = curation_duplicates_envelope(
        CurationListDuplicateCandidatesResponse {
            clusters: vec![CurationDuplicateCandidateCluster {
                cluster_id: "cluster-1".to_string(),
                source: "browser.history".to_string(),
                event_type: "page.visited".to_string(),
                equivalence_key: "visit-1".to_string(),
                event_count: 3,
                material_count: 2,
                events: vec![duplicate_event()],
            }],
        },
        duplicate_query(1, 1),
    );
    let partial_count = envelope
        .caveats
        .iter()
        .filter(|caveat| caveat.id == "window.partial")
        .count();

    assert_eq!(
        partial_count, 2,
        "cluster-limit and per-cluster event cap should both be explicit"
    );
    Ok(())
}
