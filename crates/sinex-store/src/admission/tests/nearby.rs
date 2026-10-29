//! Exact neighbour-window reuse preserves the affected-key union and freshness.
use super::*;
use crate::derivations::{InputRange, Kind, Window};
use crate::privacy::{self, Action, NewRule};
use std::collections::BTreeSet;

const NAME: &str = "fixture.nearby_union";
fn at(second: i64) -> Timestamp {
    Timestamp::from_unix_secs(1_700_000_000 + second).unwrap()
}
fn command(id: u8, scope: &str, second: i64, trigger: bool, masked: bool) -> Event {
    let template = keyed("fixture.nearby", id, id, 1);
    let mut payload: CommandExecuted = template.payload_as().unwrap();
    payload.session = scope.into();
    payload.started_at = at(second);
    payload.intent = Some(if trigger { "trigger" } else { "candidate" }.into());
    Event::build(
        &payload,
        if masked {
            "fixture.nearby.masked"
        } else {
            "fixture.nearby"
        },
        template.origin,
        ReadingOrder(1),
        *blake3::hash(&serde_json::to_vec(&payload).unwrap()).as_bytes(),
        Class::Open,
    )
    .unwrap()
}
fn window(scope: &str, start: i64, end: i64) -> Window {
    Window {
        kinds: vec![Kind {
            event_type: "command.executed".into(),
            contains: json!({"session":scope,"intent":"candidate"}),
        }],
        start: Some(at(start).0),
        end: Some(at(end).0),
        range: None,
        unknown_times: false,
        before: vec![],
        after: vec![],
        intervals: vec![],
    }
}
fn connected(gap: i128) -> Window {
    let mut query = window("A", 0, 100);
    query.range = Some(InputRange::Connected {
        kinds: query.kinds.clone(),
        at: at(40).0,
        end_field: "started_at",
        gap_ns: gap * 1_000_000_000,
    });
    query
}
struct Neighbours;
impl DirtyKeys for Neighbours {
    fn keys(&self, input: &KeyInput<'_>) -> Vec<(String, Value)> {
        vec![(NAME.into(), json!({"id":input.payload["native_id"]}))]
    }
    fn nearby_inputs(&self, input: &KeyInput<'_>) -> Vec<Window> {
        if input.payload["intent"] != "trigger" {
            return vec![];
        }
        let first = window("A", 0, 10);
        vec![
            first.clone(),
            first,
            window("B", 0, 10),
            window("A", 80, 86),
            connected(5),
            connected(6),
        ]
    }
}
async fn mark(conn: &mut crate::AdmissionContext, events: Vec<Event>) -> i64 {
    let mut tx = conn.begin().await.unwrap();
    let outcome = tx.admit(&batch(events), &Neighbours).await.unwrap();
    tx.commit().await.unwrap();
    let position = crate::query::current_seq(conn).await.unwrap();
    assert_eq!(outcome.commit_seq.unwrap().1, position);
    position
}
async fn markers(conn: &mut PgConnection) -> BTreeMap<String, i64> {
    sqlx::query_as("SELECT key->>'id',last_dirty_seq FROM derivation.dirty WHERE derivation=$1")
        .bind(NAME)
        .fetch_all(conn)
        .await
        .unwrap()
        .into_iter()
        .collect()
}
fn ids(values: &[u8]) -> BTreeSet<String> {
    values.iter().map(|id| format!("k{id}")).collect()
}

#[devkit::test]
async fn exact_neighbour_windows_preserve_scope_union_and_refresh_between_calls(
    db: devkit::pg::TestDatabase,
) {
    let mut conn = crate::AdmissionContext::connect(&db.url(None))
        .await
        .unwrap();
    privacy::add_rule(
        &mut conn,
        &NewRule {
            name: "synthetic neighbour scope projection".into(),
            source: "fixture.nearby.masked".into(),
            event_type: Some("command.executed".into()),
            field: Some("/session".into()),
            matcher: "all".into(),
            matcher_arg: None,
            class: Class::Open,
            action: Action::Mask,
            author: "fixture".into(),
        },
    )
    .await
    .unwrap();
    privacy::projection::ensure_current(&mut conn)
        .await
        .unwrap();
    let masked = command(30, "A", 4, false, true);
    let masked_id = masked.id;
    let mut candidates: Vec<_> = [
        (1, "A", 0),
        (2, "A", 4),
        (3, "A", 8),
        (4, "A", 14),
        (5, "A", 40),
        (6, "A", 44),
        (7, "A", 49),
        (8, "A", 65),
        (9, "A", 80),
        (10, "A", 84),
        (11, "A", 90),
        (12, "B", 0),
        (13, "B", 4),
        (14, "B", 8),
        (15, "B", 80),
        (16, "C", 4),
    ]
    .into_iter()
    .map(|(id, scope, second)| command(id, scope, second, false, false))
    .collect();
    let old = candidates[0].id;
    candidates.push(masked);
    admit_one(&mut conn, &batch(candidates)).await;
    let projected: String =
        sqlx::query_scalar("SELECT payload->>'session' FROM core.derivation_events WHERE id=$1")
            .bind(masked_id.0)
            .fetch_one(&mut conn)
            .await
            .unwrap();
    assert_ne!(
        projected, "A",
        "the raw matching record is outside current projected scope"
    );

    let first = mark(
        &mut conn,
        vec![
            command(200, "trigger", 200, true, false),
            command(201, "trigger", 201, true, false),
            command(202, "trigger", 202, true, false),
        ],
    )
    .await;
    // A0/4/8, B0/4/8, A80/84, and the A40/44/49 connected component.
    // Gap5 alone stops before49; gap6 must remain a distinct query. Neither
    // connected component reaches A14/A65, and no B80 or C4 is selected.
    let first_expected = ids(&[1, 2, 3, 5, 6, 7, 9, 10, 12, 13, 14, 200, 201, 202]);
    let first_markers = markers(&mut conn).await;
    assert_eq!(
        first_markers.keys().cloned().collect::<BTreeSet<_>>(),
        first_expected
    );
    assert!(first_markers.values().all(|position| *position == first));

    // Change membership outside the marker invocation, through normal
    // admission/projection and archival. No dirty table reset hides old marks.
    admit_one(&mut conn, &batch(vec![command(50, "A", 6, false, false)])).await;
    let mut hide = batch(vec![]);
    hide.archive.push(ArchiveRequest::Hide {
        target: old,
        operation: OperationId::new(),
    });
    admit_one(&mut conn, &hide).await;
    let second = mark(
        &mut conn,
        vec![
            command(203, "trigger", 203, true, false),
            command(204, "trigger", 204, true, false),
        ],
    )
    .await;
    assert!(second > first);
    let second_expected = ids(&[2, 3, 5, 6, 7, 9, 10, 12, 13, 14, 50, 203, 204]);
    let current = markers(&mut conn).await;
    let remarked: BTreeSet<_> = current
        .iter()
        .filter(|(_, position)| **position == second)
        .map(|(key, _)| key.clone())
        .collect();
    assert_eq!(
        remarked, second_expected,
        "the same window descriptions read new projected membership at the new frontier"
    );
    assert_eq!(
        current["k1"], first,
        "the archived neighbour is not selected again"
    );
    assert_eq!(
        current.keys().cloned().collect::<BTreeSet<_>>(),
        first_expected.union(&second_expected).cloned().collect(),
        "direct trigger keys and all distinct-window unions are preserved without extra scopes"
    );
}
