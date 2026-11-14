use super::*;
const FOCUS: &[SubjectField] = &[
    SubjectField::Text {
        field: "window",
        prefix: "",
    },
    SubjectField::Text {
        field: "class",
        prefix: "class:",
    },
];
const WORKSPACE: &[SubjectField] = &[
    SubjectField::Scalar {
        field: "workspace_id",
        prefix: "workspace:",
    },
    SubjectField::Scalar {
        field: "workspace_name",
        prefix: "workspace:",
    },
];
use crate::{AdmissionContext, NoDerivations};
use sinex_model::event_types::{
    desktop::{WindowFocused, WorkspaceSwitched},
    operator::LogEntry,
};
use sinex_model::{Batch, BatchKind, EventPayload, RecordOutcome, RunId};

const DAY: i64 = 86_400;
fn at(seconds: i64) -> OffsetDateTime {
    OffsetDateTime::from_unix_timestamp(1_800_000_000 + seconds).unwrap()
}
fn focus(seconds: i64, scope: Option<&str>, subject: Option<&str>, class: Option<&str>) -> Event {
    Event::build(
        &WindowFocused {
            scope: scope.map(str::to_owned),
            host: None,
            received_at: Timestamp(at(seconds)),
            window: subject.map(str::to_owned),
            class: class.map(str::to_owned),
            title: Some(format!("observation-{seconds}")),
        },
        "fixture.transition-bounds",
        Origin::Record {
            location: RecordLocation {
                material: MaterialId::new(),
                anchor: 0,
            },
            output_index: 0,
        },
        ReadingOrder(seconds.max(0)),
        *blake3::hash(&seconds.to_le_bytes()).as_bytes(),
        Class::Open,
    )
    .unwrap()
}
async fn admit(conn: &mut AdmissionContext, events: Vec<Event>) {
    let mut tx = conn.begin().await.unwrap();
    tx.admit(
        &Batch {
            run: RunId::new(),
            kind: BatchKind::Api {
                actor: "fixture".into(),
            },
            records: vec![RecordOutcome::Events {
                location: None,
                events,
            }],
            archive: vec![],
        },
        &NoDerivations,
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
}
fn kinds(scope: Option<&str>) -> Vec<Kind> {
    vec![Kind {
        event_type: WindowFocused::EVENT_TYPE.into(),
        contains: scope.map_or_else(
            || serde_json::json!({}),
            |scope| serde_json::json!({"scope": scope}),
        ),
    }]
}
#[devkit::test]
async fn set_bounds_keep_prior_refreshes_late_closers_and_live_membership(
    db: devkit::pg::TestDatabase,
) {
    let mut conn = AdmissionContext::connect(&db.url(None)).await.unwrap();
    let archived = focus(DAY + 1, Some("alpha"), Some("changed"), None);
    let retracted = focus(800, Some("retracted"), Some("a"), None);
    let archived_ids = vec![archived.id, retracted.id];
    let mut events = vec![
        focus(-10_000, Some("alpha"), Some("a"), None),
        focus(-10, Some("alpha"), Some("a"), None),
        focus(100, Some("alpha"), Some("a"), None),
        archived,
        retracted,
        focus(30 * DAY, Some("alpha"), Some("b"), None),
        focus(-20, Some("beta"), Some("c"), None),
        focus(200, Some("beta"), Some("d"), None),
        focus(DAY + 20, Some("beta"), Some("d"), None),
        focus(DAY + 200, Some("beta"), Some("e"), None),
        focus(300, None, Some("unknown-scope"), None),
        focus(301, None, Some("unknown-scope"), None),
        focus(-100, Some("untouched"), Some("x"), None),
        focus(40 * DAY, Some("untouched"), Some("y"), None),
        focus(400, Some("no-subject"), None, None),
        focus(DAY + 50, Some("no-subject"), None, None),
        focus(500, Some("empty-window"), Some(""), Some("first")),
        focus(DAY + 60, Some("empty-window"), Some(""), Some("second")),
        focus(600, Some("class-only"), None, Some("first")),
        focus(DAY + 70, Some("class-only"), None, Some("second")),
    ];
    events.extend((1..=130).map(|n| focus(DAY + n, Some("alpha"), Some("a"), None)));
    admit(&mut conn, events).await;
    crate::lifecycle::move_events(
        &mut conn,
        sinex_model::lifecycle::Act::Archive,
        archived_ids,
        &NoDerivations,
    )
    .await;
    // The recorded sequence wins over reversed commit order at one time.
    let mut a = focus(700, Some("tie"), Some("a"), None);
    a.reading_order = ReadingOrder(20);
    let mut b = focus(700, Some("tie"), Some("b"), None);
    b.reading_order = ReadingOrder(10);
    admit(&mut conn, vec![a]).await;
    admit(&mut conn, vec![b]).await;
    admit(
        &mut conn,
        vec![
            focus(DAY + 5, Some("tie"), Some("a"), None),
            focus(DAY + 15, Some("tie"), Some("b"), None),
        ],
    )
    .await;
    for (scope, from, until) in [
        (Some("alpha"), -10, 30 * DAY),
        (Some("beta"), -20, DAY + 200),
        (Some("no-subject"), 0, DAY + 50),
        (Some("empty-window"), 0, DAY + 60),
        (Some("class-only"), 0, DAY + 70),
        (Some("tie"), 0, DAY + 15),
        (Some("absent"), 0, DAY),
        (Some("retracted"), 0, DAY),
        (None, -20, 30 * DAY),
    ] {
        let selected = kinds(scope);
        let expected = (at(from), at(until) + time::Duration::microseconds(1));
        assert_eq!(
            bounds(&mut conn, &selected, at(0), at(DAY), FOCUS, &[])
                .await
                .unwrap(),
            expected
        );
    }
    let mut selected = kinds(None);
    selected.push(selected[0].clone());
    assert_eq!(
        bounds(&mut conn, &selected, at(0), at(DAY), FOCUS, &[])
            .await
            .unwrap(),
        (at(-20), at(30 * DAY) + time::Duration::microseconds(1))
    );
    assert_eq!(
        bounds(&mut conn, &[], at(0), at(DAY), FOCUS, &[])
            .await
            .unwrap(),
        (at(0), at(DAY) + time::Duration::microseconds(1))
    );
}

#[devkit::test]
async fn set_bounds_keep_workspace_fallback_and_exclude_unknown_event_time(
    db: devkit::pg::TestDatabase,
) {
    let mut conn = AdmissionContext::connect(&db.url(None)).await.unwrap();
    let mut events = Vec::new();
    for (seconds, id, name) in [
        (1, None, "first"),
        (DAY + 1, None, "first"),
        (DAY + 2, Some(2), "second"),
    ] {
        events.push(
            Event::build(
                &WorkspaceSwitched {
                    scope: Some("workspace".into()),
                    host: None,
                    received_at: Timestamp(at(seconds)),
                    workspace_id: id,
                    workspace_name: Some(name.into()),
                },
                "fixture.transition-bounds",
                Origin::Record {
                    location: RecordLocation {
                        material: MaterialId::new(),
                        anchor: 0,
                    },
                    output_index: 0,
                },
                ReadingOrder(seconds),
                [1; 32],
                Class::Open,
            )
            .unwrap(),
        );
    }
    events.push(
        Event::build(
            &LogEntry {
                written_at: None,
                label: None,
                text: "unknown original time".into(),
            },
            "fixture.transition-bounds",
            Origin::Record {
                location: RecordLocation {
                    material: MaterialId::new(),
                    anchor: 0,
                },
                output_index: 0,
            },
            ReadingOrder(1),
            [2; 32],
            Class::Open,
        )
        .unwrap(),
    );
    admit(&mut conn, events).await;
    let selected = vec![
        Kind {
            event_type: WorkspaceSwitched::EVENT_TYPE.into(),
            contains: serde_json::json!({}),
        },
        Kind {
            event_type: LogEntry::EVENT_TYPE.into(),
            contains: serde_json::json!({}),
        },
    ];
    let expected = (at(0), at(DAY + 2) + time::Duration::microseconds(1));
    assert_eq!(
        bounds(&mut conn, &selected, at(0), at(DAY), WORKSPACE, &[])
            .await
            .unwrap(),
        expected
    );
}

fn scan_work(node: &Value) -> f64 {
    let loops = node["Actual Loops"].as_f64().unwrap_or(0.);
    let own = if node["Node Type"]
        .as_str()
        .is_some_and(|kind| kind.contains("Scan"))
    {
        (node["Actual Rows"].as_f64().unwrap_or(0.)
            + node["Rows Removed by Filter"].as_f64().unwrap_or(0.)
            + node["Rows Removed by Index Recheck"].as_f64().unwrap_or(0.))
            * loops
    } else {
        0.
    };
    own + node["Rows Removed by Join Filter"].as_f64().unwrap_or(0.) * loops
        + node["Plans"]
            .as_array()
            .into_iter()
            .flatten()
            .map(scan_work)
            .sum::<f64>()
}

#[devkit::test]
async fn set_bounds_do_not_repeat_history_for_each_scope(db: devkit::pg::TestDatabase) {
    let mut conn = AdmissionContext::connect(&db.url(None)).await.unwrap();
    let mut events = Vec::new();
    for n in 0..128 {
        let scope = format!("scope-{n}");
        for (seconds, subject) in [(-1, "a"), (1, "a"), (DAY + 1, "b")] {
            events.push(focus(seconds, Some(&scope), Some(subject), None));
        }
        events.push(focus(2, None, Some("unknown-scope"), None));
    }
    admit(&mut conn, events).await;
    let selected = kinds(None);
    let (types, contains) = kind_arrays(&selected);
    let fields: Vec<String> = state_transition::payload_fields(FOCUS)
        .into_iter()
        .map(str::to_owned)
        .collect();
    for (start, end) in [(0, DAY), (2 * DAY, 3 * DAY)] {
        let old: Value = sqlx::query_scalar(&format!(
            "EXPLAIN (ANALYZE, BUFFERS, VERBOSE, FORMAT JSON) {}",
            include_str!("previous.sql")
        ))
        .bind(&types)
        .bind(&contains)
        .bind(at(start))
        .bind(at(end))
        .bind("scope")
        .bind(vec!["window", "class"])
        .bind(at(10 * DAY))
        .fetch_one(&mut *conn)
        .await
        .unwrap();
        let before = scan_work(&old[0]["Plan"]);
        let mut after = 0.;
        for day_only in [true, false] {
            let plan: Value = sqlx::query_scalar(&format!(
                "EXPLAIN (ANALYZE, BUFFERS, VERBOSE, FORMAT JSON) {}",
                include_str!("../sql/state_transition_rows.sql")
            ))
            .bind(&types)
            .bind(&contains)
            .bind(at(start))
            .bind(at(end))
            .bind(&fields)
            .bind(&types)
            .bind(&contains)
            .bind(day_only)
            .fetch_one(&mut *conn)
            .await
            .unwrap();
            after += scan_work(&plan[0]["Plan"]);
            if start != 0 {
                assert_eq!(
                    plan[0]["Plan"]["Actual Rows"].as_f64(),
                    Some(0.),
                    "empty day must not project history: {plan:#}"
                );
            }
        }
        println!(
            "transition metadata start={start}, scan and join-filter tuple examinations: previous={before}, two fixed passes={after}"
        );
        let expected = if start == 0 {
            assert!(
                before > after * 8.,
                "scope count must not multiply history scans: before={before}, after={after}"
            );
            (at(-1), at(DAY + 1) + time::Duration::microseconds(1))
        } else {
            (at(start), at(end) + time::Duration::microseconds(1))
        };
        assert_eq!(
            bounds(&mut conn, &selected, at(start), at(end), FOCUS, &[])
                .await
                .unwrap(),
            expected
        );
    }
}
