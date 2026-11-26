//! A window read over interval kinds returns exactly what the predicate over
//! every stored event of those kinds returns, from the live intervals index.
use super::*;
use crate::{AdmissionContext, NoDerivations};
use serde_json::json;
use sinex_model::clock::{IntervalEnd, IntervalKind};
use sinex_model::event_types::CommandExecuted;
use sinex_model::event_types::derived::{
    IntervalTimeBoundary, IntervalTimeEvidence, StateInterval,
};
use sinex_model::event_types::work::WorkAct;
use sinex_model::lifecycle::Act;
use sinex_model::{Batch, BatchKind, EventPayload, RecordOutcome, RunId, TimeQuality};

fn at(second: i64) -> Timestamp {
    Timestamp::from_unix_secs(1_800_000_000 + second).unwrap()
}

fn event<P: EventPayload>(payload: &P, name: &str) -> Event {
    Event::build(
        payload,
        "fixture.reaching",
        Origin::Record {
            location: RecordLocation {
                material: MaterialId::new(),
                record_offset: 0,
            },
            output_index: 0,
        },
        ReadingOrder(1),
        *blake3::hash(name.as_bytes()).as_bytes(),
        Class::Open,
    )
    .unwrap()
}

/// A state interval of `state` from `start`, open without an `end`.
fn state(name: &str, state: &str, start: i64, end: Option<i64>) -> Event {
    event(
        &StateInterval {
            interval_id: name.into(),
            state_kind: state.into(),
            subject_id: None,
            label: Some(name.into()),
            started_at: at(start),
            ended_at: end.map(at),
            started_at_evidence: Some(IntervalTimeEvidence {
                parent: EventId::new(),
                boundary: IntervalTimeBoundary::Transition,
                quality: TimeQuality::Intrinsic,
            }),
            ended_at_evidence: None,
            fenced_at: None,
            start_event_type: "fixture.transition".into(),
            end_event_type: None,
            attributes: Default::default(),
        },
        name,
    )
}

/// A state interval of `state` from `start` whose end is unknown and bounded
/// at `fence`.
fn fenced(name: &str, state_kind: &str, start: i64, fence: i64) -> Event {
    let mut event = state(name, state_kind, start, None);
    event.payload["fenced_at"] = json!(at(fence));
    event
}

/// A command from `start` lasting `duration_ns`, an instant without one.
fn command(name: &str, start: i64, duration_ns: Option<i64>) -> Event {
    event(
        &CommandExecuted {
            native_id: name.into(),
            command: name.into(),
            cwd: "/".into(),
            session: "s".into(),
            hostname: "h".into(),
            started_at: at(start),
            duration_ns,
            exit_code: Some(0),
            deleted_at: None,
            author: None,
            intent: None,
            shell: None,
            extra: Default::default(),
        },
        name,
    )
}

/// A work act at `start`, an instant without an `end`.
fn act(name: &str, start: i64, end: Option<i64>) -> Event {
    let cited = json!({"event": EventId::new(), "record": name, "event_type": "fixture.entry", "source": "fixture"});
    let payload: WorkAct = serde_json::from_value(json!({
        "act": name, "session": "s", "episode": null, "objective": null, "label": null,
        "label_source": cited, "machine": null, "at": at(start),
        "at_evidence": {"quality": "intrinsic", "precision": "instant", "until": null},
        "ended_at": end.map(at), "evidence": cited, "agency": "operator", "action": "edit",
        "path": null,
    }))
    .unwrap();
    event(&payload, name)
}

/// The window is seconds 1000 to 4600. Names say what each interval does.
fn fixture() -> Vec<Event> {
    let microsecond_before_start = at(1000).unix_ns() - 1_000;
    let mut just_short = state(
        "focus ends a microsecond before the window",
        "desktop.focus",
        700,
        None,
    );
    just_short.payload["ended_at"] =
        json!(Timestamp::from_unix_ns(microsecond_before_start).unwrap());
    vec![
        state(
            "focus over before the window",
            "desktop.focus",
            100,
            Some(200),
        ),
        state(
            "focus reaching into the window",
            "desktop.focus",
            500,
            Some(1500),
        ),
        state(
            "focus ending at the window start",
            "desktop.focus",
            600,
            Some(1000),
        ),
        just_short,
        state(
            "focus open since long ago",
            "desktop.focus",
            -90_000_000,
            None,
        ),
        state("focus inside the window", "desktop.focus", 2000, Some(2100)),
        state(
            "focus opening inside the window",
            "desktop.focus",
            3000,
            None,
        ),
        state(
            "focus starting at the window end",
            "desktop.focus",
            4600,
            Some(4700),
        ),
        state("focus after the window", "desktop.focus", 9000, None),
        state(
            "focus spanning the window",
            "desktop.focus",
            900,
            Some(9000),
        ),
        state(
            "focus reaching but archived",
            "desktop.focus",
            800,
            Some(2000),
        ),
        state("focus open but archived", "desktop.focus", 50, None),
        fenced("focus fenced before the window", "desktop.focus", 60, 700),
        fenced("focus fenced inside the window", "desktop.focus", 450, 1200),
        fenced("unit fenced before the window", "systemd.unit", 20, 400),
        fenced("unit fenced at the window start", "systemd.unit", 30, 1000),
        fenced("unit fenced after the window", "systemd.unit", 40, 4800),
        state(
            "unit reaching into the window",
            "systemd.unit",
            300,
            Some(5000),
        ),
        state("unit open since long ago", "systemd.unit", 5, None),
        state("unit inside the window", "systemd.unit", 1200, None),
        command("command running into the window", 990, Some(20_000_000_000)),
        command("command over before the window", 990, Some(5_000_000_000)),
        // A duration is whole microseconds, rounded up, so this one reaches.
        command(
            "command ending within the microsecond before the window",
            999,
            Some(999_999_001),
        ),
        command("command without a duration", 995, None),
        command("command inside the window", 1500, Some(1)),
        act("act reaching into the window", 950, Some(1200)),
        act("act without an end", 960, None),
        act("act inside the window", 1100, None),
    ]
}

fn kind(event_type: &str, contains: Value) -> Kind {
    Kind {
        event_type: event_type.into(),
        contains,
    }
}

fn window(kinds: Vec<Kind>, intervals: &[&str], bounds: Option<(i64, i64)>) -> Window {
    Window {
        range: None,
        unknown_times: false,
        kinds,
        start: bounds.map(|(start, _)| at(start).0),
        end: bounds.map(|(_, end)| at(end).0),
        before: vec![],
        intervals: intervals
            .iter()
            .map(|event_type| IntervalKind::of(event_type).unwrap())
            .collect(),
        after: vec![],
    }
}

/// Windows over the fixture: every kind narrowed and whole, a kind read by
/// its start only, a field no index holds, and the unbounded read.
fn windows() -> Vec<(&'static str, Window)> {
    let focus = || kind("state.interval", json!({"state_kind": "desktop.focus"}));
    let unit = || kind("state.interval", json!({"state_kind": "systemd.unit"}));
    let commands = || kind("command.executed", json!({}));
    let acts = || kind("work.act", json!({}));
    let all = ["state.interval", "command.executed", "work.act"];
    let hour = Some((1000, 4600));
    vec![
        (
            "one state kind with commands and acts",
            window(vec![focus(), commands(), acts()], &all, hour),
        ),
        ("two state kinds", window(vec![focus(), unit()], &all, hour)),
        (
            "the whole state type",
            window(vec![kind("state.interval", json!({}))], &all, hour),
        ),
        (
            "a state kind beside the whole type",
            window(vec![focus(), kind("state.interval", json!({}))], &all, hour),
        ),
        (
            "a field outside the index",
            window(
                vec![kind(
                    "state.interval",
                    json!({"state_kind": "desktop.focus", "label": "focus open since long ago"}),
                )],
                &all,
                hour,
            ),
        ),
        (
            "commands by their start only",
            window(vec![focus(), commands()], &["state.interval"], hour),
        ),
        (
            "no overlap read",
            window(vec![focus(), commands(), acts()], &[], hour),
        ),
        (
            "an interval kind the window does not read",
            window(vec![commands()], &all, hour),
        ),
        (
            "a later window",
            window(
                vec![focus(), unit(), commands(), acts()],
                &all,
                Some((5000, 8600)),
            ),
        ),
        (
            "a window before every event",
            window(
                vec![focus(), commands(), acts()],
                &all,
                Some((-100_000_000, -99_000_000)),
            ),
        ),
        (
            "no bounds",
            window(vec![focus(), commands(), acts()], &all, None),
        ),
    ]
}

/// The read as one statement over every event of the window's kinds before
/// its end, computing how far each interval reaches from its payload: its
/// end, else the bound its kind declares, else forever.
async fn predicate_read(conn: &mut PgConnection, w: &Window) -> Vec<Uuid> {
    let (types, contains) = kind_arrays(&w.kinds);
    let interval_types: Vec<String> = w.intervals.iter().map(|k| k.event_type.into()).collect();
    let fields: Vec<String> = w.intervals.iter().map(|k| k.end.field().into()).collect();
    let bounds: Vec<Option<String>> = w
        .intervals
        .iter()
        .map(|k| k.bounded_by.map(str::to_owned))
        .collect();
    let durations: Vec<bool> = w
        .intervals
        .iter()
        .map(|k| matches!(k.end, IntervalEnd::AfterNs(_)))
        .collect();
    let open: Vec<bool> = w
        .intervals
        .iter()
        .map(|k| matches!(k.end, IntervalEnd::At(_)))
        .collect();
    sqlx::query_scalar(
        "SELECT e.id FROM core.events e
         WHERE e.event_type = ANY($1) AND e.ts_orig IS NOT NULL
           AND ($3::timestamptz IS NULL OR e.ts_orig < $3)
           AND ($2::timestamptz IS NULL OR e.ts_orig >= $2 OR EXISTS (
             SELECT 1 FROM unnest($5::text[], $6::text[], $7::bool[], $8::bool[], $9::text[]) AS i(event_type, field, duration, open, bound)
             WHERE i.event_type = e.event_type
               AND CASE WHEN i.duration THEN e.ts_orig + interval '1 microsecond' *
                     ceil(greatest(coalesce((e.payload->>i.field)::numeric, 0), 0) / 1000)::double precision
                   ELSE coalesce((e.payload->>i.field)::timestamptz, (e.payload->>i.bound)::timestamptz,
                                 CASE WHEN i.open THEN coalesce($3, 'infinity'::timestamptz) ELSE e.ts_orig END) END >= $2))
           AND EXISTS (SELECT 1 FROM unnest($1::text[], $4::jsonb[]) AS k(event_type, contains)
                       WHERE k.event_type = e.event_type AND e.payload @> k.contains)
           AND NOT EXISTS (SELECT 1 FROM audit.archived a WHERE a.event_id = e.id)
         ORDER BY e.ts_orig, e.commit_seq",
    )
    .bind(&types)
    .bind(w.start)
    .bind(w.end)
    .bind(&contains)
    .bind(&interval_types)
    .bind(&fields)
    .bind(&durations)
    .bind(&open)
    .bind(&bounds)
    .fetch_all(conn)
    .await
    .unwrap()
}

/// Every window reads what the predicate returns, in its order.
async fn assert_reads_agree(conn: &mut PgConnection, state: &str) {
    for (name, w) in windows() {
        let read: Vec<Uuid> = read_window(conn, &w)
            .await
            .unwrap()
            .iter()
            .map(|c| c.event.id.0)
            .collect();
        assert_eq!(read, predicate_read(conn, &w).await, "{name}, {state}");
    }
}

#[devkit::test]
async fn window_reads_over_intervals_equal_the_predicate_over_every_event(
    db: devkit::pg::TestDatabase,
) {
    let mut conn = AdmissionContext::connect(&db.url(None)).await.unwrap();
    let events = fixture();
    let named = |label: &str| {
        events
            .iter()
            .find(|e| {
                e.payload["label"] == label
                    || e.payload["command"] == label
                    || e.payload["act"] == label
            })
            .unwrap_or_else(|| panic!("{label}"))
            .id
    };
    let mut tx = conn.begin().await.unwrap();
    tx.admit(
        &Batch {
            run: RunId::new(),
            kind: BatchKind::Api {
                actor: "fixture".into(),
            },
            records: vec![RecordOutcome::Events {
                location: None,
                events: events.clone(),
            }],
            archive: vec![],
        },
        &NoDerivations,
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
    let archived = vec![
        named("focus reaching but archived"),
        named("focus open but archived"),
    ];
    crate::lifecycle::move_events(&mut conn, Act::Archive, archived.clone(), &NoDerivations).await;

    // The independently known answer for the first window, by name.
    let (_, first) = &windows()[0];
    let read: Vec<EventId> = read_window(&mut conn, first)
        .await
        .unwrap()
        .iter()
        .map(|c| c.event.id)
        .collect();
    let expected: Vec<EventId> = [
        "focus open since long ago",
        "focus fenced inside the window",
        "focus reaching into the window",
        "focus ending at the window start",
        "focus spanning the window",
        "act reaching into the window",
        "command running into the window",
        "command ending within the microsecond before the window",
        "act inside the window",
        "command inside the window",
        "focus inside the window",
        "focus opening inside the window",
    ]
    .into_iter()
    .map(named)
    .collect();
    assert_eq!(read, expected);
    assert_reads_agree(&mut conn, "as admitted").await;

    sqlx::query("SELECT compress_chunk(c) FROM show_chunks('core.events') c")
        .execute(&mut *conn)
        .await
        .unwrap();
    assert_reads_agree(&mut conn, "compressed").await;

    // Liveness moves both ways: a restored interval is read again, a newly
    // archived one is not.
    crate::lifecycle::move_events(&mut conn, Act::Restore, archived, &NoDerivations).await;
    crate::lifecycle::move_events(
        &mut conn,
        Act::Archive,
        vec![
            named("focus open since long ago"),
            named("command running into the window"),
        ],
        &NoDerivations,
    )
    .await;
    let read: Vec<EventId> = read_window(&mut conn, first)
        .await
        .unwrap()
        .iter()
        .map(|c| c.event.id)
        .collect();
    assert!(read.contains(&named("focus reaching but archived")));
    assert!(read.contains(&named("focus open but archived")));
    assert!(!read.contains(&named("focus open since long ago")));
    assert!(!read.contains(&named("command running into the window")));
    assert_reads_agree(&mut conn, "after a restore and an archive").await;
}

/// The identities a window over unit runs reads, in order.
async fn runs_read(conn: &mut PgConnection, kind: Kind, start: i64) -> Vec<EventId> {
    let w = window(vec![kind], &["state.interval"], Some((start, start + 100)));
    read_window(conn, &w)
        .await
        .unwrap()
        .iter()
        .map(|c| c.event.id)
        .collect()
}

/// A fenced interval keeps its end unknown and reaches no window that starts
/// after its fence; one with neither end nor fence reaches every later
/// window. Admission and restore store the same reach.
#[devkit::test]
async fn a_fenced_interval_reaches_no_window_after_its_fence(db: devkit::pg::TestDatabase) {
    let mut conn = AdmissionContext::connect(&db.url(None)).await.unwrap();
    let fenced_run = fenced("run fenced by the next boot", "machine.unit", 100, 500);
    let open_run = state("run still open", "machine.unit", 100, None);
    assert_eq!(fenced_run.payload["ended_at"], Value::Null);
    let (fenced_id, open_id) = (fenced_run.id, open_run.id);
    let mut tx = conn.begin().await.unwrap();
    tx.admit(
        &Batch {
            run: RunId::new(),
            kind: BatchKind::Api {
                actor: "fixture".into(),
            },
            records: vec![RecordOutcome::Events {
                location: None,
                events: vec![fenced_run, open_run],
            }],
            archive: vec![],
        },
        &NoDerivations,
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();

    let narrowed: fn() -> Kind = || kind("state.interval", json!({"state_kind": "machine.unit"}));
    let whole: fn() -> Kind = || kind("state.interval", json!({}));
    for restored in [false, true] {
        if restored {
            let ids = vec![fenced_id];
            crate::lifecycle::move_events(&mut conn, Act::Archive, ids.clone(), &NoDerivations)
                .await;
            assert_eq!(runs_read(&mut conn, whole(), 300).await, [open_id]);
            crate::lifecycle::move_events(&mut conn, Act::Restore, ids, &NoDerivations).await;
        }
        for asked in [narrowed, whole] {
            let both = runs_read(&mut conn, asked(), 300).await;
            assert_eq!(both.len(), 2, "before the fence, restored: {restored}");
            assert!(both.contains(&fenced_id) && both.contains(&open_id));
            assert_eq!(
                runs_read(&mut conn, asked(), 500).await.len(),
                2,
                "a window starting at the fence, restored: {restored}"
            );
            for later in [501, 90_000, 90_000_000] {
                assert_eq!(
                    runs_read(&mut conn, asked(), later).await,
                    [open_id],
                    "a window starting {later} s on, restored: {restored}"
                );
            }
        }
    }
}
