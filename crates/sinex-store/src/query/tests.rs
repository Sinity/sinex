//! Queries against a real database: each test admits a few commands (an
//! observation and the fused command derived from it) and reads them back.
use super::*;
use crate::read_models::tests::catch_up_search_text;
use crate::{NoDerivations, admit};
use serde_json::json;
use sinex_model::event_types::{CommandExecuted, CommandFused};
use sinex_model::query::{Context, bind};
use sinex_model::{
    ArchiveRequest, Batch, BatchKind, Class, Event, EventId, MaterialId, OperationId, Origin,
    ReadingOrder, RecordLocation, RecordOutcome, RunId, Timestamp,
};
use sqlx::Connection;
use uuid::Uuid;

const BASE: i64 = 1_789_372_800; // 2026-09-14T08:00:00Z

/// Admit, for each `(command, cwd)`, an observation and its fused command a
/// minute apart from the previous pair. Exit codes alternate 0 and 1; the
/// third command has none. Returns `(fused, observation)` IDs.
pub(crate) async fn admit_commands(
    conn: &mut PgConnection,
    commands: &[(&str, &str)],
) -> Vec<(Uuid, Uuid)> {
    let mut events = Vec::new();
    let mut ids = Vec::new();
    for (i, (command, cwd)) in commands.iter().enumerate() {
        let started_at = Timestamp::from_unix_secs(BASE + 60 * i as i64).unwrap();
        let exit_code = if i == 2 { None } else { Some((i % 2) as i64) };
        let observed = Event::build(
            &CommandExecuted {
                native_id: format!("n{i}"),
                command: command.to_string(),
                cwd: cwd.to_string(),
                session: "s".into(),
                hostname: "kestrel".into(),
                started_at,
                duration_ns: Some(2_000_000_000),
                exit_code,
                deleted_at: None,
                author: None,
                intent: None,
                shell: None,
                extra: Default::default(),
            },
            "terminal.atuin",
            Origin::Record {
                location: RecordLocation {
                    material: MaterialId::new(),
                    anchor: 0,
                },
                output_index: 0,
            },
            ReadingOrder(1),
            *blake3::hash(command.as_bytes()).as_bytes(),
            Class::Open,
        )
        .unwrap();
        let fused = Event::build(
            &CommandFused {
                record: format!("command.executed:{}", observed.id),
                command: command.to_string(),
                host: "kestrel".into(),
                started_at,
                cwd: Some(cwd.to_string()),
                session: Some("s".into()),
                duration_ns: Some(2_000_000_000),
                exit_code,
                observed_by: vec!["terminal.atuin".into()],
            },
            "derive.shell-fusion",
            Origin::Derived {
                parents: vec![observed.id],
            },
            ReadingOrder(1),
            *blake3::hash(format!("f{command}").as_bytes()).as_bytes(),
            Class::Open,
        )
        .unwrap();
        ids.push((fused.id.0, observed.id.0));
        events.push(observed);
        events.push(fused);
    }
    let batch = Batch {
        run: RunId::new(),
        kind: BatchKind::Api {
            actor: "test".into(),
        },
        records: vec![RecordOutcome::Events {
            location: None,
            events,
        }],
        archive: vec![],
    };
    let mut tx = conn.begin().await.unwrap();
    admit(&mut tx, &batch, &NoDerivations).await.unwrap();
    tx.commit().await.unwrap();
    ids
}

fn ctx() -> Context {
    Context::utc("2026-09-15T00:00:00Z".parse().unwrap())
}

async fn ask(conn: &mut PgConnection, text: &str) -> Answer {
    ask_page(conn, text, Page::default()).await
}

async fn ask_page(conn: &mut PgConnection, text: &str, page: Page) -> Answer {
    let ctx = ctx();
    run(conn, text, None, &ctx, &page)
        .await
        .unwrap_or_else(|e| panic!("{text}: {e}"))
}

fn column(a: &Answer, name: &str) -> Vec<serde_json::Value> {
    a.rows.iter().map(|r| r[name].clone()).collect()
}

async fn world(conn: &mut PgConnection) -> Vec<(Uuid, Uuid)> {
    let ids = admit_commands(
        conn,
        &[
            ("cargo test --workspace", "/src/tide"),
            ("git push origin master", "/src/tide/tools"),
            ("cargo build", "/src/tidepool"),
            ("ls -la", "/home/iris"),
            ("cargo test -p store", "/src/tide"),
        ],
    )
    .await;
    catch_up_search_text(conn).await.unwrap();
    ids
}

#[devkit::test]
async fn aliases_filter_on_promoted_fields_with_unknown_kept_unknown(db: devkit::pg::TestDatabase) {
    let mut conn = <sqlx::PgConnection as sqlx::Connection>::connect(&db.url(None))
        .await
        .unwrap();
    world(&mut conn).await;
    let a = ask(&mut conn, "commands").await;
    assert_eq!(a.total, 5);
    // `!= 0` never returns the command whose exit code is unknown.
    let a = ask(&mut conn, "commands where exit_code != 0").await;
    assert_eq!(
        column(&a, "command"),
        [json!("git push origin master"), json!("ls -la")]
    );
    let a = ask(&mut conn, "commands where not exit_code exists").await;
    assert_eq!(column(&a, "command"), [json!("cargo build")]);
}

#[devkit::test]
async fn path_prefix_matches_whole_components(db: devkit::pg::TestDatabase) {
    let mut conn = <sqlx::PgConnection as sqlx::Connection>::connect(&db.url(None))
        .await
        .unwrap();
    world(&mut conn).await;
    let a = ask(&mut conn, "commands where cwd ^ /src/tide").await;
    assert_eq!(a.total, 3, "{:?}", column(&a, "cwd"));
    assert!(!column(&a, "cwd").contains(&json!("/src/tidepool")));
    let a = ask(&mut conn, "commands where cwd ^ /src/tide/").await;
    assert_eq!(a.total, 3);
}

#[devkit::test]
async fn two_conditions_on_one_field_are_both_required(db: devkit::pg::TestDatabase) {
    let mut conn = <sqlx::PgConnection as sqlx::Connection>::connect(&db.url(None))
        .await
        .unwrap();
    world(&mut conn).await;
    let a = ask(
        &mut conn,
        "events where source = terminal.atuin and source = derive.shell-fusion",
    )
    .await;
    assert_eq!(a.total, 0);
    let a = ask(
        &mut conn,
        "events where source = [terminal.atuin, derive.shell-fusion]",
    )
    .await;
    assert_eq!(a.total, 10);
    let a = ask(&mut conn, "events where source = terminal.*").await;
    assert_eq!(a.total, 5);
}

#[devkit::test]
async fn words_search_ranks_and_marks_snippets(db: devkit::pg::TestDatabase) {
    let mut conn = <sqlx::PgConnection as sqlx::Connection>::connect(&db.url(None))
        .await
        .unwrap();
    world(&mut conn).await;
    // Each command is an observation and a fused command: both match.
    let a = ask(&mut conn, "cargo test").await;
    assert_eq!(a.total, 4, "{:?}", a.rows);
    assert!(a.read_as.contains("most relevant first"));
    assert!(a.rows[0]["snippet"].as_str().unwrap().contains("[cargo]"));
    let a = ask(&mut conn, r#"commands where "test --workspace""#).await;
    assert_eq!(column(&a, "command"), [json!("cargo test --workspace")]);
    let a = ask(&mut conn, "commands where tide* and not cargo").await;
    assert_eq!(column(&a, "command"), [json!("git push origin master")]);
    // Words are case-folded.
    let a = ask(&mut conn, "commands where CARGO").await;
    assert_eq!(a.total, 3);
}

#[devkit::test]
async fn pages_read_one_answer_at_one_commit_position(db: devkit::pg::TestDatabase) {
    let mut conn = <sqlx::PgConnection as sqlx::Connection>::connect(&db.url(None))
        .await
        .unwrap();
    world(&mut conn).await;
    let first = ask_page(
        &mut conn,
        "commands sort when desc",
        Page {
            size: Some(2),
            cursor: None,
        },
    )
    .await;
    assert_eq!(first.total, 5);
    assert_eq!(first.rows.len(), 2);
    // More commands arrive between pages; the answer does not change.
    admit_commands(&mut conn, &[("echo later", "/")]).await;
    let mut seen: Vec<_> = column(&first, "command");
    let mut cursor = first.page.cursor.clone();
    while let Some(c) = cursor {
        let next = ask_page(
            &mut conn,
            "commands sort when desc",
            Page {
                size: Some(2),
                cursor: Some(c),
            },
        )
        .await;
        assert_eq!(next.total, 5);
        seen.extend(column(&next, "command"));
        cursor = next.page.cursor;
    }
    assert_eq!(seen.len(), 5);
    assert_eq!(seen[0], json!("cargo test -p store"));
    assert!(!seen.contains(&json!("echo later")));
    // A new query sees it.
    assert_eq!(ask(&mut conn, "commands").await.total, 6);
}

#[devkit::test]
async fn limit_is_part_of_the_answer(db: devkit::pg::TestDatabase) {
    let mut conn = <sqlx::PgConnection as sqlx::Connection>::connect(&db.url(None))
        .await
        .unwrap();
    world(&mut conn).await;
    let a = ask(&mut conn, "commands sort when limit 2").await;
    assert_eq!(a.total, 2);
    assert_eq!(
        column(&a, "command"),
        [
            json!("cargo test --workspace"),
            json!("git push origin master")
        ]
    );
}

#[devkit::test]
async fn grouping_makes_rows_of_groups(db: devkit::pg::TestDatabase) {
    let mut conn = <sqlx::PgConnection as sqlx::Connection>::connect(&db.url(None))
        .await
        .unwrap();
    world(&mut conn).await;
    let a = ask(
        &mut conn,
        "commands group by cwd count sort count desc limit 1",
    )
    .await;
    assert_eq!(a.rows, [json!({"cwd": "/src/tide", "count": 2})]);
    let a = ask(&mut conn, "commands group by cwd count where count > 1").await;
    assert_eq!(a.total, 1);
    let a = ask(&mut conn, "commands count").await;
    assert_eq!(a.rows, [json!({"count": 5})]);
    let a = ask(&mut conn, "commands group by day(when) sum(duration)").await;
    assert_eq!(a.rows.len(), 1);
    assert_eq!(a.rows[0]["day"], json!("2026-09-14"));
    assert_eq!(a.rows[0]["sum_duration"].as_f64(), Some(10.0));
    let a = ask(&mut conn, "events group by source count sort source").await;
    assert_eq!(
        a.rows,
        [
            json!({"source": "derive.shell-fusion", "count": 5}),
            json!({"source": "terminal.atuin", "count": 5})
        ]
    );
}

#[devkit::test]
async fn as_of_reads_what_was_live_at_a_commit_position(db: devkit::pg::TestDatabase) {
    let mut conn = <sqlx::PgConnection as sqlx::Connection>::connect(&db.url(None))
        .await
        .unwrap();
    let ids = world(&mut conn).await;
    let before = current_seq(&mut conn).await.unwrap();
    let hide = Batch {
        run: RunId::new(),
        kind: BatchKind::Api {
            actor: "test".into(),
        },
        records: vec![],
        archive: vec![ArchiveRequest::Hide {
            target: EventId(ids[0].0),
            operation: OperationId::new(),
        }],
    };
    let mut tx = conn.begin().await.unwrap();
    admit(&mut tx, &hide, &NoDerivations).await.unwrap();
    tx.commit().await.unwrap();
    // Archiving records the position it happened at; move past it.
    admit_commands(&mut conn, &[("echo after", "/")]).await;
    assert_eq!(
        ask(&mut conn, "commands where command ~ workspace")
            .await
            .total,
        0
    );
    let old = ask(
        &mut conn,
        &format!(
            "commands where command ~ workspace as of seq {}",
            before - 1
        ),
    )
    .await;
    assert_eq!(old.total, 1);
    assert_eq!(old.resolved.commit_seq, before - 1);
    let archive = ask(&mut conn, "archive where reason = hide").await;
    assert_eq!(archive.total, 1);
}

#[devkit::test]
async fn near_joins_pick_the_nearest_match_once(db: devkit::pg::TestDatabase) {
    let mut conn = <sqlx::PgConnection as sqlx::Connection>::connect(&db.url(None))
        .await
        .unwrap();
    world(&mut conn).await;
    // Observations within 90 s of a failing command (08:01, 08:03): all five
    // observations, a minute apart, each once.
    let a = ask(
        &mut conn,
        "events where source = terminal.atuin near (commands where exit_code = 1) within 90s",
    )
    .await;
    assert_eq!(a.total, 5, "{:?}", a.rows);
    for row in &a.rows {
        assert!(row["near"].is_string());
        assert!(row["offset"].as_f64().unwrap().abs() <= 90.0);
    }
    let a = ask(
        &mut conn,
        "events where source = terminal.atuin near (commands where exit_code = 7) within 1h",
    )
    .await;
    assert_eq!(a.total, 0, "an empty inner query never widens to a scan");
    let a = ask(
        &mut conn,
        "events where source = terminal.atuin near (commands where exit_code = 1) within 90s fields when, offset sort offset desc",
    )
    .await;
    assert_eq!(a.columns, ["when", "offset"]);
    let offsets: Vec<f64> = a
        .rows
        .iter()
        .map(|r| r["offset"].as_f64().unwrap())
        .collect();
    let mut sorted = offsets.clone();
    sorted.sort_by(|x, y| y.partial_cmp(x).unwrap());
    assert_eq!(offsets, sorted);
}

#[devkit::test]
async fn walks_follow_provenance(db: devkit::pg::TestDatabase) {
    let mut conn = <sqlx::PgConnection as sqlx::Connection>::connect(&db.url(None))
        .await
        .unwrap();
    let ids = world(&mut conn).await;
    let (fused, observed) = ids[0];
    let a = ask(&mut conn, &format!("trace {fused}")).await;
    assert_eq!(a.total, 2);
    assert_eq!(a.rows[0]["origin"], json!("derived"));
    assert_eq!(a.rows[1]["id"], json!(observed.to_string()));
    assert_eq!(a.rows[1]["origin"], json!("material"));
    let a = ask(&mut conn, &format!("parents sinex:ev/{fused}")).await;
    assert_eq!(column(&a, "id"), [json!(observed.to_string())]);
    let a = ask(&mut conn, &format!("descendants {observed}")).await;
    assert_eq!(column(&a, "id"), [json!(fused.to_string())]);
    let shown = show(&mut conn, &sinex_model::query::Reference::Event(fused))
        .await
        .unwrap();
    assert_eq!(shown["state"], json!("live"));
    assert_eq!(shown["event_type"], json!("command.fused"));
}

#[devkit::test]
async fn recall_reports_every_source_and_says_no_data(db: devkit::pg::TestDatabase) {
    let mut conn = <sqlx::PgConnection as sqlx::Connection>::connect(&db.url(None))
        .await
        .unwrap();
    world(&mut conn).await;
    let a = ask(&mut conn, "recall 2026-09-14T08:02:00Z ±90s").await;
    // Commands at 08:01, 08:02 and 08:03, each observed and fused.
    assert_eq!(a.total, 6, "{:?}", a.rows);
    let summary = a.recall.unwrap();
    assert_eq!(summary["by_source"].as_array().unwrap().len(), 2);
    let a = ask(&mut conn, "recall 2026-09-13T08:00:00Z ±1m").await;
    assert_eq!(a.total, 0);
    assert_eq!(
        a.recall.unwrap()["no_data"],
        json!(["derive.shell-fusion", "terminal.atuin"])
    );
}

#[devkit::test]
async fn sources_lists_every_source_with_events(db: devkit::pg::TestDatabase) {
    let mut conn = <sqlx::PgConnection as sqlx::Connection>::connect(&db.url(None))
        .await
        .unwrap();
    world(&mut conn).await;
    let a = ask(&mut conn, "sources sort binding").await;
    assert_eq!(
        column(&a, "binding"),
        [json!("derive.shell-fusion"), json!("terminal.atuin")]
    );
    assert_eq!(a.rows[0]["events"], json!(5));
}

#[devkit::test]
async fn time_functions_bucket_in_the_zone_with_logical_days(db: devkit::pg::TestDatabase) {
    let mut conn = <sqlx::PgConnection as sqlx::Connection>::connect(&db.url(None))
        .await
        .unwrap();
    world(&mut conn).await;
    let a = ask(&mut conn, "commands where hour(when) = 8").await;
    assert_eq!(a.total, 5);
    let a = ask(
        &mut conn,
        "commands where weekday(when) = mon and day(when) = 2026-09-14",
    )
    .await;
    assert_eq!(a.total, 5);
    let a = ask(&mut conn, "commands where when in 2026-09-14").await;
    assert_eq!(a.total, 5);
    // At 00:00 with days from 04:00, the logical day is still the 14th.
    let a = ask(&mut conn, "commands where when in today").await;
    assert_eq!(a.total, 5);
    let a = ask(&mut conn, "commands where when in yesterday").await;
    assert_eq!(a.total, 0);
}

#[devkit::test]
async fn payload_paths_compare_by_the_literal_type(db: devkit::pg::TestDatabase) {
    let mut conn = <sqlx::PgConnection as sqlx::Connection>::connect(&db.url(None))
        .await
        .unwrap();
    world(&mut conn).await;
    let a = ask(&mut conn, "events where payload.exit_code >= 1").await;
    assert_eq!(a.total, 4);
    let a = ask(&mut conn, "events where payload.hostname = kestrel").await;
    assert_eq!(a.total, 5);
    let a = ask(
        &mut conn,
        "events where event_type = command.fused fields id, payload.cwd",
    )
    .await;
    assert_eq!(a.columns, ["id", "payload.cwd"]);
    assert_eq!(a.rows[0]["payload.cwd"], json!("/src/tide"));
}

#[devkit::test]
async fn read_only_sql_reads_and_cannot_write(db: devkit::pg::TestDatabase) {
    let mut conn = <sqlx::PgConnection as sqlx::Connection>::connect(&db.url(None))
        .await
        .unwrap();
    world(&mut conn).await;
    let rows = read_sql::run(
        &mut conn,
        "SELECT event_type, count(*) AS n FROM core.visible_events GROUP BY 1 ORDER BY 1;",
    )
    .await
    .unwrap();
    assert_eq!(rows[0], json!({"event_type": "command.executed", "n": 5}));
    let e = read_sql::run(&mut conn, "DELETE FROM core.events RETURNING id")
        .await
        .unwrap_err();
    assert!(!e.to_string().is_empty());
    assert_eq!(
        read_sql::run(&mut conn, "SELECT count(*) AS n FROM core.visible_events")
            .await
            .unwrap()[0],
        json!({"n": 10})
    );
}

mod cursor;

/// Admit derived events built from `payloads`, each with one fused command as
/// its parent.
async fn admit_derived<P: sinex_model::EventPayload>(conn: &mut PgConnection, payloads: &[P]) {
    let parent = EventId(admit_commands(conn, &[("ls", "/")]).await[0].0);
    let events = payloads
        .iter()
        .enumerate()
        .map(|(i, p)| {
            Event::build(
                p,
                "derive.test",
                Origin::Derived {
                    parents: vec![parent],
                },
                ReadingOrder(1),
                *blake3::hash(format!("d{i}").as_bytes()).as_bytes(),
                Class::Open,
            )
            .unwrap()
        })
        .collect();
    let batch = Batch {
        run: RunId::new(),
        kind: BatchKind::Api {
            actor: "test".into(),
        },
        records: vec![RecordOutcome::Events {
            location: None,
            events,
        }],
        archive: vec![],
    };
    let mut tx = conn.begin().await.unwrap();
    admit(&mut tx, &batch, &NoDerivations).await.unwrap();
    tx.commit().await.unwrap();
}

#[devkit::test]
async fn derived_aliases_read_what_the_derivations_write(db: devkit::pg::TestDatabase) {
    use sinex_model::event_types::fused::FocusFused;
    use sinex_model::event_types::{MetricFigure, StateInterval};
    let mut conn = <sqlx::PgConnection as sqlx::Connection>::connect(&db.url(None))
        .await
        .unwrap();
    let at = |s: i64| Timestamp::from_unix_secs(BASE + s).unwrap();
    let interval = |id: &str, ended: Option<i64>| StateInterval {
        interval_id: id.into(),
        state_kind: "desktop.focus".into(),
        subject_id: Some(id.into()),
        label: Some(format!("{id} window")),
        started_at: at(0),
        ended_at: ended.map(at),
        start_event_type: "window.focused".into(),
        end_event_type: ended.map(|_| "window.focused".into()),
        attributes: Default::default(),
    };
    admit_derived(&mut conn, &[interval("a", Some(90)), interval("b", None)]).await;
    let figure = |grain: &str, dimension: &str, value: f64| MetricFigure {
        metric: "shell.commands".into(),
        grain: grain.into(),
        period_start: at(0),
        period_end: at(3600),
        day: "2026-09-14".into(),
        dimension: dimension.into(),
        dimension_value: if dimension.is_empty() { "" } else { "/src" }.into(),
        value: Some(value),
        numerator: None,
        denominator: None,
        rows: value as u64,
    };
    admit_derived(
        &mut conn,
        &[
            figure("hour", "", 3.0),
            figure("day", "", 3.0),
            figure("day", "cwd", 2.0),
        ],
    )
    .await;
    let focus = FocusFused {
        record: "r".into(),
        started_at: at(0),
        ended_at: Some(at(120)),
        app: Some("kitty".into()),
        title: Some("vim".into()),
        observed_by: vec!["desktop.hyprland".into(), "desktop.activitywatch".into()],
    };
    admit_derived(&mut conn, &[focus]).await;

    let now = ask(&mut conn, "now").await;
    assert_eq!(now.total, 1);
    assert_eq!(column(&now, "subject"), vec![json!("b window")]);
    let closed = ask(
        &mut conn,
        "intervals where open = false fields duration, state",
    )
    .await;
    assert_eq!(closed.total, 1);
    assert_eq!(column(&closed, "state"), vec![json!("desktop.focus")]);
    let days = ask(
        &mut conn,
        "figures where grain = day and dimension != cwd fields day, value",
    )
    .await;
    assert_eq!(days.total, 1);
    assert_eq!(column(&days, "value")[0].as_f64(), Some(3.0));
    let focus = ask(
        &mut conn,
        "focus where app = kitty fields duration, observed_by",
    )
    .await;
    assert_eq!(column(&focus, "duration")[0].as_f64(), Some(120.0));
    assert_eq!(
        column(&focus, "observed_by"),
        vec![json!("desktop.hyprland,desktop.activitywatch")]
    );
}

mod gaps;

mod kinds;
