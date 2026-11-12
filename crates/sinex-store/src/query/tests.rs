//! Queries against a real database: each test admits a few commands (an
//! observation and the fused command derived from it) and reads them back.
use super::*;
use crate::NoDerivations;
use crate::read_models::tests::catch_up_search_text;
use serde_json::json;
use sinex_model::event_types::{CommandExecuted, CommandFused};
use sinex_model::query::{Context, bind};
use sinex_model::{
    Batch, BatchKind, Class, Event, EventId, MaterialId, OperationId, Origin, ReadingOrder,
    RecordLocation, RecordOutcome, RunId, Timestamp,
};
use sqlx::Connection;
use uuid::Uuid;

const BASE: i64 = 1_789_372_800; // 2026-09-14T08:00:00Z

/// Admit, for each `(command, cwd)`, an observation and its fused command a
/// minute apart from the previous pair. Exit codes alternate 0 and 1; the
/// third command has none. Returns `(fused, observation)` IDs.
pub(crate) async fn admit_commands(
    conn: &mut crate::AdmissionContext,
    commands: &[(&str, &str)],
) -> Vec<(Uuid, Uuid)> {
    admit_commands_with_duration(conn, commands, 2_000_000_000).await
}

async fn admit_commands_with_duration(
    conn: &mut crate::AdmissionContext,
    commands: &[(&str, &str)],
    duration_ns: i64,
) -> Vec<(Uuid, Uuid)> {
    let mut events = Vec::new();
    let mut ids = Vec::new();
    // Source record coordinates remain distinct across fixture acquisitions.
    let acquisition = Uuid::now_v7();
    for (i, (command, cwd)) in commands.iter().enumerate() {
        let started_at = Timestamp::from_unix_secs(BASE + 60 * i as i64).unwrap();
        let exit_code = if i == 2 { None } else { Some((i % 2) as i64) };
        let observed = Event::build(
            &CommandExecuted {
                native_id: format!("{acquisition}-n{i}"),
                command: command.to_string(),
                cwd: cwd.to_string(),
                session: "s".into(),
                hostname: "kestrel".into(),
                started_at,
                duration_ns: Some(duration_ns),
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
                duration_ns: Some(duration_ns),
                exit_code,
                execution: None,
                members: vec![sinex_model::event_types::fused::Member {
                    record: format!("fixture:{}", "terminal.atuin"),
                    source: "terminal.atuin".into(),
                    event_type: "command.executed".into(),
                    joined: sinex_model::event_types::fused::Joined::Lead,
                }],
                basis: sinex_model::event_types::fused::Basis::Rule,
                ambiguous: 1,
                competitors: vec![],
                rule_version: 1,
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
    // Derived inputs are committed readings, including in fixtures.
    for derived in [false, true] {
        let selected: Vec<_> = events
            .iter()
            .filter(|event| matches!(&event.origin, Origin::Derived { .. }) == derived)
            .cloned()
            .collect();
        if selected.is_empty() {
            continue;
        }
        let batch = Batch {
            run: RunId::new(),
            kind: BatchKind::Api {
                actor: "test".into(),
            },
            records: vec![RecordOutcome::Events {
                location: None,
                events: selected,
            }],
            archive: vec![],
        };
        let mut tx = conn.begin().await.unwrap();
        tx.admit(&batch, &NoDerivations).await.unwrap();
        tx.commit().await.unwrap();
    }
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
    let result = if sinex_model::query::native::is_command(text) {
        let operation = sinex_model::query::native::parse(text, &ctx).unwrap();
        run_operation(conn, text, &operation, &ctx, &page).await
    } else {
        run(conn, text, None, &ctx, &page).await
    };
    result.unwrap_or_else(|e| panic!("{text}: {e}"))
}

fn column(a: &Answer, name: &str) -> Vec<serde_json::Value> {
    a.rows.iter().map(|r| r[name].clone()).collect()
}

/// A first page reads its declaration snapshot once, even though both the
/// question identity and the bound query need that snapshot.
#[devkit::test]
async fn a_first_page_loads_the_current_declarations_once(db: devkit::pg::TestDatabase) {
    let mut conn = PgConnection::connect(&db.url(None)).await.unwrap();
    async fn loads(conn: &mut PgConnection) -> i64 {
        sqlx::query_scalar::<_, i64>(
            "SELECT coalesce(sum(generic_plans + custom_plans), 0)::bigint FROM pg_prepared_statements WHERE statement = $1",
        )
        .bind("SELECT name, version FROM registry.event_types ORDER BY name, version")
        .fetch_one(conn)
        .await
        .unwrap()
    }
    let before = loads(&mut conn).await;
    let answer = run(&mut conn, "events", None, &ctx(), &Page::default())
        .await
        .unwrap();
    assert_eq!(answer.total, 0);
    assert_eq!(loads(&mut conn).await - before, 1);
}

async fn world(conn: &mut crate::AdmissionContext) -> Vec<(Uuid, Uuid)> {
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
    let mut conn = crate::AdmissionContext::connect(&db.url(None))
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
    let a = ask(&mut conn, "commands where NOT exit_code:known").await;
    assert_eq!(column(&a, "command"), [json!("cargo build")]);
}

#[devkit::test]
async fn path_prefix_matches_whole_components(db: devkit::pg::TestDatabase) {
    let mut conn = crate::AdmissionContext::connect(&db.url(None))
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
    let mut conn = crate::AdmissionContext::connect(&db.url(None))
        .await
        .unwrap();
    world(&mut conn).await;
    let a = ask(
        &mut conn,
        "events where source = terminal.atuin AND source = derive.shell-fusion",
    )
    .await;
    assert_eq!(a.total, 0);
    let a = ask(
        &mut conn,
        "events where source = (terminal.atuin | derive.shell-fusion)",
    )
    .await;
    assert_eq!(a.total, 10);
    let a = ask(
        &mut conn,
        "events where glob(source, \"terminal.*\") = true",
    )
    .await;
    assert_eq!(a.total, 5);
}

#[devkit::test]
async fn words_search_ranks_and_marks_snippets(db: devkit::pg::TestDatabase) {
    let mut conn = crate::AdmissionContext::connect(&db.url(None))
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
    let a = ask(&mut conn, "commands where tide* AND NOT cargo").await;
    assert_eq!(column(&a, "command"), [json!("git push origin master")]);
    // Words are case-folded.
    let a = ask(&mut conn, "commands where CARGO").await;
    assert_eq!(a.total, 3);
}

#[devkit::test]
async fn pages_read_one_answer_at_one_commit_position(db: devkit::pg::TestDatabase) {
    let mut conn = crate::AdmissionContext::connect(&db.url(None))
        .await
        .unwrap();
    world(&mut conn).await;
    let first = ask_page(
        &mut conn,
        "commands | sort by when desc",
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
            "commands | sort by when desc",
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
    let mut conn = crate::AdmissionContext::connect(&db.url(None))
        .await
        .unwrap();
    world(&mut conn).await;
    let a = ask(&mut conn, "commands | sort by when | limit 2").await;
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
    let mut conn = crate::AdmissionContext::connect(&db.url(None))
        .await
        .unwrap();
    world(&mut conn).await;
    let a = ask(
        &mut conn,
        "commands | group by cwd | count | sort by count desc | limit 1",
    )
    .await;
    assert_eq!(a.rows, [json!({"cwd": "/src/tide", "count": 2})]);
    let a = ask(
        &mut conn,
        "commands | group by cwd | count | where count > 1",
    )
    .await;
    assert_eq!(a.total, 1);
    let a = ask(&mut conn, "commands | count").await;
    assert_eq!(a.rows, [json!({"count": 5})]);
    let a = ask(
        &mut conn,
        "commands | group by day(when) | agg sum(duration)",
    )
    .await;
    assert_eq!(a.rows.len(), 1);
    assert_eq!(a.rows[0]["day"], json!("2026-09-14"));
    assert_eq!(a.rows[0]["sum_duration"].as_f64(), Some(10.0));
    let a = ask(
        &mut conn,
        "events | group by source | count | sort by source",
    )
    .await;
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
    let mut conn = crate::AdmissionContext::connect(&db.url(None))
        .await
        .unwrap();
    let ids = world(&mut conn).await;
    let before = current_seq(&mut conn).await.unwrap();
    // The command is archived at its source record; its fused record follows
    // as part of the chain (docs/lifecycle.md: "It moves the scope's live
    // readings and their whole live chains"). Archiving only the fused record
    // would leave its member to be read as the command (docs/derivations.md:
    // "A consumer of a fused family reads the fused records plus the family's
    // unmatched source records").
    crate::lifecycle::move_events(
        &mut conn,
        sinex_model::lifecycle::Act::Archive,
        vec![EventId(ids[0].1)],
        &NoDerivations,
    )
    .await;
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
            "commands where command ~ workspace | as of seq {}",
            before - 1
        ),
    )
    .await;
    assert_eq!(old.total, 1);
    assert_eq!(old.resolved.commit_seq, before - 1);
    let archive = ask(&mut conn, "archive where reason = archive").await;
    assert_eq!(archive.total, 2, "the source record and its fused record");
}

#[devkit::test]
async fn near_conditions_pick_the_nearest_match_once(db: devkit::pg::TestDatabase) {
    let mut conn = crate::AdmissionContext::connect(&db.url(None))
        .await
        .unwrap();
    world(&mut conn).await;
    // Observations within 90 s of a failing command (08:01, 08:03): all five
    // observations, a minute apart, each once.
    let a = ask(
        &mut conn,
        "events where source = terminal.atuin AND near (commands where exit_code = 1) within 90s | fields id, match.id, match.distance",
    )
    .await;
    assert_eq!(a.total, 5, "{:?}", a.rows);
    for row in &a.rows {
        assert!(row["match.id"].is_string());
        assert!(row["match.distance"].as_f64().unwrap().abs() <= 90.0);
    }
    let a = ask(
        &mut conn,
        "events where source = terminal.atuin AND near (commands where exit_code = 7) within 1h",
    )
    .await;
    assert_eq!(a.total, 0, "an empty inner query never widens to a scan");
    let a = ask(
        &mut conn,
        "events where source = terminal.atuin AND near (commands where exit_code = 1) within 90s | fields when, match.distance | sort by match.distance desc",
    )
    .await;
    assert_eq!(a.columns, ["when", "match.distance"]);
    let offsets: Vec<f64> = a
        .rows
        .iter()
        .map(|r| r["match.distance"].as_f64().unwrap())
        .collect();
    let mut sorted = offsets.clone();
    sorted.sort_by(|x, y| y.partial_cmp(x).unwrap());
    assert_eq!(offsets, sorted);
}

#[devkit::test]
async fn near_pair_joins_return_every_matching_pair(db: devkit::pg::TestDatabase) {
    let mut conn = crate::AdmissionContext::connect(&db.url(None))
        .await
        .unwrap();
    let ids = world(&mut conn).await;
    let answer = ask(&mut conn, "events where source = terminal.atuin | join near (commands where exit_code = 1) within 90s | fields id, match.id").await;
    let pairs = answer
        .rows
        .iter()
        .map(|row| {
            (
                row["id"].as_str().unwrap().parse::<Uuid>().unwrap(),
                row["match.id"].as_str().unwrap().parse::<Uuid>().unwrap(),
            )
        })
        .collect::<std::collections::BTreeSet<_>>();
    let expected = [(0, 1), (1, 1), (2, 1), (2, 3), (3, 3), (4, 3)]
        .into_iter()
        .map(|(outer, inner)| (ids[outer].1, ids[inner].0))
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(pairs, expected);
    assert_eq!(answer.total, 6);
    assert_eq!(answer.rows.len(), 6);
}

#[devkit::test]
async fn walks_follow_provenance(db: devkit::pg::TestDatabase) {
    let mut conn = crate::AdmissionContext::connect(&db.url(None))
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
    let mut conn = crate::AdmissionContext::connect(&db.url(None))
        .await
        .unwrap();
    world(&mut conn).await;
    // A registered source that never recorded anything is not configured
    // here, not silent, so it is never listed as having no data.
    sqlx::query("INSERT INTO registry.sources(name,shapes,version,event_types,purpose,expectation) VALUES ('export.never-staged',ARRAY['export-archive'],1,ARRAY['synthetic.record'],'synthetic query test','export')")
        .execute(&mut *conn)
        .await
        .unwrap();
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
    let mut conn = crate::AdmissionContext::connect(&db.url(None))
        .await
        .unwrap();
    world(&mut conn).await;
    let a = ask(&mut conn, "sources | sort by binding").await;
    assert_eq!(
        column(&a, "binding"),
        [json!("derive.shell-fusion"), json!("terminal.atuin")]
    );
    assert_eq!(a.rows[0]["events"], json!(5));
}

#[devkit::test]
async fn time_functions_bucket_in_the_zone_with_logical_days(db: devkit::pg::TestDatabase) {
    let mut conn = crate::AdmissionContext::connect(&db.url(None))
        .await
        .unwrap();
    world(&mut conn).await;
    let a = ask(&mut conn, "commands where hour(when) = 8").await;
    assert_eq!(a.total, 5);
    let a = ask(
        &mut conn,
        "commands where weekday(when) = mon AND day(when) = 2026-09-14",
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
    let mut conn = crate::AdmissionContext::connect(&db.url(None))
        .await
        .unwrap();
    world(&mut conn).await;
    let a = ask(&mut conn, "events where payload.exit_code >= 1").await;
    assert_eq!(a.total, 4);
    let a = ask(&mut conn, "events where payload.hostname = kestrel").await;
    assert_eq!(a.total, 5);
    let a = ask(
        &mut conn,
        "events where event_type = command.fused | fields id, payload.cwd",
    )
    .await;
    assert_eq!(a.columns, ["id", "payload.cwd"]);
    assert_eq!(a.rows[0]["payload.cwd"], json!("/src/tide"));
}

#[devkit::test]
async fn read_only_sql_reads_and_cannot_write(db: devkit::pg::TestDatabase) {
    let mut conn = crate::AdmissionContext::connect(&db.url(None))
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
mod event_labels;

/// Admit derived events built from `payloads`, each with one fused command as
/// its parent.
async fn admit_derived<P: sinex_model::EventPayload>(
    conn: &mut crate::AdmissionContext,
    payloads: &[P],
) {
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
    tx.admit(&batch, &NoDerivations).await.unwrap();
    tx.commit().await.unwrap();
}

#[devkit::test]
async fn derived_aliases_read_what_the_derivations_write(db: devkit::pg::TestDatabase) {
    use sinex_model::event_types::{MetricFigure, StateInterval};
    let mut conn = crate::AdmissionContext::connect(&db.url(None))
        .await
        .unwrap();
    let at = |s: i64| Timestamp::from_unix_secs(BASE + s).unwrap();
    let interval = |state: &str, id: &str, ended: Option<i64>| StateInterval {
        fenced_at: None,
        interval_id: id.into(),
        state_kind: state.into(),
        subject_id: Some(id.into()),
        label: Some(format!("{id} window")),
        started_at: at(0),
        ended_at: ended.map(at),
        start_event_type: "window.focused".into(),
        end_event_type: ended.map(|_| "window.focused".into()),
        attributes: [("class", id), ("title", "vim")]
            .into_iter()
            .map(|(k, v)| (k.to_owned(), v.to_owned()))
            .collect(),
    };
    admit_derived(
        &mut conn,
        &[
            interval("desktop.focus", "kitty", Some(90)),
            interval("desktop.focus", "b", None),
        ],
    )
    .await;
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

    let now = ask(&mut conn, "now").await;
    assert_eq!(now.total, 1);
    assert_eq!(column(&now, "subject"), vec![json!("b window")]);
    let closed = ask(
        &mut conn,
        "intervals where open = false | fields duration, state",
    )
    .await;
    assert_eq!(closed.total, 1);
    assert_eq!(column(&closed, "state"), vec![json!("desktop.focus")]);
    let days = ask(
        &mut conn,
        "figures where grain = day AND dimension != cwd | fields day, value",
    )
    .await;
    assert_eq!(days.total, 1);
    assert_eq!(column(&days, "value")[0].as_f64(), Some(3.0));
    let focus = ask(
        &mut conn,
        "focus where app = kitty | fields duration, title",
    )
    .await;
    assert_eq!(column(&focus, "duration")[0].as_f64(), Some(90.0));
    assert_eq!(column(&focus, "title"), vec![json!("vim")]);
    // Other state intervals are not focus.
    admit_derived(&mut conn, &[interval("desktop.lock", "locked", Some(30))]).await;
    assert_eq!(ask(&mut conn, "focus").await.total, 2);
    assert_eq!(ask(&mut conn, "intervals").await.total, 3);
}

/// A command two shells saw is its fused record; a command one shell saw is
/// that shell's record. The `commands` alias shows each once.
#[devkit::test]
async fn commands_show_each_occurrence_once(db: devkit::pg::TestDatabase) {
    use sinex_model::event_types::CommandImported;
    let mut conn = crate::AdmissionContext::connect(&db.url(None))
        .await
        .unwrap();
    let at = |s: i64| Timestamp::from_unix_secs(BASE + s).unwrap();
    let location = |anchor| Origin::Record {
        location: RecordLocation {
            material: MaterialId::new(),
            anchor,
        },
        output_index: 0,
    };
    let zsh = |command: &str, secs: i64, anchor: u64| {
        Event::build(
            &CommandImported {
                command: command.into(),
                hostname: "kestrel".into(),
                started_at: Some(at(secs)),
                header_elapsed: Some(0),
                paths: vec![],
                n: 0,
            },
            "terminal.zsh",
            location(anchor),
            ReadingOrder(1),
            *blake3::hash(command.as_bytes()).as_bytes(),
            Class::Open,
        )
        .unwrap()
    };
    let atuin = Event::build(
        &CommandExecuted {
            native_id: "both".into(),
            command: "make".into(),
            cwd: "/src".into(),
            session: "s".into(),
            hostname: "kestrel".into(),
            started_at: at(0),
            duration_ns: Some(2_000_000_000),
            exit_code: Some(0),
            deleted_at: None,
            author: None,
            intent: None,
            shell: None,
            extra: Default::default(),
        },
        "terminal.atuin",
        location(0),
        ReadingOrder(1),
        [1; 32],
        Class::Open,
    )
    .unwrap();
    let joined = zsh("make", 0, 1);
    let alone = zsh("ls", 60, 2);
    let fused = Event::build(
        &CommandFused {
            record: "command.executed:both".into(),
            command: "make".into(),
            host: "kestrel".into(),
            started_at: at(0),
            cwd: Some("/src".into()),
            session: Some("s".into()),
            duration_ns: Some(2_000_000_000),
            exit_code: Some(0),
            execution: None,
            members: vec![
                sinex_model::event_types::fused::Member {
                    record: format!("fixture:{}", "terminal.atuin"),
                    source: "terminal.atuin".into(),
                    event_type: "command.executed".into(),
                    joined: sinex_model::event_types::fused::Joined::Lead,
                },
                sinex_model::event_types::fused::Member {
                    record: format!("fixture:{}", "terminal.zsh"),
                    source: "terminal.zsh".into(),
                    event_type: "command.executed".into(),
                    joined: sinex_model::event_types::fused::Joined::Lead,
                },
            ],
            basis: sinex_model::event_types::fused::Basis::Rule,
            ambiguous: 1,
            competitors: vec![],
            rule_version: 1,
        },
        "command.fusion",
        Origin::Derived {
            parents: vec![atuin.id, joined.id],
        },
        ReadingOrder(1),
        [2; 32],
        Class::Open,
    )
    .unwrap();
    for events in [vec![atuin, joined, alone], vec![fused]] {
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
        tx.admit(&batch, &NoDerivations).await.unwrap();
        tx.commit().await.unwrap();
    }
    let commands = ask(
        &mut conn,
        "commands | sort by when | fields command, observed_by, end",
    )
    .await;
    assert_eq!(commands.total, 2);
    assert_eq!(
        column(&commands, "command"),
        vec![json!("make"), json!("ls")]
    );
    assert_eq!(
        column(&commands, "observed_by"),
        vec![json!("terminal.atuin,terminal.zsh"), json!("terminal.zsh")]
    );
    // A history file records no duration: its command is an instant.
    let ends = column(&commands, "end");
    assert!(!ends[1].is_null(), "{ends:?}");
}

#[cfg(test)]
mod search;

mod gaps;

mod kinds;

mod signals;

mod calendar;

mod follow;

mod projection;

mod temporal_selectors;
