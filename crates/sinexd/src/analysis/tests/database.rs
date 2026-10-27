//! Analysis against a real database: the runner derives the daily figures,
//! and panels, recipes and the brief read them with the coverage behind
//! them.
use super::super::*;
use crate::derivation_runner::DerivationRunner;
use crate::dirty_keys::Registered as Keys;
use crate::writer::{self, Priority};
use sinex_analysis::{Cell, Eligibility};
use sinex_model::event_types::code::GitCommit;
use sinex_model::query::Context;
use sinex_model::{
    Batch, BatchKind, Class, Event, MaterialId, Origin, ReadingOrder, RecordLocation,
    RecordOutcome, RunId,
};
use sqlx::Connection;
use sqlx::PgConnection;
use std::sync::Arc;

/// 2026-09-14 10:00 CEST.
const T: i64 = 1_789_372_800;

fn at(hours: i64) -> Timestamp {
    Timestamp::from_unix_secs(T + hours * 3600).unwrap()
}

fn commit(id: &str, hours: i64) -> Event {
    Event::build(
        &GitCommit {
            repository: "/r".into(),
            commit: id.into(),
            parents: vec![],
            tree: "t".into(),
            author_name: "a".into(),
            author_email: "a@x".into(),
            authored_at: at(hours),
            committer_name: "a".into(),
            committer_email: "a@x".into(),
            committed_at: at(hours),
            message: "m".into(),
            refs: vec![],
        },
        "code.git",
        Origin::Record {
            location: RecordLocation {
                material: MaterialId::new(),
                anchor: 0,
            },
            output_index: 0,
        },
        ReadingOrder(0),
        *blake3::hash(id.as_bytes()).as_bytes(),
        Class::Open,
    )
    .unwrap()
}

fn context() -> Context {
    Context::new(
        jiff::Timestamp::now(),
        jiff::tz::TimeZone::UTC,
        jiff::civil::Time::midnight(),
    )
}

struct World {
    _db: devkit::pg::TestDatabase,
    url: String,
    handle: writer::WriterHandle,
    _writer: tokio::task::JoinHandle<()>,
}

impl World {
    async fn new(db: devkit::pg::TestDatabase) -> Self {
        let url = db.url(None);
        let connection = <sqlx::PgConnection as sqlx::Connection>::connect(&url)
            .await
            .unwrap();
        let (handle, queues) = writer::channels();
        let task = tokio::spawn(writer::run(
            url.clone(),
            connection,
            queues,
            Arc::new(Keys::new(sinex_derivations::keyed())),
            None,
        ));
        Self {
            _db: db,
            url,
            handle,
            _writer: task,
        }
    }

    async fn admit(&self, events: Vec<Event>) {
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
        self.handle.submit(Priority::Live, batch).await.unwrap();
    }

    async fn conn(&self) -> PgConnection {
        PgConnection::connect(&self.url).await.unwrap()
    }
}

impl World {
    /// Admit `events` and run every derivation until nothing is dirty.
    async fn derive(&self, events: Vec<Event>) {
        self.admit(events).await;
        let read = PgConnection::connect(&self.url).await.unwrap();
        let mut runner = DerivationRunner::start(
            read,
            self.handle.clone(),
            sinex_derivations::keyed(),
            Vec::new(),
            "test",
        )
        .await
        .unwrap();
        runner.catch_up().await.unwrap();
    }
}

#[devkit::test]
async fn a_panel_reads_the_derived_figures_with_their_coverage(db: devkit::pg::TestDatabase) {
    let w = World::new(db).await;
    w.derive(vec![commit("a", 0), commit("b", 2), commit("c", 24)])
        .await;
    let mut conn = w.conn().await;
    let commits = sinex_derivations::figures::metric("git.commits").unwrap();
    let p = panel(
        &mut conn,
        &context(),
        &Settings::operator_default(),
        &[commits],
        parse_day("2026-09-14").unwrap(),
        parse_day("2026-09-15").unwrap(),
    )
    .await
    .unwrap();
    let totals: Vec<(&str, &Cell<f64>)> = p
        .cells
        .iter()
        .filter(|c| c.dimension.is_empty())
        .map(|c| (c.unit.label.as_str(), &c.cell))
        .collect();
    // The store has git records from 10:00 on the 14th to 10:00 on the 15th,
    // so both days are partly observed; the counts are what was seen.
    assert_eq!(
        totals,
        [
            (
                "2026-09-14",
                &Cell::Partial {
                    value: 2.0,
                    observed_fraction: 0.75
                }
            ),
            (
                "2026-09-15",
                &Cell::Partial {
                    value: 1.0,
                    observed_fraction: 0.25
                }
            )
        ]
    );
    let total = p.cells.iter().find(|c| c.dimension.is_empty()).unwrap();
    assert!(total.reference.as_deref().unwrap().starts_with("sinex:ev/"));
    assert!(
        p.cells
            .iter()
            .any(|c| c.dimension == "repository" && c.dimension_value == "/r")
    );
}

#[devkit::test]
async fn a_recipe_answers_from_the_figures_and_refuses_when_it_must(db: devkit::pg::TestDatabase) {
    let w = World::new(db).await;
    w.derive(vec![commit("a", 0)]).await;
    let mut conn = w.conn().await;
    let (from, to) = (
        parse_day("2026-09-01").unwrap(),
        parse_day("2026-09-14").unwrap(),
    );
    let first = run(
        &mut conn,
        &context(),
        &Settings::operator_default(),
        Recipe::Trends,
        from,
        to,
        Eligibility::default(),
    )
    .await
    .unwrap();
    // No tracked metric has an observed series here: every method refused,
    // and the refusals are the answer.
    let refusal = &first.tables["metrics"][0]["trend"];
    assert_eq!(refusal["status"], "not_estimable");
    let again = run(
        &mut conn,
        &context(),
        &Settings::operator_default(),
        Recipe::Trends,
        from,
        to,
        Eligibility::default(),
    )
    .await
    .unwrap();
    assert_eq!(again.as_of, first.as_of);
    assert_eq!(
        again.tables, first.tables,
        "the same question, the same answer"
    );
}

#[devkit::test]
async fn the_brief_reports_coverage_first_and_never_shows_missing_as_zero(
    db: devkit::pg::TestDatabase,
) {
    let w = World::new(db).await;
    w.derive(vec![commit("a", 0), commit("b", 2)]).await;
    let mut conn = w.conn().await;
    let p = brief(
        &mut conn,
        &context(),
        &Settings::operator_default(),
        parse_day("2026-09-14").unwrap(),
        6000,
    )
    .await
    .unwrap();
    let titles: Vec<_> = p.sections.iter().map(|s| s.title.as_str()).collect();
    assert_eq!(titles[0], "Data quality");
    assert!(
        p.markdown
            .contains("Commits made (git.commits) on 2026-09-14: 2"),
        "{}",
        p.markdown
    );
    // Nothing from the shells was stored, so the day's command count is
    // unknown, not zero.
    assert!(
        p.markdown
            .contains("shell.commands on 2026-09-14 is not known: no coverage"),
        "{}",
        p.markdown
    );
}

/// Gaps are the stretches a source's coverage does not reach; a source Sinex
/// holds nothing of is unknown over the whole span, never "no gap".
#[devkit::test]
async fn gaps_are_where_coverage_does_not_reach(db: devkit::pg::TestDatabase) {
    let w = World::new(db).await;
    w.admit(vec![commit("a", 0), commit("b", 24)]).await;
    let mut conn = w.conn().await;
    let settings = Settings::operator_default();
    let day = |d: &str| parse_day(d).unwrap();
    let all = gaps(
        &mut conn,
        &settings,
        None,
        day("2026-09-14"),
        day("2026-09-16"),
    )
    .await
    .unwrap();
    let spans: Vec<(&str, &str, &str)> = all
        .iter()
        .map(|g| (g.source.as_str(), g.from.as_str(), g.state))
        .collect();
    assert_eq!(
        spans,
        [
            ("code.git", "2026-09-14T02:00:00Z", "gap"),
            ("code.git", "2026-09-15T08:00:00.000001Z", "gap"),
        ],
        "{all:#?}"
    );
    assert_eq!(all[0].to, "2026-09-14T08:00:00Z");
    assert_eq!(all[1].to, "2026-09-17T02:00:00Z");
    let unknown = gaps(
        &mut conn,
        &settings,
        Some(&["desktop.hyprland".to_owned()]),
        day("2026-09-14"),
        day("2026-09-14"),
    )
    .await
    .unwrap();
    assert_eq!(unknown.len(), 1);
    assert_eq!(unknown[0].state, "unknown");
}
