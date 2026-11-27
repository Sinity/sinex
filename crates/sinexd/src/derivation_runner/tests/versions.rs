//! Recomputes: one that changes nothing stores nothing, and a version that
//! keys its windows differently never leaves an input counted twice.
use super::*;

/// Counts commands per day: version 1 per UTC day, version 2 per logical day
/// in the operator's zone, so the two versions key the same inputs
/// differently.
struct Days<const V: u32>;

impl<const V: u32> Days<V> {
    fn key(t: Timestamp, env: &Env) -> Value {
        let secs = i64::try_from(t.unix_ns().div_euclid(1_000_000_000)).unwrap();
        if V == 1 {
            json!({ "utc": secs.div_euclid(86_400) })
        } else {
            let start = env
                .time_settings
                .day_start(env.time_settings.day(t).unwrap())
                .unwrap();
            let start = i64::try_from(start.unix_ns().div_euclid(1_000_000_000)).unwrap();
            json!({ "local": start })
        }
    }

    fn window(key: &Value, env: &Env) -> Option<(Timestamp, Timestamp)> {
        if V == 1 {
            let day = key.get("utc")?.as_i64()?;
            Some((
                Timestamp::from_unix_secs(day * 86_400).ok()?,
                Timestamp::from_unix_secs((day + 1) * 86_400).ok()?,
            ))
        } else {
            let start = Timestamp::from_unix_secs(key.get("local")?.as_i64()?).ok()?;
            let end = env
                .time_settings
                .day_end(env.time_settings.day(start).ok()?)
                .ok()?;
            Some((start, end))
        }
    }
}

impl<const V: u32> Keyed for Days<V> {
    type Key = Value;
    const NAME: &'static str = "test.days";
    const VERSION: u32 = V;
    const HISTORY: sinex_model::DerivationHistory = if V == 3 {
        sinex_model::DerivationHistory::KeepHistory
    } else {
        sinex_model::DerivationHistory::Recompute
    };
    const INPUTS: &'static [&'static str] = &["command.imported"];
    const OUTPUTS: &'static [&'static str] = &["command.fused"];

    fn keys_for(&self, input: &KeyView<'_>, env: &Env) -> Vec<Self::Key> {
        input
            .ts_orig
            .map(|t| Self::key(t, env))
            .into_iter()
            .collect()
    }
    fn inputs(&self, key: &Self::Key, env: &Env) -> Option<InputQuery> {
        let (start, end) = Self::window(key, env)?;
        Some(InputQuery::window(Self::INPUTS, start, end))
    }
    fn compute(&self, key: &Self::Key, inputs: &[Event], _: &Env) -> Computed {
        if inputs.is_empty() {
            return Computed::default();
        }
        let payload = CommandFused {
            record: key.to_string(),
            command: format!("v{V}:{}", inputs.len()),
            host: "box".into(),
            started_at: inputs[0].ts_orig.at.unwrap(),
            cwd: None,
            session: None,
            duration_ns: None,
            exit_code: None,
            execution: None,
            members: vec![],
            basis: sinex_model::event_types::fused::Basis::Rule,
            ambiguous: 1,
            competitors: vec![],
            rule_version: 1,
        };
        Computed {
            outputs: vec![
                sinex_derivations::derived_event::derived(&payload, "test.days", inputs).unwrap(),
            ],
            waiting: false,
            error: None,
            woken_by: None,
        }
    }
}

/// Live outputs of `test.days` with the version of the run that wrote each.
async fn days_by_version(w: &World) -> Vec<(String, String)> {
    let mut conn = PgConnection::connect(&w.url).await.unwrap();
    sqlx::query_as(
        "SELECT e.payload->>'command', m.version FROM core.events e
         JOIN core.runs r ON r.id = e.run_id JOIN core.manifests m ON m.id = r.manifest_id
         WHERE e.source = 'test.days' AND EXISTS (SELECT 1 FROM core.live_equivalence l WHERE l.event_id = e.id)
         ORDER BY e.ts_orig",
    )
    .fetch_all(&mut conn)
    .await
    .unwrap()
}

/// Inputs counted by more than one live output of `test.days`.
const COUNTED_TWICE: &str = "SELECT count(*) FROM (SELECT p FROM core.events e, unnest(e.parents) p
     WHERE e.source = 'test.days' AND EXISTS (SELECT 1 FROM core.live_equivalence l WHERE l.event_id = e.id)
     GROUP BY p HAVING count(*) > 1) twice";

#[devkit::test]
async fn a_version_that_keys_differently_never_counts_an_input_twice(db: devkit::pg::TestDatabase) {
    let w = World::with_keys(db, vec![Box::new(Days::<1>)]).await;
    // 01:00 UTC is 03:00 in Warsaw, before the 04:00 day start: one UTC day,
    // two logical days.
    w.admit(vec![
        zsh("early", -7 * 3_600, 1),
        zsh("morning", 0, 2),
        zsh("next day", 20 * 3_600, 3),
    ])
    .await;
    let mut v1 = w.driver_with(vec![Box::new(Days::<1>)], vec![]).await;
    v1.catch_up().await.unwrap();
    let one = |c: &str| (c.to_owned(), "1".to_owned());
    assert_eq!(days_by_version(&w).await, [one("v1:2"), one("v1:1")]);
    drop(v1);

    // Starting another version preserves current results until a recorded comparison is selected.
    let mut v2 = w.driver_with(vec![Box::new(Days::<2>)], vec![]).await;
    let mut conn = PgConnection::connect(&w.url).await.unwrap();
    assert!(
        derivations::dirty(&mut conn, &["test.days".to_owned()], 100)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(v2.pass().await.unwrap(), 0);
    assert_eq!(days_by_version(&w).await, [one("v1:2"), one("v1:1")]);
    let comparison = v2.compare("test.days", None, 10).await.unwrap();
    assert_eq!(days_by_version(&w).await, [one("v1:2"), one("v1:1")]);
    assert_eq!(
        v2.switch("test.days", comparison.id.unwrap(), "test")
            .await
            .unwrap(),
        5
    );
    assert!(
        days_by_version(&w).await.is_empty(),
        "switch archives replaced output chains before publishing another version"
    );
    let mut dirty = derivations::dirty(&mut conn, &["test.days".to_owned()], 100)
        .await
        .unwrap();
    assert_eq!(dirty.len(), 5, "three logical days and two UTC days");

    // Half the keys computed: each live output names the version that wrote it.
    let env = v2.env();
    let half: Vec<KeyWork> = dirty
        .drain(..2)
        .map(|k| (k.key, k.last_dirty_seq))
        .collect();
    v2.compute_keys(0, half, &env).await.unwrap();
    let mid = days_by_version(&w).await;
    assert_eq!(
        i64::try_from(mid.len()).unwrap(),
        w.count(
            "SELECT count(*) FROM core.events e WHERE e.source = 'test.days'
             AND EXISTS (SELECT 1 FROM core.live_equivalence l WHERE l.event_id = e.id)"
        )
        .await,
        "every live output names its run's version"
    );
    assert!(
        mid.iter()
            .all(|(command, version)| command.starts_with(&format!("v{version}:"))),
        "{mid:?}"
    );

    v2.catch_up().await.unwrap();
    let two = |c: &str| (c.to_owned(), "2".to_owned());
    assert_eq!(
        days_by_version(&w).await,
        [two("v2:1"), two("v2:1"), two("v2:1")]
    );
    assert_eq!(w.count(COUNTED_TWICE).await, 0);
}

/// A switch selects its version in one transaction and archives the previous
/// outputs in later ones. A daemon stopped in between leaves the switch
/// unfinished with its keys held, so nothing is computed beside outputs of
/// the version it replaced; the next start archives them and ends it.
#[devkit::test]
async fn a_switch_stopped_before_its_archive_ends_at_the_next_start(db: devkit::pg::TestDatabase) {
    let w = World::with_keys(db, vec![Box::new(Days::<1>)]).await;
    w.admit(vec![
        zsh("early", -7 * 3_600, 1),
        zsh("morning", 0, 2),
        zsh("next day", 20 * 3_600, 3),
    ])
    .await;
    let mut v1 = w.driver_with(vec![Box::new(Days::<1>)], vec![]).await;
    v1.catch_up().await.unwrap();
    drop(v1);
    let mut v2 = w.driver_with(vec![Box::new(Days::<2>)], vec![]).await;
    let comparison = v2.compare("test.days", None, 10).await.unwrap();
    assert_eq!(
        v2.select("test.days", comparison.id.unwrap(), "test")
            .await
            .unwrap(),
        5
    );
    let one = |c: &str| (c.to_owned(), "1".to_owned());
    assert_eq!(days_by_version(&w).await, [one("v1:2"), one("v1:1")]);
    let unfinished =
        "SELECT count(*) FROM core.operations WHERE kind = 'producer_switch' AND result IS NULL";
    assert_eq!(w.count(unfinished).await, 1);
    assert_eq!(
        v2.catch_up().await.map(|()| 0).unwrap(),
        w.count(
            "SELECT count(*) FROM core.events e JOIN core.runs r ON r.id = e.run_id
             JOIN core.manifests m ON m.id = r.manifest_id
             WHERE e.source = 'test.days' AND m.version = '2'"
        )
        .await,
        "the held keys are not computed while the previous outputs are live"
    );
    drop(v2);

    let mut again = w.driver_with(vec![Box::new(Days::<2>)], vec![]).await;
    assert!(days_by_version(&w).await.is_empty());
    assert_eq!(w.count(unfinished).await, 0);
    assert_eq!(
        w.count(
            "SELECT count(*) FROM audit.archived a JOIN core.operations o ON o.id = a.operation_id
             WHERE a.reason = 'version_change' AND o.kind = 'producer_switch' AND o.result = 'ok'
               AND o.end_seq >= o.start_seq"
        )
        .await,
        2
    );
    again.catch_up().await.unwrap();
    let two = |c: &str| (c.to_owned(), "2".to_owned());
    assert_eq!(
        days_by_version(&w).await,
        [two("v2:1"), two("v2:1"), two("v2:1")]
    );
    assert_eq!(w.count(COUNTED_TWICE).await, 0);
}

/// Every live derived event: its ID, type and payload.
async fn derived_dump(w: &World) -> Vec<(uuid::Uuid, String, String)> {
    let mut conn = PgConnection::connect(&w.url).await.unwrap();
    sqlx::query_as(
        "SELECT e.id, e.event_type, e.payload::text FROM core.events e
         WHERE e.parents IS NOT NULL
           AND (EXISTS (SELECT 1 FROM core.live_equivalence l WHERE l.event_id = e.id)
             OR EXISTS (SELECT 1 FROM core.live_locations l WHERE l.event_id = e.id))
         ORDER BY e.id",
    )
    .fetch_all(&mut conn)
    .await
    .unwrap()
}

/// Running every derivation's recompute again over unchanged inputs stores
/// nothing new: every output is a repeat of the live one.
#[devkit::test]
async fn recomputing_unchanged_inputs_changes_nothing(db: devkit::pg::TestDatabase) {
    let w = World::new(db).await;
    w.admit(vec![
        atuin("1", "make", 0, 0, 1),
        zsh("make", 1, 10),
        atuin("2", "cargo test", 60, 101, 1),
        zsh("cargo test", 60, 20),
        zsh("ls", 2_000, 30),
        focus("editor", 100),
        focus("browser", 400),
    ])
    .await;
    let mut d = w.runner().await;
    // Open intervals end at the runner's clock: keep it still.
    d.clock = Some(at(10_000));
    d.catch_up().await.unwrap();
    let before = derived_dump(&w).await;
    assert!(!before.is_empty());
    let events = w.count("SELECT count(*) FROM core.events").await;

    let names: Vec<&'static str> = sinex_derivations::keyed()
        .iter()
        .map(|k| k.name())
        .collect();
    for name in names {
        d.recompute_all(name, None).await.unwrap();
    }
    d.catch_up().await.unwrap();
    assert_eq!(derived_dump(&w).await, before);
    assert_eq!(
        w.count("SELECT count(*) FROM core.events").await,
        events,
        "nothing stored again"
    );
}

/// Inputs keep arriving between a comparison and its switch. The switch is
/// not refused, and each of those inputs is computed by the selected version:
/// one in a key the comparison saw, one in a key it never saw.
#[devkit::test]
async fn inputs_committed_after_a_comparison_are_computed_by_the_switched_version(
    db: devkit::pg::TestDatabase,
) {
    let w = World::with_keys(db, vec![Box::new(Days::<1>)]).await;
    w.admit(vec![zsh("first", 0, 1)]).await;
    let mut v1 = w.driver_with(vec![Box::new(Days::<1>)], vec![]).await;
    v1.catch_up().await.unwrap();
    drop(v1);
    let one = |c: &str| (c.to_owned(), "1".to_owned());
    let two = |c: &str| (c.to_owned(), "2".to_owned());
    assert_eq!(days_by_version(&w).await, [one("v1:1")]);

    let mut v2 = w.driver_with(vec![Box::new(Days::<2>)], vec![]).await;
    let comparison = v2.compare("test.days", None, 3).await.unwrap();
    // A version change still needs its switch before history is recomputed.
    let refused = v2.recompute_all("test.days", None).await.unwrap_err();
    assert!(refused.to_string().contains("compare"), "{refused}");
    w.admit(vec![zsh("same day", 60, 2), zsh("next day", 20 * 3_600, 3)])
        .await;
    v2.switch("test.days", comparison.id.unwrap(), "test")
        .await
        .unwrap();
    let mut conn = PgConnection::connect(&w.url).await.unwrap();
    assert_eq!(
        registry::selected(&mut conn, "test.days").await.unwrap(),
        Some(2)
    );
    v2.catch_up().await.unwrap();
    assert_eq!(days_by_version(&w).await, [two("v2:2"), two("v2:1")]);
    assert_eq!(w.count(COUNTED_TWICE).await, 0);
    assert!(
        derivations::dirty(&mut conn, &["test.days".to_owned()], 100)
            .await
            .unwrap()
            .is_empty()
    );
    // The selected version recomputes on request from here on: its two
    // logical days and the day the earlier version left outputs under.
    assert_eq!(
        v2.recompute_all("test.days", None)
            .await
            .unwrap()
            .keys_marked,
        3
    );
}

/// A comparison holds nothing still: a key whose newest input the stored
/// outputs were not yet computed from is compared and counted as behind.
#[devkit::test]
async fn a_comparison_counts_the_keys_behind_their_inputs(db: devkit::pg::TestDatabase) {
    let w = World::with_keys(db, vec![Box::new(Days::<1>)]).await;
    w.admit(vec![zsh("first", 0, 1), zsh("next day", 24 * 3_600, 2)])
        .await;
    let mut v1 = w.driver_with(vec![Box::new(Days::<1>)], vec![]).await;
    v1.catch_up().await.unwrap();
    w.admit(vec![zsh("second", 60, 3)]).await;
    let head = w
        .count("SELECT value FROM core.counters WHERE name = 'commit_seq'")
        .await;
    let behind = v1.compare("test.days", None, 3).await.unwrap();
    assert_eq!(
        (behind.keys, behind.behind, behind.same, behind.changed),
        (2, 1, 1, 1),
        "{behind}"
    );
    let mut conn = PgConnection::connect(&w.url).await.unwrap();
    let context: Value =
        sqlx::query_scalar("SELECT context FROM derivation.comparisons WHERE id = $1")
            .bind(behind.id.unwrap())
            .fetch_one(&mut conn)
            .await
            .unwrap();
    assert_eq!(context["behind"], 1);
    assert_eq!(context["positions"], json!({"from": head, "through": head}));
    v1.catch_up().await.unwrap();
    let settled = v1.compare("test.days", None, 3).await.unwrap();
    assert_eq!(
        (settled.keys, settled.behind, settled.same, settled.changed),
        (2, 0, 2, 0),
        "{settled}"
    );
}

const EARLIER_BUILD: &str = "1111111111111111111111111111111111111111;dirty=false";
const LATER_BUILD: &str = "2222222222222222222222222222222222222222;dirty=false";

/// A build that declares the selected version runs it. A deploy that changes
/// no version leaves outputs another build wrote; a recompute of a time
/// range marks their keys with no comparison, and nothing is stored again.
#[devkit::test]
async fn another_build_of_the_selected_version_recomputes_a_time_range(
    db: devkit::pg::TestDatabase,
) {
    let w = World::with_keys(db, vec![Box::new(Days::<1>)]).await;
    w.admit(vec![zsh("first", 0, 1), zsh("next day", 24 * 3_600, 2)])
        .await;
    let mut earlier = w
        .driver_built_by(EARLIER_BUILD, vec![Box::new(Days::<1>)], vec![])
        .await;
    earlier.catch_up().await.unwrap();
    drop(earlier);
    let one = |c: &str| (c.to_owned(), "1".to_owned());
    let outputs = derived_dump(&w).await;
    assert_eq!(days_by_version(&w).await, [one("v1:1"), one("v1:1")]);

    let mut later = w
        .driver_built_by(LATER_BUILD, vec![Box::new(Days::<1>)], vec![])
        .await;
    let mut conn = PgConnection::connect(&w.url).await.unwrap();
    assert_eq!(
        registry::selected(&mut conn, "test.days").await.unwrap(),
        Some(1)
    );
    // The first day's input is in the range; the next day's is not.
    assert_eq!(
        later
            .recompute_all("test.days", Some((at(-3_600), at(3_600))))
            .await
            .unwrap()
            .keys_marked,
        1
    );
    let dirty = derivations::dirty(&mut conn, &["test.days".to_owned()], 100)
        .await
        .unwrap();
    assert_eq!(dirty.len(), 1, "{dirty:?}");
    later.catch_up().await.unwrap();
    assert_eq!(later.counts["test.days"].keys, 1, "the marked key computed");
    assert_eq!(derived_dump(&w).await, outputs, "nothing stored again");
    let builds: Vec<String> = sqlx::query_scalar(
        "SELECT m.build FROM core.manifests m WHERE m.name = 'test.days' ORDER BY m.build",
    )
    .fetch_all(&mut conn)
    .await
    .unwrap();
    assert_eq!(builds, [EARLIER_BUILD, LATER_BUILD]);
}

/// A comparison is a trial of one build's code, so another build does not
/// switch on it; it makes its own.
#[devkit::test]
async fn a_switch_consumes_a_comparison_its_own_build_made(db: devkit::pg::TestDatabase) {
    let w = World::with_keys(db, vec![Box::new(Days::<1>)]).await;
    w.admit(vec![zsh("first", 0, 1)]).await;
    let mut v1 = w.driver_with(vec![Box::new(Days::<1>)], vec![]).await;
    v1.catch_up().await.unwrap();
    drop(v1);
    let mut earlier = w
        .driver_built_by(EARLIER_BUILD, vec![Box::new(Days::<2>)], vec![])
        .await;
    let tried = earlier.compare("test.days", None, 3).await.unwrap();
    drop(earlier);
    let mut later = w
        .driver_built_by(LATER_BUILD, vec![Box::new(Days::<2>)], vec![])
        .await;
    let refused = later
        .switch("test.days", tried.id.unwrap(), "test")
        .await
        .unwrap_err();
    assert!(
        refused.to_string().contains("version and build"),
        "{refused}"
    );
    let own = later.compare("test.days", None, 3).await.unwrap();
    later
        .switch("test.days", own.id.unwrap(), "test")
        .await
        .unwrap();
    let scope: Value =
        sqlx::query_scalar("SELECT scope FROM core.operations WHERE kind = 'producer_switch'")
            .fetch_one(&mut PgConnection::connect(&w.url).await.unwrap())
            .await
            .unwrap();
    assert_eq!(scope["build"], LATER_BUILD);
    assert_eq!(scope["version"], 2);
}

#[devkit::test]
async fn a_keep_history_switch_preserves_outputs_and_records_its_actor(
    db: devkit::pg::TestDatabase,
) {
    let w = World::with_keys(db, vec![Box::new(Days::<1>)]).await;
    w.admit(vec![zsh("kept", 0, 1)]).await;
    let mut first = w.driver_with(vec![Box::new(Days::<1>)], vec![]).await;
    first.catch_up().await.unwrap();
    let before = derived_dump(&w).await;
    let mut next = w.driver_with(vec![Box::new(Days::<3>)], vec![]).await;
    let comparison = next.compare("test.days", None, 3).await.unwrap();
    assert_eq!(
        next.switch("test.days", comparison.id.unwrap(), "agent:fixture")
            .await
            .unwrap(),
        0
    );
    assert_eq!(derived_dump(&w).await, before);
    let mut conn = PgConnection::connect(&w.url).await.unwrap();
    let operation: (String, String, String) = sqlx::query_as(
        "SELECT kind,actor,scope->>'history' FROM core.operations WHERE scope->>'comparison'=$1",
    )
    .bind(comparison.id.unwrap().to_string())
    .fetch_one(&mut conn)
    .await
    .unwrap();
    assert_eq!(
        operation,
        (
            "producer_switch".into(),
            "agent:fixture".into(),
            "keep_history".into()
        )
    );
    assert!(
        next.switch("test.days", comparison.id.unwrap(), "agent:fixture")
            .await
            .unwrap_err()
            .to_string()
            .contains("already selected")
    );
}

#[devkit::test]
async fn a_pending_version_computes_new_dirty_keys_without_recomputing_history(
    db: devkit::pg::TestDatabase,
) {
    let w = World::with_keys(db, vec![Box::new(Count::<1>)]).await;
    w.admit(vec![zsh("first", 0, 1)]).await;
    let mut first = w.driver_with(vec![Box::new(Count::<1>)], vec![]).await;
    first.catch_up().await.unwrap();
    let mut next = w.driver_with(vec![Box::new(Count::<2>)], vec![]).await;
    assert_eq!(next.pass().await.unwrap(), 0);
    w.admit(vec![zsh("new", 60, 2)]).await;
    next.catch_up().await.unwrap();
    let facts: Vec<CommandFused> = w.live().await;
    assert_eq!(
        facts.iter().filter(|fact| fact.command == "v2:2").count(),
        1
    );
    let mut conn = PgConnection::connect(&w.url).await.unwrap();
    assert_eq!(
        registry::selected(&mut conn, "test.count").await.unwrap(),
        Some(1)
    );
}

#[devkit::test]
async fn comparison_selection_requires_same_day_setting(db: devkit::pg::TestDatabase) {
    let w = World::with_keys(db, vec![Box::new(Days::<1>)]).await;
    w.admit(vec![zsh("first", 0, 1)]).await;
    let mut next = w.driver_with(vec![Box::new(Days::<2>)], vec![]).await;
    let comparison = next.compare("test.days", None, 3).await.unwrap();
    next.time_settings = TimeSettings::new("UTC", 0, 0).unwrap();
    let error = next
        .switch("test.days", comparison.id.unwrap(), "fixture")
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("logical-day settings changed"),
        "{error}"
    );
    assert_eq!(
        w.count("SELECT count(*) FROM core.operations WHERE kind='producer_switch'")
            .await,
        0
    );
}
