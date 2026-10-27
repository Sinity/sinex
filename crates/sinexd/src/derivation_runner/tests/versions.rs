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
    const NAME: &'static str = "test.days";
    const VERSION: u32 = V;
    const INPUTS: &'static [&'static str] = &["command.imported"];
    const OUTPUTS: &'static [&'static str] = &["command.fused"];

    fn keys_for(&self, input: &KeyView<'_>, env: &Env) -> Vec<Value> {
        input
            .ts_orig
            .map(|t| Self::key(t, env))
            .into_iter()
            .collect()
    }
    fn inputs(&self, key: &Value, env: &Env) -> Option<InputQuery> {
        let (start, end) = Self::window(key, env)?;
        Some(InputQuery::window(Self::INPUTS, start, end))
    }
    fn compute(&self, key: &Value, inputs: &[Event], _: &Env) -> Computed {
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
            observed_by: vec![],
        };
        Computed {
            outputs: vec![sinex_derivations::derived_event::derived(
                &payload,
                "test.days",
                inputs,
            )],
            waiting: false,
        }
    }
    fn key_state(&self, _: &Value, _: &Env) -> KeyState {
        KeyState::Closed
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

    // Version 2 marks every key it produces and every key version 1 wrote.
    let mut v2 = w.driver_with(vec![Box::new(Days::<2>)], vec![]).await;
    let mut conn = PgConnection::connect(&w.url).await.unwrap();
    let mut dirty = derivations::dirty(&mut conn, &["test.days".to_owned()], 100)
        .await
        .unwrap();
    assert_eq!(dirty.len(), 5, "three logical days and two UTC days");

    // Half the keys computed: each live output names the version that wrote it.
    let env = v2.env();
    let half: Vec<KeyWork> = dirty
        .drain(..2)
        .map(|k| (k.key, k.last_dirty_seq, false))
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
