//! Work reads stay within one session or objective; stored parent chains move
//! out of the live set before any recomputation runs.
use super::*;
use sinex_derivations::work::{ActivityDays, ContextPacks, WorkActs, WorkEpisodes};
use sinex_model::event_types::work::{ContextPack, WorkAct, WorkEpisode};

fn record(kind: &str, payload: Value, anchor: u64) -> Event {
    if kind.starts_with("polylogue.") {
        return external::record(kind, payload, location(anchor));
    }
    (sinex_model::event_types::find(kind).unwrap().build)(
        &payload,
        "synthetic.work",
        location(anchor),
        ReadingOrder(0),
        *blake3::hash(&serde_json::to_vec(&payload).unwrap()).as_bytes(),
        Class::Open,
    )
    .unwrap()
}
fn session(id: &str, branch: &str) -> Event {
    record(
        "polylogue.session",
        json!({"session":id,"origin":"synthetic","kind":"main", "cwd":["/synthetic/repo"],
        "repo":"synthetic/repo","branch":branch,"machine":"box","started":null,"last_activity":null,"links":[]}),
        0,
    )
}
fn prompt(id: &str, seconds: i64) -> Event {
    record(
        "polylogue.message",
        json!({"message":format!("{id}/{seconds}"),"session":id,"role":"user","written_by":"human",
        "prompt_kind":"prose","at":at(seconds),"text":"Continue the objective","usage":{},"attachments":[],"copies":[]}),
        seconds as u64,
    )
}
fn work_derivations() -> Vec<Box<dyn AnyKeyed>> {
    vec![
        Box::new(WorkActs),
        Box::new(WorkEpisodes),
        Box::new(ContextPacks),
        Box::new(ActivityDays),
    ]
}
async fn world(db: devkit::pg::TestDatabase) -> World {
    let world = World::with_keys(db, work_derivations()).await;
    external::register(&world.url).await;
    world
}
async fn runner(w: &World) -> DerivationRunner {
    let mut runner = w.driver_with(work_derivations(), vec![]).await;
    runner.clock = Some(at(2_000_000));
    runner
}
const LIVE_WITH_ARCHIVED_PARENT: &str = "SELECT count(*) FROM core.events e JOIN core.event_children c ON c.child = e.id
    JOIN audit.archived a ON a.event_id = c.parent WHERE NOT EXISTS (SELECT 1 FROM audit.archived a WHERE a.event_id = e.id)";

#[devkit::test]
async fn work_reads_the_objective_and_next_boundary_with_unknown_time_metadata(
    db: devkit::pg::TestDatabase,
) {
    let w = world(db).await;
    w.admit(vec![
        session("a", "first"),
        session("b", "second"),
        prompt("a", 0),
        prompt("b", 300),
        prompt("a", 1000),
        prompt("b", 100_000),
        prompt("b", 100_001),
    ])
    .await;
    let mut runner = runner(&w).await;
    runner.catch_up().await.unwrap();
    let episodes = w.live::<WorkEpisode>().await;
    assert_eq!(episodes.len(), 2);
    let first = episodes
        .iter()
        .find(|episode| episode.label == "first")
        .unwrap();
    assert_eq!(first.trail.len(), 2);
    assert_eq!(first.suspensions.len(), 2);
    let env = Env::at(at(2_000_000), sinex_model::time_settings::TimeSettings::new("Europe/Warsaw", 4, 0).expect("fixture day setting"));
    let query = Keyed::inputs(&WorkEpisodes, &json!({"episode": first.episode}), &env).unwrap();
    let mut conn = PgConnection::connect(&w.url).await.unwrap();
    let selected = derivations::read_window(&mut conn, &to_window(&query, env.now))
        .await
        .unwrap();
    assert_eq!(selected.len(), 4);
    assert!(
        selected
            .iter()
            .all(|event| event.event.event_type == "work.act"
                && event.event.ts_orig.at <= Some(at(100_000)))
    );
    let packs = w.live::<ContextPack>().await;
    assert_eq!(packs.len(), 2);
    assert!(packs.iter().all(|pack| {
        pack.trail
            .iter()
            .all(|entry| entry.event_type == "work.act")
    }));
    w.admit(vec![prompt("b", 500)]).await;
    let dirty: i64 = sqlx::query_scalar("SELECT count(*) FROM derivation.dirty WHERE derivation='work.episodes' AND key->>'episode'=$1")
        .bind(&first.episode).fetch_one(&mut conn).await.unwrap();
    assert_eq!(
        dirty, 0,
        "the source message first dirties its session's acts"
    );
    runner.catch_up().await.unwrap();
    assert_eq!(
        w.live::<WorkEpisode>()
            .await
            .iter()
            .find(|episode| episode.label == "first")
            .unwrap()
            .suspensions
            .len(),
        2
    );
}

#[devkit::test]
async fn archiving_a_parent_moves_all_stored_work_levels_before_recomputation(
    db: devkit::pg::TestDatabase,
) {
    let w = world(db).await;
    let first = prompt("a", 0);
    let later = prompt("a", 1000);
    w.admit(vec![session("a", "task"), first.clone(), later])
        .await;
    let mut runner = runner(&w).await;
    runner.catch_up().await.unwrap();
    let mut conn = PgConnection::connect(&w.url).await.unwrap();
    let old_pack: uuid::Uuid = sqlx::query_scalar(
        "SELECT event_id FROM core.live_equivalence WHERE event_type='context.pack'",
    )
    .fetch_one(&mut conn)
    .await
    .unwrap();
    w.batch(
        vec![],
        vec![ArchiveRequest::Hide {
            target: first.id,
            operation: OperationId::new(),
        }],
    )
    .await;
    assert!(w.live::<ContextPack>().await.is_empty());
    assert!(w.live::<WorkEpisode>().await.is_empty());
    assert_eq!(w.live::<WorkAct>().await.len(), 1);
    assert_eq!(w.count(LIVE_WITH_ARCHIVED_PARENT).await, 0);
    let before: Value = sqlx::query_scalar("SELECT payload FROM core.events WHERE id=$1")
        .bind(old_pack)
        .fetch_one(&mut conn)
        .await
        .unwrap();
    runner.catch_up().await.unwrap();
    assert_eq!(w.live::<ContextPack>().await[0].trail.len(), 1);
    w.batch(
        vec![],
        vec![ArchiveRequest::Unhide {
            target: sinex_model::EventId(old_pack),
            operation: OperationId::new(),
        }],
    )
    .await;
    assert_eq!(w.count(LIVE_WITH_ARCHIVED_PARENT).await, 0);
    assert_eq!(
        w.count(&format!(
            "SELECT count(*) FROM audit.archived WHERE event_id='{old_pack}'"
        ))
        .await,
        1
    );
    runner.catch_up().await.unwrap();
    w.batch(
        vec![],
        vec![ArchiveRequest::Unhide {
            target: first.id,
            operation: OperationId::new(),
        }],
    )
    .await;
    runner.catch_up().await.unwrap();
    assert_eq!(w.live::<ContextPack>().await[0].trail.len(), 2);
    assert_eq!(w.count(LIVE_WITH_ARCHIVED_PARENT).await, 0);
    let after: Value = sqlx::query_scalar("SELECT payload FROM core.events WHERE id=$1")
        .bind(old_pack)
        .fetch_one(&mut conn)
        .await
        .unwrap();
    assert_eq!(
        before, after,
        "archive keeps the stored interpretation unchanged"
    );
}

#[devkit::test]
async fn wake_day_reads_stop_at_the_next_recorded_wake(db: devkit::pg::TestDatabase) {
    let w = world(db).await;
    let sleep = |start, end| {
        record(
            "rest.fused",
            json!({"record":format!("{start}"),"started_at":at(start),"ended_at":at(end),"kind":"sleep","observed_by":[]}),
            start as u64,
        )
    };
    w.admit(vec![sleep(0, 100), sleep(900, 1000), sleep(1900, 2000)])
        .await;
    let env = Env::at(at(3000), sinex_model::time_settings::TimeSettings::new("Europe/Warsaw", 4, 0).expect("fixture day setting"));
    let query = Keyed::inputs(&ActivityDays, &json!({"start":at(100)}), &env).unwrap();
    let mut conn = PgConnection::connect(&w.url).await.unwrap();
    let selected = derivations::read_window(&mut conn, &to_window(&query, env.now))
        .await
        .unwrap();
    assert_eq!(selected.len(), 2);
    let input: Vec<_> = selected.into_iter().map(|event| event.event).collect();
    let output = Keyed::compute(&ActivityDays, &json!({"start":at(100)}), &input, &env);
    assert_eq!(output.outputs.len(), 1);
    assert_eq!(output.outputs[0].payload["ended_at"], json!(at(1000)));
    assert_eq!(output.outputs[0].payload["provisional"], false);
}

#[devkit::test]
async fn an_episode_becomes_dormant_without_a_new_act(db: devkit::pg::TestDatabase) {
    let w = world(db).await;
    w.admit(vec![session("a", "task"), prompt("a", 0)]).await;
    let mut runner = w.driver_with(work_derivations(), vec![]).await;
    runner.clock = Some(at(0));
    runner.catch_up().await.unwrap();
    assert_eq!(w.live::<WorkEpisode>().await[0].state, "open");
    runner.clock = Some(at(14 * 86_400 + 1));
    runner.catch_up().await.unwrap();
    assert_eq!(w.live::<WorkEpisode>().await[0].state, "dormant");
    assert_eq!(
        w.count("SELECT count(*) FROM derivation.dirty WHERE derivation='work.episodes'")
            .await,
        0
    );
    assert_eq!(w.count(LIVE_WITH_ARCHIVED_PARENT).await, 0);
}

#[devkit::test]
async fn an_open_wake_day_updates_for_later_spans_without_dirtying_itself(
    db: devkit::pg::TestDatabase,
) {
    let w = World::new(db).await;
    let sleep = record(
        "rest.fused",
        json!({"record":"sleep","started_at":at(0),"ended_at":at(100),"kind":"sleep","observed_by":[]}),
        0,
    );
    w.admit(vec![
        sleep,
        super::activity::physical(150),
        super::activity::physical(170),
    ])
    .await;
    let mut runner = w.runner().await;
    runner.clock = Some(at(200));
    runner.catch_up().await.unwrap();
    let first = w
        .live::<sinex_model::event_types::work::ActivityDay>()
        .await;
    assert_eq!(first[0].ended_at, at(200));
    runner.clock = Some(at(500));
    w.admit(vec![
        super::activity::physical(400),
        super::activity::physical(420),
    ])
    .await;
    runner.catch_up().await.unwrap();
    assert_eq!(
        w.live::<sinex_model::event_types::work::ActivityDay>()
            .await[0]
            .ended_at,
        at(500)
    );
    assert_eq!(
        w.count("SELECT count(*) FROM derivation.dirty WHERE derivation='activity.days'")
            .await,
        0
    );
}
