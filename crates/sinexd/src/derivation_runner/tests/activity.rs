use super::*;
use sinex_model::event_types::input::KeyPressed;
use sinex_model::event_types::{ActivitySession, ActivitySpan, ActivityWindow};

pub(super) fn physical(start: i64) -> Event {
    let payload = KeyPressed {
        session: "test-input".into(),
        logged_at: at(start),
        kernel_at: None,
        key: "KEY_A".into(),
        code: Some(30),
        value: Some(1),
        changed: Some(true),
        clipboard: None,
        window: None,
        n: 0,
        extra: serde_json::from_value(json!({"host":"test","layer":"evdev","virtual":false}))
            .unwrap(),
    };
    Event::build(
        &payload,
        "synthetic.input",
        location(start as u64),
        ReadingOrder(0),
        *blake3::hash(&serde_json::to_vec(&payload).unwrap()).as_bytes(),
        Class::Open,
    )
    .unwrap()
}

#[devkit::test]
async fn physical_activity_crosses_days_and_replaces_outputs_after_hiding_a_bridge(
    db: devkit::pg::TestDatabase,
) {
    let world = World::new(db).await;
    let starts = [64_780, 64_800, 65_100, 65_120];
    let events: Vec<_> = starts.into_iter().map(physical).collect();
    let bridge = events[2].id;
    world.admit(events).await;
    let mut runner = world.runner().await;
    runner.clock = Some(at(86_400 * 5));
    runner.catch_up().await.unwrap();
    let sessions = world.live::<ActivitySession>().await;
    assert_eq!(sessions.len(), 1, "{sessions:#?}");
    assert_eq!(sessions[0].operator_present_s, 40.0);
    let spans = world.live::<ActivitySpan>().await;
    assert_eq!(
        spans
            .iter()
            .map(|span| span.operator_present_s)
            .sum::<f64>(),
        40.0
    );
    let windows = world.live::<ActivityWindow>().await;
    assert_eq!(
        windows
            .iter()
            .map(|window| window
                .counts
                .records
                .get("key.pressed")
                .copied()
                .unwrap_or(0))
            .sum::<u64>(),
        4
    );
    world
        .batch(
            vec![],
            vec![ArchiveRequest::Hide {
                target: bridge,
                operation: OperationId::new(),
            }],
        )
        .await;
    runner.catch_up().await.unwrap();
    let sessions = world.live::<ActivitySession>().await;
    assert_eq!(sessions.len(), 1);
    assert_eq!(sessions[0].operator_present_s, 20.0);
    world
        .batch(
            vec![],
            vec![ArchiveRequest::Unhide {
                target: bridge,
                operation: OperationId::new(),
            }],
        )
        .await;
    runner.catch_up().await.unwrap();
    assert_eq!(
        world.live::<ActivitySession>().await[0].operator_present_s,
        40.0
    );
}

#[devkit::test]
async fn late_activity_bridges_and_first_member_removal_replace_complete_groups(
    db: devkit::pg::TestDatabase,
) {
    let world = World::new(db).await;
    let first = physical(0);
    let first_id = first.id;
    world.admit(vec![first, physical(400)]).await;
    let mut runner = world.runner().await;
    runner.clock = Some(at(86_400 * 5));
    runner.catch_up().await.unwrap();
    assert_eq!(world.live::<ActivityWindow>().await.len(), 2);
    world.admit(vec![physical(200)]).await;
    runner.catch_up().await.unwrap();
    let windows = world.live::<ActivityWindow>().await;
    assert_eq!(windows.len(), 1);
    assert_eq!(windows[0].counts.records["key.pressed"], 3);
    assert_eq!(windows[0].operator_present_s, 0.0);
    let old_window = windows[0].window_id.clone();
    world
        .batch(
            vec![],
            vec![ArchiveRequest::Hide {
                target: first_id,
                operation: OperationId::new(),
            }],
        )
        .await;
    runner.catch_up().await.unwrap();
    let windows = world.live::<ActivityWindow>().await;
    assert_eq!(windows.len(), 1);
    assert_ne!(windows[0].window_id, old_window);
    assert_eq!(windows[0].started_at, at(200));
    let session = world.live::<ActivitySession>().await;
    assert_eq!(session.len(), 1);
    assert_eq!(session[0].started_at, at(200));
}

#[devkit::test]
async fn changing_the_activity_break_profile_recomputes_existing_sessions(
    db: devkit::pg::TestDatabase,
) {
    let world = World::new(db).await;
    world.admit(vec![physical(0), physical(600)]).await;
    let mut runner = world.runner().await;
    runner.clock = Some(at(86_400 * 5));
    runner.catch_up().await.unwrap();
    assert_eq!(world.live::<ActivitySession>().await.len(), 1);
    runner.end_runs().await.unwrap();
    let configuration = sinex_derivations::Configuration {
        activity: sinex_derivations::activity::ActivityProfile { break_seconds: 300 },
        ..Default::default()
    };
    let mut changed = world
        .driver_with(sinex_derivations::keyed_with(&configuration), vec![])
        .await;
    changed.clock = Some(at(86_400 * 5));
    // Installing a profile preserves the selected producer until comparison.
    assert_eq!(world.live::<ActivitySession>().await.len(), 1);
    let comparison = changed
        .compare("activity.sessions", None, 10)
        .await
        .unwrap();
    changed
        .switch("activity.sessions", comparison.id.unwrap(), "test")
        .await
        .unwrap();
    changed.catch_up().await.unwrap();
    let sessions = world.live::<ActivitySession>().await;
    assert_eq!(sessions.len(), 2);
    assert!(sessions.iter().all(|session| session.break_seconds == 300));
}

#[devkit::test]
async fn sessions_select_connected_windows_and_create_new_owners_after_removal(
    db: devkit::pg::TestDatabase,
) {
    let w = World::new(db).await;
    let middle = physical(800);
    w.admit(vec![
        physical(0),
        middle.clone(),
        physical(1600),
        physical(10_000_000),
    ])
    .await;
    let mut runner = w.runner().await;
    runner.clock = Some(at(11_000_000));
    runner.catch_up().await.unwrap();
    let sessions = w.live::<ActivitySession>().await;
    assert_eq!(sessions.len(), 2);
    let windows = w.live::<ActivityWindow>().await;
    let first = windows
        .iter()
        .find(|window| window.started_at == at(0))
        .unwrap();
    let env = Env::at(
        at(11_000_000),
        sinex_model::time_settings::TimeSettings::new("Europe/Warsaw", 4, 0)
            .expect("fixture day setting"),
    );
    let query = Keyed::inputs(
        &sinex_derivations::activity::ActivitySessions(Default::default()),
        &json!({"at":first.started_at,"scope":first.scope}),
        &env,
    )
    .unwrap();
    let mut conn = PgConnection::connect(&w.url).await.unwrap();
    let selected = derivations::read_window(&mut conn, &to_window(&query, env.now))
        .await
        .unwrap();
    let starts: Vec<_> = selected
        .iter()
        .filter_map(|row| row.event.payload_as::<ActivityWindow>())
        .map(|window| window.started_at)
        .collect();
    assert_eq!(starts, [at(0), at(800), at(1600)]);
    let largest_break = sinex_derivations::activity::ActivitySessions(
        sinex_derivations::activity::ActivityProfile {
            break_seconds: u64::MAX,
        },
    );
    let query = Keyed::inputs(
        &largest_break,
        &json!({"at":first.started_at,"scope":first.scope}),
        &env,
    )
    .unwrap();
    let all = derivations::read_window(&mut conn, &to_window(&query, env.now))
        .await
        .unwrap();
    assert_eq!(
        all.iter()
            .filter_map(|row| row.event.payload_as::<ActivityWindow>())
            .count(),
        4,
        "a valid large break must not overflow a database interval"
    );

    w.batch(
        vec![],
        vec![ArchiveRequest::Hide {
            target: middle.id,
            operation: OperationId::new(),
        }],
    )
    .await;
    runner.catch_up().await.unwrap();
    let sessions = w.live::<ActivitySession>().await;
    assert_eq!(sessions.len(), 3);
    assert_eq!(
        sessions
            .iter()
            .map(|session| session.started_at)
            .collect::<Vec<_>>(),
        [at(0), at(1600), at(10_000_000)]
    );
}

#[devkit::test]
async fn quiet_windows_and_sessions_close_without_another_input(db: devkit::pg::TestDatabase) {
    let w = World::new(db).await;
    w.admit(vec![physical(0), physical(20)]).await;
    let mut runner = w.runner().await;
    runner.clock = Some(at(20));
    runner.catch_up().await.unwrap();
    assert!(w.live::<ActivityWindow>().await[0].provisional);
    runner.clock = Some(at(321));
    runner.catch_up().await.unwrap();
    assert!(!w.live::<ActivityWindow>().await[0].provisional);
    assert!(w.live::<ActivitySession>().await[0].provisional);
    runner.clock = Some(at(921));
    runner.catch_up().await.unwrap();
    let sessions = w.live::<ActivitySession>().await;
    assert!(!sessions[0].provisional);
    assert_eq!(sessions[0].close_reason.as_deref(), Some("gap"));
}
