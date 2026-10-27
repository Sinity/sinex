use super::*;
use sinex_derivations::activity::ActivitySpans;
use sinex_model::EventPayload;
use sinex_model::event_types::ActivitySpan;
use sinex_model::event_types::desktop::PresenceInterval;
use sinex_model::event_types::fused::FocusFused;

fn captured<P: EventPayload>(payload: &P, anchor: u64) -> Event {
    Event::build(
        payload,
        "synthetic.activity",
        location(anchor),
        ReadingOrder(0),
        *blake3::hash(&serde_json::to_vec(payload).unwrap()).as_bytes(),
        Class::Open,
    )
    .unwrap()
}

#[devkit::test]
async fn earlier_commands_focus_and_absence_are_read_when_they_overlap_the_day(
    db: devkit::pg::TestDatabase,
) {
    let keys = || vec![Box::new(ActivitySpans) as Box<dyn AnyKeyed>];
    let world = World::with_keys(db, keys()).await;
    let command = |start: i64, duration: i64| {
        captured(
            &CommandFused {
                record: format!("command-{start}"),
                command: "synthetic".into(),
                host: "test".into(),
                started_at: at(start),
                cwd: None,
                session: None,
                duration_ns: Some(duration * 1_000_000_000),
                exit_code: None,
                observed_by: vec![],
            },
            start as u64,
        )
    };
    let focus = |start: i64, end: i64| {
        captured(
            &FocusFused {
                record: format!("focus-{start}"),
                started_at: at(start),
                ended_at: Some(at(end)),
                app: Some("synthetic".into()),
                title: None,
                observed_by: vec![],
            },
            start as u64,
        )
    };
    let absence = captured(
        &PresenceInterval {
            day: "2024-06-15".into(),
            started_at: at(64_600),
            ended_at: at(65_000),
            state: "absent".into(),
            sparse: false,
            reported: "synthetic".into(),
            evidence: 0,
        },
        64_600,
    );
    world
        .admit(vec![
            command(64_000, 1_400),
            command(64_010, 10),
            command(64_020, 10),
            focus(63_000, 65_300),
            focus(64_030, 64_040),
            absence,
        ])
        .await;
    world
        .driver_with(keys(), vec![])
        .await
        .catch_up()
        .await
        .unwrap();
    let spans = world.live::<ActivitySpan>().await;
    let next: Vec<_> = spans
        .iter()
        .filter(|span| span.day == "2024-06-16")
        .collect();
    assert_eq!(next.len(), 1, "{spans:#?}");
    assert_eq!(next[0].started_at, at(65_000));
    assert_eq!(next[0].ended_at, at(65_400));
    assert_eq!(next[0].records["command.fused"], 1);
    assert_eq!(next[0].records["focus.fused"], 1);
}
