//! Outputs computed for an input that stops being live before they are stored
//! are dropped, never admitted citing a non-live parent.
use super::*;
use sinex_derivations::PerEvent;

/// Replay `event`'s material: its reading is archived and read again, with a
/// new ID. Returns the new reading.
async fn replay(w: &World, event: &Event) -> Event {
    let Origin::Record { location, .. } = &event.origin else {
        panic!("a captured event")
    };
    let mut again = event.clone();
    again.id = sinex_model::EventId::new();
    let operation = OperationId::new();
    w.handle
        .submit(
            Priority::Live,
            Batch {
                run: RunId::new(),
                kind: BatchKind::Replay {
                    operation,
                    source: None,
                    host: None,
                },
                records: vec![RecordOutcome::Events {
                    location: Some(*location),
                    events: vec![again.clone()],
                }],
                archive: vec![ArchiveRequest::Replay {
                    scope: sinex_model::ReplayScope {
                        materials: vec![location.material],
                        ..Default::default()
                    },
                    operation,
                }],
            },
        )
        .await
        .unwrap();
    again
}

const LIVE_CHILD_OF_ARCHIVED: &str = "SELECT count(*) FROM core.events e, unnest(e.parents) p
     WHERE (EXISTS (SELECT 1 FROM core.live_equivalence l WHERE l.event_id = e.id)
         OR EXISTS (SELECT 1 FROM core.live_locations l WHERE l.event_id = e.id))
       AND NOT EXISTS (SELECT 1 FROM core.live_equivalence l WHERE l.event_id = p)
       AND NOT EXISTS (SELECT 1 FROM core.live_locations l WHERE l.event_id = p)";

#[devkit::test]
async fn a_key_computed_from_an_input_archived_meanwhile_is_computed_again(
    db: devkit::pg::TestDatabase,
) {
    let w = World::with_keys(db, vec![Box::new(Count::<1>)]).await;
    let first = zsh("make", 0, 1);
    w.admit(vec![first.clone()]).await;
    let mut d = w.driver_with(vec![Box::new(Count::<1>)], vec![]).await;
    d.catch_up().await.unwrap();
    let before: Vec<CommandFused> = w.live().await;
    assert_eq!(before.len(), 1);

    // Computed from the first reading, which a replay reads again before the
    // outputs reach the writer.
    let stale = Keyed::compute(
        &Count::<1>,
        &json!("all"),
        std::slice::from_ref(&first),
        &Env::at(
            at(0),
            sinex_model::time_settings::TimeSettings::new("Europe/Warsaw", 4, 0)
                .expect("fixture day setting"),
        ),
    );
    let newer = replay(&w, &first).await;
    let out = w
        .handle
        .submit(
            Priority::Derived,
            Batch {
                run: RunId::new(),
                kind: BatchKind::Derived {
                    derivation: "test.count".into(),
                    scope: DerivedScope::Keys {
                        keys: vec![ComputedKey {
                            key: json!("all"),
                            dirty_seq: i64::MAX,
                            waiting: false,
                            refresh_at: None,
                        }],
                        read: Default::default(),
                    }
                    .to_value(),
                },
                records: vec![RecordOutcome::Events {
                    location: None,
                    events: stale.outputs,
                }],
                archive: vec![],
            },
        )
        .await
        .unwrap();
    assert_eq!((out.admitted, out.superseded_input), (0, 1), "{out:?}");
    assert_eq!(w.count(LIVE_CHILD_OF_ARCHIVED).await, 0);
    assert_eq!(
        w.count("SELECT count(*) FROM derivation.dirty WHERE derivation = 'test.count'")
            .await,
        1,
        "the key stays dirty"
    );

    // Computed again, the key's output cites the newer reading.
    d.catch_up().await.unwrap();
    assert_eq!(w.count(LIVE_CHILD_OF_ARCHIVED).await, 0);
    let cites_newer = format!(
        "SELECT count(*) FROM core.events e WHERE e.source = 'test.count' AND e.parents = ARRAY['{}'::uuid]
         AND EXISTS (SELECT 1 FROM core.live_equivalence l WHERE l.event_id = e.id)",
        newer.id.0
    );
    assert_eq!(w.count(&cites_newer).await, 1);
    assert_eq!(d.counts["test.count"].superseded_input, 0);
}

#[devkit::test]
async fn a_per_event_output_of_an_input_archived_meanwhile_is_dropped(
    db: devkit::pg::TestDatabase,
) {
    let w = World::new(db).await;
    let first = zsh("make", 0, 1);
    let other = zsh("ls", 10, 2);
    w.admit(vec![first.clone(), other.clone()]).await;
    let env = Env::at(
        at(0),
        sinex_model::time_settings::TimeSettings::new("Europe/Warsaw", 4, 0)
            .expect("fixture day setting"),
    );
    let derive = |e: &Event| {
        PerEvent::derive(
            &Echo,
            sinex_derivations::Input {
                event: e,
                bytes: None,
            },
            &env,
        )
        .unwrap()
    };
    let mut outputs = derive(&first);
    outputs.extend(derive(&other));
    replay(&w, &first).await;
    let mut conn = PgConnection::connect(&w.url).await.unwrap();
    let position: i64 =
        sqlx::query_scalar("SELECT max(commit_seq) FROM core.commit_log WHERE event_id = ANY($1)")
            .bind(vec![first.id.0, other.id.0])
            .fetch_one(&mut conn)
            .await
            .unwrap();
    let out = w
        .handle
        .submit(
            Priority::Derived,
            Batch {
                run: RunId::new(),
                kind: BatchKind::Derived {
                    derivation: "test.echo".into(),
                    scope: DerivedScope::Position {
                        inputs: vec![first.id.0, other.id.0],
                        position,
                        read: Default::default(),
                    }
                    .to_value(),
                },
                records: vec![RecordOutcome::Events {
                    location: None,
                    events: outputs,
                }],
                archive: vec![],
            },
        )
        .await
        .unwrap();
    assert_eq!((out.admitted, out.superseded_input), (1, 1), "{out:?}");
    assert_eq!(w.count(LIVE_CHILD_OF_ARCHIVED).await, 0);

    // The newer reading committed after the position, so it gets its own.
    let mut d = w.driver_with(vec![], vec![Box::new(Echo)]).await;
    d.catch_up().await.unwrap();
    let echoed: Vec<CommandFused> = w.live().await;
    let commands: Vec<&str> = echoed.iter().map(|f| f.command.as_str()).collect();
    assert_eq!(commands, ["MAKE", "LS"]);
    assert_eq!(w.count(LIVE_CHILD_OF_ARCHIVED).await, 0);
}
