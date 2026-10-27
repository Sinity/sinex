use super::*;
use sinex_derivations::PerEvent;
use sinex_derivations::tags::TagApplier;
fn tags() -> TagApplier {
    TagApplier::new(
        serde_json::from_value(json!([
            {"tag": "sys.source.terminal", "event_type": "command.imported"},
            {"tag": "sys.source.terminal", "event_type": "command.executed"}
        ]))
        .unwrap(),
    )
    .unwrap()
}
use sinex_model::event_types::derived::TagApplied;

#[devkit::test]
async fn tag_rules_catch_up_and_do_not_add_duplicate_assignments(db: devkit::pg::TestDatabase) {
    let w = World::new(db).await;
    w.admit(vec![zsh("make", 0, 1), zsh("ls", 10, 2)]).await;
    let mut runner = w.driver_with(vec![], vec![Box::new(tags())]).await;
    runner.catch_up().await.unwrap();
    let tags: Vec<TagApplied> = w.live().await;
    assert_eq!(tags.len(), 2);
    assert!(tags.iter().all(|tag| tag.tag == "sys.source.terminal"));
    runner.catch_up().await.unwrap();
    assert_eq!(w.live::<TagApplied>().await, tags);
    assert_eq!(
        w.count("SELECT count(*) FROM core.events WHERE event_type = 'tag.applied'")
            .await,
        2
    );
}

struct NoTags;
impl PerEvent for NoTags {
    const NAME: &'static str = "tag.applier";
    const VERSION: u32 = 2;
    const INPUTS: &'static [&'static str] = &["command.imported"];
    const OUTPUTS: &'static [&'static str] = &["tag.applied"];
    fn derive(&self, _: sinex_derivations::Input<'_>, _: &Env) -> Result<Vec<Event>, String> {
        Ok(vec![])
    }
}

#[devkit::test]
async fn a_per_event_version_removes_outputs_whose_rules_no_longer_match(
    db: devkit::pg::TestDatabase,
) {
    let w = World::new(db).await;
    w.admit(vec![zsh("make", 0, 1)]).await;
    let mut old = w.driver_with(vec![], vec![Box::new(tags())]).await;
    old.catch_up().await.unwrap();
    assert_eq!(w.live::<TagApplied>().await.len(), 1);
    old.end_runs().await.unwrap();
    let mut new = w.driver_with(vec![], vec![Box::new(NoTags)]).await;
    new.catch_up().await.unwrap();
    assert!(w.live::<TagApplied>().await.is_empty());
    assert_eq!(
        w.count("SELECT count(*) FROM audit.archived WHERE reason = 'recomputed'")
            .await,
        1
    );
    assert_eq!(
        w.count("SELECT count(*) FROM core.live_equivalence WHERE event_type = 'command.imported'")
            .await,
        1
    );
}

#[devkit::test]
async fn hiding_an_input_removes_its_tags_and_unhiding_computes_them_again(
    db: devkit::pg::TestDatabase,
) {
    let w = World::new(db).await;
    let event = zsh("make", 0, 1);
    w.admit(vec![event.clone()]).await;
    let mut runner = w.driver_with(vec![], vec![Box::new(tags())]).await;
    runner.catch_up().await.unwrap();
    let tags = w.live::<TagApplied>().await;
    assert_eq!(tags.len(), 1);
    w.batch(
        vec![],
        vec![ArchiveRequest::Hide {
            target: event.id,
            operation: OperationId::new(),
        }],
    )
    .await;
    assert!(w.live::<TagApplied>().await.is_empty());
    runner.catch_up().await.unwrap();
    assert!(w.live::<TagApplied>().await.is_empty());
    w.batch(
        vec![],
        vec![ArchiveRequest::Unhide {
            target: event.id,
            operation: OperationId::new(),
        }],
    )
    .await;
    runner.catch_up().await.unwrap();
    assert_eq!(w.live::<TagApplied>().await, tags);
}

#[devkit::test]
async fn replacing_an_input_removes_the_previous_readings_tags(db: devkit::pg::TestDatabase) {
    let w = World::new(db).await;
    w.admit(vec![atuin("one", "make", 0, -1, 1)]).await;
    let mut runner = w.driver_with(vec![], vec![Box::new(tags())]).await;
    runner.catch_up().await.unwrap();
    assert_eq!(w.live::<TagApplied>().await.len(), 1);
    w.admit(vec![atuin("one", "make", 0, 0, 2)]).await;
    assert!(w.live::<TagApplied>().await.is_empty());
    runner.catch_up().await.unwrap();
    assert_eq!(w.live::<TagApplied>().await.len(), 1);
}

#[devkit::test]
async fn changed_rules_replace_even_a_large_previous_reading_order(db: devkit::pg::TestDatabase) {
    let w = World::new(db).await;
    let input = zsh("make", 0, 1);
    w.admit(vec![input.clone()]).await;
    let mut previous = w.driver_with(vec![], vec![Box::new(tags())]).await;
    previous.catch_up().await.unwrap();
    let mut payload = w.live::<TagApplied>().await.remove(0);
    payload.rules = vec!["previous rule".into()];
    let high_order = Event::build(
        &payload,
        TagApplier::NAME,
        Origin::Derived {
            parents: vec![input.id],
        },
        ReadingOrder(1 << 40),
        [7; 32],
        Class::Open,
    )
    .unwrap();
    w.handle
        .submit(
            Priority::Derived,
            Batch {
                run: previous.runs[TagApplier::NAME],
                kind: BatchKind::Derived {
                    derivation: TagApplier::NAME.into(),
                    scope: DerivedScope::Position {
                        inputs: vec![input.id.0],
                        position: 1,
                        read: Default::default(),
                    }
                    .to_value(),
                },
                records: vec![RecordOutcome::Events {
                    location: None,
                    events: vec![high_order],
                }],
                archive: vec![],
            },
        )
        .await
        .unwrap();
    assert_eq!(w.live::<TagApplied>().await[0].rules, ["previous rule"]);
    previous.end_runs().await.unwrap();
    let changed = TagApplier::new(serde_json::from_value(json!([
        {"tag": "sys.source.terminal", "event_type": "command.imported", "when": [{"field": "/command", "is": "make"}]}
    ])).unwrap()).unwrap();
    let mut current = w.driver_with(vec![], vec![Box::new(changed)]).await;
    current.catch_up().await.unwrap();
    let live = w.live::<TagApplied>().await;
    assert_eq!(live.len(), 1);
    assert!(live[0].rules[0].contains("make"));
}

#[devkit::test]
async fn a_new_per_event_derivation_reads_inputs_after_commit_log_pruning(
    db: devkit::pg::TestDatabase,
) {
    let w = World::new(db).await;
    w.admit(vec![zsh("make", 0, 1)]).await;
    let mut conn = PgConnection::connect(&w.url).await.unwrap();
    sqlx::query("DELETE FROM core.commit_log")
        .execute(&mut conn)
        .await
        .unwrap();
    let mut runner = w.driver_with(vec![], vec![Box::new(tags())]).await;
    runner.catch_up().await.unwrap();
    assert_eq!(w.live::<TagApplied>().await.len(), 1);
}
