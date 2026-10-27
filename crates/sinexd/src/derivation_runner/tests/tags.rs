use super::*;
use sinex_derivations::PerEvent;
use sinex_derivations::tags::Tags;
use sinex_model::event_types::derived::TagApplied;

#[devkit::test]
async fn tag_rules_catch_up_and_do_not_add_duplicate_assignments(db: devkit::pg::TestDatabase) {
    let w = World::new(db).await;
    w.admit(vec![zsh("make", 0, 1), zsh("ls", 10, 2)]).await;
    let mut runner = w.driver_with(vec![], vec![Box::new(Tags)]).await;
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
    const NAME: &'static str = "tags";
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
    let mut old = w.driver_with(vec![], vec![Box::new(Tags)]).await;
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
    let mut runner = w.driver_with(vec![], vec![Box::new(Tags)]).await;
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
    let mut runner = w.driver_with(vec![], vec![Box::new(Tags)]).await;
    runner.catch_up().await.unwrap();
    assert_eq!(w.live::<TagApplied>().await.len(), 1);
    w.admit(vec![atuin("one", "make", 0, 0, 2)]).await;
    assert!(w.live::<TagApplied>().await.is_empty());
    runner.catch_up().await.unwrap();
    assert_eq!(w.live::<TagApplied>().await.len(), 1);
}
