use super::*;
use crate::privacy::{self, Action, NewRule};

fn rule(source: &str, name: &str, action: Action) -> NewRule {
    NewRule {
        source: source.into(),
        name: name.into(),
        event_type: None,
        field: None,
        matcher: "all".into(),
        matcher_arg: None,
        class: Class::Guarded,
        action,
        author: "test".into(),
    }
}

async fn stored_class(conn: &mut PgConnection, id: EventId) -> String {
    privacy::projection::ensure_current(conn).await.unwrap();
    sqlx::query_scalar("SELECT class FROM privacy.event_outputs WHERE event_id=$1")
        .bind(id.0)
        .fetch_one(conn)
        .await
        .unwrap()
}

#[devkit::test]
async fn warm_batches_see_rules_floors_and_new_sources(db: devkit::pg::TestDatabase) {
    let mut conn = crate::AdmissionContext::connect(&db.url(None))
        .await
        .unwrap();
    let mut changes = PgConnection::connect(&db.url(None)).await.unwrap();
    // Load the complete set before a source's first event, then use that source
    // without a version change.
    let seq = privacy::add_rule(&mut changes, &rule("later", "later rule", Action::Classify))
        .await
        .unwrap();
    privacy::set_source_class(&mut changes, "unseen floor", Class::Guarded, "test")
        .await
        .unwrap();
    admit_one(&mut conn, &batch(vec![keyed("first", 1, 1, 1)])).await;
    let event = keyed("later", 1, 1, 1);
    admit_one(&mut conn, &batch(vec![event.clone()])).await;
    assert_eq!(stored_class(&mut conn, event.id).await, "guarded");

    let floor = keyed("unseen floor", 1, 1, 1);
    admit_one(&mut conn, &batch(vec![floor.clone()])).await;
    assert_eq!(stored_class(&mut conn, floor.id).await, "guarded");

    privacy::end_rule(&mut changes, "later rule", "test")
        .await
        .unwrap();
    let event = keyed("later", 2, 1, 1);
    admit_one(&mut conn, &batch(vec![event.clone()])).await;
    assert_eq!(stored_class(&mut conn, event.id).await, "open");
    privacy::set_source_class(&mut changes, "first", Class::Guarded, "test")
        .await
        .unwrap();
    let event = keyed("first", 2, 1, 1);
    let run = RunId::new();
    sqlx::query("INSERT INTO core.runs(id,name,host) VALUES($1,'test','h')")
        .bind(run.0)
        .execute(&mut conn)
        .await
        .unwrap();
    let mut b = batch(vec![event.clone()]);
    b.run = run;
    admit_one(&mut conn, &b).await;
    assert_eq!(stored_class(&mut conn, event.id).await, "guarded");
    let recorded: i64 = sqlx::query_scalar("SELECT privacy_rules_seq FROM core.runs WHERE id=$1")
        .bind(run.0)
        .fetch_one(&mut conn)
        .await
        .unwrap();
    assert!(recorded > seq);
    privacy::set_source_class(&mut changes, "first", Class::Open, "test")
        .await
        .unwrap();
    let event = keyed("first", 3, 1, 1);
    admit_one(&mut conn, &batch(vec![event.clone()])).await;
    assert_eq!(stored_class(&mut conn, event.id).await, "open");
    let mut changed = rule("first", "changed pattern", Action::Classify);
    changed.matcher = "pattern".into();
    changed.matcher_arg = Some("cmd-1".into());
    changed.field = Some("/command".into());
    privacy::add_rule(&mut changes, &changed).await.unwrap();
    let matches = keyed("first", 4, 1, 1);
    let differs = keyed("first", 5, 2, 1);
    admit_one(&mut conn, &batch(vec![matches.clone(), differs.clone()])).await;
    assert_eq!(stored_class(&mut conn, matches.id).await, "guarded");
    assert_eq!(stored_class(&mut conn, differs.id).await, "open");
}

#[devkit::test]
async fn rolled_back_rules_do_not_survive_in_the_cache(db: devkit::pg::TestDatabase) {
    let mut conn = crate::AdmissionContext::connect(&db.url(None))
        .await
        .unwrap();
    admit_one(&mut conn, &batch(vec![keyed("s", 1, 1, 1)])).await;
    let mut tx = conn.begin().await.unwrap();
    privacy::add_rule(&mut tx, &rule("s", "aborted", Action::Classify))
        .await
        .unwrap();
    tx.admit(&batch(vec![keyed("s", 2, 1, 1)]), &NoDerivations)
        .await
        .unwrap();
    tx.rollback().await.unwrap();
    let event = keyed("s", 2, 1, 1);
    admit_one(&mut conn, &batch(vec![event.clone()])).await;
    assert_eq!(stored_class(&mut conn, event.id).await, "open");
}

#[devkit::test]
async fn hash_initialization_commits_before_a_failed_admission(db: devkit::pg::TestDatabase) {
    let mut raw = PgConnection::connect(&db.url(None)).await.unwrap();
    // An aborted initialization must never supply a context's cached key.
    let mut tx = raw.begin().await.unwrap();
    sqlx::query("INSERT INTO privacy.settings(id,hash_key) VALUES(true,$1)")
        .bind(vec![9u8; 32])
        .execute(&mut *tx)
        .await
        .unwrap();
    tx.rollback().await.unwrap();
    let mut conn = crate::AdmissionContext::claim(raw).await.unwrap();
    let key: Vec<u8> = sqlx::query_scalar("SELECT hash_key FROM privacy.settings WHERE id")
        .fetch_one(&mut conn)
        .await
        .unwrap();
    assert_ne!(key, vec![9u8; 32]);
    privacy::add_rule(&mut conn, &rule("s", "hash", Action::Hash))
        .await
        .unwrap();
    let event = keyed("s", 1, 1, 1);
    let mut tx = conn.begin().await.unwrap();
    tx.admit(&batch(vec![event.clone()]), &NoDerivations)
        .await
        .unwrap();
    tx.rollback().await.unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, Vec<u8>>("SELECT hash_key FROM privacy.settings WHERE id")
            .fetch_one(&mut conn)
            .await
            .unwrap(),
        key
    );
    admit_one(&mut conn, &batch(vec![event.clone()])).await;
    privacy::projection::ensure_current(&mut conn).await.unwrap();
    let first: String = sqlx::query_scalar("SELECT equivalence_key FROM privacy.event_outputs WHERE event_id=$1")
        .bind(event.id.0)
        .fetch_one(&mut conn)
        .await
        .unwrap();
    conn.close().await.unwrap();
    let mut conn = crate::AdmissionContext::connect(&db.url(None))
        .await
        .unwrap();
    let repeated = admit_one(&mut conn, &batch(vec![event])).await;
    assert_eq!((repeated.admitted, repeated.repeats), (0, 1));
    assert!(first.starts_with("hash:"));
}

#[devkit::test]
async fn rules_and_floors_serialize_the_policy_epoch(db: devkit::pg::TestDatabase) {
    let url=db.url(None);let mut conn=crate::AdmissionContext::connect(&url).await.unwrap();
    let mut changes=PgConnection::connect(&url).await.unwrap();let mut tx=changes.begin().await.unwrap();
    privacy::add_rule(&mut tx,&rule("s","pending",Action::Classify)).await.unwrap();
    let mut contender=conn.begin().await.unwrap();
    let error=sqlx::query("SELECT desired_epoch FROM privacy.policy_state WHERE id FOR UPDATE NOWAIT").execute(&mut *contender).await.unwrap_err();
    assert_eq!(error.as_database_error().unwrap().code().as_deref(),Some("55P03"));contender.rollback().await.unwrap();
    tx.commit().await.unwrap();
    privacy::set_source_class(&mut changes,"floor",Class::Guarded,"test").await.unwrap();
    let first=keyed("s",1,1,1);let floor=keyed("floor",1,1,1);admit_one(&mut conn,&batch(vec![first.clone(),floor.clone()])).await;
    assert_eq!(stored_class(&mut conn,first.id).await,"guarded");assert_eq!(stored_class(&mut conn,floor.id).await,"guarded");
}

#[devkit::test]
async fn rolled_back_rule_epoch_does_not_survive_in_the_writer_cache(db: devkit::pg::TestDatabase) {
    let mut conn=crate::AdmissionContext::connect(&db.url(None)).await.unwrap();
    let mut tx=conn.begin().await.unwrap();privacy::add_rule(&mut tx,&rule("s","aborted",Action::Classify)).await.unwrap();
    tx.admit(&batch(vec![keyed("s",1,1,1)]),&NoDerivations).await.unwrap();tx.rollback().await.unwrap();
    privacy::add_rule(&mut conn,&rule("t","committed",Action::Classify)).await.unwrap();
    let open=keyed("s",2,1,1);let guarded=keyed("t",2,1,1);admit_one(&mut conn,&batch(vec![open.clone(),guarded.clone()])).await;
    assert_eq!(stored_class(&mut conn,open.id).await,"open");assert_eq!(stored_class(&mut conn,guarded.id).await,"guarded");
}
