use super::*;

#[devkit::test]
async fn retirement_waits_for_online_child_edges_without_blocking_start(
    db: devkit::pg::TestDatabase,
) {
    let w = World::new(db).await;
    let mut removed = zsh("synthetic removed output", 10, 1);
    removed.source = "clipboard.fusion".into();
    let id = removed.id;
    w.admit(vec![removed]).await;
    let mut conn = PgConnection::connect(&w.url).await.unwrap();
    sqlx::raw_sql(
        "INSERT INTO registry.derivations (name, kind, version, inputs, outputs)
         VALUES ('clipboard.fusion', 'keyed', 1, '{clipboard.copied}', '{clipboard.fused}');
         CREATE TABLE sinex_schema.event_children_fill (
           id boolean PRIMARY KEY CHECK (id), filled_through uuid NOT NULL);
         INSERT INTO sinex_schema.event_children_fill VALUES
           (true, '00000000-0000-7000-8000-000000000000')",
    )
    .execute(&mut conn)
    .await
    .unwrap();
    let registered = "SELECT count(*) FROM registry.derivations WHERE name='clipboard.fusion'";
    let operations = "SELECT count(*) FROM core.operations WHERE actor='sinex'";
    let mut runner = w.driver_with(vec![], vec![]).await;
    assert_eq!(w.count(operations).await, 0);
    assert_eq!(runner.pass().await.unwrap(), 0);
    assert_eq!(w.count(registered).await, 1);
    assert_eq!(w.count(operations).await, 0);
    // Restarting before the online worker finishes preserves the obligation.
    drop(runner);
    let mut runner = w.driver_with(vec![], vec![]).await;
    sinex_schema::online::finish(&mut conn).await.unwrap();
    runner.pass().await.unwrap();
    assert_eq!(w.count(registered).await, 0);
    assert_eq!(w.count(operations).await, 1);
    assert_eq!(
        w.count(&format!(
            "SELECT count(*) FROM audit.archived WHERE event_id='{}'",
            id.0
        ))
        .await,
        1
    );
    runner.pass().await.unwrap();
    assert_eq!(w.count(operations).await, 1, "retire only once");
}

#[devkit::test]
async fn outputs_of_removed_derivations_are_archived_once_through_the_lifecycle(
    db: devkit::pg::TestDatabase,
) {
    let w = World::new(db).await;
    // A reading the removed clipboard fusion wrote while an earlier build
    // registered it, and a reading no removed producer wrote.
    let mut singleton = zsh("copied text", 10, 1);
    singleton.source = "clipboard.fusion".into();
    let kept = zsh("make", 20, 2);
    let (singleton_id, kept_id) = (singleton.id, kept.id);
    w.admit(vec![singleton, kept]).await;
    {
        let mut conn = PgConnection::connect(&w.url).await.unwrap();
        sqlx::query(
            "INSERT INTO registry.derivations (name, kind, version, inputs, outputs)
             VALUES ('clipboard.fusion', 'keyed', 1, '{clipboard.copied}', '{clipboard.fused}')",
        )
        .execute(&mut conn)
        .await
        .unwrap();
    }
    let _runner = w.runner().await;
    let archived = |id: sinex_model::EventId| {
        format!(
            "SELECT count(*) FROM audit.archived a JOIN core.operations o ON o.id = a.operation_id
             WHERE a.event_id = '{}' AND o.kind = 'archive' AND o.actor = 'sinex' AND o.result = 'ok'",
            id.0
        )
    };
    assert_eq!(
        w.count(&archived(singleton_id)).await,
        1,
        "archived, not deleted"
    );
    assert_eq!(
        w.count(&format!(
            "SELECT count(*) FROM core.events WHERE id = '{}'",
            singleton_id.0
        ))
        .await,
        1
    );
    assert_eq!(w.count(&archived(kept_id)).await, 0);
    // The removed producer's registration ends, so a later start records no
    // operation.
    let registered = "SELECT count(*) FROM registry.derivations WHERE name = 'clipboard.fusion'";
    assert_eq!(w.count(registered).await, 0);
    let operations = "SELECT count(*) FROM core.operations WHERE actor = 'sinex'";
    assert_eq!(w.count(operations).await, 1);
    let _again = w.runner().await;
    assert_eq!(w.count(operations).await, 1);
    // The archive is an ordinary operation: undo brings the record back.
    let operation: uuid::Uuid = {
        let mut conn = PgConnection::connect(&w.url).await.unwrap();
        sqlx::query_scalar("SELECT id FROM core.operations WHERE actor = 'sinex'")
            .fetch_one(&mut conn)
            .await
            .unwrap()
    };
    let mut read = PgConnection::connect(&w.url).await.unwrap();
    let answer = crate::lifecycle::run(
        &mut read,
        &w.handle,
        Act::Undo,
        "agent:test",
        Scope::Operation {
            operation: sinex_model::OperationId(operation),
        },
        false,
    )
    .await
    .unwrap();
    assert!(
        matches!(answer, crate::lifecycle::Answer::Done(_)),
        "{answer:?}"
    );
    assert_eq!(
        w.count(&format!(
            "SELECT count(*) FROM audit.archived WHERE event_id = '{}'",
            singleton_id.0
        ))
        .await,
        0
    );
    // An undo stands: the next start does not archive the reading again.
    let _after_undo = w.runner().await;
    assert_eq!(w.count(&archived(singleton_id)).await, 0);
    assert_eq!(w.count(operations).await, 1, "no second archive");
}
