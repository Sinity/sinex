use super::*;
use serde_json::json;
use sqlx::Connection;

#[devkit::test]
async fn accepted_request_is_scoped_and_keeps_original_arguments(db: devkit::pg::TestDatabase) {
    let mut conn = PgConnection::connect(&db.url(None)).await.unwrap();
    let arguments =
        json!({"selection":"events where fixture", "steps":[{"verb":"save","name":"fixture"}]});
    let request = Request {
        caller: "agent",
        id: "same",
        actor: "agent",
        arguments: &arguments,
    };
    let first = begin(&mut conn, &request).await.unwrap();
    let repeat = begin(&mut conn, &request).await.unwrap();
    assert_eq!(first.operation, repeat.operation);
    assert_eq!(first.operation.0.get_version_num(), 7);
    let other = begin(
        &mut conn,
        &Request {
            caller: "operator",
            ..request
        },
    )
    .await
    .unwrap();
    assert_ne!(first.operation, other.operation);
    assert!(matches!(
        begin(
            &mut conn,
            &Request {
                arguments: &json!({"selection":"different"}),
                ..request
            }
        )
        .await,
        Err(Error::RequestConflict)
    ));
}

#[devkit::test]
async fn committed_step_is_immutable_and_rolls_back_with_its_transaction(
    db: devkit::pg::TestDatabase,
) {
    let mut conn = PgConnection::connect(&db.url(None)).await.unwrap();
    let accepted = begin(
        &mut conn,
        &Request {
            caller: "agent",
            id: "receipt",
            actor: "agent",
            arguments: &json!({}),
        },
    )
    .await
    .unwrap();
    let reply = json!({"event":"original", "citation":"original source"});
    let mut tx = conn.begin().await.unwrap();
    save_step(&mut tx, accepted.operation, 0, &reply)
        .await
        .unwrap();
    tx.rollback().await.unwrap();
    assert_eq!(step(&mut conn, accepted.operation, 0).await.unwrap(), None);
    let mut tx = conn.begin().await.unwrap();
    save_step(&mut tx, accepted.operation, 0, &reply)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    assert_eq!(
        step(&mut conn, accepted.operation, 0).await.unwrap(),
        Some(reply.clone())
    );
    assert!(
        save_step(
            &mut conn,
            accepted.operation,
            0,
            &json!({"event":"replacement"})
        )
        .await
        .is_err()
    );
    assert_eq!(
        step(&mut conn, accepted.operation, 0).await.unwrap(),
        Some(reply)
    );
}
