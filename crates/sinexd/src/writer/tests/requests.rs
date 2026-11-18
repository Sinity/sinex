use super::*;

#[devkit::test]
async fn request_retry_after_restart_returns_committed_identity_without_second_effect(
    db: devkit::pg::TestDatabase,
) {
    let url = db.url(None);
    let arguments =
        serde_json::json!({"selection":"events", "steps":[{"verb":"save","name":"fixture"}]});
    let mut receipt = None;
    let mut operation = None;
    for attempt in 0..2 {
        let connection = sqlx::PgConnection::connect(&url).await.unwrap();
        let (handle, queues) = channels();
        let writer = tokio::spawn(run(
            url.clone(),
            claim(connection).await.unwrap(),
            queues,
            Arc::new(sinex_store::NoDerivations),
            None,
        ));
        let accepted = handle
            .begin_request(
                "agent".into(),
                "lost-reply".into(),
                "agent".into(),
                arguments.clone(),
            )
            .await
            .unwrap();
        if let Some(operation) = operation {
            assert_eq!(accepted.operation, operation);
        }
        operation = Some(accepted.operation);
        let response = Arc::new(|outcome: &Outcome| {
            Ok(serde_json::json!({"events":outcome.admitted_ids, "commit":outcome.commit_seq}))
        });
        let done = handle
            .submit_request(
                Priority::Live,
                batch(if attempt == 0 {
                    "first"
                } else {
                    "must not commit"
                }),
                RequestStep {
                    operation: accepted.operation,
                    index: 0,
                    response,
                },
                Arc::new(|| Ok(())),
            )
            .await
            .unwrap();
        if let Some(receipt) = &receipt {
            assert_eq!(&done.reply, receipt);
        }
        receipt = Some(done.reply);
        assert_eq!(done.outcome.admitted, 1);
        drop(handle);
        writer.await.unwrap();
    }
    let mut conn = sqlx::PgConnection::connect(&url).await.unwrap();
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM core.events")
        .fetch_one(&mut conn)
        .await
        .unwrap();
    assert_eq!(count, 1);
}

#[devkit::test]
async fn failed_request_response_rolls_back_effect_and_receipt(db: devkit::pg::TestDatabase) {
    let url = db.url(None);
    let connection = sqlx::PgConnection::connect(&url).await.unwrap();
    let (handle, queues) = channels();
    let writer = tokio::spawn(run(
        url.clone(),
        claim(connection).await.unwrap(),
        queues,
        Arc::new(sinex_store::NoDerivations),
        None,
    ));
    let accepted = handle
        .begin_request(
            "agent".into(),
            "rollback".into(),
            "agent".into(),
            serde_json::json!({}),
        )
        .await
        .unwrap();
    let result = handle
        .submit_request(
            Priority::Live,
            batch("rollback"),
            RequestStep {
                operation: accepted.operation,
                index: 0,
                response: Arc::new(|_| Err(Error::internal("response could not be constructed"))),
            },
            Arc::new(|| Ok(())),
        )
        .await;
    assert!(matches!(result, Err(WriteError::Refused(_))));
    drop(handle);
    writer.await.unwrap();
    let mut conn = sqlx::PgConnection::connect(&url).await.unwrap();
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM core.events")
        .fetch_one(&mut conn)
        .await
        .unwrap();
    assert_eq!(count, 0);
    assert!(
        sinex_store::requests::step(&mut conn, accepted.operation, 0)
            .await
            .unwrap()
            .is_none()
    );
}
