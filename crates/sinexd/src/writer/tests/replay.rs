use super::*;

fn sequence(batches: Vec<Batch>) -> ReplaySequence {
    ReplaySequence {
        operation: OperationId::new(),
        actor: "test".into(),
        scope: serde_json::json!({}),
        root: uuid::Uuid::now_v7(),
        compare: false,
        batches: crate::replay::prepared::Batches::ready(batches),
        ingress: None,
        check: Arc::new(|| Ok(())),
    }
}

#[devkit::test]
async fn disconnected_queued_replay_never_admits_its_prepared_events(db: devkit::pg::TestDatabase) {
    let url = db.url(None);
    let connection = <PgConnection as Connection>::connect(&url).await.unwrap();
    let (handle, mut queues) = channels();
    let replay_handle = handle.clone();
    let replaying = tokio::spawn(async move {
        replay_handle
            .replay(sequence(vec![batch("disconnected-replay")]))
            .await
    });
    // Receive the exact queued job before disconnecting its caller. Put that
    // same job back, then run the real writer after cancellation is certain.
    let job = queues.derived.recv().await.unwrap();
    replaying.abort();
    assert!(replaying.await.unwrap_err().is_cancelled());
    handle.derived.send(job).await.unwrap();
    let writer = tokio::spawn(run(
        url.clone(),
        claim(connection).await.unwrap(),
        queues,
        Arc::new(sinex_store::NoDerivations),
        None,
    ));
    drop(handle);
    writer.await.unwrap();
    let mut connection = <PgConnection as Connection>::connect(&url).await.unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM core.events")
            .fetch_one(&mut connection)
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM core.replayed_sequences")
            .fetch_one(&mut connection)
            .await
            .unwrap(),
        0
    );
}

#[devkit::test]
async fn a_later_prepared_batch_refusal_rolls_back_the_entire_replay(db: devkit::pg::TestDatabase) {
    let url = db.url(None);
    let connection = <PgConnection as Connection>::connect(&url).await.unwrap();
    let (handle, queues) = channels();
    let writer = tokio::spawn(run(
        url.clone(),
        claim(connection).await.unwrap(),
        queues,
        Arc::new(sinex_store::NoDerivations),
        None,
    ));
    let mut refused = batch("refused");
    if let RecordOutcome::Events { events, .. } = &mut refused.records[0] {
        events[0].payload["command"] = serde_json::json!("nul\u{0}");
    }
    let request = sequence(vec![batch("prepared-before-refusal"), refused]);
    let operation = request.operation;
    assert!(matches!(
        handle.replay(request).await,
        Err(WriteError::Refused(_))
    ));
    let mut connection = <PgConnection as Connection>::connect(&url).await.unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM core.events")
            .fetch_one(&mut connection)
            .await
            .unwrap(),
        0,
        "an earlier prepared batch cannot survive a later refusal"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM core.replayed_sequences WHERE operation_id=$1"
        )
        .bind(operation.0)
        .fetch_one(&mut connection)
        .await
        .unwrap(),
        0
    );
    assert_eq!(
        handle
            .submit(Priority::Live, batch("live-after-refusal"))
            .await
            .unwrap()
            .admitted,
        1
    );
    drop(handle);
    writer.await.unwrap();
}
