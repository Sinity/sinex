//! Operations through the writer: batches interleave with live admission, a
//! comparison writes nothing and equals the cascade, a refusal after the
//! first batch stops early and `resume` finishes the operation.
use super::*;
use crate::writer::{Priority, channels, claim, run as writer_run};
use sinex_model::{
    Batch, BatchKind, Class, Event, MaterialId, Origin, ReadingOrder, RecordLocation,
    RecordOutcome, RunId, Timestamp, event_types::CommandExecuted,
};
use sqlx::Connection;
use std::sync::Arc;

fn reading(key: u32, content: u32) -> Event {
    let command = format!("cmd-{content}");
    Event::build(
        &CommandExecuted {
            native_id: format!("k{key}"),
            command: command.clone(),
            cwd: "/".into(),
            session: "s".into(),
            hostname: "h".into(),
            started_at: Timestamp::from_unix_secs(1_700_000_000 + i64::from(key)).unwrap(),
            duration_ns: Some(1),
            exit_code: Some(0),
            deleted_at: None,
            author: None,
            intent: None,
            shell: None,
            extra: Default::default(),
        },
        "ops",
        Origin::Record {
            location: RecordLocation {
                material: MaterialId::new(),
                record_offset: 0,
            },
            output_index: 0,
        },
        ReadingOrder(1),
        *blake3::hash(command.as_bytes()).as_bytes(),
        Class::Open,
    )
    .unwrap()
}

fn batch(events: Vec<Event>) -> Batch {
    Batch {
        run: RunId::new(),
        kind: BatchKind::Api { actor: "t".into() },
        records: vec![RecordOutcome::Events {
            location: None,
            events,
        }],
        archive: vec![],
    }
}

async fn live(read: &mut PgConnection) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM core.live_equivalence WHERE source = 'ops'")
        .fetch_one(read)
        .await
        .unwrap()
}

#[devkit::test]
async fn a_search_page_does_not_hold_the_writer_behind_a_tombstone(db: devkit::pg::TestDatabase) {
    let url = db.url(None);
    let connection = PgConnection::connect(&url).await.unwrap();
    let mut read = PgConnection::connect(&url).await.unwrap();
    let mut search = PgConnection::connect(&url).await.unwrap();
    let mut vectors = PgConnection::connect(&url).await.unwrap();
    let (writer, queues) = channels();
    let task = tokio::spawn(writer_run(
        url.clone(),
        claim(connection).await.unwrap(),
        queues,
        Arc::new(sinex_store::NoDerivations),
        None,
    ));
    let mut events: Vec<_> = (1..=3).map(|key| reading(key, key)).collect();
    events.sort_by_key(|event| event.id.0);
    let ids: Vec<_> = events.iter().map(|event| event.id.0).collect();
    writer.submit(Priority::Live, batch(events)).await.unwrap();
    let scope = Scope::Source {
        source: "ops".into(),
        after: None,
        before: None,
    };
    assert!(matches!(
        run(
            &mut read,
            &writer,
            Act::Archive,
            "operator",
            scope.clone(),
            false
        )
        .await
        .unwrap(),
        Answer::Done(_)
    ));

    let operation = lifecycle::begin(&mut read, Act::Tombstone, "operator", scope)
        .await
        .unwrap();
    let page = sinex_store::commit_log::after(&mut search, 0, 64, i64::MAX)
        .await
        .unwrap();
    let roots = |id| lifecycle::Roots::Source {
        operation: operation.id,
        ids: vec![id],
        complete: false,
    };
    let first = writer
        .lifecycle_step(&operation, roots(ids[0]))
        .await
        .unwrap();
    let Step::Moved {
        progress: saved,
        done: false,
        ..
    } = first.step
    else {
        panic!("the first batch did not save a prefix");
    };
    assert_eq!(saved.after, Some(ids[0]));
    assert_eq!(saved.batches, 1);

    let mut updating = search.begin().await.unwrap();
    sinex_store::read_models::apply_search_text(&mut updating, &page.events)
        .await
        .unwrap();
    let mut vector_update = vectors.begin().await.unwrap();
    sqlx::query("LOCK TABLE core.embedding_vectors IN SHARE ROW EXCLUSIVE MODE")
        .execute(&mut *vector_update)
        .await
        .unwrap();

    // Once a writer job is accepted, disconnecting the caller leaves the
    // request running. Hold the index updates across that disconnect.
    let (entered, deferred) = tokio::sync::oneshot::channel();
    let (finished, receipt) = tokio::sync::oneshot::channel();
    let worker = writer.clone();
    let request = operation.clone();
    let next = roots(ids[1]);
    let mut client = Box::pin(crate::request_deadline::run(None, async move {
        let mut conn = PgConnection::connect(&url).await?;
        let attempt = worker.lifecycle_step(&request, next).await?;
        assert!(matches!(attempt.step, Step::ReadModelsBusy));
        entered.send(()).unwrap();
        let answer = retry(&mut conn, &worker, &request).await?;
        finished.send(answer).unwrap();
        Ok(())
    }));
    tokio::select! {
        ready = deferred => ready.unwrap(),
        result = &mut client => panic!("the contended request ended: {result:?}"),
        _ = tokio::time::sleep(std::time::Duration::from_secs(5)) => {
            panic!("the writer waited on the search transaction");
        }
    }
    drop(client);
    writer
        .submit(Priority::Live, batch(vec![reading(4, 4)]))
        .await
        .unwrap();
    assert_eq!(live(&mut read).await, 1);
    updating.commit().await.unwrap();
    let attempt = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        writer.lifecycle_step(&operation, roots(ids[1])),
    )
    .await
    .expect("the writer waited on the embedding transaction")
    .unwrap();
    assert!(matches!(attempt.step, Step::ReadModelsBusy));
    let recorded = lifecycle::recorded(&mut read, operation.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(recorded.progress, saved);
    assert_eq!(recorded.counts.tombstoned.get("command.executed"), Some(&1));
    assert!(recorded.result.is_none());
    let search_rows: i64 =
        sqlx::query_scalar("SELECT count(*) FROM core.search_text WHERE event_id = ANY($1)")
            .bind(&ids)
            .fetch_one(&mut read)
            .await
            .unwrap();
    assert_eq!(search_rows, 2);
    vector_update.rollback().await.unwrap();

    let report = tokio::time::timeout(std::time::Duration::from_secs(5), receipt)
        .await
        .expect("the disconnected request did not finish")
        .unwrap();
    assert!(
        matches!(report, Answer::Done(ref r) if r["cascade"]["tombstoned"]["command.executed"] == 3 && r["batches"] == 2),
        "{report:?}"
    );
    let repeated = retry(&mut read, &writer, &operation).await.unwrap();
    assert!(matches!((report, repeated), (Answer::Done(a), Answer::Done(b)) if a == b));
    let mut delayed = search.begin().await.unwrap();
    sinex_store::read_models::apply_search_text(&mut delayed, &page.events)
        .await
        .unwrap();
    delayed.commit().await.unwrap();
    let state: (i64, i64, i64) = sqlx::query_as(
        "SELECT (SELECT count(*) FROM core.event_tombstones WHERE event_id = ANY($1)),\n                (SELECT count(*) FROM core.events WHERE id = ANY($1)),\n                (SELECT count(*) FROM core.search_text WHERE event_id = ANY($1))",
    ).bind(&ids).fetch_one(&mut read).await.unwrap();
    assert_eq!(state, (3, 0, 0));
    drop(writer);
    task.await.unwrap();
}

#[devkit::test]
async fn queued_lifecycle_retry_uses_committed_progress_and_completed_counts(
    db: devkit::pg::TestDatabase,
) {
    let url = db.url(None);
    let connection = PgConnection::connect(&url).await.unwrap();
    let mut read = PgConnection::connect(&url).await.unwrap();
    let (writer, queues) = channels();
    let task = tokio::spawn(writer_run(
        url,
        claim(connection).await.unwrap(),
        queues,
        Arc::new(sinex_store::NoDerivations),
        None,
    ));
    let mut readings: Vec<_> = (0..3).map(|key| (key, reading(key, key))).collect();
    readings.sort_by_key(|(_, event)| event.id);
    let conflict_key = readings.last().unwrap().0;
    writer
        .submit(
            Priority::Live,
            batch(readings.into_iter().map(|(_, event)| event).collect()),
        )
        .await
        .unwrap();
    let operation = lifecycle::begin(
        &mut read,
        Act::Archive,
        "agent:fixture",
        Scope::Source {
            source: "ops".into(),
            after: None,
            before: None,
        },
    )
    .await
    .unwrap();
    let first = writer
        .lifecycle_step(&operation, lifecycle::Roots::Query(1))
        .await
        .unwrap();
    let Step::Moved {
        progress: first, ..
    } = first.step
    else {
        panic!("first move refused")
    };
    assert_eq!(live(&mut read).await, 2);
    let stale = writer
        .lifecycle_step(
            &operation,
            lifecycle::Roots::Source {
                operation: operation.id,
                ids: vec![first.after.expect("first root committed")],
                complete: false,
            },
        )
        .await
        .unwrap();
    let Step::Moved {
        progress: unchanged,
        counts,
        ..
    } = stale.step
    else {
        panic!("stale source prefix refused")
    };
    assert_eq!(unchanged, first);
    assert_eq!(counts, Counts::default());
    assert_eq!(live(&mut read).await, 2);
    // The caller prepared this job before seeing the first job's commit.
    let repeated = writer
        .lifecycle_step(&operation, lifecycle::Roots::Query(1))
        .await
        .unwrap();
    let Step::Moved {
        progress: second, ..
    } = repeated.step
    else {
        panic!("retry refused")
    };
    assert_eq!(second.batches, first.batches + 1);
    assert_ne!(second.after, first.after);
    assert_eq!(live(&mut read).await, 1);
    let Answer::Done(done) = retry(&mut read, &writer, &operation).await.unwrap() else {
        panic!("retry did not finish")
    };
    assert_eq!(done["cascade"]["archived"]["command.executed"], 3);
    assert_eq!(live(&mut read).await, 0);
    let repeated = writer
        .lifecycle_step(&operation, lifecycle::Roots::Query(1))
        .await
        .unwrap();
    assert!(matches!(repeated.step, Step::Moved { done: true, .. }));
    let Answer::Done(again) = retry(&mut read, &writer, &operation).await.unwrap() else {
        panic!("completed operation did not retain its result")
    };
    assert_eq!(again, done);
    let restore = lifecycle::begin(
        &mut read,
        Act::Restore,
        "agent:fixture",
        Scope::Operation {
            operation: operation.id,
        },
    )
    .await
    .unwrap();
    let first = writer
        .lifecycle_step(&restore, lifecycle::Roots::Query(1))
        .await
        .unwrap();
    assert!(matches!(first.step, Step::Moved { done: false, .. }));
    writer
        .submit(Priority::Live, batch(vec![reading(conflict_key, 99)]))
        .await
        .unwrap();
    // A restarted reader can still hold the zero from before that first batch.
    let Answer::StoppedEarly(_, stopped) = drive(&writer, &restore, Progress::default(), None)
        .await
        .unwrap()
    else {
        panic!("a refusal after a committed batch must report the moved prefix")
    };
    assert_eq!(stopped["batches"], 1);
    let Answer::StoppedEarly(_, recorded) = retry(&mut read, &writer, &restore).await.unwrap()
    else {
        panic!("the stopped prefix must remain available to retry")
    };
    assert_eq!(recorded["cascade"]["restored"]["command.executed"], 1);
    assert_eq!(live(&mut read).await, 2);
    drop(writer);
    task.await.unwrap();
}

#[devkit::test]
async fn operations_run_in_writer_batches_and_resume(db: devkit::pg::TestDatabase) {
    let url = db.url(None);
    let connection = <PgConnection as sqlx::Connection>::connect(&url)
        .await
        .unwrap();
    let mut read = <PgConnection as sqlx::Connection>::connect(&url)
        .await
        .unwrap();
    let (writer, queues) = channels();
    let task = tokio::spawn(writer_run(
        url,
        claim(connection).await.unwrap(),
        queues,
        Arc::new(sinex_store::NoDerivations),
        None,
    ));
    let readings: Vec<Event> = (0..600).map(|k| reading(k, k)).collect();
    writer
        .submit(Priority::Live, batch(readings.clone()))
        .await
        .unwrap();
    let source = Scope::Source {
        source: "ops".into(),
        after: None,
        before: None,
    };

    let Answer::Done(compared) = run(
        &mut read,
        &writer,
        Act::Archive,
        "agent:t",
        source.clone(),
        true,
    )
    .await
    .unwrap() else {
        panic!("comparison refused");
    };
    assert_eq!(live(&mut read).await, 600, "a comparison moves nothing");
    let Answer::Done(archived) = run(&mut read, &writer, Act::Archive, "agent:t", source, false)
        .await
        .unwrap()
    else {
        panic!("archive refused");
    };
    assert_eq!(archived["cascade"], compared["cascade"]);
    assert_eq!(archived["cascade"]["archived"]["command.executed"], 600);
    assert!(
        archived["batches"].as_u64().unwrap() > 1,
        "more roots than one batch takes: {archived}"
    );
    assert_eq!(live(&mut read).await, 0);

    // Restore the operation while one record has another live reading: the
    // first batch moves, a later one refuses, and the operation stops early.
    let operation: uuid::Uuid = serde_json::from_value(archived["operation"].clone()).unwrap();
    let blocker = reading(599, 9999);
    writer
        .submit(Priority::Live, batch(vec![blocker.clone()]))
        .await
        .unwrap();
    let scope = Scope::Operation {
        operation: OperationId(operation),
    };
    let Answer::StoppedEarly(reason, moved) =
        run(&mut read, &writer, Act::Restore, "agent:t", scope, false)
            .await
            .unwrap()
    else {
        panic!("expected a stop");
    };
    assert!(reason.contains("resume"), "{reason}");
    let restore: uuid::Uuid = serde_json::from_value(moved["operation"].clone()).unwrap();
    let partly = live(&mut read).await;
    assert!(
        partly > 1 && partly < 600,
        "the first batch stays restored, the refused one does not: {partly}"
    );

    // Clearing the cause and resuming finishes it.
    let Answer::Done(_) = run(
        &mut read,
        &writer,
        Act::Archive,
        "agent:t",
        Scope::Events {
            events: vec![blocker.id],
            query: None,
        },
        false,
    )
    .await
    .unwrap() else {
        panic!("archive refused");
    };
    let Answer::Done(resumed) = resume(&mut read, &writer, OperationId(restore))
        .await
        .unwrap()
    else {
        panic!("resume refused");
    };
    assert_eq!(resumed["result"], "ok");
    assert_eq!(live(&mut read).await, 600);
    drop(writer);
    task.await.unwrap();
}

#[devkit::test]
async fn removed_source_archive_resumes_while_read_waits(db: devkit::pg::TestDatabase) {
    use futures::StreamExt;
    use sqlx::Connection;
    use tokio::sync::{Notify, oneshot};
    let url = db.url(None);
    let connection = PgConnection::connect(&url).await.unwrap();
    let mut read = PgConnection::connect(&url).await.unwrap();
    let (writer, queues) = channels();
    let task = tokio::spawn(writer_run(
        url.clone(),
        claim(connection).await.unwrap(),
        queues,
        Arc::new(sinex_store::NoDerivations),
        None,
    ));
    let readings: Vec<_> = (0..600).map(|k| reading(k, k)).collect();
    writer
        .submit(Priority::Live, batch(readings.clone()))
        .await
        .unwrap();
    let op = lifecycle::begin(
        &mut read,
        Act::Archive,
        "sinex",
        Scope::Source {
            source: "ops".into(),
            after: None,
            before: None,
        },
    )
    .await
    .unwrap();
    let prefix: Vec<_> = readings.iter().take(64).map(|e| e.id.0).collect();
    let first = writer
        .lifecycle_step(
            &op,
            lifecycle::Roots::Source {
                operation: op.id,
                ids: prefix,
                complete: false,
            },
        )
        .await
        .unwrap();
    let Step::Moved {
        progress: saved, ..
    } = first.step
    else {
        panic!("first batch refused")
    };
    assert_eq!(saved.batches, 1);
    // A stalled producer of selected identities owns no writer transaction.
    let pause = Arc::new(Notify::new());
    let release = pause.clone();
    let (entered, waiting) = oneshot::channel();
    let worker = writer.clone();
    let operation = op.clone();
    let reader = tokio::spawn(async move {
        let mut conn = PgConnection::connect(&url).await.unwrap();
        let roots = lifecycle::source_roots(&mut conn, &operation, saved.after).unwrap();
        let mut entered = Some(entered);
        let selected = roots
            .then(move |id| {
                let first = entered.take();
                let pause = pause.clone();
                async move {
                    if let Some(first) = first {
                        first.send(()).unwrap();
                        pause.notified().await;
                    }
                    id
                }
            })
            .boxed();
        drive(&worker, &operation, saved, Some(selected)).await
    });
    waiting.await.unwrap();
    let new = reading(9999, 9999);
    let new_id = new.id.0;
    let admitted = writer
        .submit(Priority::Live, batch(vec![new]))
        .await
        .unwrap();
    assert!(admitted.commit_seq.unwrap().0 > op.start_seq);
    release.notify_one();
    let Answer::Done(done) = reader.await.unwrap().unwrap() else {
        panic!("source drive refused")
    };
    assert_eq!(done["operation"], serde_json::to_value(op.id).unwrap());
    assert_eq!(live(&mut read).await, 1);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM core.live_equivalence WHERE event_id=$1"
        )
        .bind(new_id)
        .fetch_one(&mut read)
        .await
        .unwrap(),
        1
    );
    // Archive a deliberately unfinished operation through the
    // startup helper. It must use the saved identity and original start position.
    let tail = reading(10000, 10000);
    let tail_id = tail.id.0;
    writer
        .submit(Priority::Live, batch(vec![tail]))
        .await
        .unwrap();
    let op = lifecycle::begin(
        &mut read,
        Act::Archive,
        "sinex",
        Scope::Source {
            source: "ops".into(),
            after: None,
            before: None,
        },
    )
    .await
    .unwrap();
    let part = writer
        .lifecycle_step(
            &op,
            lifecycle::Roots::Source {
                operation: op.id,
                ids: vec![new_id],
                complete: false,
            },
        )
        .await
        .unwrap();
    assert!(matches!(part.step, Step::Moved { done: false, .. }));
    let Answer::Done(done) = archive_removed_source(&mut read, &writer, "ops".into())
        .await
        .unwrap()
    else {
        panic!("removed source archive refused")
    };
    assert_eq!(done["operation"], serde_json::to_value(op.id).unwrap());
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM core.operations WHERE actor='sinex' AND kind='archive'"
        )
        .fetch_one(&mut read)
        .await
        .unwrap(),
        2
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM audit.archived WHERE event_id=$1")
            .bind(tail_id)
            .fetch_one(&mut read)
            .await
            .unwrap(),
        1
    );
    drop(writer);
    task.await.unwrap();
}
