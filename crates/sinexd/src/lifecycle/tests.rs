//! Operations through the writer: batches interleave with live admission, a
//! comparison writes nothing and equals the cascade, a refusal after the
//! first batch stops early and `resume` finishes the operation.
use super::*;
use crate::writer::{Priority, channels, claim, run as writer_run};
use sinex_model::{
    Batch, BatchKind, Class, Event, MaterialId, Origin, ReadingOrder, RecordLocation,
    RecordOutcome, RunId, Timestamp, event_types::CommandExecuted,
};
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
                anchor: 0,
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
async fn retired_source_resumes_identity_and_live_admission_while_read_waits(
    db: devkit::pg::TestDatabase,
) {
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
            Progress::default(),
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
    let gate = Arc::new(Notify::new());
    let release = gate.clone();
    let (entered, waiting) = oneshot::channel();
    let worker = writer.clone();
    let operation = op.clone();
    let reader = tokio::spawn(async move {
        let mut conn = PgConnection::connect(&url).await.unwrap();
        let native = lifecycle::source_roots(&mut conn, &operation, saved.after).unwrap();
        let mut entered = Some(entered);
        let selected = native
            .then(move |id| {
                let first = entered.take();
                let gate = gate.clone();
                async move {
                    if let Some(first) = first {
                        first.send(()).unwrap();
                        gate.notified().await;
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
    // Retire a deliberately unfinished native operation through the actual
    // startup helper. It must use the saved identity and original cut.
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
            Progress::default(),
            lifecycle::Roots::Source {
                operation: op.id,
                ids: vec![new_id],
                complete: false,
            },
        )
        .await
        .unwrap();
    assert!(matches!(part.step, Step::Moved { done: false, .. }));
    let Answer::Done(done) = retire_source(&mut read, &writer, "ops".into())
        .await
        .unwrap()
    else {
        panic!("retirement refused")
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
