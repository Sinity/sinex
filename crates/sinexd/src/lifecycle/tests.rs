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
