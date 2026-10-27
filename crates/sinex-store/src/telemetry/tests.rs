use super::*;
use sqlx::Executor;
use std::time::Duration;

#[devkit::test]
async fn a_sample_keeps_one_snapshot_while_admission_commits(db: devkit::pg::TestDatabase) {
    let url = db.url(None);
    let mut writer = PgConnection::connect(&url).await.unwrap();
    writer
        .execute("INSERT INTO core.source_counts (source, events, last_seen) VALUES ('test.source', 1, now())")
        .await
        .unwrap();
    writer
        .execute("UPDATE core.counters SET value = 1 WHERE name = 'commit_seq'")
        .await
        .unwrap();
    let mut reader = PgConnection::connect(&url).await.unwrap();
    let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut reader)
        .await
        .unwrap();
    let mut observer = PgConnection::connect(&url).await.unwrap();
    let mut transaction = writer.begin().await.unwrap();
    transaction
        .execute("LOCK TABLE core.source_counts IN ACCESS EXCLUSIVE MODE")
        .await
        .unwrap();
    let sample = tokio::spawn(async move { read(&mut reader).await.unwrap() });

    // Admission changes the counts after the sample read its commit position.
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let waiting: bool = sqlx::query_scalar(
                "SELECT EXISTS (SELECT 1 FROM pg_stat_activity
                 WHERE pid = $1 AND wait_event_type = 'Lock'
                   AND query LIKE '%FROM core.source_counts%')",
            )
            .bind(pid)
            .fetch_one(&mut observer)
            .await
            .unwrap();
            if waiting {
                break;
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .expect("the sample did not reach its source counts");
    transaction
        .execute("UPDATE core.counters SET value = 2 WHERE name = 'commit_seq'")
        .await
        .unwrap();
    transaction
        .execute("UPDATE core.source_counts SET events = 2 WHERE source = 'test.source'")
        .await
        .unwrap();
    transaction.commit().await.unwrap();

    let sample = sample.await.unwrap();
    let measures = |sample: &[Reading], component, name| {
        sample
            .iter()
            .find(|reading| reading.component == component && reading.name == name)
            .unwrap()
            .measures
            .clone()
    };
    assert_eq!(measures(&sample, "writer", "")["commit_seq"], 1.0);
    assert_eq!(measures(&sample, "source", "test.source")["events"], 1.0);
    let next = read(&mut writer).await.unwrap();
    assert_eq!(measures(&next, "writer", "")["commit_seq"], 2.0);
    assert_eq!(measures(&next, "source", "test.source")["events"], 2.0);
}
