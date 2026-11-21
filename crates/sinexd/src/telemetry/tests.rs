use super::*;
use sinex_protocol::Subjects;
use sqlx::Connection;

#[devkit::test]
async fn spooled_publisher_position_belongs_to_the_current_confirmed_stream(
    db: devkit::pg::TestDatabase,
) {
    let mut conn = PgConnection::connect(&db.url(None)).await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let mut writer = SpoolWriter::open(dir.path(), Rotation::default()).unwrap();
    let nats = devkit::nats::test_nats().unwrap();
    let subjects = Subjects::new(nats.prefix());
    let js = publisher::context(async_nats::connect(nats.url()).await.unwrap());
    let confirmed = subjects.confirmed_stream();
    let broker = (&js, confirmed.as_str());
    publisher::ensure_stream(&mut conn, &js, &subjects)
        .await
        .unwrap();
    let stream = js.get_stream(&confirmed).await.unwrap();
    let previous = publisher::consumer_for(&publisher::stream_instance(&stream));
    sqlx::query("UPDATE core.counters SET value = 3 WHERE name = 'commit_seq'")
        .execute(&mut conn)
        .await
        .unwrap();
    sinex_store::commit_log::set_consumer_position(&mut conn, &previous, 3, 0)
        .await
        .unwrap();
    sinex_store::commit_log::set_consumer_position(&mut conn, "publisher", 1, 0)
        .await
        .unwrap();
    js.delete_stream(&confirmed).await.unwrap();

    let written = sample(&mut conn, &mut writer, broker).await.unwrap();
    assert!(
        js.get_stream(&confirmed).await.is_err(),
        "sampling created the missing stream"
    );
    publisher::ensure_stream(&mut conn, &js, &subjects)
        .await
        .unwrap();
    let stream = js.get_stream(&confirmed).await.unwrap();
    let active = publisher::consumer_for(&publisher::stream_instance(&stream));
    assert_ne!(previous, active);
    sqlx::query("UPDATE core.counters SET value = 10 WHERE name = 'commit_seq'")
        .execute(&mut conn)
        .await
        .unwrap();
    // A different confirmed stream can already be further ahead.
    sinex_store::commit_log::set_consumer_position(&mut conn, "publisher/OTHER_CONFIRMED/1", 9, 0)
        .await
        .unwrap();
    assert_eq!(
        sample(&mut conn, &mut writer, broker).await.unwrap(),
        written
    );
    sinex_store::commit_log::set_consumer_position(&mut conn, &active, 7, 0)
        .await
        .unwrap();
    assert_eq!(
        sample(&mut conn, &mut writer, broker).await.unwrap(),
        written
    );
    drop(writer);
    let mut lines = Vec::new();
    for entry in std::fs::read_dir(dir.path()).unwrap() {
        let text = std::fs::read_to_string(entry.unwrap().path()).unwrap();
        lines.extend(text.lines().map(str::to_owned));
    }
    assert_eq!(lines.len(), 3 * written);
    let samples: Vec<Sample> = lines
        .iter()
        .map(|l| {
            let record: serde_json::Map<String, serde_json::Value> =
                serde_json::from_str(l).unwrap();
            let (_, bytes) = sinex_source_core::drivers::live_stream::received(&record).unwrap();
            serde_json::from_slice(&bytes).unwrap()
        })
        .collect();
    for round in samples.chunks(written) {
        let parts: Vec<_> = round.iter().map(|s| s.component.as_str()).collect();
        assert_eq!(parts, ["writer", "publisher", "search index"]);
    }
    let publishers: Vec<_> = samples
        .iter()
        .filter(|s| s.component == "publisher")
        .collect();
    assert!(publishers[0].name.is_empty() && publishers[0].measures.is_empty());
    assert_eq!(publishers[1].name, active);
    assert!(
        publishers[1].measures.is_empty(),
        "new instance has no checkpoint yet"
    );
    assert_eq!(publishers[2].name, active);
    assert_eq!(publishers[2].measures["position"], 7.0);
    assert_eq!(publishers[2].measures["behind"], 3.0);
}

#[devkit::test]
async fn sampling_reconnects_after_its_database_connection_dies(db: devkit::pg::TestDatabase) {
    let nats = devkit::nats::test_nats().unwrap();
    let subjects = Subjects::new(nats.prefix());
    let js = publisher::context(async_nats::connect(nats.url()).await.unwrap());
    let confirmed = subjects.confirmed_stream();
    let broker = (&js, confirmed.as_str());
    let database = db.url(None);
    let mut conn = PgConnection::connect(&database).await.unwrap();
    publisher::ensure_stream(&mut conn, &js, &subjects)
        .await
        .unwrap();
    let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut conn)
        .await
        .unwrap();
    let mut observer = PgConnection::connect(&database).await.unwrap();
    let terminated: bool = sqlx::query_scalar("SELECT pg_terminate_backend($1)")
        .bind(pid)
        .fetch_one(&mut observer)
        .await
        .unwrap();
    assert!(terminated);
    assert!(conn.ping().await.is_err());
    let dir = tempfile::tempdir().unwrap();
    let mut writer = SpoolWriter::open(dir.path(), Rotation::default()).unwrap();
    assert_eq!(
        sample_reconnecting(&mut conn, &database, &mut writer, broker)
            .await
            .unwrap(),
        3
    );
    let replacement: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut conn)
        .await
        .unwrap();
    assert_ne!(replacement, pid);
    assert_eq!(
        sample_reconnecting(&mut conn, &database, &mut writer, broker)
            .await
            .unwrap(),
        3
    );
    drop(writer);
    let count: usize = std::fs::read_dir(dir.path())
        .unwrap()
        .map(|entry| {
            std::fs::read_to_string(entry.unwrap().path())
                .unwrap()
                .lines()
                .count()
        })
        .sum();
    assert_eq!(
        count, 6,
        "the failed read wrote no partial snapshot or duplicate lines"
    );
}
